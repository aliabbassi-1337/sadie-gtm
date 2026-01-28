"""RMS Booking Engine Service.

Service layer for RMS hotel discovery and enrichment.
Uses dependency injection for repo, scraper, and queue - enabling unit tests.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Callable
import asyncio

from loguru import logger

from services.enrichment.rms_repo import IRMSRepo, RMSRepo, RMSHotelRecord
from services.enrichment.rms_scraper import (
    IRMSScraper,
    ScraperPool,
    ExtractedRMSData,
)
from services.enrichment.rms_queue import IRMSQueue, RMSQueue, QueueStats


# Configuration
BATCH_SAVE_SIZE = 50
MAX_CONSECUTIVE_FAILURES = 30
MAX_QUEUE_DEPTH = 1000


@dataclass
class IngestResult:
    """Result of RMS ingestion."""
    total_scanned: int
    hotels_found: int
    hotels_saved: int


@dataclass
class EnrichResult:
    """Result of RMS enrichment."""
    processed: int
    enriched: int
    failed: int


@dataclass
class EnqueueResult:
    """Result of enqueueing hotels."""
    total_found: int
    enqueued: int
    skipped: bool
    reason: Optional[str] = None


@dataclass
class ConsumeResult:
    """Result of consuming from queue."""
    messages_processed: int
    hotels_processed: int
    hotels_enriched: int
    hotels_failed: int


class IRMSService(ABC):
    """Interface for RMS Service.
    
    Provides methods for discovering and enriching RMS hotels.
    All methods are designed to be easily unit tested via mocks.
    """
    
    @abstractmethod
    async def ingest_from_id_range(
        self,
        start_id: int,
        end_id: int,
        concurrency: int = 6,
        dry_run: bool = False,
    ) -> IngestResult:
        """Scan RMS IDs and ingest discovered hotels to database.
        
        Args:
            start_id: Starting ID to scan
            end_id: Ending ID to scan (exclusive)
            concurrency: Number of concurrent scrapers
            dry_run: If True, don't save to database
            
        Returns:
            IngestResult with scan statistics
        """
        pass
    
    @abstractmethod
    async def enrich_hotels(
        self,
        hotels: List[RMSHotelRecord],
        concurrency: int = 6,
    ) -> EnrichResult:
        """Enrich a list of RMS hotels by scraping their booking pages.
        
        Args:
            hotels: List of hotel records with booking URLs
            concurrency: Number of concurrent scrapers
            
        Returns:
            EnrichResult with enrichment statistics
        """
        pass
    
    @abstractmethod
    async def enqueue_for_enrichment(
        self,
        limit: int = 5000,
        batch_size: int = 10,
    ) -> EnqueueResult:
        """Find hotels needing enrichment and enqueue them.
        
        Args:
            limit: Maximum hotels to enqueue
            batch_size: Hotels per queue message
            
        Returns:
            EnqueueResult with enqueue statistics
        """
        pass
    
    @abstractmethod
    async def consume_enrichment_queue(
        self,
        concurrency: int = 6,
        max_messages: int = 0,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> ConsumeResult:
        """Consume and process messages from enrichment queue.
        
        Args:
            concurrency: Number of concurrent scrapers
            max_messages: Max messages to process (0 = infinite)
            should_stop: Callback to check if should stop
            
        Returns:
            ConsumeResult with consumption statistics
        """
        pass
    
    @abstractmethod
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        """Get RMS hotels that need enrichment."""
        pass
    
    @abstractmethod
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        pass
    
    @abstractmethod
    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment."""
        pass
    
    @abstractmethod
    def get_queue_stats(self) -> QueueStats:
        """Get queue statistics."""
        pass


class RMSService(IRMSService):
    """Implementation of RMS Service with dependency injection."""
    
    def __init__(
        self,
        repo: Optional[IRMSRepo] = None,
        queue: Optional[IRMSQueue] = None,
    ):
        """Initialize with optional dependencies.
        
        Args:
            repo: Repository for database operations (default: RMSRepo)
            queue: Queue for message operations (default: RMSQueue)
        """
        self._repo = repo or RMSRepo()
        self._queue = queue or RMSQueue()
        self._shutdown_requested = False
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        self._shutdown_requested = True
        logger.info("Shutdown requested")
    
    # =========================================================================
    # Ingestion (scan IDs to discover hotels)
    # =========================================================================
    
    async def ingest_from_id_range(
        self,
        start_id: int,
        end_id: int,
        concurrency: int = 6,
        dry_run: bool = False,
    ) -> IngestResult:
        """Scan RMS IDs and ingest discovered hotels to database."""
        
        booking_engine_id = await self._repo.get_booking_engine_id()
        logger.info(f"RMS Cloud booking engine ID: {booking_engine_id}")
        
        found_hotels: List[ExtractedRMSData] = []
        total_saved = 0
        total_scanned = 0
        consecutive_failures = 0
        
        async with ScraperPool(concurrency) as pool:
            semaphore = asyncio.Semaphore(concurrency)
            
            async def scan_id(id_num: int, scraper_idx: int) -> Optional[ExtractedRMSData]:
                nonlocal consecutive_failures
                
                async with semaphore:
                    scraper = pool.get_scraper(scraper_idx)
                    data = await scraper.scan_id(id_num)
                    
                    if data:
                        consecutive_failures = 0
                        return data
                    
                    consecutive_failures += 1
                    return None
            
            # Process in batches
            batch_size = concurrency * 2
            for batch_start in range(start_id, end_id, batch_size):
                if self._shutdown_requested:
                    break
                
                batch_end = min(batch_start + batch_size, end_id)
                
                # Run batch
                tasks = [
                    scan_id(id_num, i) 
                    for i, id_num in enumerate(range(batch_start, batch_end))
                ]
                results = await asyncio.gather(*tasks)
                total_scanned += len(tasks)
                
                # Collect found hotels
                for hotel in results:
                    if hotel:
                        found_hotels.append(hotel)
                        logger.success(f"Found: {hotel.name} ({hotel.booking_url})")
                
                # Save batch to DB
                if len(found_hotels) >= BATCH_SAVE_SIZE and not dry_run:
                    saved = await self._save_hotels_batch(found_hotels, booking_engine_id)
                    total_saved += saved
                    logger.info(f"Saved {saved} hotels (total: {total_saved})")
                    found_hotels = []
                
                # Progress
                progress = (batch_end - start_id) / (end_id - start_id) * 100
                logger.info(f"Progress: {progress:.1f}% - Found: {total_saved + len(found_hotels)}")
                
                # Check for sparse region
                if consecutive_failures > MAX_CONSECUTIVE_FAILURES:
                    logger.warning(f"Many failures at {batch_end}, region may be sparse")
                    consecutive_failures = 0
                
                await asyncio.sleep(0.5)
            
            # Save remaining hotels
            if found_hotels and not dry_run:
                saved = await self._save_hotels_batch(found_hotels, booking_engine_id)
                total_saved += saved
        
        return IngestResult(
            total_scanned=total_scanned,
            hotels_found=total_saved + len(found_hotels) if dry_run else total_saved,
            hotels_saved=total_saved,
        )
    
    async def _save_hotels_batch(
        self,
        hotels: List[ExtractedRMSData],
        booking_engine_id: int,
    ) -> int:
        """Save a batch of hotels to the database."""
        saved = 0
        
        for hotel in hotels:
            try:
                hotel_id = await self._repo.insert_hotel(
                    name=hotel.name,
                    address=hotel.address,
                    city=hotel.city,
                    state=hotel.state,
                    country=hotel.country,
                    phone=hotel.phone,
                    email=hotel.email,
                    website=hotel.website,
                    source="rms_scan",
                    status=1,
                )
                
                if hotel_id:
                    await self._repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=booking_engine_id,
                        booking_url=hotel.booking_url,
                        enrichment_status="enriched",
                    )
                    saved += 1
                    
            except Exception as e:
                logger.error(f"Error saving hotel {hotel.name}: {e}")
        
        return saved
    
    # =========================================================================
    # Enrichment (scrape existing hotels)
    # =========================================================================
    
    async def enrich_hotels(
        self,
        hotels: List[RMSHotelRecord],
        concurrency: int = 6,
    ) -> EnrichResult:
        """Enrich a list of RMS hotels by scraping their booking pages."""
        
        processed = 0
        enriched = 0
        failed = 0
        
        async with ScraperPool(concurrency) as pool:
            semaphore = asyncio.Semaphore(concurrency)
            
            async def enrich_hotel(hotel: RMSHotelRecord, scraper_idx: int) -> tuple[bool, bool]:
                """Returns (processed, enriched)"""
                async with semaphore:
                    scraper = pool.get_scraper(scraper_idx)
                    
                    url = hotel.booking_url
                    if not url.startswith("http"):
                        url = f"https://{url}"
                    
                    slug = url.split("/")[-1]
                    
                    data = await scraper.extract_from_url(url, slug)
                    
                    if data and data.has_data():
                        await self._repo.update_hotel(
                            hotel_id=hotel.hotel_id,
                            name=data.name,
                            address=data.address,
                            city=data.city,
                            state=data.state,
                            country=data.country,
                            phone=data.phone,
                            email=data.email,
                            website=data.website,
                        )
                        await self._repo.update_enrichment_status(hotel.booking_url, "enriched")
                        logger.info(f"Enriched hotel {hotel.hotel_id}: {data.name}")
                        return (True, True)
                    else:
                        await self._repo.update_enrichment_status(hotel.booking_url, "no_data")
                        return (True, False)
            
            tasks = [
                enrich_hotel(hotel, i)
                for i, hotel in enumerate(hotels)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Enrichment error: {result}")
                    failed += 1
                else:
                    p, e = result
                    processed += 1 if p else 0
                    enriched += 1 if e else 0
                    if p and not e:
                        failed += 1
        
        return EnrichResult(processed=processed, enriched=enriched, failed=failed)
    
    # =========================================================================
    # Queue operations
    # =========================================================================
    
    async def enqueue_for_enrichment(
        self,
        limit: int = 5000,
        batch_size: int = 10,
    ) -> EnqueueResult:
        """Find hotels needing enrichment and enqueue them."""
        
        # Check queue depth
        stats = self._queue.get_stats()
        logger.info(f"Current queue: {stats.pending} pending, {stats.in_flight} in flight")
        
        if stats.pending > MAX_QUEUE_DEPTH:
            logger.warning(f"Queue already has {stats.pending} messages, skipping")
            return EnqueueResult(
                total_found=0,
                enqueued=0,
                skipped=True,
                reason=f"Queue depth exceeds {MAX_QUEUE_DEPTH}",
            )
        
        # Get hotels needing enrichment
        hotels = await self._repo.get_hotels_needing_enrichment(limit)
        logger.info(f"Found {len(hotels)} RMS hotels needing enrichment")
        
        if not hotels:
            return EnqueueResult(
                total_found=0,
                enqueued=0,
                skipped=False,
            )
        
        # Enqueue
        enqueued = self._queue.enqueue_hotels(hotels, batch_size)
        logger.success(f"Enqueued {enqueued} hotels")
        
        return EnqueueResult(
            total_found=len(hotels),
            enqueued=enqueued,
            skipped=False,
        )
    
    async def consume_enrichment_queue(
        self,
        concurrency: int = 6,
        max_messages: int = 0,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> ConsumeResult:
        """Consume and process messages from enrichment queue."""
        
        messages_processed = 0
        hotels_processed = 0
        hotels_enriched = 0
        hotels_failed = 0
        
        should_stop = should_stop or (lambda: self._shutdown_requested)
        
        logger.info(f"Starting consumer (concurrency={concurrency})")
        
        while not should_stop():
            # Check for max messages limit
            if max_messages > 0 and messages_processed >= max_messages:
                break
            
            # Check queue
            stats = self._queue.get_stats()
            if stats.pending == 0 and stats.in_flight == 0:
                if max_messages > 0:
                    break  # Finite mode, queue empty
                logger.info("Queue empty, waiting...")
                await asyncio.sleep(30)
                continue
            
            # Receive messages
            messages = self._queue.receive_messages(min(concurrency, 10))
            
            if not messages:
                continue
            
            logger.info(f"Processing {len(messages)} messages ({stats.pending} pending)")
            
            # Process each message
            for msg in messages:
                if should_stop():
                    break
                
                if not msg.hotels:
                    self._queue.delete_message(msg.receipt_handle)
                    continue
                
                try:
                    result = await self.enrich_hotels(msg.hotels, concurrency)
                    hotels_processed += result.processed
                    hotels_enriched += result.enriched
                    hotels_failed += result.failed
                    self._queue.delete_message(msg.receipt_handle)
                    messages_processed += 1
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    hotels_failed += len(msg.hotels)
            
            logger.info(
                f"Progress: {hotels_processed} processed, "
                f"{hotels_enriched} enriched, {hotels_failed} failed"
            )
        
        return ConsumeResult(
            messages_processed=messages_processed,
            hotels_processed=hotels_processed,
            hotels_enriched=hotels_enriched,
            hotels_failed=hotels_failed,
        )
    
    # =========================================================================
    # Query methods
    # =========================================================================
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        """Get RMS hotels that need enrichment."""
        return await self._repo.get_hotels_needing_enrichment(limit)
    
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        return await self._repo.get_stats()
    
    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment."""
        return await self._repo.count_needing_enrichment()
    
    def get_queue_stats(self) -> QueueStats:
        """Get queue statistics."""
        return self._queue.get_stats()
