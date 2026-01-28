"""RMS Booking Engine Service.

Orchestrates RMS hotel discovery and enrichment.
Uses dependency injection for repo, scanner, scraper, and queue.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Callable
import asyncio

from loguru import logger
from playwright.async_api import async_playwright, BrowserContext, Browser

from services.enrichment.rms_repo import IRMSRepo, RMSRepo, RMSHotelRecord
from services.enrichment.rms_scanner import IRMSScanner, RMSScanner, ScannedURL
from services.enrichment.rms_scraper import IRMSScraper, RMSScraper, ExtractedRMSData
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
    """Interface for RMS Service."""
    
    @abstractmethod
    async def ingest_from_id_range(
        self,
        start_id: int,
        end_id: int,
        concurrency: int = 6,
        dry_run: bool = False,
    ) -> IngestResult:
        """Scan RMS IDs and ingest discovered hotels."""
        pass
    
    @abstractmethod
    async def enrich_hotels(
        self,
        hotels: List[RMSHotelRecord],
        concurrency: int = 6,
    ) -> EnrichResult:
        """Enrich hotels by scraping their booking pages."""
        pass
    
    @abstractmethod
    async def enqueue_for_enrichment(
        self,
        limit: int = 5000,
        batch_size: int = 10,
    ) -> EnqueueResult:
        """Find and enqueue hotels needing enrichment."""
        pass
    
    @abstractmethod
    async def consume_enrichment_queue(
        self,
        concurrency: int = 6,
        max_messages: int = 0,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> ConsumeResult:
        """Consume and process enrichment queue."""
        pass
    
    @abstractmethod
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        """Get hotels needing enrichment."""
        pass
    
    @abstractmethod
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        pass
    
    @abstractmethod
    async def count_needing_enrichment(self) -> int:
        """Count hotels needing enrichment."""
        pass
    
    @abstractmethod
    def get_queue_stats(self) -> QueueStats:
        """Get queue statistics."""
        pass


class RMSService(IRMSService):
    """Implementation of RMS Service."""
    
    def __init__(
        self,
        repo: Optional[IRMSRepo] = None,
        queue: Optional[IRMSQueue] = None,
    ):
        self._repo = repo or RMSRepo()
        self._queue = queue or RMSQueue()
        self._shutdown_requested = False
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        self._shutdown_requested = True
        logger.info("Shutdown requested")
    
    # =========================================================================
    # Ingestion
    # =========================================================================
    
    async def ingest_from_id_range(
        self,
        start_id: int,
        end_id: int,
        concurrency: int = 6,
        dry_run: bool = False,
    ) -> IngestResult:
        """Scan RMS IDs and ingest discovered hotels."""
        
        booking_engine_id = await self._repo.get_booking_engine_id()
        logger.info(f"RMS Cloud booking engine ID: {booking_engine_id}")
        
        found_hotels: List[ExtractedRMSData] = []
        total_saved = 0
        total_scanned = 0
        consecutive_failures = 0
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create scanner/scraper pairs
            contexts: List[BrowserContext] = []
            scanners: List[RMSScanner] = []
            scrapers: List[RMSScraper] = []
            
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                scanners.append(RMSScanner(page))
                scrapers.append(RMSScraper(page))
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def scan_and_extract(id_num: int, idx: int) -> Optional[ExtractedRMSData]:
                nonlocal consecutive_failures
                
                async with semaphore:
                    scanner = scanners[idx % len(scanners)]
                    scraper = scrapers[idx % len(scrapers)]
                    
                    # Scan for valid URL
                    scanned = await scanner.scan_id(id_num)
                    if not scanned:
                        consecutive_failures += 1
                        return None
                    
                    consecutive_failures = 0
                    
                    # Extract data
                    data = await scraper.extract(scanned.url, scanned.slug)
                    return data
            
            # Process in batches
            batch_size = concurrency * 2
            for batch_start in range(start_id, end_id, batch_size):
                if self._shutdown_requested:
                    break
                
                batch_end = min(batch_start + batch_size, end_id)
                
                tasks = [
                    scan_and_extract(id_num, i) 
                    for i, id_num in enumerate(range(batch_start, batch_end))
                ]
                results = await asyncio.gather(*tasks)
                total_scanned += len(tasks)
                
                for hotel in results:
                    if hotel:
                        found_hotels.append(hotel)
                        logger.success(f"Found: {hotel.name} ({hotel.booking_url})")
                
                # Save batch
                if len(found_hotels) >= BATCH_SAVE_SIZE and not dry_run:
                    saved = await self._save_hotels_batch(found_hotels, booking_engine_id)
                    total_saved += saved
                    logger.info(f"Saved {saved} hotels (total: {total_saved})")
                    found_hotels = []
                
                # Progress
                progress = (batch_end - start_id) / (end_id - start_id) * 100
                logger.info(f"Progress: {progress:.1f}% - Found: {total_saved + len(found_hotels)}")
                
                if consecutive_failures > MAX_CONSECUTIVE_FAILURES:
                    logger.warning(f"Sparse region at {batch_end}")
                    consecutive_failures = 0
                
                await asyncio.sleep(0.5)
            
            # Save remaining
            if found_hotels and not dry_run:
                saved = await self._save_hotels_batch(found_hotels, booking_engine_id)
                total_saved += saved
            
            # Cleanup
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
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
        """Save a batch of hotels."""
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
                    external_id=hotel.slug,  # Use slug as external_id
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
                logger.error(f"Error saving {hotel.name}: {e}")
        
        return saved
    
    # =========================================================================
    # Enrichment
    # =========================================================================
    
    async def enrich_hotels(
        self,
        hotels: List[RMSHotelRecord],
        concurrency: int = 6,
    ) -> EnrichResult:
        """Enrich hotels by scraping their booking pages."""
        
        processed = 0
        enriched = 0
        failed = 0
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            contexts: List[BrowserContext] = []
            scrapers: List[RMSScraper] = []
            
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                scrapers.append(RMSScraper(page))
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def enrich_one(hotel: RMSHotelRecord, idx: int) -> tuple[bool, bool]:
                async with semaphore:
                    scraper = scrapers[idx % len(scrapers)]
                    
                    url = hotel.booking_url
                    if not url.startswith("http"):
                        url = f"https://{url}"
                    
                    slug = url.split("/")[-1]
                    data = await scraper.extract(url, slug)
                    
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
                        logger.info(f"Enriched {hotel.hotel_id}: {data.name}")
                        return (True, True)
                    else:
                        await self._repo.update_enrichment_status(hotel.booking_url, "no_data")
                        return (True, False)
            
            tasks = [enrich_one(h, i) for i, h in enumerate(hotels)]
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
            
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        return EnrichResult(processed=processed, enriched=enriched, failed=failed)
    
    # =========================================================================
    # Queue Operations
    # =========================================================================
    
    async def enqueue_for_enrichment(
        self,
        limit: int = 5000,
        batch_size: int = 10,
    ) -> EnqueueResult:
        """Find and enqueue hotels needing enrichment."""
        
        stats = self._queue.get_stats()
        logger.info(f"Queue: {stats.pending} pending, {stats.in_flight} in flight")
        
        if stats.pending > MAX_QUEUE_DEPTH:
            return EnqueueResult(
                total_found=0,
                enqueued=0,
                skipped=True,
                reason=f"Queue depth exceeds {MAX_QUEUE_DEPTH}",
            )
        
        hotels = await self._repo.get_hotels_needing_enrichment(limit)
        logger.info(f"Found {len(hotels)} hotels needing enrichment")
        
        if not hotels:
            return EnqueueResult(total_found=0, enqueued=0, skipped=False)
        
        enqueued = self._queue.enqueue_hotels(hotels, batch_size)
        logger.success(f"Enqueued {enqueued} hotels")
        
        return EnqueueResult(total_found=len(hotels), enqueued=enqueued, skipped=False)
    
    async def consume_enrichment_queue(
        self,
        concurrency: int = 6,
        max_messages: int = 0,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> ConsumeResult:
        """Consume and process enrichment queue."""
        
        messages_processed = 0
        hotels_processed = 0
        hotels_enriched = 0
        hotels_failed = 0
        
        should_stop = should_stop or (lambda: self._shutdown_requested)
        
        logger.info(f"Starting consumer (concurrency={concurrency})")
        
        while not should_stop():
            if max_messages > 0 and messages_processed >= max_messages:
                break
            
            stats = self._queue.get_stats()
            if stats.pending == 0 and stats.in_flight == 0:
                if max_messages > 0:
                    break
                logger.info("Queue empty, waiting...")
                await asyncio.sleep(30)
                continue
            
            messages = self._queue.receive_messages(min(concurrency, 10))
            if not messages:
                continue
            
            logger.info(f"Processing {len(messages)} messages")
            
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
                    logger.error(f"Error: {e}")
                    hotels_failed += len(msg.hotels)
            
            logger.info(f"Progress: {hotels_processed} processed, {hotels_enriched} enriched")
        
        return ConsumeResult(
            messages_processed=messages_processed,
            hotels_processed=hotels_processed,
            hotels_enriched=hotels_enriched,
            hotels_failed=hotels_failed,
        )
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        return await self._repo.get_hotels_needing_enrichment(limit)
    
    async def get_stats(self) -> Dict[str, int]:
        return await self._repo.get_stats()
    
    async def count_needing_enrichment(self) -> int:
        return await self._repo.count_needing_enrichment()
    
    def get_queue_stats(self) -> QueueStats:
        return self._queue.get_stats()
