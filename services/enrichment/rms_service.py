"""RMS Booking Engine Enrichment Service.

Enriches existing RMS hotels by scraping their booking pages.
For ingestion (discovering new hotels), use services.ingestor.ingestors.rms.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Callable
import asyncio

from pydantic import BaseModel
from loguru import logger
from playwright.async_api import async_playwright, BrowserContext
from playwright_stealth import Stealth

from services.enrichment.rms_repo import IRMSRepo, RMSRepo, RMSHotelRecord
from services.enrichment.rms_scraper import RMSScraper, ExtractedRMSData
from services.enrichment.rms_queue import IRMSQueue, RMSQueue, QueueStats


# Configuration
MAX_QUEUE_DEPTH = 1000


class EnrichResult(BaseModel):
    """Result of RMS enrichment."""
    processed: int
    enriched: int
    failed: int


class EnqueueResult(BaseModel):
    """Result of enqueueing hotels."""
    total_found: int
    enqueued: int
    skipped: bool
    reason: Optional[str] = None


class ConsumeResult(BaseModel):
    """Result of consuming from queue."""
    messages_processed: int
    hotels_processed: int
    hotels_enriched: int
    hotels_failed: int


class IRMSEnrichmentService(ABC):
    """Interface for RMS Enrichment Service."""
    
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


class RMSEnrichmentService(IRMSEnrichmentService):
    """Implementation of RMS Enrichment Service."""
    
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
                # Apply stealth mode to avoid detection
                stealth = Stealth()
                await stealth.apply_stealth_async(page)
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


# Backward compatibility aliases
RMSService = RMSEnrichmentService
IRMSService = IRMSEnrichmentService
