"""RMS Booking Engine Service.

Service layer for RMS hotel discovery and enrichment.
Uses the repo layer for database operations and the scraper for data extraction.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import asyncio
import signal

from loguru import logger
from playwright.async_api import async_playwright

from services.enrichment import rms_repo as repo
from services.enrichment.rms_scraper import RMSScraper, ExtractedRMSData


# Configuration
BATCH_SAVE_SIZE = 50
MAX_CONSECUTIVE_FAILURES = 30


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


class IRMSService(ABC):
    """Interface for RMS Service.
    
    Provides methods for discovering and enriching RMS hotels.
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
        hotels: List[repo.RMSHotelRecord],
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
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[repo.RMSHotelRecord]:
        """Get RMS hotels that need enrichment.
        
        Args:
            limit: Maximum number of hotels to return
            
        Returns:
            List of hotel records needing enrichment
        """
        pass
    
    @abstractmethod
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics.
        
        Returns:
            Dict with total, with_name, with_city, with_email, etc. counts
        """
        pass
    
    @abstractmethod
    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment.
        
        Returns:
            Number of hotels needing enrichment
        """
        pass


class RMSService(IRMSService):
    """Implementation of RMS Service."""
    
    def __init__(self):
        self._shutdown_requested = False
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        self._shutdown_requested = True
        logger.info("Shutdown requested")
    
    async def ingest_from_id_range(
        self,
        start_id: int,
        end_id: int,
        concurrency: int = 6,
        dry_run: bool = False,
    ) -> IngestResult:
        """Scan RMS IDs and ingest discovered hotels to database."""
        
        # Get booking engine ID
        booking_engine_id = await repo.get_booking_engine_id()
        logger.info(f"RMS Cloud booking engine ID: {booking_engine_id}")
        
        found_hotels: List[ExtractedRMSData] = []
        total_saved = 0
        total_scanned = 0
        consecutive_failures = 0
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create scraper pool
            scrapers: List[RMSScraper] = []
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await ctx.new_page()
                scraper = RMSScraper(browser)
                scraper._context = ctx
                scraper._page = page
                scrapers.append(scraper)
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def scan_id(id_num: int, scraper_idx: int) -> Optional[ExtractedRMSData]:
                nonlocal consecutive_failures
                
                async with semaphore:
                    scraper = scrapers[scraper_idx % len(scrapers)]
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
            
            # Cleanup
            for scraper in scrapers:
                if scraper._context:
                    await scraper._context.close()
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
        """Save a batch of hotels to the database."""
        saved = 0
        
        for hotel in hotels:
            try:
                hotel_id = await repo.insert_hotel(
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
                    await repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=booking_engine_id,
                        booking_url=hotel.booking_url,
                        enrichment_status="enriched",
                    )
                    saved += 1
                    
            except Exception as e:
                logger.error(f"Error saving hotel {hotel.name}: {e}")
        
        return saved
    
    async def enrich_hotels(
        self,
        hotels: List[repo.RMSHotelRecord],
        concurrency: int = 6,
    ) -> EnrichResult:
        """Enrich a list of RMS hotels by scraping their booking pages."""
        
        processed = 0
        enriched = 0
        failed = 0
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create scraper pool
            scrapers: List[RMSScraper] = []
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await ctx.new_page()
                scraper = RMSScraper(browser)
                scraper._context = ctx
                scraper._page = page
                scrapers.append(scraper)
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def enrich_hotel(hotel: repo.RMSHotelRecord, scraper_idx: int) -> tuple[bool, bool]:
                """Returns (processed, enriched)"""
                async with semaphore:
                    scraper = scrapers[scraper_idx % len(scrapers)]
                    
                    url = hotel.booking_url
                    if not url.startswith("http"):
                        url = f"https://{url}"
                    
                    # Extract slug from URL
                    slug = url.split("/")[-1]
                    
                    data = await scraper.extract_from_url(url, slug)
                    
                    if data and data.has_data():
                        await repo.update_hotel(
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
                        await repo.update_enrichment_status(hotel.booking_url, "enriched")
                        logger.info(f"Enriched hotel {hotel.hotel_id}: {data.name}")
                        return (True, True)
                    else:
                        await repo.update_enrichment_status(hotel.booking_url, "no_data")
                        return (True, False)
            
            # Process all hotels
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
            
            # Cleanup
            for scraper in scrapers:
                if scraper._context:
                    await scraper._context.close()
            await browser.close()
        
        return EnrichResult(processed=processed, enriched=enriched, failed=failed)
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[repo.RMSHotelRecord]:
        """Get RMS hotels that need enrichment."""
        return await repo.get_hotels_needing_enrichment(limit)
    
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        return await repo.get_stats()
    
    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment."""
        return await repo.count_needing_enrichment()
