"""
RMS Cloud Booking Engine Ingestor

Scans RMS booking engine IDs to discover valid hotels.
Uses lib/rms for scanner/scraper, local rms_repo.py for DB operations.
"""

import asyncio
from typing import Optional, List

from pydantic import BaseModel
from loguru import logger
from playwright.async_api import async_playwright, BrowserContext
from playwright_stealth import Stealth

from services.ingestor.registry import register
from services.ingestor.rms_repo import RMSRepo
from lib.rms import RMSScanner, RMSScraper, ExtractedRMSData
from lib.rms.api_client import RMSApiClient, extract_with_fallback


# =============================================================================
# Configuration
# =============================================================================

BATCH_SAVE_SIZE = 50
MAX_CONSECUTIVE_FAILURES = 30


# =============================================================================
# Models
# =============================================================================

class RMSIngestResult(BaseModel):
    """Result of RMS ingestion."""
    total_scanned: int
    hotels_found: int
    hotels_saved: int


# =============================================================================
# Ingestor
# =============================================================================

@register("rms")
class RMSIngestor:
    """Scan RMS booking engine IDs to discover and ingest hotels."""
    
    source_name = "rms_scan"
    external_id_type = "rms_slug"
    
    def __init__(self):
        self._repo = RMSRepo()
        self._shutdown_requested = False
    
    def request_shutdown(self):
        self._shutdown_requested = True
        logger.info("Shutdown requested")
    
    async def ingest(
        self,
        start_id: int,
        end_id: int,
        concurrency: int = 6,
        dry_run: bool = False,
    ) -> RMSIngestResult:
        booking_engine_id = await self._repo.get_booking_engine_id()
        logger.info(f"RMS Cloud booking engine ID: {booking_engine_id}")
        
        found_hotels: List[ExtractedRMSData] = []
        total_saved = 0
        total_scanned = 0
        consecutive_failures = 0
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            contexts: List[BrowserContext] = []
            scanners: List[RMSScanner] = []
            scrapers: List[RMSScraper] = []
            
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await ctx.new_page()
                stealth = Stealth()
                await stealth.apply_stealth_async(page)
                contexts.append(ctx)
                scanners.append(RMSScanner(page))
                scrapers.append(RMSScraper(page))
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def scan_and_extract(id_num: int, idx: int) -> Optional[ExtractedRMSData]:
                nonlocal consecutive_failures
                async with semaphore:
                    scanner = scanners[idx % len(scanners)]
                    scraper = scrapers[idx % len(scrapers)]
                    scanned = await scanner.scan_id(id_num)
                    if not scanned:
                        consecutive_failures += 1
                        return None
                    consecutive_failures = 0
                    return await scraper.extract(scanned.url, scanned.slug)
            
            batch_size = concurrency * 2
            for batch_start in range(start_id, end_id, batch_size):
                if self._shutdown_requested:
                    break
                batch_end = min(batch_start + batch_size, end_id)
                tasks = [scan_and_extract(id_num, i) for i, id_num in enumerate(range(batch_start, batch_end))]
                results = await asyncio.gather(*tasks)
                total_scanned += len(tasks)
                
                for hotel in results:
                    if hotel:
                        found_hotels.append(hotel)
                        logger.success(f"Found: {hotel.name} ({hotel.booking_url})")
                
                if len(found_hotels) >= BATCH_SAVE_SIZE and not dry_run:
                    saved = await self._save_batch(found_hotels, booking_engine_id, self.source_name)
                    total_saved += saved
                    logger.info(f"Saved {saved} hotels (total: {total_saved})")
                    found_hotels = []
                
                progress = (batch_end - start_id) / (end_id - start_id) * 100
                logger.info(f"Progress: {progress:.1f}% - Found: {total_saved + len(found_hotels)}")
                
                if consecutive_failures > MAX_CONSECUTIVE_FAILURES:
                    logger.warning(f"Sparse region at {batch_end}")
                    consecutive_failures = 0
                
                await asyncio.sleep(0.5)
            
            if found_hotels and not dry_run:
                saved = await self._save_batch(found_hotels, booking_engine_id, self.source_name)
                total_saved += saved
            
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        return RMSIngestResult(
            total_scanned=total_scanned,
            hotels_found=total_saved + len(found_hotels) if dry_run else total_saved,
            hotels_saved=total_saved,
        )
    
    async def ingest_slugs(
        self,
        slugs: List[str],
        concurrency: int = 6,
        dry_run: bool = False,
        source_name: str = "rms_archive_discovery",
        use_api: bool = True,
    ) -> RMSIngestResult:
        """Ingest hotels from a list of known slugs (numeric or hex).
        
        Uses API first (fast, no browser), falls back to Playwright scraper.
        
        Args:
            slugs: List of RMS client IDs (numeric or hex)
            concurrency: Number of concurrent requests
            dry_run: If True, don't save to database
            source_name: Source name for hotel records
            use_api: If True, try API first before Playwright
        """
        booking_engine_id = await self._repo.get_booking_engine_id()
        logger.info(f"RMS Cloud booking engine ID: {booking_engine_id}")
        logger.info(f"Ingesting {len(slugs)} slugs (source: {source_name}, use_api: {use_api})")
        
        found_hotels: List[ExtractedRMSData] = []
        total_saved = 0
        total_scanned = 0
        api_hits = 0
        scraper_hits = 0
        
        # API client for fast extraction
        api_client = RMSApiClient() if use_api else None
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            contexts: List[BrowserContext] = []
            scrapers: List[RMSScraper] = []
            
            # Only create browser contexts if we might need fallback
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await ctx.new_page()
                stealth = Stealth()
                await stealth.apply_stealth_async(page)
                contexts.append(ctx)
                scrapers.append(RMSScraper(page))
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def extract_slug(slug: str, idx: int) -> tuple[Optional[ExtractedRMSData], str]:
                """Extract hotel data, returns (data, method_used)."""
                async with semaphore:
                    scraper = scrapers[idx % len(scrapers)]
                    
                    if use_api and api_client:
                        # Try API first
                        data = await api_client.extract(slug)
                        if data and data.name:
                            # Check if we need fallback for missing data
                            has_contact = data.email or data.phone
                            has_location = data.city or data.state or data.country
                            
                            if has_contact or has_location:
                                return data, "api"
                            
                            # Partial data - try scraper to fill gaps
                            url = f"https://bookings.rmscloud.com/Search/Index/{slug}/90/"
                            scraped = await scraper.extract(url, slug)
                            if scraped:
                                # Merge: keep API name, fill missing from scraper
                                if not data.email:
                                    data.email = scraped.email
                                if not data.phone:
                                    data.phone = scraped.phone
                                if not data.address:
                                    data.address = scraped.address
                                if not data.city:
                                    data.city = scraped.city
                                if not data.state:
                                    data.state = scraped.state
                                if not data.country:
                                    data.country = scraped.country
                                if not data.website:
                                    data.website = scraped.website
                            return data, "api+scraper"
                    
                    # Fallback to scraper only
                    url = f"https://bookings.rmscloud.com/Search/Index/{slug}/90/"
                    data = await scraper.extract(url, slug)
                    return data, "scraper"
            
            batch_size = concurrency * 2
            for batch_start in range(0, len(slugs), batch_size):
                if self._shutdown_requested:
                    break
                batch_end = min(batch_start + batch_size, len(slugs))
                batch_slugs = slugs[batch_start:batch_end]
                
                tasks = [extract_slug(slug, i) for i, slug in enumerate(batch_slugs)]
                results = await asyncio.gather(*tasks)
                total_scanned += len(tasks)
                
                for hotel, method in results:
                    if hotel:
                        found_hotels.append(hotel)
                        if "api" in method:
                            api_hits += 1
                        else:
                            scraper_hits += 1
                        logger.success(f"Found [{method}]: {hotel.name}")
                
                if len(found_hotels) >= BATCH_SAVE_SIZE and not dry_run:
                    saved = await self._save_batch(found_hotels, booking_engine_id, source_name)
                    total_saved += saved
                    logger.info(f"Saved {saved} hotels (total: {total_saved})")
                    found_hotels = []
                
                progress = batch_end / len(slugs) * 100
                logger.info(f"Progress: {progress:.1f}% - Scanned: {total_scanned}, Found: {total_saved + len(found_hotels)} (API: {api_hits}, Scraper: {scraper_hits})")
                
                await asyncio.sleep(0.5)  # Rate limiting
            
            if found_hotels and not dry_run:
                saved = await self._save_batch(found_hotels, booking_engine_id, source_name)
                total_saved += saved
            
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        logger.info(f"Extraction stats - API: {api_hits}, Scraper: {scraper_hits}")
        
        return RMSIngestResult(
            total_scanned=total_scanned,
            hotels_found=total_saved + len(found_hotels) if dry_run else total_saved,
            hotels_saved=total_saved,
        )
    
    async def _save_batch(
        self, 
        hotels: List[ExtractedRMSData], 
        booking_engine_id: int,
        source_name: Optional[str] = None,
    ) -> int:
        saved = 0
        source = source_name or self.source_name
        for hotel in hotels:
            try:
                hotel_id = await self._repo.insert_hotel(
                    name=hotel.name, address=hotel.address, city=hotel.city,
                    state=hotel.state, country=hotel.country, phone=hotel.phone,
                    email=hotel.email, website=hotel.website, external_id=hotel.slug,
                    source=source, status=1,
                )
                if hotel_id:
                    await self._repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id, booking_engine_id=booking_engine_id,
                        booking_url=hotel.booking_url, enrichment_status=1,
                    )
                    saved += 1
            except Exception as e:
                logger.error(f"Error saving {hotel.name}: {e}")
        return saved
