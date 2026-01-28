"""
RMS Cloud Booking Engine Ingestor

Scans RMS booking engine IDs to discover valid hotels.
Uses shared lib/rms for scanner and scraper.
"""

import asyncio
from typing import Optional, List

from pydantic import BaseModel
from loguru import logger
from playwright.async_api import async_playwright, BrowserContext
from playwright_stealth import Stealth

from services.ingestor.registry import register
from db.client import queries, get_conn
from lib.rms import RMSScanner, RMSScraper, ExtractedRMSData


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
# Repository (DB operations)
# =============================================================================

class RMSRepo:
    """Database operations for RMS ingestion."""
    
    def __init__(self):
        self._booking_engine_id: Optional[int] = None
    
    async def get_booking_engine_id(self) -> int:
        if self._booking_engine_id is None:
            async with get_conn() as conn:
                result = await queries.get_rms_booking_engine_id(conn)
                if result:
                    self._booking_engine_id = result["id"]
                else:
                    raise ValueError("RMS Cloud booking engine not found")
        return self._booking_engine_id
    
    async def insert_hotel(
        self,
        name: Optional[str],
        address: Optional[str],
        city: Optional[str],
        state: Optional[str],
        country: Optional[str],
        phone: Optional[str],
        email: Optional[str],
        website: Optional[str],
        external_id: Optional[str],
        source: str = "rms_scan",
        status: int = 1,
    ) -> Optional[int]:
        async with get_conn() as conn:
            return await queries.insert_rms_hotel(
                conn, name=name, address=address, city=city, state=state,
                country=country, phone=phone, email=email, website=website,
                external_id=external_id, source=source, status=status,
            )
    
    async def insert_hotel_booking_engine(
        self,
        hotel_id: int,
        booking_engine_id: int,
        booking_url: str,
        enrichment_status: str = "enriched",
    ) -> None:
        async with get_conn() as conn:
            await queries.insert_rms_hotel_booking_engine(
                conn, hotel_id=hotel_id, booking_engine_id=booking_engine_id,
                booking_url=booking_url, enrichment_status=enrichment_status,
            )


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
                    saved = await self._save_batch(found_hotels, booking_engine_id)
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
                saved = await self._save_batch(found_hotels, booking_engine_id)
                total_saved += saved
            
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        return RMSIngestResult(
            total_scanned=total_scanned,
            hotels_found=total_saved + len(found_hotels) if dry_run else total_saved,
            hotels_saved=total_saved,
        )
    
    async def _save_batch(self, hotels: List[ExtractedRMSData], booking_engine_id: int) -> int:
        saved = 0
        for hotel in hotels:
            try:
                hotel_id = await self._repo.insert_hotel(
                    name=hotel.name, address=hotel.address, city=hotel.city,
                    state=hotel.state, country=hotel.country, phone=hotel.phone,
                    email=hotel.email, website=hotel.website, external_id=hotel.slug,
                    source=self.source_name, status=1,
                )
                if hotel_id:
                    await self._repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id, booking_engine_id=booking_engine_id,
                        booking_url=hotel.booking_url, enrichment_status="enriched",
                    )
                    saved += 1
            except Exception as e:
                logger.error(f"Error saving {hotel.name}: {e}")
        return saved
