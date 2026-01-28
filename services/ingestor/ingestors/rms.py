"""
RMS Cloud Booking Engine Ingestor

Scans RMS booking engine IDs to discover valid hotels.
Self-contained - no imports from other services.
"""

import asyncio
import re
from typing import Optional, List, Dict

from pydantic import BaseModel
from loguru import logger
from playwright.async_api import async_playwright, BrowserContext, Page
from playwright_stealth import Stealth

from services.ingestor.registry import register
from db.client import queries, get_conn


# =============================================================================
# Configuration
# =============================================================================

BATCH_SAVE_SIZE = 50
MAX_CONSECUTIVE_FAILURES = 30
RMS_SUBDOMAINS = ["ibe12", "ibe"]
PAGE_TIMEOUT = 15000
SCRAPE_TIMEOUT = 20000


# =============================================================================
# Models
# =============================================================================

class ScannedURL(BaseModel):
    """Result of a successful URL scan."""
    id_num: int
    url: str
    slug: str
    subdomain: str


class ExtractedData(BaseModel):
    """Data extracted from RMS booking page."""
    slug: str
    booking_url: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    def has_data(self) -> bool:
        return bool(self.name and self.name.lower() not in ['online bookings', 'search', 'error', 'loading', ''])


class RMSIngestResult(BaseModel):
    """Result of RMS ingestion."""
    total_scanned: int
    hotels_found: int
    hotels_saved: int


# =============================================================================
# Helpers
# =============================================================================

def decode_cloudflare_email(encoded: str) -> str:
    try:
        r = int(encoded[:2], 16)
        return ''.join(chr(int(encoded[i:i+2], 16) ^ r) for i in range(2, len(encoded), 2))
    except Exception:
        return ""


def normalize_country(country: str) -> str:
    if not country:
        return ""
    country_map = {
        "united states": "USA", "us": "USA", "usa": "USA",
        "australia": "AU", "canada": "CA", "new zealand": "NZ",
        "united kingdom": "GB", "uk": "GB", "mexico": "MX",
    }
    return country_map.get(country.lower().strip(), country.upper()[:2])


# =============================================================================
# Scanner
# =============================================================================

class RMSScanner:
    """Scans RMS IDs to find valid booking URLs."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        formats = [str(id_num), f"{id_num:04d}", f"{id_num:05d}"]
        for fmt in formats:
            for subdomain in RMS_SUBDOMAINS:
                url = f"https://{subdomain}.rmscloud.com/{fmt}"
                if await self._is_valid_page(url):
                    return ScannedURL(id_num=id_num, url=url, slug=fmt, subdomain=subdomain)
        return None
    
    async def _is_valid_page(self, url: str) -> bool:
        try:
            response = await self._page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            if not response or response.status >= 400:
                return False
            await asyncio.sleep(2)
            content = await self._page.content()
            body_text = await self._page.evaluate("document.body.innerText")
            if "Error" in content[:500] and "application issues" in content:
                return False
            if "Page Not Found" in content or "404" in content[:1000]:
                return False
            if not body_text or len(body_text) < 100:
                return False
            title = await self._page.title()
            return bool(title and title.lower() not in ['', 'error', '404'])
        except Exception:
            return False


# =============================================================================
# Scraper
# =============================================================================

class RMSScraper:
    """Extracts hotel data from RMS booking pages."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def extract(self, url: str, slug: str) -> Optional[ExtractedData]:
        data = ExtractedData(slug=slug, booking_url=url)
        try:
            await self._page.goto(url, timeout=SCRAPE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            content = await self._page.content()
            body_text = await self._page.evaluate("document.body.innerText")
            
            if not self._is_valid(content, body_text):
                return None
            
            data.name = await self._extract_name()
            data.phone = self._extract_phone(body_text)
            data.email = self._extract_email(content, body_text)
            data.website = await self._extract_website()
            data.address = self._extract_address(body_text)
            if data.address:
                data.state, data.country = self._parse_address(data.address)
            
            return data if data.has_data() else None
        except Exception as e:
            logger.debug(f"Error extracting {url}: {e}")
            return None
    
    def _is_valid(self, content: str, body_text: str) -> bool:
        if "Error" in content[:500] and "application issues" in content:
            return False
        if "Page Not Found" in content or "404" in content[:1000]:
            return False
        return bool(body_text and len(body_text) >= 100)
    
    async def _extract_name(self) -> Optional[str]:
        for selector in ['h1', '.property-name', '.header-title']:
            try:
                el = await self._page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and 2 < len(text) < 100 and text.lower() not in ['online bookings', 'search', 'book now']:
                        return text
            except Exception:
                pass
        title = await self._page.title()
        if title and title.lower() not in ['online bookings', 'search', '']:
            title = re.sub(r'\s*[-|]\s*RMS.*$', '', title, flags=re.IGNORECASE)
            if title and len(title) > 2:
                return title.strip()
        return None
    
    def _extract_phone(self, body_text: str) -> Optional[str]:
        patterns = [
            r'(?:tel|phone|call)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
        ]
        for pattern in patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                if len(re.sub(r'\D', '', phone)) >= 7:
                    return phone
        return None
    
    def _extract_email(self, content: str, body_text: str) -> Optional[str]:
        cf_match = re.search(r'data-cfemail="([a-f0-9]+)"', content)
        if cf_match:
            return decode_cloudflare_email(cf_match.group(1))
        email_match = re.search(r'[\w\.\-+]+@[\w\.-]+\.\w{2,}', body_text)
        if email_match:
            email = email_match.group(0)
            if not any(x in email.lower() for x in ['rmscloud', 'example', 'test', 'noreply']):
                return email
        return None
    
    async def _extract_website(self) -> Optional[str]:
        try:
            links = await self._page.query_selector_all('a[href^="http"]')
            for link in links[:10]:
                href = await link.get_attribute('href')
                if href and 'rmscloud' not in href and 'google' not in href:
                    if any(x in href.lower() for x in ['.com', '.com.au', '.co.nz', '.co.uk']):
                        return href
        except Exception:
            pass
        return None
    
    def _extract_address(self, body_text: str) -> Optional[str]:
        patterns = [
            r'(?:address|location)[:\s]*([^\n]{10,100})',
            r'(\d+\s+[A-Za-z]+\s+(?:St|Street|Rd|Road|Ave|Avenue)[^\n]{0,50})',
        ]
        for pattern in patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                addr = match.group(1).strip()
                if len(addr) > 10:
                    return addr
        return None
    
    def _parse_address(self, address: str) -> tuple[Optional[str], Optional[str]]:
        state = None
        country = None
        state_match = re.search(r',\s*([A-Z]{2,3})\s*(?:\d|$)', address)
        if state_match:
            state = state_match.group(1)
        country_match = re.search(r'(?:Australia|USA|Canada|New Zealand|UK)', address, re.IGNORECASE)
        if country_match:
            country = normalize_country(country_match.group(0))
        return state, country


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
        
        found_hotels: List[ExtractedData] = []
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
            
            async def scan_and_extract(id_num: int, idx: int) -> Optional[ExtractedData]:
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
    
    async def _save_batch(self, hotels: List[ExtractedData], booking_engine_id: int) -> int:
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
