"""RMS Booking Engine Scraper.

Abstracts the Playwright-based scraping of RMS booking pages.
Used by the RMS service for both ingestion and enrichment.
"""

import asyncio
import re
from dataclasses import dataclass
from typing import Optional, List, Protocol, runtime_checkable

from loguru import logger
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Playwright


# RMS subdomain variations for new engine
RMS_SUBDOMAINS = ["ibe12", "ibe"]

# Configuration
PAGE_TIMEOUT = 20000  # 20 seconds


@dataclass
class ExtractedRMSData:
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
        """Check if we extracted useful data."""
        return bool(
            self.name 
            and self.name.lower() not in ['online bookings', 'search', 'error', 'loading', '']
        )


def decode_cloudflare_email(encoded: str) -> str:
    """Decode Cloudflare-protected email addresses."""
    try:
        r = int(encoded[:2], 16)
        return ''.join(
            chr(int(encoded[i:i+2], 16) ^ r)
            for i in range(2, len(encoded), 2)
        )
    except Exception:
        return ""


def normalize_country(country: str) -> str:
    """Normalize country names to ISO codes."""
    if not country:
        return ""
    
    country_map = {
        "united states": "USA",
        "united states of america": "USA",
        "us": "USA",
        "usa": "USA",
        "australia": "AU",
        "canada": "CA",
        "new zealand": "NZ",
        "united kingdom": "GB",
        "uk": "GB",
        "mexico": "MX",
    }
    
    return country_map.get(country.lower().strip(), country.upper()[:2])


@runtime_checkable
class IRMSScraper(Protocol):
    """Protocol for RMS scraper operations."""
    
    async def extract_from_url(self, url: str, slug: str) -> Optional[ExtractedRMSData]:
        """Extract data from a single RMS booking page."""
        ...
    
    async def scan_id(self, id_num: int) -> Optional[ExtractedRMSData]:
        """Scan an RMS ID across subdomains and slug formats."""
        ...


class RMSScraper(IRMSScraper):
    """Playwright-based scraper for RMS booking pages."""
    
    def __init__(self, page: Page):
        self._page = page
    
    @property
    def page(self) -> Page:
        return self._page
    
    async def extract_from_url(self, url: str, slug: str) -> Optional[ExtractedRMSData]:
        """Extract data from a single RMS booking page."""
        data = ExtractedRMSData(slug=slug, booking_url=url)
        
        try:
            await self.page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(3)  # Wait for React to render
            
            content = await self.page.content()
            body_text = await self.page.evaluate("document.body.innerText")
            
            # Check for error pages
            if "Error" in content[:500] and "application issues" in content:
                return None
            if "Page Not Found" in content or "404" in content[:1000]:
                return None
            if not body_text or len(body_text) < 100:
                return None
            
            # Extract property name
            data.name = await self._extract_name(content, body_text)
            
            # Extract contact info
            data.phone = self._extract_phone(body_text)
            data.email = self._extract_email(content, body_text)
            data.website = await self._extract_website()
            
            # Extract address
            data.address = self._extract_address(body_text)
            if data.address:
                data.state, data.country = self._parse_address(data.address)
            
            return data if data.has_data() else None
            
        except Exception as e:
            logger.debug(f"Error extracting {url}: {e}")
            return None
    
    async def scan_id(self, id_num: int) -> Optional[ExtractedRMSData]:
        """Scan an RMS ID across subdomains and slug formats."""
        formats = [str(id_num), f"{id_num:04d}", f"{id_num:05d}"]
        
        for fmt in formats:
            for subdomain in RMS_SUBDOMAINS:
                url = f"https://{subdomain}.rmscloud.com/{fmt}"
                data = await self.extract_from_url(url, fmt)
                if data:
                    return data
        
        return None
    
    async def _extract_name(self, content: str, body_text: str) -> Optional[str]:
        """Extract property name from page."""
        name_selectors = ['h1', '.property-name', '[data-testid="property-name"]', '.header-title']
        
        for selector in name_selectors:
            try:
                el = await self.page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and 2 < len(text) < 100:
                        if text.lower() not in ['online bookings', 'search', 'book now', 'cart']:
                            return text
            except Exception:
                pass
        
        # Fallback to page title
        title = await self.page.title()
        if title and title.lower() not in ['online bookings', 'search', '']:
            title = re.sub(r'\s*[-|]\s*RMS.*$', '', title, flags=re.IGNORECASE)
            if title and len(title) > 2:
                return title.strip()
        
        return None
    
    def _extract_phone(self, body_text: str) -> Optional[str]:
        """Extract phone number from page content."""
        phone_patterns = [
            r'(?:tel|phone|call)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
            r'(?<!\d)(\d{2,4}[\s\-]\d{3,4}[\s\-]\d{3,4})(?!\d)',
        ]
        
        for pattern in phone_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                if len(re.sub(r'\D', '', phone)) >= 7:
                    return phone
        
        return None
    
    def _extract_email(self, content: str, body_text: str) -> Optional[str]:
        """Extract email from page content."""
        # Check for Cloudflare protection first
        cf_match = re.search(r'data-cfemail="([a-f0-9]+)"', content)
        if cf_match:
            return decode_cloudflare_email(cf_match.group(1))
        
        # Direct email pattern
        email_match = re.search(r'[\w\.\-+]+@[\w\.-]+\.\w{2,}', body_text)
        if email_match:
            email = email_match.group(0)
            if not any(x in email.lower() for x in ['rmscloud', 'example', 'test', 'noreply']):
                return email
        
        return None
    
    async def _extract_website(self) -> Optional[str]:
        """Extract website from page links."""
        try:
            links = await self.page.query_selector_all('a[href^="http"]')
            for link in links[:10]:
                href = await link.get_attribute('href')
                if href and 'rmscloud' not in href and 'google' not in href:
                    if any(x in href.lower() for x in ['.com', '.com.au', '.co.nz', '.co.uk', '.ca']):
                        return href
        except Exception:
            pass
        
        return None
    
    def _extract_address(self, body_text: str) -> Optional[str]:
        """Extract address from page content."""
        address_patterns = [
            r'(?:address|location)[:\s]*([^\n]{10,100})',
            r'(\d+\s+[A-Za-z]+\s+(?:St|Street|Rd|Road|Ave|Avenue|Blvd|Boulevard|Dr|Drive)[^\n]{0,50})',
        ]
        
        for pattern in address_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                addr = match.group(1).strip()
                if len(addr) > 10:
                    return addr
        
        return None
    
    def _parse_address(self, address: str) -> tuple[Optional[str], Optional[str]]:
        """Parse state and country from address."""
        state = None
        country = None
        
        # Extract state
        state_match = re.search(r',\s*([A-Z]{2,3})\s*(?:\d|$)', address)
        if state_match:
            state = state_match.group(1)
        
        # Extract country
        country_match = re.search(r'(?:Australia|USA|Canada|New Zealand|UK)', address, re.IGNORECASE)
        if country_match:
            country = normalize_country(country_match.group(0))
        
        return state, country


class ScraperPool:
    """Manages a pool of RMS scrapers for concurrent processing."""
    
    def __init__(self, concurrency: int = 6):
        self.concurrency = concurrency
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._scrapers: List[RMSScraper] = []
        self._contexts: List[BrowserContext] = []
    
    async def __aenter__(self) -> "ScraperPool":
        """Initialize browser and create scraper pool."""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        
        for _ in range(self.concurrency):
            ctx = await self._browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await ctx.new_page()
            self._contexts.append(ctx)
            self._scrapers.append(RMSScraper(page))
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up browser resources."""
        for ctx in self._contexts:
            await ctx.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
    
    def get_scraper(self, index: int) -> RMSScraper:
        """Get a scraper by index (wraps around)."""
        return self._scrapers[index % len(self._scrapers)]
    
    @property
    def scrapers(self) -> List[RMSScraper]:
        """Get all scrapers."""
        return self._scrapers


class MockScraper(IRMSScraper):
    """Mock scraper for unit testing."""
    
    def __init__(self, results: Optional[dict[str, ExtractedRMSData]] = None):
        """Initialize with optional URL->result mapping."""
        self._results = results or {}
        self.calls: List[tuple[str, str]] = []
    
    async def extract_from_url(self, url: str, slug: str) -> Optional[ExtractedRMSData]:
        """Return mocked result for URL."""
        self.calls.append((url, slug))
        return self._results.get(url)
    
    async def scan_id(self, id_num: int) -> Optional[ExtractedRMSData]:
        """Return mocked result for ID."""
        for subdomain in RMS_SUBDOMAINS:
            url = f"https://{subdomain}.rmscloud.com/{id_num}"
            if url in self._results:
                return self._results[url]
        return None
