"""RMS URL Scanner.

Scans RMS booking engine IDs to find valid hotel URLs.
Tries different subdomains and slug formats.
"""

import asyncio
from dataclasses import dataclass
from typing import Optional, List, Protocol, runtime_checkable

from playwright.async_api import Page

# RMS subdomain variations
RMS_SUBDOMAINS = ["ibe12", "ibe"]

# Timeout for page load
PAGE_TIMEOUT = 15000  # 15 seconds


@dataclass
class ScannedURL:
    """Result of a successful URL scan."""
    id_num: int
    url: str
    slug: str
    subdomain: str


@runtime_checkable
class IRMSScanner(Protocol):
    """Protocol for RMS URL scanner."""
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        """Scan an ID and return the valid URL if found."""
        ...
    
    async def is_valid_page(self, url: str) -> bool:
        """Check if a URL is a valid RMS booking page."""
        ...


class RMSScanner(IRMSScanner):
    """Playwright-based scanner for RMS booking URLs."""
    
    def __init__(self, page: Page):
        self._page = page
    
    @property
    def page(self) -> Page:
        return self._page
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        """Scan an ID across subdomains and slug formats."""
        formats = [str(id_num), f"{id_num:04d}", f"{id_num:05d}"]
        
        for fmt in formats:
            for subdomain in RMS_SUBDOMAINS:
                url = f"https://{subdomain}.rmscloud.com/{fmt}"
                if await self.is_valid_page(url):
                    return ScannedURL(
                        id_num=id_num,
                        url=url,
                        slug=fmt,
                        subdomain=subdomain,
                    )
        
        return None
    
    async def is_valid_page(self, url: str) -> bool:
        """Check if a URL is a valid RMS booking page."""
        try:
            response = await self._page.goto(
                url, 
                timeout=PAGE_TIMEOUT, 
                wait_until="domcontentloaded"
            )
            
            if not response or response.status >= 400:
                return False
            
            await asyncio.sleep(2)  # Wait for React to render
            
            content = await self._page.content()
            body_text = await self._page.evaluate("document.body.innerText")
            
            # Check for error indicators
            if "Error" in content[:500] and "application issues" in content:
                return False
            if "Page Not Found" in content or "404" in content[:1000]:
                return False
            if not body_text or len(body_text) < 100:
                return False
            
            # Check for valid booking page indicators
            title = await self._page.title()
            if title and title.lower() not in ['', 'error', '404']:
                return True
            
            return False
            
        except Exception:
            return False


class MockScanner(IRMSScanner):
    """Mock scanner for unit testing."""
    
    def __init__(self, valid_ids: Optional[set[int]] = None):
        """Initialize with optional set of valid IDs."""
        self._valid_ids = valid_ids or set()
        self.scanned_ids: List[int] = []
        self.checked_urls: List[str] = []
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        """Return mock result for ID."""
        self.scanned_ids.append(id_num)
        
        if id_num in self._valid_ids:
            return ScannedURL(
                id_num=id_num,
                url=f"https://ibe.rmscloud.com/{id_num}",
                slug=str(id_num),
                subdomain="ibe",
            )
        return None
    
    async def is_valid_page(self, url: str) -> bool:
        """Return mock result for URL."""
        self.checked_urls.append(url)
        # Extract ID from URL and check
        try:
            slug = url.split("/")[-1]
            id_num = int(slug)
            return id_num in self._valid_ids
        except ValueError:
            return False
