"""RMS URL Scanner.

Scans RMS booking engine IDs to find valid hotel URLs.
"""

import asyncio
from typing import Optional, Protocol, runtime_checkable

from playwright.async_api import Page

from lib.rms.models import ScannedURL


RMS_SUBDOMAINS = ["ibe12", "ibe"]
PAGE_TIMEOUT = 15000


@runtime_checkable
class IRMSScanner(Protocol):
    """RMS Scanner interface."""
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]: ...
    async def is_valid_page(self, url: str) -> bool: ...


class RMSScanner(IRMSScanner):
    """Scans RMS IDs to find valid booking URLs."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        """Scan an ID across different URL formats and subdomains."""
        formats = [str(id_num), f"{id_num:04d}", f"{id_num:05d}"]
        for fmt in formats:
            for subdomain in RMS_SUBDOMAINS:
                url = f"https://{subdomain}.rmscloud.com/{fmt}"
                if await self.is_valid_page(url):
                    return ScannedURL(id_num=id_num, url=url, slug=fmt, subdomain=subdomain)
        return None
    
    async def is_valid_page(self, url: str) -> bool:
        """Check if URL returns a valid RMS booking page."""
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
