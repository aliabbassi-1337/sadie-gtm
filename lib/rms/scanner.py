"""RMS URL Scanner.

Scans RMS booking engine IDs to find valid hotel URLs.
Uses bookings.rmscloud.com/Search/Index/{id}/90/ format which returns hotel names.
"""

import asyncio
from typing import Optional, Protocol, runtime_checkable

from playwright.async_api import Page

from lib.rms.models import ScannedURL


PAGE_TIMEOUT = 15000


@runtime_checkable
class IRMSScanner(Protocol):
    """RMS Scanner interface."""
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]: ...
    async def is_valid_page(self, url: str) -> tuple[bool, Optional[str]]: ...


class RMSScanner(IRMSScanner):
    """Scans RMS IDs to find valid booking URLs with hotel names."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        """Scan an ID using bookings.rmscloud.com format."""
        # Use the format that returns hotel names
        url = f"https://bookings.rmscloud.com/Search/Index/{id_num}/90/"
        is_valid, hotel_name = await self.is_valid_page(url)
        if is_valid and hotel_name:
            return ScannedURL(
                id_num=id_num, 
                url=url, 
                slug=str(id_num), 
                subdomain="bookings"
            )
        return None
    
    async def is_valid_page(self, url: str) -> tuple[bool, Optional[str]]:
        """Check if URL returns a valid RMS booking page with hotel name."""
        try:
            response = await self._page.goto(url, timeout=PAGE_TIMEOUT, wait_until="networkidle")
            if not response or response.status >= 400:
                return False, None
            await asyncio.sleep(2)
            
            title = await self._page.title()
            if title == "Error":
                return False, None
            
            body_text = await self._page.evaluate("document.body.innerText")
            if not body_text or len(body_text) < 100:
                return False, None
            
            # First line is the hotel name
            first_line = body_text.split('\n')[0].strip()
            
            # Reject generic/garbage names
            garbage = ['cart', 'error', 'online bookings', '', 'book your accommodation']
            if first_line.lower() in garbage:
                return False, None
            
            return True, first_line
        except Exception:
            return False, None
