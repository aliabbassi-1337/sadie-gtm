"""RMS Data Scraper.

Extracts hotel data from RMS booking pages.
"""

import asyncio
import re
from typing import Optional, Protocol, runtime_checkable

from loguru import logger
from playwright.async_api import Page

from lib.rms.models import ExtractedRMSData
from lib.rms.utils import decode_cloudflare_email, normalize_country


SCRAPE_TIMEOUT = 20000


@runtime_checkable
class IRMSScraper(Protocol):
    """RMS Scraper interface."""
    async def extract(self, url: str, slug: str) -> Optional[ExtractedRMSData]: ...


class RMSScraper(IRMSScraper):
    """Extracts hotel data from RMS booking pages."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def extract(self, url: str, slug: str) -> Optional[ExtractedRMSData]:
        """Extract hotel data from RMS booking page."""
        data = ExtractedRMSData(slug=slug, booking_url=url)
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
