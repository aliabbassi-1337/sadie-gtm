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


def extract_rms_id(url: str) -> Optional[str]:
    """Extract numeric ID from any RMS URL format.
    
    Handles:
    - ibe12.rmscloud.com/{id}
    - ibe13.rmscloud.com/{id}
    - bookings.rmscloud.com/Search/Index/{id}/...
    - bookings{N}.rmscloud.com/Search/Index/{id}/...
    - bookings.rmscloud.com/obookings3/Search/Index/{id}/...
    """
    patterns = [
        r'ibe1[234]\.rmscloud\.com/(\d+)',  # ibe12, ibe13, ibe14
        r'bookings\d*\.rmscloud\.com/(?:obookings\d*/)?[Ss]earch/[Ii]ndex/(\d+)',  # bookings format
        r'rmscloud\.com/.*?/(\d+)/?',  # fallback - any numeric ID in path
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def convert_to_bookings_url(url: str) -> str:
    """Convert any RMS URL to bookings format that shows hotel names.
    
    All formats -> bookings.rmscloud.com/Search/Index/{id}/90/
    """
    id_num = extract_rms_id(url)
    if id_num:
        return f"https://bookings.rmscloud.com/Search/Index/{id_num}/90/"
    return url


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
        # Convert ibe12/ibe13 URLs to bookings format
        scrape_url = convert_to_bookings_url(url)
        
        data = ExtractedRMSData(slug=slug, booking_url=url)  # Keep original URL
        try:
            await self._page.goto(scrape_url, timeout=SCRAPE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(5)  # RMS pages need time for JS to render
            content = await self._page.content()
            body_text = await self._page.evaluate("document.body.innerText")
            
            if not self._is_valid(content, body_text):
                return None
            
            data.name = await self._extract_name(body_text)
            data.phone = self._extract_phone(body_text)
            data.email = self._extract_email(content, body_text)
            data.website = await self._extract_website()
            data.address = self._extract_address(body_text)
            if data.address:
                data.city, data.state, data.country = self._parse_address(data.address)
            
            return data if data.has_data() else None
        except Exception as e:
            logger.debug(f"Error extracting {url}: {e}")
            return None
    
    def _is_valid(self, content: str, body_text: str) -> bool:
        # Reject error pages
        content_lower = content.lower()
        body_lower = body_text.lower()
        
        logger.info(f"_is_valid check: content_len={len(content)}, body_len={len(body_text)}")
        logger.info(f"_is_valid: first 100 chars of body: {repr(body_text[:100])}")
        
        error_patterns = [
            "application issues",
            "page not found",
            "object reference not set",
            "error page",
            "does not exist",
            "no longer available",
        ]
        
        for pattern in error_patterns:
            if pattern in body_lower or pattern in content_lower[:2000]:
                logger.info(f"Rejecting page: found error pattern '{pattern}'")
                return False
        
        if body_text.strip().startswith("Error"):
            logger.info("Rejecting page: body starts with 'Error'")
            return False
        
        # Check for error title
        if "<title>error</title>" in content_lower:
            logger.info("Rejecting page: error title")
            return False
        
        # Must have substantial content
        if not body_text or len(body_text) < 100:
            logger.info(f"Rejecting page: insufficient content ({len(body_text) if body_text else 0} chars)")
            return False
        
        logger.info("_is_valid: PASSED all checks")
        return True
    
    async def _extract_name(self, body_text: str) -> Optional[str]:
        # Garbage names to reject
        garbage = ['online bookings', 'search', 'book now', 'unknown', 'error', 'loading', 
                   'cart', 'book your accommodation', 'dates', 'check in', 'check out', 'guests',
                   'looks like we', 'application issues', 'page not found', '404']
        
        # Try standard selectors first
        for selector in ['h1', '.property-name', '.header-title']:
            try:
                el = await self._page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and 2 < len(text) < 100:
                        if text.lower() not in garbage and 'rmscloud.com' not in text.lower():
                            return text
            except Exception:
                pass
        
        # Try first line of body text (often the property name)
        lines = [l.strip() for l in body_text.split('\n') if l.strip()]
        for line in lines[:5]:  # Check first 5 non-empty lines
            line_lower = line.lower()
            if 2 < len(line) < 80:
                # Check exact match and substring match for garbage
                if line_lower not in garbage and not any(g in line_lower for g in garbage):
                    # Skip lines that look like UI elements
                    if not any(x in line_lower for x in ['(0)', 'search', 'select', 'type:', 'length:']):
                        # Skip dates and timestamps (e.g., "1/28/2026 7:14:06 PM")
                        if not re.match(r'^\d{1,2}/\d{1,2}/\d{4}', line):
                            # Skip version strings (e.g., "V 5.25.345.4")
                            if not re.match(r'^V\s+\d+\.\d+', line):
                                if 'rmscloud.com' not in line_lower:
                                    return line
        
        # Fallback to page title
        title = await self._page.title()
        if title and title.lower() not in garbage and title.strip():
            title = re.sub(r'\s*[-|]\s*RMS.*$', '', title, flags=re.IGNORECASE)
            if title and len(title) > 2 and 'rmscloud.com' not in title.lower():
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
    
    def _parse_address(self, address: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse address to extract city, state, country.
        
        Returns (city, state, country).
        
        Handles formats:
        - Australian: "Street, City STATE Postcode, Australia"
        - US: "Street, City, STATE ZIP"
        - NZ: "Street, City Postcode, New Zealand"
        """
        city = None
        state = None
        country = None
        
        # Handle None or empty address
        if not address:
            return city, state, country
        
        # Australian pattern: "City STATE Postcode, Australia" or "City STATE Postcode , Australia"
        au_match = re.search(
            r',\s*([A-Za-z\s\-\']+)\s+(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)\s+(\d{4})\s*,?\s*Australia',
            address, re.IGNORECASE
        )
        if au_match:
            city = au_match.group(1).strip()
            state = au_match.group(2).upper()
            country = 'AU'
            return city, state, country
        
        # US pattern: "City, STATE ZIP" or "City STATE ZIP"
        us_states = 'AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC'
        us_match = re.search(
            rf',\s*([A-Za-z\s\-\'\.]+)[,\s]+({us_states})\s+(\d{{5}}(?:-\d{{4}})?)',
            address, re.IGNORECASE
        )
        if us_match:
            city = us_match.group(1).strip().rstrip(',')
            state = us_match.group(2).upper()
            country = 'USA'
            return city, state, country
        
        # NZ pattern: "City Postcode, New Zealand"
        nz_match = re.search(
            r',\s*([A-Za-z\s\-\']+)\s+(\d{4})\s*,?\s*New Zealand',
            address, re.IGNORECASE
        )
        if nz_match:
            city = nz_match.group(1).strip()
            country = 'NZ'
            return city, state, country
        
        # Fallback: try to extract just state code and country name
        state_match = re.search(r',\s*([A-Z]{2,3})\s+\d', address)
        if state_match:
            state = state_match.group(1)
        
        country_match = re.search(r'(?:Australia|USA|Canada|New Zealand|UK|United States)', address, re.IGNORECASE)
        if country_match:
            country = normalize_country(country_match.group(0))
        
        return city, state, country
