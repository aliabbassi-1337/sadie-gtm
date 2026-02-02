"""IPMS247 / eZee scraper.

Extracts hotel data from IPMS247 booking pages.
Data is server-side rendered in HTML, no API needed.

URL format: https://live.ipms247.com/booking/book-rooms-{slug}
"""

import os
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel


class ExtractedIPMS247Data(BaseModel):
    """Extracted hotel data from IPMS247 page."""
    
    slug: str
    booking_url: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    zip_code: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    def has_data(self) -> bool:
        """Check if we extracted any useful data."""
        return bool(self.name or self.email or self.phone or self.latitude)


def _get_brightdata_proxy() -> Optional[str]:
    """Get Brightdata datacenter proxy URL."""
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
    dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
    if customer_id and dc_zone and dc_password:
        username = f"brd-customer-{customer_id}-zone-{dc_zone}"
        return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
    return None


class IPMS247Scraper:
    """Scrape hotel data from IPMS247 booking pages.
    
    Usage:
        scraper = IPMS247Scraper()
        data = await scraper.extract("safarihotelboardwalk")
    """
    
    def __init__(self, timeout: float = 15.0, use_proxy: bool = False):
        self.timeout = timeout
        self.use_proxy = use_proxy
        self._proxy_url: Optional[str] = None
        if use_proxy:
            self._proxy_url = _get_brightdata_proxy()
    
    def _get_client_kwargs(self) -> dict:
        """Get httpx client kwargs."""
        kwargs = {"timeout": self.timeout}
        if self._proxy_url:
            kwargs["proxy"] = self._proxy_url
            kwargs["verify"] = False
        return kwargs
    
    async def extract(self, slug: str) -> Optional[ExtractedIPMS247Data]:
        """Extract hotel data from IPMS247 booking page.
        
        Args:
            slug: The hotel slug (e.g., "safarihotelboardwalk")
            
        Returns:
            ExtractedIPMS247Data if successful, None if page not found
        """
        url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        
        try:
            async with httpx.AsyncClient(**self._get_client_kwargs()) as client:
                resp = await client.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                    },
                    follow_redirects=True,
                )
                
                if resp.status_code != 200:
                    logger.debug(f"IPMS247 page not found: {slug} (status {resp.status_code})")
                    return None
                
                html = resp.text
                if len(html) < 1000:
                    return None
                
                return self._parse_html(html, slug, url)
                
        except Exception as e:
            logger.debug(f"IPMS247 error for {slug}: {e}")
            return None
    
    def _parse_html(self, html: str, slug: str, url: str) -> Optional[ExtractedIPMS247Data]:
        """Parse hotel data from IPMS247 HTML.
        
        Note: Full hotel details are loaded via AJAX modal, so we extract
        what we can from the initial page (name, country from title).
        """
        soup = BeautifulSoup(html, "html.parser")
        
        data = ExtractedIPMS247Data(slug=slug, booking_url=url)
        
        # Extract hotel name from page title
        # Format: "HOTEL NAME , Country" or "HOTEL NAME"
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Parse "HOTEL NAME , Country" format
            if " , " in title:
                parts = title.split(" , ", 1)
                data.name = parts[0].strip()
                if len(parts) > 1:
                    country = parts[1].strip()
                    # Normalize country names
                    country_map = {
                        "united states of america": "United States",
                        "usa": "United States",
                        "united states": "United States",
                        "australia": "Australia",
                        "canada": "Canada",
                        "united kingdom": "United Kingdom",
                        "new zealand": "New Zealand",
                    }
                    data.country = country_map.get(country.lower(), country)
            else:
                data.name = title
        
        # Try brandname h1 as fallback
        if not data.name:
            brand = soup.find("h1", class_="brandname")
            if brand:
                data.name = brand.get_text(strip=True)
        
        # Try htl-title (in modal, may not be present on initial load)
        if not data.name:
            title_elem = soup.find("h4", class_="htl-title")
            if title_elem:
                for small in title_elem.find_all("small"):
                    small.decompose()
                data.name = title_elem.get_text(strip=True)
        
        # Extract hotel ID for potential API calls
        hotel_id_match = re.search(r'HotelId\s*[=:]\s*["\']?(\d+)', html)
        if hotel_id_match:
            # Store as property for future API integration
            pass
        
        # Extract address from pl-address div (if modal is server-rendered)
        addr_elem = soup.find("p", class_="pl-address")
        if addr_elem:
            self._parse_address_element(addr_elem, data)
        
        # Extract from cnt-detail elements (if modal is server-rendered)
        for detail in soup.find_all("p", class_="cnt-detail"):
            text = detail.get_text(strip=True)
            if text.startswith("Phone"):
                phone = re.sub(r'^Phone\s*:\s*', '', text)
                if phone and not data.phone:
                    data.phone = phone
            elif text.startswith("Email"):
                email = re.sub(r'^Email\s*:\s*', '', text)
                if "@" in email and not data.email:
                    data.email = email
        
        # Extract lat/lng from JavaScript
        lat_match = re.search(r"var\s+lat\s*=\s*['\"]([0-9.-]+)['\"]", html)
        lng_match = re.search(r"var\s+lng\s*=\s*['\"]([0-9.-]+)['\"]", html)
        if lat_match and lng_match:
            try:
                data.latitude = float(lat_match.group(1))
                data.longitude = float(lng_match.group(1))
            except ValueError:
                pass
        
        return data if data.has_data() else None
    
    def _parse_address_element(self, addr_elem, data: ExtractedIPMS247Data) -> None:
        """Parse address from pl-address element."""
        addr_text = addr_elem.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in addr_text.split("\n") if l.strip() and l.strip() != "Address:"]
        
        if lines:
            data.address = lines[0].rstrip(",")
            
            for line in lines[1:]:
                line = line.strip().rstrip(",").rstrip(".")
                
                # Check for US state + ZIP pattern
                us_match = re.search(r'([A-Z]{2})\s*[-–]?\s*(\d{5}(?:-\d{4})?)', line)
                if us_match:
                    data.state = us_match.group(1)
                    data.zip_code = us_match.group(2)
                    continue
                
                # Check for country
                if any(c in line.lower() for c in ["united states", "usa", "america"]):
                    data.country = "United States"
                elif "australia" in line.lower():
                    data.country = "Australia"
                elif "canada" in line.lower():
                    data.country = "Canada"
                elif line and not data.city:
                    data.city = line.rstrip(",")
    
    def _extract_field(self, soup: BeautifulSoup, field_name: str) -> Optional[str]:
        """Extract a field value from the page.
        
        Looks for patterns like:
        <span class="detail-title">Phone</span> : +14102896411
        """
        for span in soup.find_all("span", class_="detail-title"):
            if span.get_text(strip=True).lower() == field_name.lower():
                # Get the parent and extract text after the span
                parent = span.parent
                if parent:
                    text = parent.get_text(strip=True)
                    # Remove the label
                    text = re.sub(rf'^{field_name}\s*:\s*', '', text, flags=re.IGNORECASE)
                    if text:
                        return text.strip()
        return None
