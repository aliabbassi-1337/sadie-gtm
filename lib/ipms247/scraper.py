"""IPMS247 / eZee scraper.

Extracts hotel data from IPMS247 booking pages.
Uses Playwright to render page and open hotel info modal.

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
    reservation_phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    hotel_id: Optional[str] = None
    
    # Additional fields
    hotel_type: Optional[str] = None  # Hotels, Resorts, B&B, etc.
    check_in_time: Optional[str] = None
    check_out_time: Optional[str] = None
    description: Optional[str] = None  # Hotel Information
    facilities: Optional[str] = None
    parking_policy: Optional[str] = None
    check_in_policy: Optional[str] = None
    children_policy: Optional[str] = None
    things_to_do: Optional[str] = None
    landmarks: Optional[str] = None
    travel_directions: Optional[str] = None
    
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
        scraper = IPMS247Scraper(use_proxy=True)
        data = await scraper.extract("safarihotelboardwalk")
    """
    
    def __init__(self, timeout: float = 15.0, use_proxy: bool = False):
        self.timeout = timeout
        self.use_proxy = use_proxy
        self._proxy_url: Optional[str] = None
        if use_proxy:
            self._proxy_url = _get_brightdata_proxy()
            if self._proxy_url:
                logger.debug("IPMS247 scraper using Brightdata proxy")
    
    def _get_client_kwargs(self) -> dict:
        """Get httpx client kwargs."""
        kwargs = {"timeout": self.timeout}
        if self._proxy_url:
            kwargs["proxy"] = self._proxy_url
            kwargs["verify"] = False
        return kwargs
    
    async def extract(self, slug: str) -> Optional[ExtractedIPMS247Data]:
        """Extract hotel data from IPMS247 booking page.
        
        Uses two requests:
        1. Main booking page to get session + hotel ID
        2. propertyinfo.php to get full hotel details
        
        Args:
            slug: The hotel slug (e.g., "safarihotelboardwalk")
            
        Returns:
            ExtractedIPMS247Data if successful, None if page not found
        """
        booking_url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        
        try:
            async with httpx.AsyncClient(**self._get_client_kwargs(), follow_redirects=True) as client:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                }
                
                # Step 1: Get main page to extract hotel ID and establish session
                main_resp = await client.get(booking_url, headers=headers)
                
                if main_resp.status_code != 200:
                    logger.debug(f"IPMS247 page not found: {slug} (status {main_resp.status_code})")
                    return None
                
                main_html = main_resp.text
                if len(main_html) < 1000:
                    return None
                
                # Extract hotel ID
                hotel_id_match = re.search(r'HotelId["\s:=]+(\d+)', main_html)
                if not hotel_id_match:
                    # Try to parse from main page only
                    return self._parse_html(main_html, slug, booking_url)
                
                hotel_id = hotel_id_match.group(1)
                
                # Step 2: Fetch propertyinfo.php with session (has full hotel details)
                info_url = f"https://live.ipms247.com/booking/propertyinfo.php?HotelId={hotel_id}"
                info_resp = await client.get(info_url, headers=headers)
                
                if info_resp.status_code == 200 and len(info_resp.text) > 1000:
                    # Parse the full propertyinfo page
                    return self._parse_full_html(info_resp.text, slug, booking_url)
                else:
                    # Fallback to main page parsing
                    return self._parse_html(main_html, slug, booking_url)
                
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
    
    async def extract_with_playwright(self, slug: str) -> Optional[ExtractedIPMS247Data]:
        """Extract hotel data using Playwright to render JavaScript.
        
        This opens the hotel info modal to get full details including
        address, phone, email, and coordinates.
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not installed - falling back to HTTP scraper")
            return await self.extract(slug)
        
        url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Navigate to booking page
                await page.goto(url, wait_until="networkidle", timeout=30000)
                
                # Wait for page to load
                await page.wait_for_timeout(2000)
                
                # Click the hotel info button to open modal
                # Look for info icon or "Hotel Info" text
                info_selectors = [
                    'a[data-target="#propertyinfoModal"]',
                    'a[onclick*="propertyinfo"]',
                    '.fa-info-circle',
                    'a:has-text("Hotel Info")',
                    'a:has-text("Property Info")',
                    '[data-type="hotel_info"]',
                ]
                
                clicked = False
                for selector in info_selectors:
                    try:
                        elem = page.locator(selector).first
                        if await elem.is_visible():
                            await elem.click()
                            clicked = True
                            break
                    except:
                        continue
                
                if clicked:
                    # Wait for modal to load
                    await page.wait_for_timeout(2000)
                
                # Get page content
                html = await page.content()
                await browser.close()
                
                return self._parse_full_html(html, slug, url)
                
        except Exception as e:
            logger.debug(f"Playwright error for {slug}: {e}")
            return await self.extract(slug)
    
    def _parse_full_html(self, html: str, slug: str, url: str) -> Optional[ExtractedIPMS247Data]:
        """Parse full HTML including modal content."""
        soup = BeautifulSoup(html, "html.parser")
        data = ExtractedIPMS247Data(slug=slug, booking_url=url)
        
        # Get hotel ID
        id_match = re.search(r'HotelId["\s:=]+(\d+)', html)
        if id_match:
            data.hotel_id = id_match.group(1)
        
        # Get name from htl-title (modal) or title tag
        title_elem = soup.find("h4", class_="htl-title")
        if title_elem:
            for small in title_elem.find_all("small"):
                small.decompose()
            data.name = title_elem.get_text(strip=True)
        
        if not data.name:
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)
                if " , " in title:
                    data.name = title.split(" , ")[0].strip()
                else:
                    data.name = title
        
        # Parse address from pl-address or cnt-detail
        addr_elem = soup.find("p", class_="pl-address")
        if addr_elem:
            self._parse_address_from_html(addr_elem.get_text(separator="\n"), data)
        
        # Parse detail fields
        for p in soup.find_all("p", class_="cnt-detail"):
            text = p.get_text(strip=True)
            
            # Skip address (already parsed)
            if text.startswith("Address"):
                continue
            
            # Phone numbers
            if text.startswith("Phone") and not text.startswith("Reservation"):
                phone = re.sub(r'^Phone\s*:\s*', '', text, flags=re.IGNORECASE)
                if phone:
                    data.phone = phone.strip()
            elif text.startswith("Reservation Phone"):
                phone = re.sub(r'^Reservation Phone\s*:\s*', '', text, flags=re.IGNORECASE)
                if phone:
                    data.reservation_phone = phone.strip()
                    if not data.phone:
                        data.phone = phone.strip()
            
            # Email
            elif text.startswith("Email"):
                email = re.sub(r'^Email\s*:\s*', '', text, flags=re.IGNORECASE)
                if "@" in email:
                    data.email = email.strip()
            
            # Hotel Type
            elif text.startswith("Hotel Type"):
                data.hotel_type = re.sub(r'^Hotel Type\s*:\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Check-In/Out Times
            elif text.startswith("Check-In Time"):
                data.check_in_time = re.sub(r'^Check-In Time\s*', '', text, flags=re.IGNORECASE).strip()
            elif text.startswith("Check-Out Time"):
                data.check_out_time = re.sub(r'^Check-Out Time\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Hotel Information/Description
            elif text.startswith("Hotel Information"):
                desc = re.sub(r'^Hotel Information\s*', '', text, flags=re.IGNORECASE).strip()
                if desc:
                    data.description = desc
            
            # Facilities
            elif text.startswith("Facilities"):
                data.facilities = re.sub(r'^Facilities\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Parking Policy
            elif text.startswith("Parking Policy"):
                data.parking_policy = re.sub(r'^Parking Policy\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Check-In Policy
            elif text.startswith("Check-In Policy"):
                data.check_in_policy = re.sub(r'^Check-In Policy\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Children & Extra Guest Details
            elif "Children" in text and "Extra" in text:
                data.children_policy = re.sub(r'^Children\s*&?\s*Extra Guest Details\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Things To Do
            elif text.startswith("Things To Do"):
                data.things_to_do = re.sub(r'^Things To Do\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Important Landmarks Nearby
            elif text.startswith("Important Landmarks"):
                data.landmarks = re.sub(r'^Important Landmarks Nearby\s*', '', text, flags=re.IGNORECASE).strip()
            
            # Travel Directions
            elif text.startswith("Travel Directions"):
                data.travel_directions = re.sub(r'^Travel Directions\s*', '', text, flags=re.IGNORECASE).strip()
        
        # Extract lat/lng from JavaScript
        lat_match = re.search(r"var\s+lat\s*=\s*'([0-9.-]+)'", html)
        lng_match = re.search(r"var\s+lng\s*=\s*'([0-9.-]+)'", html)
        if lat_match and lng_match:
            try:
                data.latitude = float(lat_match.group(1))
                data.longitude = float(lng_match.group(1))
            except ValueError:
                pass
        
        return data if data.has_data() else None
    
    def _parse_address_from_html(self, addr_text: str, data: ExtractedIPMS247Data) -> None:
        """Parse address text to extract components.
        
        Handles format like:
        Address:
        1219 Atlantic Ave,,
        Ocean City,
        MD - 21842, United States of America.
        """
        # Clean up whitespace and remove "Address:" label
        addr_text = re.sub(r'\s+', ' ', addr_text).strip()
        addr_text = re.sub(r'^Address:\s*', '', addr_text, flags=re.IGNORECASE)
        
        # Split by <br/> or commas, filtering empty
        lines = [l.strip().rstrip(",").rstrip(".") for l in re.split(r'[,\n]', addr_text) if l.strip()]
        lines = [l for l in lines if l and l.lower() != "address:"]
        
        if not lines:
            return
        
        # First line is street address
        data.address = lines[0]
        
        # Join remaining for pattern matching
        remaining = " ".join(lines[1:])
        
        # Extract US state + ZIP (e.g., "MD - 21842" or "MD 21842")
        us_match = re.search(r'\b([A-Z]{2})\s*[-–]?\s*(\d{5}(?:-\d{4})?)', remaining)
        if us_match:
            data.state = us_match.group(1)
            data.zip_code = us_match.group(2)
        
        # Extract country
        country_map = {
            "united states of america": "United States",
            "united states": "United States",
            "usa": "United States",
            "australia": "Australia",
            "canada": "Canada",
            "new zealand": "New Zealand",
            "united kingdom": "United Kingdom",
        }
        remaining_lower = remaining.lower()
        for pattern, country in country_map.items():
            if pattern in remaining_lower:
                data.country = country
                break
        
        # City is usually second line (before state/zip/country)
        if len(lines) > 1:
            city_candidate = lines[1]
            # Make sure it's not state/zip/country
            if not re.search(r'\b[A-Z]{2}\s*[-–]?\s*\d{5}', city_candidate):
                if not any(c in city_candidate.lower() for c in country_map.keys()):
                    data.city = city_candidate
    
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
