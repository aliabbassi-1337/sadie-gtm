"""IPMS247 / eZee scraper.

Extracts hotel data from IPMS247 booking pages.
Uses Playwright to render page and open hotel info modal.

URL format: https://live.ipms247.com/booking/book-rooms-{slug}
"""

import asyncio
import os
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel

# US state code to full name mapping
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
}

# Indian state/territory codes to full names
INDIAN_STATES = {
    "AN": "Andaman and Nicobar Islands", "AP": "Andhra Pradesh", "AR": "Arunachal Pradesh",
    "AS": "Assam", "BR": "Bihar", "CH": "Chandigarh", "CT": "Chhattisgarh", "DD": "Daman and Diu",
    "DL": "Delhi", "GA": "Goa", "GJ": "Gujarat", "HP": "Himachal Pradesh", "HR": "Haryana",
    "JH": "Jharkhand", "JK": "Jammu and Kashmir", "KA": "Karnataka", "KL": "Kerala",
    "LA": "Ladakh", "LD": "Lakshadweep", "MH": "Maharashtra", "ML": "Meghalaya", "MN": "Manipur",
    "MP": "Madhya Pradesh", "MZ": "Mizoram", "NL": "Nagaland", "OD": "Odisha", "PB": "Punjab",
    "PY": "Puducherry", "RJ": "Rajasthan", "SK": "Sikkim", "TN": "Tamil Nadu", "TS": "Telangana",
    "TR": "Tripura", "UK": "Uttarakhand", "UP": "Uttar Pradesh", "WB": "West Bengal",
}


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
        """Check if we extracted any useful data (not an error page)."""
        if not self.name:
            return False
        # Filter out error pages and junk
        error_patterns = ["No Access", "Not Found", "Sorry", "Incorrect URL", "error", "Oops", "eZee Reservation", "Demo -", "Google"]
        for pattern in error_patterns:
            if pattern.lower() in self.name.lower():
                return False
        return True


def _get_brightdata_proxy(use_residential: bool = False, session_id: str = None) -> Optional[str]:
    """Get Brightdata proxy URL for httpx.
    
    Args:
        use_residential: If True, use residential proxy (more reliable, higher cost)
        session_id: Unique ID for residential proxy to get new IP
    """
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    
    if use_residential:
        res_zone = os.getenv("BRIGHTDATA_RES_ZONE", "")
        res_password = os.getenv("BRIGHTDATA_RES_PASSWORD", "")
        if customer_id and res_zone and res_password:
            username = f"brd-customer-{customer_id}-zone-{res_zone}"
            if session_id:
                username += f"-session-{session_id}"
            return f"http://{username}:{res_password}@brd.superproxy.io:22225"
    else:
        dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
        dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
        if customer_id and dc_zone and dc_password:
            username = f"brd-customer-{customer_id}-zone-{dc_zone}"
            return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
    return None


def _get_brightdata_proxy_for_playwright(session_id: Optional[str] = None) -> Optional[dict]:
    """Get Brightdata residential proxy config for Playwright.
    
    Args:
        session_id: Unique session ID to get a dedicated IP. Each session_id gets a different IP.
    
    Returns dict with server, username, password keys.
    Uses residential proxy - required for IPMS247 (datacenter blocked by Brightdata policy).
    """
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    res_zone = os.getenv("BRIGHTDATA_RES_ZONE", "")
    res_password = os.getenv("BRIGHTDATA_RES_PASSWORD", "")
    if customer_id and res_zone and res_password:
        username = f"brd-customer-{customer_id}-zone-{res_zone}"
        if session_id:
            username += f"-session-{session_id}"
        return {
            "server": "http://brd.superproxy.io:22225",
            "username": username,
            "password": res_password,
        }
    return None


class PlaywrightPool:
    """Singleton browser pool for reusing Playwright browser across requests.
    
    Each page gets its own context with a unique session ID = unique IP from Brightdata.
    This allows concurrent requests without overwhelming a single IP.
    """
    
    _instance = None
    _playwright = None
    _browser = None
    _has_proxy = False
    _session_counter = 0
    _init_lock = None
    
    @classmethod
    async def get_instance(cls):
        # Create lock on first access
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        
        async with cls._init_lock:
            if cls._instance is None:
                cls._instance = cls()
                await cls._instance._init()
        return cls._instance
    
    async def _init(self):
        from playwright.async_api import async_playwright
        import uuid
        self._playwright = await async_playwright().start()
        self._session_counter = 0
        
        # Check if proxy is configured
        test_config = _get_brightdata_proxy_for_playwright()
        self._has_proxy = test_config is not None
        
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-extensions',
            ]
        )
        if self._has_proxy:
            logger.info(f"Playwright browser pool initialized with Brightdata residential proxy (unique IP per session)")
        else:
            logger.warning("Playwright browser pool initialized WITHOUT proxy - BRIGHTDATA env vars not set!")
    
    async def new_page(self, session_id: Optional[str] = None):
        """Get a new page with its own context and unique IP.
        
        Args:
            session_id: Optional session ID. If not provided, generates a unique one.
                       Each unique session_id gets a different IP from Brightdata.
        """
        import uuid
        
        # Generate unique session ID if not provided
        if session_id is None:
            self._session_counter += 1
            session_id = f"s{self._session_counter}_{uuid.uuid4().hex[:8]}"
        
        context_opts = {
            'viewport': {'width': 1280, 'height': 720},
            'java_script_enabled': True,
        }
        
        if self._has_proxy:
            # Get proxy config with unique session ID = unique IP
            proxy_config = _get_brightdata_proxy_for_playwright(session_id)
            context_opts['proxy'] = proxy_config
            context_opts['ignore_https_errors'] = True
        
        context = await self._browser.new_context(**context_opts)
        page = await context.new_page()
        return page, context
    
    async def close(self):
        """Clean up browser resources."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        PlaywrightPool._instance = None
        PlaywrightPool._playwright = None
        PlaywrightPool._browser = None
        logger.info("Playwright browser pool closed")


class IPMS247Scraper:
    """Scrape hotel data from IPMS247 booking pages.
    
    Usage:
        scraper = IPMS247Scraper()
        
        # Full data with Playwright (email, phone, address, lat/lng)
        data = await scraper.scrape("safarihotelboardwalk")
        
        # Quick HTTP-only (name only, for initial discovery)
        data = await scraper.extract("safarihotelboardwalk")
    """
    
    # Booking engine ID in database
    ENGINE_ID = 22
    
    def __init__(self, timeout: float = 15.0, use_proxy: bool = False):
        self.timeout = timeout
        self.use_proxy = use_proxy
        self._proxy_url: Optional[str] = None
        if use_proxy:
            self._proxy_url = _get_brightdata_proxy()
            if self._proxy_url:
                logger.debug("IPMS247 scraper using Brightdata proxy")
    
    @staticmethod
    def extract_slug_from_url(url: str) -> Optional[str]:
        """Extract hotel slug from IPMS247 booking URL.
        
        Handles various URL patterns:
        - book-rooms-{slug}
        - gmap-{slug}
        - reviewslist-{slug}
        """
        if not url:
            return None
        
        patterns = [
            (r'book-rooms-([A-Za-z0-9_-]+)', 'book-rooms-'),
            (r'gmap-([A-Za-z0-9_-]+)', 'gmap-'),
            (r'reviewslist-([A-Za-z0-9_-]+)', 'reviewslist-'),
        ]
        
        for regex, prefix in patterns:
            if prefix in url:
                match = re.search(regex, url)
                if match:
                    slug = match.group(1).split('/')[0].split('?')[0]
                    # Filter out non-hotel slugs
                    if slug and not slug.endswith('.php') and len(slug) > 3:
                        return slug
        
        return None
    
    @staticmethod
    def build_booking_url(slug: str) -> str:
        """Build booking URL from slug."""
        return f"https://live.ipms247.com/booking/book-rooms-{slug}"
    
    async def scrape(self, slug_or_url: str, skip_playwright: bool = False) -> Optional[ExtractedIPMS247Data]:
        """Scrape full hotel data using httpx POST to rminfo endpoint.
        
        Gets all data including:
        - Name, address, city, state, country, zip
        - Phone, email
        - Lat/lng coordinates
        - Hotel type, check-in/out times
        
        Args:
            slug_or_url: Hotel slug or full URL
            skip_playwright: If True, don't fall back to Playwright
        """
        result = await self.extract(slug_or_url)
        if result and result.has_data():
            return result
        
        if skip_playwright:
            return None
        
        # Fallback to Playwright for edge cases
        logger.debug(f"httpx extract failed for {slug_or_url}, trying Playwright")
        return await self.extract_with_playwright(slug_or_url)
    
    def _get_client_kwargs(self) -> dict:
        """Get httpx client kwargs."""
        kwargs = {"timeout": self.timeout}
        if self._proxy_url:
            kwargs["proxy"] = self._proxy_url
            kwargs["verify"] = False
        return kwargs
    
    async def extract(self, slug_or_url: str, _retry_count: int = 0) -> Optional[ExtractedIPMS247Data]:
        """Extract hotel data from IPMS247 booking page.
        
        Uses two requests:
        1. Main booking page to get session + hotel ID
        2. POST to rminfo-{slug} to get full hotel details (phone, email, address)
        
        On rate limit (403/429), retries up to 3 times with Brightdata residential proxy (new IP each time).
        
        Args:
            slug_or_url: Either a slug (e.g., "safarihotelboardwalk"), numeric ID (e.g., "126545"),
                         or full URL (e.g., "https://live.ipms247.com/booking/book-rooms-safarihotelboardwalk")
            _retry_count: Internal counter for retries (0 = first attempt without proxy)
            
        Returns:
            ExtractedIPMS247Data if successful, None if page not found
        """
        import uuid
        
        MAX_RETRIES = 3
        
        # Handle full URLs - extract slug
        if slug_or_url.startswith("http"):
            match = re.search(r'book-rooms-([^/?]+)', slug_or_url)
            if match:
                slug = match.group(1)
                booking_url = slug_or_url  # Use original URL
            else:
                logger.debug(f"Could not extract slug from URL: {slug_or_url}")
                return None
        else:
            slug = slug_or_url
            booking_url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        
        # Normalize URL
        booking_url = booking_url.replace(":80/", "/").replace("//booking", "/booking")
        if not booking_url.startswith("https://"):
            booking_url = booking_url.replace("http://", "https://")
        
        # Setup client - use residential proxy on retries (new IP each time)
        client_kwargs = {"timeout": self.timeout, "follow_redirects": True}
        if _retry_count > 0:
            # Generate unique session ID for new IP
            session_id = f"{slug[:8]}_{uuid.uuid4().hex[:8]}"
            proxy_url = _get_brightdata_proxy(use_residential=True, session_id=session_id)
            if proxy_url:
                client_kwargs["proxy"] = proxy_url
                client_kwargs["verify"] = False
        
        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                }
                
                # Step 1: Get main page to extract hotel ID and establish session
                main_resp = await client.get(booking_url, headers=headers)
                
                # Rate limited - retry with new IP
                if main_resp.status_code in (403, 429) and _retry_count < MAX_RETRIES:
                    logger.debug(f"Rate limited on {slug} (attempt {_retry_count + 1}), retrying with new Brightdata IP")
                    return await self.extract(slug_or_url, _retry_count=_retry_count + 1)
                
                if main_resp.status_code != 200:
                    logger.debug(f"IPMS247 page not found: {slug} (status {main_resp.status_code})")
                    return None
                
                main_html = main_resp.text
                if len(main_html) < 1000:
                    return None
                
                # Extract hotel ID from page
                hotel_id_match = re.search(r'HotelId["\s:=]+(\d+)', main_html)
                hotel_id = hotel_id_match.group(1) if hotel_id_match else slug
                
                # Step 2: Fetch rminfo (POST) and gmap (GET) in parallel
                rminfo_url = f"https://live.ipms247.com/booking/rminfo-{slug}"
                gmap_url = f"https://live.ipms247.com/booking/gmap-{slug}"
                rminfo_payload = {
                    "HotelId": hotel_id,
                    "flag": "1",
                    "Hotel_valid": "HotelInformation"
                }
                
                # Parallel requests for speed
                rminfo_task = client.post(rminfo_url, data=rminfo_payload, headers=headers)
                gmap_task = client.get(gmap_url, headers=headers)
                rminfo_resp, gmap_resp = await asyncio.gather(rminfo_task, gmap_task, return_exceptions=True)
                
                # Parse rminfo response
                if not isinstance(rminfo_resp, Exception) and rminfo_resp.status_code == 200 and len(rminfo_resp.text) > 500:
                    result = self._parse_rminfo_html(rminfo_resp.text, slug, booking_url, hotel_id)
                    if result and result.has_data():
                        # Parse gmap for lat/lng
                        if not isinstance(gmap_resp, Exception) and gmap_resp.status_code == 200:
                            coords = re.findall(r"parseFloat\('([0-9.-]+)'\)", gmap_resp.text)
                            if len(coords) >= 2:
                                try:
                                    result.latitude = float(coords[0])
                                    result.longitude = float(coords[1])
                                except ValueError:
                                    pass
                        return result
                
                # Fallback to main page parsing
                return self._parse_html(main_html, slug, booking_url)
                
        except Exception as e:
            logger.debug(f"IPMS247 error for {slug}: {e}")
            return None
    
    def _parse_rminfo_html(self, html: str, slug: str, url: str, hotel_id: str = None) -> Optional[ExtractedIPMS247Data]:
        """Parse hotel data from rminfo POST response.
        
        This endpoint returns the full hotel info including:
        - Address, City, State, Country, Zip
        - Phone, Reservation Phone, Email
        - Hotel Type, Check-in/out times
        - Policies and descriptions
        """
        soup = BeautifulSoup(html, "html.parser")
        
        data = ExtractedIPMS247Data(slug=slug, booking_url=url, hotel_id=hotel_id)
        
        # Extract name from htl-title
        title_elem = soup.find("h4", class_="htl-title")
        if title_elem:
            for small in title_elem.find_all("small"):
                small.decompose()
            data.name = title_elem.get_text(strip=True)
        
        # Parse address from pl-address
        addr_elem = soup.find("p", class_="pl-address")
        if addr_elem:
            self._parse_address_from_rminfo(addr_elem, data)
        
        # Parse cnt-detail elements for phone, email, hotel type, check-in/out
        for detail in soup.find_all("p", class_="cnt-detail"):
            text = detail.get_text(strip=True)
            
            if "Phone" in text and "Reservation" not in text:
                match = re.search(r'Phone\s*:\s*(.+)', text)
                if match and not data.phone:
                    data.phone = match.group(1).strip()
            
            elif "Reservation Phone" in text:
                match = re.search(r'Reservation Phone\s*:\s*(.+)', text)
                if match:
                    data.reservation_phone = match.group(1).strip()
            
            elif "Email" in text:
                match = re.search(r'Email\s*:\s*(\S+@\S+)', text)
                if match and not data.email:
                    data.email = match.group(1).strip()
            
            elif "Hotel Type" in text or "Hostel Type" in text:
                match = re.search(r'(?:Hotel|Hostel) Type\s*:\s*(.+)', text)
                if match:
                    data.hotel_type = match.group(1).strip()
            
            elif "Check-In Time" in text:
                match = re.search(r'Check-In Time\s*(.+)', text)
                if match:
                    data.check_in_time = match.group(1).strip()
            
            elif "Check-Out Time" in text:
                match = re.search(r'Check-Out Time\s*(.+)', text)
                if match:
                    data.check_out_time = match.group(1).strip()
            
            elif text.startswith("Hostel Information") or text.startswith("Hotel Information"):
                # This is the description
                desc_text = text.replace("Hostel Information", "").replace("Hotel Information", "").strip()
                if desc_text:
                    data.description = desc_text
        
        return data if data.has_data() else None
    
    def _parse_address_from_rminfo(self, addr_elem, data: ExtractedIPMS247Data) -> None:
        """Parse address from rminfo pl-address element.
        
        Format: Address: street, city, state - zip, country
        """
        # Get raw text
        raw_text = addr_elem.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in raw_text.split("\n") if l.strip() and l.strip() not in [":", "Address:", "Address"]]
        
        if not lines:
            return
        
        # First line is usually street address
        if lines:
            data.address = lines[0].rstrip(",").strip()
        
        # Look for city, state, zip, country pattern
        full_text = " ".join(lines)
        
        # Try to extract city (usually second line or after first comma)
        # Skip lines that are country names or contain country patterns
        country_words = ['united states', 'usa', 'america', 'india', 'canada', 'australia', 
                         'uk', 'united kingdom', 'mexico', 'thailand', 'indonesia', 'philippines',
                         'sri lanka', 'nepal', 'malaysia', 'vietnam', 'south africa']
        if len(lines) > 1:
            city_line = lines[1].rstrip(",").rstrip(".").strip()
            # Skip if it looks like a state code, country, or contains country name
            if city_line:
                is_country = any(cw in city_line.lower() for cw in country_words)
                is_state_code = re.match(r'^[A-Z]{2}\s*[-–]?\s*\d', city_line)
                if not is_country and not is_state_code:
                    data.city = city_line
        
        # Extract state - pattern like "Karnataka -" or "CA -" or "FL - 32541"
        state_match = re.search(r',?\s*([A-Za-z\s]+)\s*[-–]\s*(\d{5,6})', full_text)
        if state_match:
            state_raw = state_match.group(1).strip()
            data.zip_code = state_match.group(2).strip()
            # Normalize US/Indian state codes
            if state_raw.upper() in US_STATES:
                data.state = US_STATES[state_raw.upper()]
                data.country = "United States"
            elif state_raw.upper() in INDIAN_STATES:
                data.state = INDIAN_STATES[state_raw.upper()]
                data.country = "India"
            else:
                data.state = state_raw
        else:
            # Try US state code pattern: "FL 32541" or "CA, 90210"
            us_state_match = re.search(r'\b([A-Z]{2})\s*[-–,]?\s*(\d{5}(?:-\d{4})?)', full_text)
            if us_state_match:
                state_code = us_state_match.group(1)
                data.zip_code = us_state_match.group(2)
                # Normalize US state code to full name
                if state_code in US_STATES:
                    data.state = US_STATES[state_code]
                    data.country = "United States"
                elif state_code in INDIAN_STATES:
                    data.state = INDIAN_STATES[state_code]
                    data.country = "India"
                else:
                    data.state = state_code
        
        # Extract country (usually last, ends with period) - only if not already set
        if not data.country:
            country_patterns = [
                (r'United States of America\.?\s*$', 'United States'),
                (r'United States\.?\s*$', 'United States'),
                (r'USA\.?\s*$', 'United States'),
                (r'U\.S\.A\.?\s*$', 'United States'),
                (r'India\.?\s*$', 'India'),
                (r'Australia\.?\s*$', 'Australia'),
                (r'Canada\.?\s*$', 'Canada'),
                (r'United Kingdom\.?\s*$', 'United Kingdom'),
                (r'UK\.?\s*$', 'United Kingdom'),
                (r'England\.?\s*$', 'United Kingdom'),
                (r'Sri Lanka\.?\s*$', 'Sri Lanka'),
                (r'Nepal\.?\s*$', 'Nepal'),
                (r'Thailand\.?\s*$', 'Thailand'),
                (r'Indonesia\.?\s*$', 'Indonesia'),
                (r'Philippines\.?\s*$', 'Philippines'),
                (r'Mexico\.?\s*$', 'Mexico'),
                (r'Malaysia\.?\s*$', 'Malaysia'),
                (r'Vietnam\.?\s*$', 'Vietnam'),
                (r'South Africa\.?\s*$', 'South Africa'),
                (r'New Zealand\.?\s*$', 'New Zealand'),
                (r'Singapore\.?\s*$', 'Singapore'),
                (r'Japan\.?\s*$', 'Japan'),
                (r'Germany\.?\s*$', 'Germany'),
                (r'France\.?\s*$', 'France'),
                (r'Spain\.?\s*$', 'Spain'),
                (r'Italy\.?\s*$', 'Italy'),
                (r'Greece\.?\s*$', 'Greece'),
                (r'Portugal\.?\s*$', 'Portugal'),
                (r'Hungary\.?\s*$', 'Hungary'),
                (r'Czech Republic\.?\s*$', 'Czech Republic'),
                (r'Poland\.?\s*$', 'Poland'),
                (r'Netherlands\.?\s*$', 'Netherlands'),
                (r'Belgium\.?\s*$', 'Belgium'),
                (r'Switzerland\.?\s*$', 'Switzerland'),
                (r'Austria\.?\s*$', 'Austria'),
            ]
            for pattern, country in country_patterns:
                if re.search(pattern, full_text, re.IGNORECASE):
                    data.country = country
                    break
    
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
    
    async def extract_with_playwright(self, slug_or_url: str) -> Optional[ExtractedIPMS247Data]:
        """Extract hotel data using Playwright to render JavaScript.
        
        Uses shared browser pool for efficiency - each page gets its own context.
        Args:
            slug_or_url: Either a slug like "safarihotel" or full URL like "https://live.ipms247.com/booking/book-rooms-safarihotel"
        """
        import time
        # Handle both URLs and slugs
        if slug_or_url.startswith("http"):
            url = slug_or_url
            slug = re.search(r'book-rooms-([^/?]+)', slug_or_url)
            slug = slug.group(1) if slug else slug_or_url
        else:
            slug = slug_or_url
            url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        page = None
        context = None
        t0 = time.time()
        
        try:
            pool = await PlaywrightPool.get_instance()
            t1 = time.time()
            
            # Retry with new IP (new context) on tunnel failures
            for attempt in range(3):
                try:
                    page, context = await pool.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    break
                except Exception as e:
                    if context:
                        await context.close()
                        context = None
                    if "ERR_TUNNEL_CONNECTION_FAILED" in str(e) and attempt < 2:
                        logger.debug(f"{slug}: new IP retry {attempt + 1}")
                        continue
                    raise
            t2 = time.time()
            logger.debug(f"{slug}: page_setup={t1-t0:.2f}s, goto={t2-t1:.2f}s")
            
            # Wait for page to be interactive
            try:
                await page.wait_for_selector('a[title="Hotel Info"], a[id^="allhoteldetails_"]', timeout=3000)
            except:
                pass  # Continue anyway
            
            # Click using JavaScript (more reliable than Playwright click)
            clicked = await page.evaluate('''() => {
                const selectors = [
                    'a[title="Hotel Info"]',
                    'a[id^="allhoteldetails_"]',
                    'a[data-target="#propertyinfoModal"]'
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }''')
            
            if clicked:
                # Wait for modal content with Phone/Email to appear (loaded via AJAX)
                try:
                    await page.wait_for_function('document.body.innerText.includes("Phone :")', timeout=10000)
                except:
                    await page.wait_for_timeout(3000)  # Fallback
            
            t3 = time.time()
            
            # Get page content AND body text (modal content renders into body)
            html = await page.content()
            modal_text = await page.evaluate('() => document.body.innerText')
            
            result = self._parse_full_html(html, slug, url, modal_text)
            t4 = time.time()
            logger.debug(f"{slug}: modal={t3-t2:.2f}s, parse={t4-t3:.2f}s, total={t4-t0:.2f}s")
            if result and result.email:
                logger.info(f"Playwright success for {slug}: got email {result.email}")
            return result
            
        except Exception as e:
            logger.warning(f"Playwright failed for {slug}: {type(e).__name__}: {e}")
            return await self.extract(slug)
        finally:
            # Always close context (which closes its page) to free resources
            if context:
                try:
                    await context.close()
                except:
                    pass
    
    def _parse_full_html(self, html: str, slug: str, url: str, modal_text: str = "") -> Optional[ExtractedIPMS247Data]:
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
        
        # Extract all fields from modal text (the actual hotel info)
        # Modal text contains: "Phone : +14102896411\nEmail : safarimotel@gmail.com\n..."
        text = modal_text or html
        
        # Email: look for "Email : xxx@xxx.xxx" pattern
        email_match = re.search(r'Email\s*:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
        if email_match:
            data.email = email_match.group(1).strip()
        
        # Phone: look for "Phone : +xxx" pattern (not Reservation Phone)
        phone_match = re.search(r'(?<!Reservation )Phone\s*:\s*([+\d\s()-]{7,20})', text)
        if phone_match:
            data.phone = phone_match.group(1).strip()
        
        # Reservation Phone
        res_phone_match = re.search(r'Reservation\s+Phone\s*:\s*([+\d\s()-]{7,20})', text)
        if res_phone_match:
            data.reservation_phone = res_phone_match.group(1).strip()
            if not data.phone:
                data.phone = data.reservation_phone
        
        # Address block: "Address:\n1219 Atlantic Ave,,\nOcean City, MD - 21842, United States"
        addr_match = re.search(r'Address:\s*\n([^\n]+(?:\n[^\n]+)*?)(?=\n\s*\n|\nPhone|\nEmail)', text, re.DOTALL)
        if addr_match:
            addr_lines = [l.strip() for l in addr_match.group(1).strip().split('\n') if l.strip()]
            if addr_lines:
                data.address = addr_lines[0].rstrip(',')
                # Parse city, state, zip, country from remaining lines
                for line in addr_lines[1:]:
                    # Pattern: "City, STATE - ZIP, Country" or "City, STATE ZIP, Country"
                    loc_match = re.match(r'([^,]+),\s*([A-Z]{2})\s*[-–]?\s*(\d{5})?,?\s*(.+)?', line)
                    if loc_match:
                        data.city = loc_match.group(1).strip()
                        data.state = loc_match.group(2).strip()
                        if loc_match.group(3):
                            data.zip_code = loc_match.group(3).strip()
                        if loc_match.group(4):
                            data.country = loc_match.group(4).strip().rstrip('.')
        
        # Website from meta or link
        website_match = re.search(r'Website\s*:\s*(https?://[^\s<>"]+)', text)
        if website_match:
            data.website = website_match.group(1).strip()
        
        # Hotel Type
        type_match = re.search(r'Hotel\s+Type\s*:\s*([^\n<]+)', text)
        if type_match:
            data.hotel_type = type_match.group(1).strip()
        
        # Check-In/Out Times
        checkin_match = re.search(r'Check-In\s+Time\s*[:\n]\s*(\d{1,2}:\d{2}\s*[AP]M)', text, re.IGNORECASE)
        if checkin_match:
            data.check_in_time = checkin_match.group(1).strip()
        
        checkout_match = re.search(r'Check-Out\s+Time\s*[:\n]\s*(\d{1,2}:\d{2}\s*[AP]M)', text, re.IGNORECASE)
        if checkout_match:
            data.check_out_time = checkout_match.group(1).strip()
        
        # Coordinates from Google Maps link or data attributes
        coords_match = re.search(r'@(-?\d+\.?\d*),(-?\d+\.?\d*)', html)
        if coords_match:
            try:
                data.latitude = float(coords_match.group(1))
                data.longitude = float(coords_match.group(2))
            except ValueError:
                pass
        
        # Also try data-lat/data-lng attributes
        if not data.latitude:
            lat_match = re.search(r'data-lat[="\']+(-?\d+\.?\d*)', html)
            lng_match = re.search(r'data-lng[="\']+(-?\d+\.?\d*)', html)
            if lat_match and lng_match:
                try:
                    data.latitude = float(lat_match.group(1))
                    data.longitude = float(lng_match.group(1))
                except ValueError:
                    pass
        
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
