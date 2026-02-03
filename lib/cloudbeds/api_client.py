"""Cloudbeds API client - fast data extraction without Playwright.

The property_info API endpoint returns structured data including lat/lng.
This is much faster and more reliable than Playwright scraping.
"""

import os
import re
from typing import Optional
from pydantic import BaseModel
import httpx
from loguru import logger


API_TIMEOUT = 15.0


class CloudbedsPropertyData(BaseModel):
    """Data extracted from Cloudbeds property_info API."""
    property_code: str
    booking_url: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    zip_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    contact_name: Optional[str] = None  # primary_name or contact_first_name + contact_last_name
    formatted_address: Optional[str] = None
    
    def has_data(self) -> bool:
        return bool(self.name and self.name.strip())
    
    def has_location(self) -> bool:
        return self.latitude is not None and self.longitude is not None


def _get_brightdata_proxy(prefer_cheap: bool = True) -> Optional[str]:
    """Build Brightdata proxy URL if credentials are available.
    
    Args:
        prefer_cheap: If True, prefer datacenter (cheapest) > residential > unlocker.
                      Datacenter: ~$0.11/GB, Residential: ~$5.5/GB, Unlocker: ~$3/request
    """
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    if not customer_id:
        return None
    
    if prefer_cheap:
        # Try datacenter first (cheapest ~$0.11/GB)
        dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
        dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
        if dc_zone and dc_password:
            username = f"brd-customer-{customer_id}-zone-{dc_zone}"
            return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
        
        # Fall back to residential (~$5.5/GB)
        res_zone = os.getenv("BRIGHTDATA_RES_ZONE", "")
        res_password = os.getenv("BRIGHTDATA_RES_PASSWORD", "")
        if res_zone and res_password:
            username = f"brd-customer-{customer_id}-zone-{res_zone}"
            return f"http://{username}:{res_password}@brd.superproxy.io:33335"
    
    # Fall back to unlocker zone (most expensive)
    zone = os.getenv("BRIGHTDATA_ZONE", "")
    password = os.getenv("BRIGHTDATA_ZONE_PASSWORD", "")
    
    if zone and password:
        username = f"brd-customer-{customer_id}-zone-{zone}"
        return f"http://{username}:{password}@brd.superproxy.io:33335"
    return None


def extract_property_code(url: str) -> Optional[str]:
    """Extract property code from Cloudbeds booking URL.
    
    Examples:
        https://hotels.cloudbeds.com/reservation/kypwgi -> kypwgi
        https://hotels.cloudbeds.com/booking/kypwgi -> kypwgi
        https://hotels.cloudbeds.com/reservation/hotels.cloudbeds.com/reservation/osbtup -> osbtup (malformed)
    """
    # Handle malformed URLs with duplicate domain
    if 'cloudbeds.com/reservation/hotels.cloudbeds.com' in url:
        url = url.replace('hotels.cloudbeds.com/reservation/hotels.cloudbeds.com/reservation/', 
                          'hotels.cloudbeds.com/reservation/')
    
    # Pattern: /reservation/{code} or /booking/{code}
    # Code is typically 6 alphanumeric chars, but can vary
    match = re.search(r'/(?:reservation|booking)/([a-zA-Z0-9]{2,10})(?:/|$|\?)', url)
    if match:
        code = match.group(1)
        # Skip if it looks like a domain part (shouldn't happen after fix above)
        if code.lower() in ('hotels', 'www', 'booking'):
            return None
        return code
    return None


def normalize_country(country_code: str) -> str:
    """Convert country code to full name."""
    country_map = {
        "US": "United States",
        "USA": "United States",
        "CA": "Canada",
        "MX": "Mexico",
        "AU": "Australia",
        "NZ": "New Zealand",
        "GB": "United Kingdom",
        "UK": "United Kingdom",
        "JM": "Jamaica",
        "BS": "Bahamas",
        "PR": "Puerto Rico",
        "VI": "US Virgin Islands",
        "BB": "Barbados",
        "TT": "Trinidad and Tobago",
        "CR": "Costa Rica",
        "PA": "Panama",
        "CO": "Colombia",
        "BR": "Brazil",
        "AR": "Argentina",
        "CL": "Chile",
        "PE": "Peru",
        "EC": "Ecuador",
        "DE": "Germany",
        "FR": "France",
        "IT": "Italy",
        "ES": "Spain",
        "PT": "Portugal",
        "NL": "Netherlands",
        "BE": "Belgium",
        "AT": "Austria",
        "CH": "Switzerland",
        "IE": "Ireland",
        "GR": "Greece",
        "TH": "Thailand",
        "VN": "Vietnam",
        "ID": "Indonesia",
        "MY": "Malaysia",
        "SG": "Singapore",
        "PH": "Philippines",
        "JP": "Japan",
        "KR": "South Korea",
        "IN": "India",
        "AE": "United Arab Emirates",
        "ZA": "South Africa",
    }
    return country_map.get(country_code.upper(), country_code)


class CloudbedsApiClient:
    """Fast Cloudbeds data extraction via property_info API.
    
    Usage:
        client = CloudbedsApiClient()
        data = await client.extract("kypwgi")
        
        # With Brightdata proxy:
        client = CloudbedsApiClient(use_brightdata=True)
        data = await client.extract("kypwgi")
    """
    
    API_URL = "https://hotels.cloudbeds.com/booking/property_info"
    
    def __init__(self, timeout: float = API_TIMEOUT, use_brightdata: bool = False):
        self.timeout = timeout
        self.use_brightdata = use_brightdata
        self._proxy_url: Optional[str] = None
        if use_brightdata:
            self._proxy_url = _get_brightdata_proxy()
            if self._proxy_url:
                logger.debug("Cloudbeds API client using Brightdata proxy")
    
    def _get_client_kwargs(self) -> dict:
        """Get httpx client configuration."""
        kwargs = {
            "timeout": httpx.Timeout(self.timeout),
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://hotels.cloudbeds.com",
                "Referer": "https://hotels.cloudbeds.com/",
            },
            "follow_redirects": True,
        }
        if self._proxy_url:
            kwargs["proxy"] = self._proxy_url
        return kwargs
    
    async def extract(self, property_code: str) -> Optional[CloudbedsPropertyData]:
        """Extract property data from Cloudbeds property_info API.
        
        Args:
            property_code: The property code (e.g., "kypwgi")
            
        Returns:
            CloudbedsPropertyData or None if extraction fails
        """
        try:
            async with httpx.AsyncClient(**self._get_client_kwargs()) as client:
                # POST to property_info API
                response = await client.post(
                    self.API_URL,
                    data={
                        "booking_engine_source": "hosted",
                        "iframe": "false",
                        "lang": "en",
                        "property_code": property_code,
                    },
                )
                
                if response.status_code != 200:
                    logger.debug(f"Cloudbeds API returned {response.status_code} for {property_code}")
                    return None
                
                json_data = response.json()
                
                if not json_data.get("success"):
                    logger.debug(f"Cloudbeds API returned success=false for {property_code}")
                    return None
                
                data = json_data.get("data", {})
                
                # Extract hotel address
                hotel_address = data.get("hotel_address", {})
                
                # Parse lat/lng (they come as strings)
                lat = None
                lng = None
                if hotel_address.get("lat"):
                    try:
                        lat = float(hotel_address["lat"])
                    except (ValueError, TypeError):
                        pass
                if hotel_address.get("lng"):
                    try:
                        lng = float(hotel_address["lng"])
                    except (ValueError, TypeError):
                        pass
                
                # Build full address
                address_parts = []
                if hotel_address.get("address1"):
                    address_parts.append(hotel_address["address1"])
                if hotel_address.get("address2"):
                    address_parts.append(hotel_address["address2"])
                full_address = ", ".join(address_parts) if address_parts else None
                
                # Normalize country
                country_code = hotel_address.get("country") or data.get("hotel_address_country")
                country = normalize_country(country_code) if country_code else None
                
                # Build contact name from available fields
                contact_name = data.get("primary_name")
                if not contact_name:
                    first = data.get("contact_first_name", "")
                    last = data.get("contact_last_name", "")
                    if first or last:
                        contact_name = f"{first} {last}".strip()
                
                result = CloudbedsPropertyData(
                    property_code=property_code,
                    booking_url=f"https://hotels.cloudbeds.com/reservation/{property_code}",
                    name=data.get("hotel_name"),
                    address=full_address,
                    city=hotel_address.get("city"),
                    state=hotel_address.get("state") or None,  # Empty string -> None
                    country=country,
                    zip_code=hotel_address.get("zip") or None,
                    latitude=lat,
                    longitude=lng,
                    phone=data.get("hotel_phone"),
                    email=data.get("hotel_email"),
                    contact_name=contact_name or None,
                    formatted_address=data.get("formatted_address"),
                )
                
                if result.has_data():
                    loc_str = f" @ ({lat}, {lng})" if lat else ""
                    logger.debug(f"Cloudbeds API success: {result.name} | {result.city}, {result.country}{loc_str}")
                    return result
                
                return None
                
        except httpx.TimeoutException:
            logger.debug(f"Cloudbeds API timeout for {property_code}")
            return None
        except Exception as e:
            logger.debug(f"Cloudbeds API error for {property_code}: {e}")
            return None
    
    async def extract_from_url(self, url: str) -> Optional[CloudbedsPropertyData]:
        """Extract property data from a Cloudbeds booking URL.
        
        Args:
            url: Full Cloudbeds URL (e.g., "https://hotels.cloudbeds.com/reservation/kypwgi")
            
        Returns:
            CloudbedsPropertyData or None if extraction fails
        """
        property_code = extract_property_code(url)
        if not property_code:
            logger.debug(f"Could not extract property code from URL: {url}")
            return None
        return await self.extract(property_code)
    
    async def extract_from_title(self, property_code: str) -> Optional[CloudbedsPropertyData]:
        """Fallback: Extract data from page title for 404/error pages.
        
        Even when Cloudbeds shows an error page, the <title> tag often contains:
        "Hotel Name - City, Country - Best Price Guarantee"
        
        This is useful for dead URLs where the API returns no data.
        """
        try:
            url = f"https://hotels.cloudbeds.com/reservation/{property_code}"
            
            async with httpx.AsyncClient(**self._get_client_kwargs()) as client:
                response = await client.get(url)
                
                # Extract title
                title_match = re.search(r'<title>([^<]+)</title>', response.text, re.IGNORECASE)
                if not title_match:
                    return None
                
                title = title_match.group(1).strip()
                
                # Skip garbage titles
                if 'Soluções online' in title or title == 'Cloudbeds':
                    return None
                
                # Parse: "Hotel Name - City, Country - Best Price Guarantee"
                parsed = re.match(r'^(.+?) - (.+?), (.+?) - Best Price Guarantee$', title)
                if not parsed:
                    return None
                
                name, city, country = parsed.groups()
                
                # Normalize country
                if country == "United States of America":
                    country = "United States"
                
                return CloudbedsPropertyData(
                    property_code=property_code,
                    booking_url=url,
                    name=name.strip(),
                    city=city.strip(),
                    country=country.strip(),
                )
                
        except Exception as e:
            logger.debug(f"Title extraction failed for {property_code}: {e}")
            return None
    
    async def extract_with_fallback(self, property_code: str) -> Optional[CloudbedsPropertyData]:
        """Extract data, falling back to title extraction if API fails.
        
        1. Try property_info API (best data: address, lat/lng, phone, email)
        2. Fall back to title extraction (name, city, country only)
        """
        # Try API first
        result = await self.extract(property_code)
        if result and result.has_data():
            return result
        
        # Fall back to title extraction
        logger.debug(f"API failed for {property_code}, trying title extraction")
        return await self.extract_from_title(property_code)
