"""SiteMinder GraphQL API client for hotel data extraction.

Uses the direct-book.com GraphQL API to extract full hotel data:
- Name, website, description
- Address, city, state, country, postal code
- Latitude, longitude
- Email, phone
- Star rating, amenities

Two endpoints:
1. 'settings' - Fast, returns name/website/social (for name enrichment)
2. 'property' - Full data with address/contact/location (for location enrichment)

Usage:
    async with SiteMinderClient() as client:
        # Full property data (address, contact, location)
        data = await client.get_property_data("thehindsheaddirect")
        print(data.name, data.city, data.state, data.country)
    
    # With Brightdata proxy:
    async with SiteMinderClient(use_brightdata=True) as client:
        data = await client.get_property_data("thehindsheaddirect")
"""

import os
import re
from typing import Optional, List
from urllib.parse import urlparse, quote

import httpx
from loguru import logger
from pydantic import BaseModel


# GraphQL endpoint
API_URL = "https://direct-book.com/api/graphql"

# Persisted query hashes
SETTINGS_QUERY_HASH = "d1a3cdf28313be40aaa5cbaa30f99bfcc1b30e65683eec73e0a04cb786764e8c"
PROPERTY_QUERY_HASH = "c1266a16d8a7e6521600961d321a88e2b2f8348639fce22c270909112408cf45"

# Request timeout
API_TIMEOUT = 30.0


class SiteMinderHotelData(BaseModel):
    """Extracted hotel data from SiteMinder API."""
    name: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    # Location
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    postal_code: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    # Contact
    email: Optional[str] = None
    phone: Optional[str] = None
    # Property details
    star_rating: Optional[float] = None
    # Social media
    facebook: Optional[str] = None
    instagram: Optional[str] = None
    twitter: Optional[str] = None
    youtube: Optional[str] = None
    linkedin: Optional[str] = None
    # Internal IDs
    siteminder_property_id: Optional[str] = None
    timezone: Optional[str] = None
    
    @property
    def is_valid(self) -> bool:
        """Check if we got meaningful data."""
        return bool(self.name and self.name != "Unknown")
    
    @property
    def has_location(self) -> bool:
        """Check if we have location data."""
        return bool(self.city or self.state or self.country)


def extract_channel_code(booking_url: str) -> Optional[str]:
    """Extract the channel code (slug) from a SiteMinder booking URL.
    
    Examples:
        https://direct-book.com/properties/thehindsheaddirect -> thehindsheaddirect
        https://direct-book.com/properties/hotelxyz?lang=en -> hotelxyz
    """
    if not booking_url:
        return None
    
    # Parse URL
    parsed = urlparse(booking_url)
    path = parsed.path
    
    # Pattern: /properties/{channel_code}
    match = re.match(r'^/properties/([^/?]+)', path)
    if match:
        return match.group(1)
    
    return None


def _get_brightdata_proxy(prefer_cheap: bool = True) -> Optional[str]:
    """Build Brightdata proxy URL if credentials are available.
    
    Args:
        prefer_cheap: If True, prefer datacenter (~$0.11/GB) > residential (~$5.5/GB) > unlocker
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
    
    # Fall back to unlocker zone
    zone = os.getenv("BRIGHTDATA_ZONE", "")
    zone_password = os.getenv("BRIGHTDATA_ZONE_PASSWORD", "")
    
    if zone and zone_password:
        username = f"brd-customer-{customer_id}-zone-{zone}"
        return f"http://{username}:{zone_password}@brd.superproxy.io:33335"
    return None


class SiteMinderClient:
    """Async client for SiteMinder GraphQL API.
    
    Usage:
        async with SiteMinderClient() as client:
            # Full property data (address, contact, coords)
            data = await client.get_property_data("thehindsheaddirect")
            
            # Quick name/website only
            data = await client.get_hotel_data("thehindsheaddirect")
            
        # With Brightdata proxy (for bypassing CloudFront blocks):
        async with SiteMinderClient(use_brightdata=True) as client:
            data = await client.get_property_data("thehindsheaddirect")
    """
    
    def __init__(self, timeout: float = API_TIMEOUT, use_brightdata: bool = False):
        self.timeout = timeout
        self.use_brightdata = use_brightdata
        self._client: Optional[httpx.AsyncClient] = None
        self._proxy_url: Optional[str] = None
    
    async def __aenter__(self):
        # Configure proxy if requested and available
        if self.use_brightdata:
            self._proxy_url = _get_brightdata_proxy()
            if self._proxy_url:
                logger.debug("SiteMinder using Brightdata proxy")
                self._client = httpx.AsyncClient(
                    timeout=self.timeout,
                    proxy=self._proxy_url,
                    verify=False,  # Brightdata uses their own SSL cert
                )
            else:
                logger.warning("Brightdata requested but credentials not found, using direct connection")
                self._client = httpx.AsyncClient(timeout=self.timeout)
        else:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    def _get_headers(self, channel_code: str) -> dict:
        """Get headers that mimic a browser request."""
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": "https://direct-book.com",
            "Referer": f"https://direct-book.com/properties/{channel_code}",
        }
    
    async def get_property_data(self, channel_code: str) -> Optional[SiteMinderHotelData]:
        """Get FULL property data from SiteMinder (address, contact, location).
        
        Uses the 'property' GraphQL operation which returns:
        - name, website, description
        - address (line1, line2, city, state, postcode, country, lat/lon)
        - contact (email, phone)
        - star rating, amenities
        
        Args:
            channel_code: The property slug (e.g., "thehindsheaddirect")
        
        Returns:
            SiteMinderHotelData if successful, None if failed
        """
        if not self._client:
            raise RuntimeError("Client not initialized. Use 'async with' context manager.")
        
        if not channel_code:
            return None
        
        # Build GraphQL query URL with persisted query
        import json
        variables = quote(json.dumps({"channelCode": channel_code, "locale": "en"}))
        extensions = quote(json.dumps({
            "persistedQuery": {"version": 1, "sha256Hash": PROPERTY_QUERY_HASH}
        }))
        
        url = f"{API_URL}?operationName=property&variables={variables}&extensions={extensions}"
        
        try:
            resp = await self._client.get(
                url,
                headers=self._get_headers(channel_code),
            )
            
            if resp.status_code != 200:
                logger.debug(f"SiteMinder property API returned {resp.status_code} for {channel_code}")
                return None
            
            data = resp.json()
            
            if "errors" in data:
                logger.debug(f"SiteMinder property API error for {channel_code}: {data['errors']}")
                return None
            
            prop = data.get("data", {}).get("property")
            if not prop:
                return None
            
            return self._parse_property_response(prop)
            
        except httpx.TimeoutException:
            logger.debug(f"SiteMinder property API timeout for {channel_code}")
            return None
        except Exception as e:
            logger.debug(f"SiteMinder property API error for {channel_code}: {e}")
            return None
    
    async def get_property_data_from_url(self, booking_url: str) -> Optional[SiteMinderHotelData]:
        """Get full property data from a booking URL.
        
        Args:
            booking_url: Full booking URL (e.g., "https://direct-book.com/properties/xyz")
        
        Returns:
            SiteMinderHotelData if successful, None if failed
        """
        channel_code = extract_channel_code(booking_url)
        if not channel_code:
            logger.debug(f"Could not extract channel code from {booking_url}")
            return None
        
        return await self.get_property_data(channel_code)
    
    def _parse_property_response(self, prop: dict) -> SiteMinderHotelData:
        """Parse property API response into SiteMinderHotelData."""
        result = SiteMinderHotelData(
            name=prop.get("name"),
            website=prop.get("website") or None,
            description=prop.get("description"),
            star_rating=prop.get("starRating"),
        )
        
        # Address
        address = prop.get("address") or {}
        if address:
            line1 = address.get("addressLine1") or ""
            line2 = address.get("addressLine2") or ""
            if line1 and line2:
                result.address = f"{line1}, {line2}".strip(", ")
            else:
                result.address = (line1 or line2).strip() or None
            
            result.city = (address.get("city") or "").strip() or None
            result.state = (address.get("state") or "").strip() or None
            result.postal_code = (address.get("postcode") or "").strip() or None
            
            # Country is nested: {"name": "United Kingdom", "code": "GB"}
            country_data = address.get("country") or {}
            if isinstance(country_data, dict):
                result.country = country_data.get("name")
                result.country_code = country_data.get("code")
            elif isinstance(country_data, str):
                result.country = country_data
            
            # Coordinates
            try:
                lat = address.get("latitude")
                lon = address.get("longitude")
                if lat is not None:
                    result.lat = float(lat)
                if lon is not None:
                    result.lon = float(lon)
            except (ValueError, TypeError):
                pass
        
        # Contact
        contact = prop.get("contact") or {}
        if contact:
            result.email = contact.get("email") or None
            
            phones = contact.get("phone") or []
            if isinstance(phones, list) and phones:
                # Take the first phone number
                result.phone = phones[0] if phones[0] else None
            elif isinstance(phones, str):
                result.phone = phones or None
        
        return result
    
    async def get_hotel_data(self, channel_code: str) -> Optional[SiteMinderHotelData]:
        """Get basic hotel data from the SiteMinder 'settings' API.
        
        Faster but only returns name/website/social. Use get_property_data()
        for full address/contact/location.
        
        Args:
            channel_code: The property slug (e.g., "thehindsheaddirect")
        
        Returns:
            SiteMinderHotelData if successful, None if failed
        """
        if not self._client:
            raise RuntimeError("Client not initialized. Use 'async with' context manager.")
        
        if not channel_code:
            return None
        
        # Build GraphQL query URL with persisted query
        variables = quote(f'{{"channelCode":"{channel_code}"}}')
        extensions = quote(f'{{"persistedQuery":{{"version":1,"sha256Hash":"{SETTINGS_QUERY_HASH}"}}}}')
        
        url = f"{API_URL}?operationName=settings&variables={variables}&extensions={extensions}"
        
        try:
            resp = await self._client.get(
                url,
                headers=self._get_headers(channel_code),
            )
            
            if resp.status_code != 200:
                logger.debug(f"SiteMinder API returned {resp.status_code} for {channel_code}")
                return None
            
            data = resp.json()
            
            if "errors" in data:
                logger.debug(f"SiteMinder API error for {channel_code}: {data['errors']}")
                return None
            
            settings = data.get("data", {}).get("settings")
            if not settings:
                return None
            
            # Extract social media links
            social = settings.get("socialMedia", {}) or {}
            
            return SiteMinderHotelData(
                name=settings.get("name"),
                website=settings.get("propertyWebsite") or None,
                facebook=social.get("facebook_link"),
                instagram=social.get("instagram_link"),
                twitter=social.get("twitter_link"),
                youtube=social.get("youtube_link"),
                linkedin=social.get("linkedin_link"),
                siteminder_property_id=settings.get("siteminderPropertyId"),
                timezone=settings.get("timezoneoffset"),
            )
            
        except httpx.TimeoutException:
            logger.debug(f"SiteMinder API timeout for {channel_code}")
            return None
        except Exception as e:
            logger.debug(f"SiteMinder API error for {channel_code}: {e}")
            return None
    
    async def get_hotel_data_from_url(self, booking_url: str) -> Optional[SiteMinderHotelData]:
        """Get basic hotel data from a booking URL (name/website only).
        
        For full data use get_property_data_from_url() instead.
        """
        channel_code = extract_channel_code(booking_url)
        if not channel_code:
            logger.debug(f"Could not extract channel code from {booking_url}")
            return None
        
        return await self.get_hotel_data(channel_code)


async def test_client():
    """Quick test of the client."""
    async with SiteMinderClient() as client:
        # Test the full property endpoint
        data = await client.get_property_data("ushawhistorichousechapelsdirect")
        if data:
            print(f"Name: {data.name}")
            print(f"Website: {data.website}")
            print(f"Address: {data.address}")
            print(f"City: {data.city}")
            print(f"State: {data.state}")
            print(f"Country: {data.country} ({data.country_code})")
            print(f"Lat/Lon: {data.lat}, {data.lon}")
            print(f"Email: {data.email}")
            print(f"Phone: {data.phone}")
        else:
            print("Failed to get data")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_client())
