"""SiteMinder GraphQL API client for hotel data extraction.

Uses the direct-book.com GraphQL API to extract hotel data without browser rendering.
Much faster than Playwright (~100ms vs 10s per hotel).

Usage:
    async with SiteMinderClient() as client:
        data = await client.get_hotel_data("thehindsheaddirect")
        print(data.name, data.website)
    
    # With Brightdata proxy (for bypassing CloudFront blocks):
    async with SiteMinderClient(use_brightdata=True) as client:
        data = await client.get_hotel_data("thehindsheaddirect")
"""

import os
import re
from typing import Optional
from urllib.parse import urlparse, quote

import httpx
from loguru import logger
from pydantic import BaseModel


# GraphQL endpoint
API_URL = "https://direct-book.com/api/graphql"

# Persisted query hash for 'settings' operation
SETTINGS_QUERY_HASH = "d1a3cdf28313be40aaa5cbaa30f99bfcc1b30e65683eec73e0a04cb786764e8c"

# Request timeout
API_TIMEOUT = 30.0


class SiteMinderHotelData(BaseModel):
    """Extracted hotel data from SiteMinder API."""
    name: Optional[str] = None
    website: Optional[str] = None
    facebook: Optional[str] = None
    instagram: Optional[str] = None
    twitter: Optional[str] = None
    youtube: Optional[str] = None
    linkedin: Optional[str] = None
    siteminder_property_id: Optional[str] = None
    timezone: Optional[str] = None
    # We can extract more fields if needed


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
            data = await client.get_hotel_data("thehindsheaddirect")
            
        # With Brightdata proxy (for bypassing CloudFront blocks):
        async with SiteMinderClient(use_brightdata=True) as client:
            data = await client.get_hotel_data("thehindsheaddirect")
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
    
    async def get_hotel_data(self, channel_code: str) -> Optional[SiteMinderHotelData]:
        """Get hotel data from the SiteMinder GraphQL API.
        
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
        """Get hotel data from a booking URL.
        
        Args:
            booking_url: Full booking URL (e.g., "https://direct-book.com/properties/xyz")
        
        Returns:
            SiteMinderHotelData if successful, None if failed
        """
        channel_code = extract_channel_code(booking_url)
        if not channel_code:
            logger.debug(f"Could not extract channel code from {booking_url}")
            return None
        
        return await self.get_hotel_data(channel_code)


async def test_client():
    """Quick test of the client."""
    async with SiteMinderClient() as client:
        data = await client.get_hotel_data("thehindsheaddirect")
        if data:
            print(f"Name: {data.name}")
            print(f"Website: {data.website}")
            print(f"Property ID: {data.siteminder_property_id}")
        else:
            print("Failed to get data")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_client())
