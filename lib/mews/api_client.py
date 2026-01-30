"""Mews API Client for extracting hotel data.

Uses Playwright to load the Mews distributor page and intercept the
configurations/get API call to get property details.

The API requires a valid session token that is generated client-side,
so we need to use a browser to make the request.
"""

import asyncio
from typing import Optional
from pydantic import BaseModel
from loguru import logger


class MewsHotelData(BaseModel):
    """Extracted hotel data from Mews API."""
    
    slug: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    chain_name: Optional[str] = None
    website: Optional[str] = None
    booking_url: str = ""
    
    @property
    def is_valid(self) -> bool:
        """Check if we got meaningful data."""
        return bool(self.name and self.name != "Unknown")


class MewsApiClient:
    """Client for Mews booking engine API using Playwright."""
    
    BOOKING_URL_TEMPLATE = "https://app.mews.com/distributor/{slug}"
    
    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout
        self._browser = None
        self._context = None
        self._playwright = None
    
    async def initialize(self):
        """Initialize browser for API calls."""
        if self._browser:
            return
        
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self._context = await self._browser.new_context()
    
    async def close(self):
        """Close browser."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
    
    async def extract(self, slug: str) -> Optional[MewsHotelData]:
        """
        Extract hotel data from Mews by loading the distributor page.
        
        Args:
            slug: The Mews enterprise UUID (e.g., "6c059d24-e493-4c6b-aa0d-a005b1e64356")
        
        Returns:
            MewsHotelData if successful, None if failed
        """
        await self.initialize()
        
        try:
            data = await self._fetch_via_browser(slug)
            if not data:
                return None
            
            return self._parse_response(slug, data)
            
        except Exception as e:
            logger.debug(f"Mews extraction error for {slug}: {e}")
            return None
    
    async def _fetch_via_browser(self, slug: str) -> Optional[dict]:
        """Load distributor page and capture API response."""
        captured_data = {}
        
        page = await self._context.new_page()
        
        try:
            async def handle_response(response):
                if "configurations/get" in response.url:
                    try:
                        data = await response.json()
                        captured_data["config"] = data
                    except Exception:
                        pass
            
            page.on("response", handle_response)
            
            url = self.BOOKING_URL_TEMPLATE.format(slug=slug)
            await page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            
            # Wait for API response
            for _ in range(10):
                if "config" in captured_data:
                    break
                await asyncio.sleep(0.5)
            
            return captured_data.get("config")
            
        finally:
            await page.close()
    
    def _parse_response(self, slug: str, data: dict) -> MewsHotelData:
        """Parse Mews API response into hotel data."""
        result = MewsHotelData(
            slug=slug,
            booking_url=self.BOOKING_URL_TEMPLATE.format(slug=slug),
        )
        
        # Get enterprise (property) data - keys can be camelCase or PascalCase
        enterprises = data.get("enterprises") or data.get("Enterprises", [])
        if enterprises:
            enterprise = enterprises[0]
            
            # Name - can be dict with language codes
            name = enterprise.get("name") or enterprise.get("Name")
            if isinstance(name, dict):
                # Prefer English, fall back to first available
                result.name = name.get("en-US") or name.get("en-GB") or next(iter(name.values()), None)
            elif isinstance(name, str):
                result.name = name
            
            # Address
            address = enterprise.get("address") or enterprise.get("Address", {})
            if address:
                line1 = address.get("line1") or address.get("Line1")
                line2 = address.get("line2") or address.get("Line2")
                result.address = line1
                if line2:
                    result.address = f"{result.address}, {line2}"
                result.city = address.get("city") or address.get("City")
                result.postal_code = address.get("postalCode") or address.get("PostalCode")
                result.country = address.get("countryCode") or address.get("CountryCode")
                result.lat = address.get("latitude") or address.get("Latitude")
                result.lon = address.get("longitude") or address.get("Longitude")
            
            # Contact info
            result.email = enterprise.get("email") or enterprise.get("Email")
            result.phone = enterprise.get("telephone") or enterprise.get("Telephone")
        
        # Get chain name
        chains = data.get("chains") or data.get("Chains", [])
        if chains:
            result.chain_name = chains[0].get("name") or chains[0].get("Name")
            # If no enterprise name, use chain name
            if not result.name and result.chain_name:
                result.name = result.chain_name
        
        return result


async def extract_mews_hotel(slug: str, timeout: float = 30.0) -> Optional[MewsHotelData]:
    """
    Convenience function to extract hotel data from Mews.
    
    Args:
        slug: Mews enterprise UUID
        timeout: Request timeout in seconds
    
    Returns:
        MewsHotelData if successful, None if failed
    """
    client = MewsApiClient(timeout=timeout)
    return await client.extract(slug)
