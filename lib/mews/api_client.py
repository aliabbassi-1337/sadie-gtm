"""Mews API Client for extracting hotel data.

Uses a hybrid approach:
1. Get session token via Playwright (once)
2. Use fast httpx API calls for all hotels
3. Refresh session when expired

The API endpoint is: https://api.mews.com/api/bookingEngine/v1/configurations/get
"""

import asyncio
import time
import httpx
from typing import Optional
from pydantic import BaseModel
from loguru import logger

# Rate limiting - Mews API is VERY strict, needs ~3 seconds between calls
_last_api_call = 0
_api_lock = None
API_RATE_LIMIT = 3.0  # 3 seconds between calls to avoid 429s


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


# Session token cache (shared across instances)
_session_cache = {
    "session": None,
    "client": None,
    "obtained_at": 0,
    "lock": None,
}

# Session expires after 30 minutes (conservative estimate)
SESSION_TTL = 30 * 60


class MewsApiClient:
    """Client for Mews booking engine API using hybrid Playwright + httpx approach."""
    
    BOOKING_URL_TEMPLATE = "https://app.mews.com/distributor/{slug}"
    API_URL = "https://api.mews.com/api/bookingEngine/v1/configurations/get"
    
    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None
        # For session refresh via Playwright
        self._browser = None
        self._playwright = None
    
    async def initialize(self):
        """Initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=self.timeout)
        if _session_cache["lock"] is None:
            _session_cache["lock"] = asyncio.Lock()
    
    async def close(self):
        """Close clients."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
    
    async def _get_session(self) -> tuple[Optional[str], Optional[str]]:
        """Get or refresh session token."""
        now = time.time()
        
        # Check if cached session is still valid
        if (_session_cache["session"] 
            and now - _session_cache["obtained_at"] < SESSION_TTL):
            return _session_cache["session"], _session_cache["client"]
        
        # Need to refresh - use lock to prevent concurrent refreshes
        async with _session_cache["lock"]:
            # Double-check after acquiring lock
            if (_session_cache["session"] 
                and now - _session_cache["obtained_at"] < SESSION_TTL):
                return _session_cache["session"], _session_cache["client"]
            
            logger.info("Refreshing Mews session token via Playwright...")
            session, client = await self._fetch_session_via_playwright()
            
            if session and client:
                _session_cache["session"] = session
                _session_cache["client"] = client
                _session_cache["obtained_at"] = now
                logger.info(f"Got new Mews session (client: {client})")
            
            return session, client
    
    async def _fetch_session_via_playwright(self) -> tuple[Optional[str], Optional[str]]:
        """Use Playwright to get a valid session token."""
        from playwright.async_api import async_playwright
        import json
        
        captured = {}
        
        try:
            if not self._playwright:
                self._playwright = await async_playwright().start()
            if not self._browser:
                self._browser = await self._playwright.chromium.launch(headless=True)
            
            context = await self._browser.new_context()
            page = await context.new_page()
            
            async def handle_route(route):
                request = route.request
                if "configurations/get" in request.url and request.post_data:
                    captured["body"] = request.post_data
                await route.continue_()
            
            await page.route("**/*", handle_route)
            
            # Use a known working hotel to get session
            await page.goto(
                "https://app.mews.com/distributor/cb6072cc-1e03-45cc-a6e8-ab0d00ea7979",
                wait_until="commit",
                timeout=45000,
            )
            
            # Wait for API call
            for _ in range(40):
                if captured.get("body"):
                    break
                await asyncio.sleep(0.5)
            
            await page.close()
            await context.close()
            
            if captured.get("body"):
                data = json.loads(captured["body"])
                return data.get("session"), data.get("client")
            
        except Exception as e:
            logger.warning(f"Failed to get Mews session: {e}")
        
        return None, None
    
    async def extract(self, slug: str) -> Optional[MewsHotelData]:
        """
        Extract hotel data from Mews using fast API call.
        
        Args:
            slug: The Mews enterprise UUID
        
        Returns:
            MewsHotelData if successful, None if failed
        """
        await self.initialize()
        
        try:
            # Try API call - returns (data, needs_session_refresh)
            data, needs_refresh = await self._fetch_via_api(slug)
            
            if data:
                return self._parse_response(slug, data)
            
            # Only retry with new session if we got a session error
            if needs_refresh:
                _session_cache["obtained_at"] = 0
                data, _ = await self._fetch_via_api(slug)
                if data:
                    return self._parse_response(slug, data)
            
            return None
            
        except Exception as e:
            logger.debug(f"Mews extraction error for {slug}: {e}")
            return None
    
    async def _fetch_via_api(self, slug: str, retry_count: int = 0) -> tuple[Optional[dict], bool]:
        """Fetch data via direct API call with rate limiting.
        
        Returns:
            Tuple of (data, needs_session_refresh). If data is None and needs_refresh
            is True, caller should refresh session and retry.
        """
        global _last_api_call, _api_lock
        
        if _api_lock is None:
            _api_lock = asyncio.Lock()
        
        session, client = await self._get_session()
        
        if not session or not client:
            logger.warning("No Mews session available")
            return None, True  # No session - need refresh
        
        # Rate limiting - wait between API calls
        async with _api_lock:
            now = time.time()
            wait_time = API_RATE_LIMIT - (now - _last_api_call)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            _last_api_call = time.time()
        
        payload = {
            "ids": [slug],
            "primaryId": slug,
            "client": client,
            "session": session,
        }
        
        try:
            resp = await self._http_client.post(
                self.API_URL,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Origin": "https://app.mews.com",
                    "Referer": "https://app.mews.com/",
                },
            )
            
            if resp.status_code == 200:
                return resp.json(), False
            elif resp.status_code == 429:
                # Rate limited - exponential backoff and retry
                if retry_count < 3:
                    wait = (retry_count + 1) * 5  # 5s, 10s, 15s
                    logger.debug(f"Rate limited, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    return await self._fetch_via_api(slug, retry_count + 1)
                logger.warning(f"Mews rate limit exceeded for {slug} after 3 retries")
                return None, False  # Rate limit - don't refresh session
            elif resp.status_code == 400:
                text = resp.text
                if "session" in text.lower():
                    # Session expired, need refresh
                    logger.debug(f"Mews session expired for {slug}")
                    return None, True
                # Invalid hotel ID - don't retry or refresh
                logger.debug(f"Invalid Mews ID {slug}: {text[:50]}")
                return None, False
            else:
                logger.debug(f"Mews API error for {slug}: HTTP {resp.status_code}")
                return None, False
                
        except Exception as e:
            logger.debug(f"Mews API request failed for {slug}: {e}")
            return None, False
    
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


async def extract_mews_hotel(slug: str, timeout: float = 45.0) -> Optional[MewsHotelData]:
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
