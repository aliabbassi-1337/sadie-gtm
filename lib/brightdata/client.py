"""Brightdata API client for web scraping.

Supports two modes:
1. Direct API access (Unlocker API) - POST to api.brightdata.com/request
2. Proxy-based access - Route requests through Brightdata proxy

Environment variables:
    BRIGHTDATA_API_KEY: API key for authentication
    BRIGHTDATA_ZONE: Zone name (required for API access)
    BRIGHTDATA_ZONE_PASSWORD: Zone password (for proxy access)
    BRIGHTDATA_CUSTOMER_ID: Customer ID (for proxy access)

Usage:
    # Direct API access
    async with BrightdataClient(zone="my_zone") as client:
        html = await client.fetch("https://example.com")
    
    # Proxy-based access with httpx
    proxy_url = get_proxy_url()
    async with httpx.AsyncClient(proxy=proxy_url) as client:
        resp = await client.get("https://example.com")
"""

import os
from typing import Optional, Literal

import httpx
from loguru import logger


# API endpoint for direct access
BRIGHTDATA_API_URL = "https://api.brightdata.com/request"

# Proxy endpoint for native access
BRIGHTDATA_PROXY_HOST = "brd.superproxy.io"
BRIGHTDATA_PROXY_PORT = 33335


def get_api_key() -> str:
    """Get Brightdata API key from environment."""
    key = os.getenv("BRIGHTDATA_API_KEY", "")
    if not key:
        raise ValueError("BRIGHTDATA_API_KEY environment variable not set")
    return key


def get_proxy_url(
    zone: Optional[str] = None,
    zone_password: Optional[str] = None,
    customer_id: Optional[str] = None,
    country: Optional[str] = None,
) -> str:
    """Build Brightdata datacenter proxy URL for httpx/aiohttp.
    
    Only uses the datacenter zone (cheapest). No fallback to residential or unlocker.
    
    Args:
        zone: Zone name (default: BRIGHTDATA_DC_ZONE)
        zone_password: Zone password (default: BRIGHTDATA_DC_PASSWORD)
        customer_id: Customer ID (default: BRIGHTDATA_CUSTOMER_ID env var)
        country: Optional country code for geo-targeting (e.g., "us", "gb")
    
    Returns:
        Proxy URL in format: http://user:pass@host:port
    
    Usage with httpx:
        proxy_url = get_proxy_url()
        async with httpx.AsyncClient(proxy=proxy_url) as client:
            resp = await client.get("https://example.com")
    """
    customer_id = customer_id or os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    
    if zone is None:
        zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
        zone_password = zone_password or os.getenv("BRIGHTDATA_DC_PASSWORD", "")
    
    if not all([zone, zone_password, customer_id]):
        raise ValueError(
            "Missing Brightdata proxy credentials. Set BRIGHTDATA_DC_ZONE/BRIGHTDATA_DC_PASSWORD "
            "and BRIGHTDATA_CUSTOMER_ID."
        )
    
    # Build username with optional country targeting
    username = f"brd-customer-{customer_id}-zone-{zone}"
    if country:
        username += f"-country-{country}"
    
    return f"http://{username}:{zone_password}@{BRIGHTDATA_PROXY_HOST}:{BRIGHTDATA_PROXY_PORT}"


class BrightdataClient:
    """Async client for Brightdata Unlocker API.
    
    Uses the direct API access method (POST to api.brightdata.com/request).
    This is the recommended method for most use cases.
    
    Args:
        zone: Brightdata zone name
        api_key: API key (default: from BRIGHTDATA_API_KEY env var)
        timeout: Request timeout in seconds
        format: Response format ("raw" for HTML, "json" for parsed JSON)
    
    Usage:
        async with BrightdataClient(zone="my_zone") as client:
            html = await client.fetch("https://example.com")
            
            # With JSON parsing
            data = await client.fetch_json("https://api.example.com/data")
    """
    
    def __init__(
        self,
        zone: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = 60.0,
        format: Literal["raw", "json"] = "raw",
    ):
        self.zone = zone or os.getenv("BRIGHTDATA_ZONE", "")
        self.api_key = api_key or get_api_key()
        self.timeout = timeout
        self.format = format
        self._client: Optional[httpx.AsyncClient] = None
        
        if not self.zone:
            raise ValueError(
                "Brightdata zone required. Pass zone= or set BRIGHTDATA_ZONE env var."
            )
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    async def fetch(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[dict] = None,
        body: Optional[str] = None,
        format: Optional[Literal["raw", "json"]] = None,
    ) -> str:
        """Fetch a URL through Brightdata Unlocker API.
        
        Args:
            url: Target URL to fetch
            method: HTTP method (GET, POST, etc.)
            headers: Optional headers to send to target
            body: Optional request body for POST requests
            format: Response format override
        
        Returns:
            Response content (HTML or JSON string)
        
        Raises:
            httpx.HTTPError: On request failure
        """
        if not self._client:
            raise RuntimeError("Client not initialized. Use 'async with' context manager.")
        
        payload = {
            "zone": self.zone,
            "url": url,
            "format": format or self.format,
        }
        
        if method != "GET":
            payload["method"] = method
        
        if headers:
            payload["headers"] = headers
        
        if body:
            payload["body"] = body
        
        resp = await self._client.post(
            BRIGHTDATA_API_URL,
            json=payload,
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        
        if resp.status_code != 200:
            logger.warning(f"Brightdata request failed: {resp.status_code} - {resp.text[:200]}")
            resp.raise_for_status()
        
        return resp.text
    
    async def fetch_json(self, url: str, **kwargs) -> dict:
        """Fetch a URL and parse as JSON.
        
        Convenience method that sets format="json" and parses response.
        """
        import json
        content = await self.fetch(url, format="json", **kwargs)
        return json.loads(content)


class BrightdataProxyClient:
    """Async HTTP client that routes all requests through Brightdata proxy.
    
    Use this when you need more control over requests or when using
    libraries that don't support the direct API.
    
    Args:
        zone: Brightdata zone name
        zone_password: Zone password
        customer_id: Customer ID
        country: Optional country code for geo-targeting
        timeout: Request timeout in seconds
    
    Usage:
        async with BrightdataProxyClient() as client:
            resp = await client.get("https://example.com")
            html = resp.text
    """
    
    def __init__(
        self,
        zone: Optional[str] = None,
        zone_password: Optional[str] = None,
        customer_id: Optional[str] = None,
        country: Optional[str] = None,
        timeout: float = 60.0,
    ):
        self.proxy_url = get_proxy_url(
            zone=zone,
            zone_password=zone_password,
            customer_id=customer_id,
            country=country,
        )
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            proxy=self.proxy_url,
            timeout=self.timeout,
            verify=False,  # Brightdata uses their own SSL cert
        )
        return self._client
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
