"""RMS URL Scanner.

Scans RMS booking engine IDs to find valid hotel URLs.
Uses OnlineApi/GetSearchOptions for fast scanning (~100ms per ID vs 15s with Playwright).

Supports automatic Brightdata fallback on rate limiting:
    async with RMSScanner(auto_brightdata=True) as scanner:
        # Starts direct, switches to Brightdata if blocked
        results = await scanner.scan_range(1, 50000)

Or force Brightdata from the start:
    async with RMSScanner(use_brightdata=True) as scanner:
        results = await scanner.scan_range(1, 50000)
"""

import asyncio
import os
import re
from typing import Optional, Protocol, runtime_checkable, Callable, Awaitable

import httpx
from loguru import logger

from lib.rms.models import ScannedURL


API_TIMEOUT = 20.0  # Increased for slow connections

# IBE servers to try for OnlineApi
IBE_SERVERS = [
    "ibe12.rmscloud.com",
    "ibe13.rmscloud.com",
    "ibe14.rmscloud.com",
]

# Rate limit detection thresholds
RATE_LIMIT_THRESHOLD = 10  # Consecutive failures before switching to Brightdata
RATE_LIMIT_WINDOW = 30  # Seconds to track failures


def _get_brightdata_proxy() -> Optional[str]:
    """Build Brightdata datacenter proxy URL if credentials are available.
    
    Only uses the datacenter zone (cheapest). No fallback to residential or unlocker.
    """
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    if not customer_id:
        return None
    
    dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
    dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
    if dc_zone and dc_password:
        username = f"brd-customer-{customer_id}-zone-{dc_zone}"
        return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
    
    return None
    return None


def _is_rate_limit_error(status_code: int, error: Optional[Exception] = None) -> bool:
    """Check if error indicates rate limiting."""
    # HTTP 429 = Too Many Requests
    if status_code == 429:
        return True
    # HTTP 403 = Forbidden (often used for IP blocking)
    if status_code == 403:
        return True
    # Connection errors often mean IP is blocked
    if error and isinstance(error, (httpx.ConnectError, httpx.ConnectTimeout)):
        return True
    return False


@runtime_checkable
class IRMSScanner(Protocol):
    """RMS Scanner interface."""
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]: ...


class RMSScanner:
    """Fast RMS ID scanner using OnlineApi (no browser needed).
    
    Usage:
        # Standard scanning (starts direct, auto-switches to Brightdata if blocked)
        async with RMSScanner(auto_brightdata=True) as scanner:
            results = await scanner.scan_range(1, 50000)
        
        # Force Brightdata from the start
        async with RMSScanner(use_brightdata=True) as scanner:
            results = await scanner.scan_range(1, 50000)
        
        # Direct only (no Brightdata fallback)
        async with RMSScanner() as scanner:
            results = await scanner.scan_range(1, 50000)
    """
    
    def __init__(
        self,
        concurrency: int = 20,
        delay: float = 0.1,
        timeout: float = API_TIMEOUT,
        use_brightdata: bool = False,
        auto_brightdata: bool = False,
    ):
        self.concurrency = concurrency
        self.delay = delay
        self.timeout = timeout
        self.use_brightdata = use_brightdata
        self.auto_brightdata = auto_brightdata
        self._client: Optional[httpx.AsyncClient] = None
        self._brightdata_client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._proxy_url: Optional[str] = None
        
        # Rate limit tracking
        self._consecutive_failures = 0
        self._using_brightdata = False
        self._switched_to_brightdata = False
    
    async def __aenter__(self):
        self._proxy_url = _get_brightdata_proxy()
        
        # Always create direct client first
        self._client = httpx.AsyncClient(timeout=self.timeout)
        
        # If force Brightdata, switch immediately
        if self.use_brightdata and self._proxy_url:
            logger.info("Using Brightdata proxy for RMS scanning (forced)")
            self._brightdata_client = httpx.AsyncClient(
                timeout=self.timeout,
                proxy=self._proxy_url,
                verify=False,
            )
            self._using_brightdata = True
        elif self.auto_brightdata and self._proxy_url:
            # Pre-create Brightdata client for quick switching
            logger.info("Auto-Brightdata enabled, will switch if rate limited")
            self._brightdata_client = httpx.AsyncClient(
                timeout=self.timeout,
                proxy=self._proxy_url,
                verify=False,
            )
        elif self.use_brightdata or self.auto_brightdata:
            logger.warning("Brightdata requested but credentials not found")
        
        self._semaphore = asyncio.Semaphore(self.concurrency)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
        if self._brightdata_client:
            await self._brightdata_client.aclose()
    
    def _get_active_client(self) -> httpx.AsyncClient:
        """Get the currently active HTTP client."""
        if self._using_brightdata and self._brightdata_client:
            return self._brightdata_client
        return self._client
    
    def _switch_to_brightdata(self):
        """Switch to Brightdata proxy after rate limiting detected."""
        if self._brightdata_client and not self._switched_to_brightdata:
            logger.warning(
                f"Rate limiting detected ({self._consecutive_failures} consecutive failures). "
                "Switching to Brightdata proxy..."
            )
            self._using_brightdata = True
            self._switched_to_brightdata = True
            self._consecutive_failures = 0
    
    def _record_success(self):
        """Record successful request."""
        self._consecutive_failures = 0
    
    def _record_failure(self, is_rate_limit: bool):
        """Record failed request, switch to Brightdata if threshold reached."""
        if is_rate_limit:
            self._consecutive_failures += 1
            if (self.auto_brightdata 
                and self._consecutive_failures >= RATE_LIMIT_THRESHOLD 
                and not self._using_brightdata):
                self._switch_to_brightdata()
    
    async def scan_id(self, id_num: int, retry_count: int = 0) -> Optional[ScannedURL]:
        """Scan a single ID using OnlineApi.
        
        Tries multiple IBE servers until one works.
        Auto-switches to Brightdata if rate limited (when auto_brightdata=True).
        Returns ScannedURL if valid property found, None otherwise.
        """
        slug = str(id_num)
        client = self._get_active_client()
        
        for server in IBE_SERVERS:
            try:
                resp = await client.get(
                    f"https://{server}/OnlineApi/GetSearchOptions",
                    params={"clientId": slug, "agentId": "90"},
                )
                
                # Check for rate limiting
                if _is_rate_limit_error(resp.status_code):
                    self._record_failure(is_rate_limit=True)
                    logger.debug(f"Rate limit response {resp.status_code} for {id_num} on {server}")
                    
                    # If we just switched to Brightdata, retry this ID
                    if self._switched_to_brightdata and retry_count < 3:
                        return await self.scan_id(id_num, retry_count + 1)
                    continue
                
                if resp.status_code == 200:
                    data = resp.json()
                    if not data:
                        continue
                    prop_opts = data.get("propertyOptions") or {}
                    name = prop_opts.get("propertyName")
                    
                    if name and len(name) > 2:
                        # Valid property found!
                        self._record_success()
                        return ScannedURL(
                            id_num=id_num,
                            url=f"https://bookings.rmscloud.com/Search/Index/{slug}/90/",
                            slug=slug,
                            subdomain="bookings",
                            name=name,
                            address=prop_opts.get("propertyAddress", "").strip() or None,
                            phone=prop_opts.get("propertyPhoneBH") or None,
                            email=prop_opts.get("propertyEmail") or None,
                        )
                    else:
                        # Valid response but no property - not a rate limit
                        self._record_success()
                        
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                # Connection errors often mean IP blocking
                self._record_failure(is_rate_limit=True)
                logger.debug(f"Connection error for {id_num} on {server}: {e}")
                
                # If we just switched to Brightdata, retry this ID
                if self._switched_to_brightdata and retry_count < 3:
                    return await self.scan_id(id_num, retry_count + 1)
                continue
                
            except Exception as e:
                # Other errors are not rate limits
                logger.debug(f"OnlineApi failed for {id_num} on {server}: {e}")
                continue
        
        return None
    
    async def _scan_with_semaphore(
        self,
        id_num: int,
        on_found: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> Optional[ScannedURL]:
        """Scan with rate limiting."""
        async with self._semaphore:
            if self.delay > 0:
                await asyncio.sleep(self.delay)
            
            result = await self.scan_id(id_num)
            
            if result and on_found:
                await on_found(result.to_dict())
            
            return result
    
    async def scan_range(
        self,
        start_id: int,
        end_id: int,
        subdomain: str = "bookings",  # Ignored, kept for compatibility
        on_found: Optional[Callable[[dict], Awaitable[None]]] = None,
        progress_interval: int = 100,
    ) -> list[dict]:
        """Scan a range of IDs.
        
        Args:
            start_id: First ID to scan
            end_id: Last ID to scan (inclusive)
            subdomain: Ignored (kept for API compatibility)
            on_found: Callback called for each found property
            progress_interval: How often to log progress
            
        Returns:
            List of found properties as dicts
        """
        total = end_id - start_id + 1
        found = []
        scanned = 0
        
        logger.info(f"Scanning {total} IDs ({start_id}-{end_id})")
        
        # Create tasks in batches to avoid memory issues
        batch_size = 1000
        
        for batch_start in range(start_id, end_id + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_id)
            
            tasks = [
                self._scan_with_semaphore(id_num, on_found)
                for id_num in range(batch_start, batch_end + 1)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                scanned += 1
                if isinstance(result, ScannedURL):
                    found.append(result.to_dict())
                
                if scanned % progress_interval == 0:
                    pct = (scanned / total) * 100
                    logger.info(f"Progress: {pct:.1f}% ({scanned}/{total}) - Found: {len(found)}")
        
        logger.info(f"Scan complete: {len(found)} properties found in {total} IDs")
        return found
    
    async def scan_all_subdomains(
        self,
        start_id: int,
        end_id: int,
        on_found: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> list[dict]:
        """Scan range - OnlineApi tries all servers automatically."""
        return await self.scan_range(start_id, end_id, on_found=on_found)


class PlaywrightRMSScanner:
    """Legacy Playwright-based scanner (slower, for fallback only)."""
    
    PAGE_TIMEOUT = 25000  # 25s for slow pages
    
    def __init__(self, page):
        self._page = page
    
    async def scan_id(self, id_num: int) -> Optional[ScannedURL]:
        """Scan an ID using bookings.rmscloud.com format."""
        url = f"https://bookings.rmscloud.com/Search/Index/{id_num}/90/"
        is_valid, hotel_name = await self.is_valid_page(url)
        if is_valid and hotel_name:
            return ScannedURL(
                id_num=id_num, 
                url=url, 
                slug=str(id_num), 
                subdomain="bookings",
                name=hotel_name,
            )
        return None
    
    async def is_valid_page(self, url: str) -> tuple[bool, Optional[str]]:
        """Check if URL returns a valid RMS booking page with hotel name."""
        try:
            response = await self._page.goto(url, timeout=self.PAGE_TIMEOUT, wait_until="networkidle")
            if not response or response.status >= 400:
                return False, None
            await asyncio.sleep(2)
            
            title = await self._page.title()
            if title == "Error":
                return False, None
            
            body_text = await self._page.evaluate("document.body.innerText")
            if not body_text or len(body_text) < 100:
                return False, None
            
            # First line is the hotel name
            first_line = body_text.split('\n')[0].strip()
            
            # Reject generic/garbage names
            garbage = ['cart', 'error', 'online bookings', '', 'book your accommodation',
                       'unhandled exception', 'processing the request', 'application issues']
            if first_line.lower() in garbage or any(g in first_line.lower() for g in garbage):
                return False, None
            
            return True, first_line
        except Exception:
            return False, None
