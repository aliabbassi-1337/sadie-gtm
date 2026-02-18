"""Shared proxy pool for HTTP request rotation.

Supports:
  - "direct" (no proxy)
  - BrightData DC/residential via env vars
  - Arbitrary HTTP proxy URLs via RMS_PROXY_URLS env var
  - Free public proxies from ProxyScrape (auto-tested)
  - Cloudflare Worker proxy via CF_WORKER_PROXY_URL env var ($5/mo for 10M requests)
"""

import asyncio
import itertools
import os
import random
from typing import Optional
from urllib.parse import urlparse

import httpx
from loguru import logger

PROXYSCRAPE_URL = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"


class ProxyPool:
    """Round-robin proxy rotation."""

    def __init__(self, mode: str = "auto"):
        self._proxies: list[Optional[str]] = []
        self._cycle = None
        self._build(mode)

    def _build(self, mode: str):
        if mode == "direct":
            self._proxies = [None]
        elif mode == "brightdata":
            self._proxies = self._build_brightdata()
            if not self._proxies:
                logger.warning("BrightData not configured, falling back to direct")
                self._proxies = [None]
        elif mode == "auto":
            bd = self._build_brightdata()
            self._proxies = [None] + bd
        elif mode == "proxy":
            urls = os.getenv("RMS_PROXY_URLS", "")
            if urls:
                self._proxies = [u.strip() for u in urls.split(",") if u.strip()]
            if not self._proxies:
                logger.warning("RMS_PROXY_URLS not set, falling back to direct")
                self._proxies = [None]
        elif mode == "free":
            self._proxies = [None]
        else:
            self._proxies = [None]

        self._cycle = itertools.cycle(self._proxies)
        proxy_names = [self._label(p) for p in self._proxies]
        logger.info(f"Proxy pool: {len(self._proxies)} proxies — {', '.join(proxy_names)}")

    async def init_free_proxies(
        self,
        test_url: str = "https://ibe12.rmscloud.com/OnlineApi/GetConnectionURLs",
        test_params: dict = None,
        test_count: int = 50,
        max_working: int = 10,
    ):
        """Fetch and test free proxies from ProxyScrape."""
        if test_params is None:
            test_params = {"clientId": "12383", "agentId": "1", "qs": "/12383/1"}

        logger.info("Fetching free proxies from ProxyScrape...")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(PROXYSCRAPE_URL)
                all_proxies = [line.strip() for line in resp.text.strip().split("\n") if line.strip()]
        except Exception as e:
            logger.warning(f"Failed to fetch proxy list: {e}")
            return

        if not all_proxies:
            logger.warning("No proxies returned from ProxyScrape")
            return

        random.shuffle(all_proxies)
        candidates = all_proxies[:test_count]
        logger.info(f"Testing {len(candidates)} proxy candidates...")

        working = []
        sem = asyncio.Semaphore(20)
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        async def test_proxy(proxy_addr: str) -> Optional[str]:
            proxy_url = f"http://{proxy_addr}"
            async with sem:
                try:
                    async with httpx.AsyncClient(proxy=proxy_url, verify=False, timeout=8.0) as c:
                        r = await c.get(test_url, params=test_params, headers=headers)
                        if r.status_code == 200:
                            return proxy_url
                except Exception:
                    pass
            return None

        results = await asyncio.gather(*[test_proxy(p) for p in candidates])
        working = [r for r in results if r is not None]

        if working:
            self._proxies = [None] + working[:max_working]
            self._cycle = itertools.cycle(self._proxies)
            proxy_names = [self._label(p) for p in self._proxies]
            logger.info(f"Proxy pool updated: {len(self._proxies)} proxies ({len(working)} free working) — {', '.join(proxy_names)}")
        else:
            logger.warning("No working free proxies found, using direct only")

    def _build_brightdata(self) -> list[Optional[str]]:
        """Build BrightData proxy URLs from env vars."""
        proxies = []
        customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
        if not customer_id:
            return proxies

        dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
        dc_pass = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
        if dc_zone and dc_pass:
            proxies.append(
                f"http://brd-customer-{customer_id}-zone-{dc_zone}:{dc_pass}@brd.superproxy.io:33335"
            )

        res_zone = os.getenv("BRIGHTDATA_RES_ZONE", "")
        res_pass = os.getenv("BRIGHTDATA_RES_PASSWORD", "")
        if res_zone and res_pass:
            proxies.append(
                f"http://brd-customer-{customer_id}-zone-{res_zone}:{res_pass}@brd.superproxy.io:22225"
            )

        return proxies

    @staticmethod
    def _label(proxy_url: Optional[str]) -> str:
        if proxy_url is None:
            return "direct"
        if "brd.superproxy.io:33335" in proxy_url:
            return "brightdata-dc"
        if "brd.superproxy.io:22225" in proxy_url:
            return "brightdata-res"
        try:
            parsed = urlparse(proxy_url)
            return f"{parsed.hostname}:{parsed.port}"
        except Exception:
            return "proxy"

    def next(self) -> Optional[str]:
        """Get next proxy URL (round-robin). None = direct."""
        return next(self._cycle)

    def create_client(self) -> httpx.AsyncClient:
        """Create an httpx.AsyncClient with the next proxy in rotation."""
        proxy_url = self.next()
        kwargs = {}
        if proxy_url:
            kwargs["proxy"] = proxy_url
            kwargs["verify"] = False
        return httpx.AsyncClient(**kwargs)


class CfWorkerProxy:
    """Cloudflare Worker proxy client.

    Routes requests through a CF Worker deployed on Cloudflare's edge network.
    $5/mo for 10M requests vs Brightdata residential at $8-12/GB.
    Datacenter IPs (not residential) - fine for WHOIS, hotel sites, gov APIs.

    Supports both HTML and JSON APIs (RDAP, crt.sh) via Accept header forwarding.

    Usage:
        proxy = CfWorkerProxy()
        async with httpx.AsyncClient() as client:
            html = await proxy.fetch(client, "https://example.com/about")
            data = await proxy.fetch_json(client, "https://rdap.org/domain/example.com")
    """

    def __init__(
        self,
        worker_url: Optional[str] = None,
        auth_key: Optional[str] = None,
    ):
        self.worker_url = worker_url or os.getenv("CF_WORKER_PROXY_URL", "")
        self.auth_key = auth_key or os.getenv("CF_WORKER_AUTH_KEY", "")

    @property
    def is_configured(self) -> bool:
        return bool(self.worker_url)

    async def fetch(
        self,
        client: httpx.AsyncClient,
        target_url: str,
        timeout: float = 20.0,
        accept: Optional[str] = None,
    ) -> Optional[str]:
        """Fetch a URL through the CF Worker proxy.

        Args:
            client: httpx async client
            target_url: URL to fetch
            timeout: Request timeout in seconds
            accept: Custom Accept header (e.g. "application/json" for JSON APIs)

        Returns text content or None on failure.
        """
        if not self.worker_url:
            logger.warning("CF Worker proxy not configured (set CF_WORKER_PROXY_URL)")
            return None

        headers = {}
        if self.auth_key:
            headers["X-Auth-Key"] = self.auth_key
        if accept:
            headers["X-Forward-Accept"] = accept

        try:
            resp = await client.get(
                self.worker_url,
                params={"url": target_url},
                headers=headers,
                timeout=timeout,
            )
            colo = resp.headers.get("X-Worker-Colo", "?")
            cache = resp.headers.get("X-Cache", "?")
            logger.debug(f"CF Worker [{colo}] {cache} {resp.status_code} {target_url}")

            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 429:
                logger.warning(f"CF Worker: target returned 429 for {target_url}")
            return None
        except Exception as e:
            logger.debug(f"CF Worker fetch failed: {e}")
            return None

    async def fetch_json(
        self,
        client: httpx.AsyncClient,
        target_url: str,
        timeout: float = 20.0,
    ) -> Optional[object]:
        """Fetch a JSON API through the CF Worker proxy.

        Returns parsed JSON (dict or list) or None on failure.
        """
        import json as _json

        text = await self.fetch(
            client, target_url, timeout=timeout,
            accept="application/json, application/rdap+json",
        )
        if not text:
            return None
        try:
            return _json.loads(text)
        except _json.JSONDecodeError as e:
            logger.debug(f"CF Worker JSON parse failed for {target_url}: {e}")
            return None

    async def fetch_with_fallback(
        self,
        client: httpx.AsyncClient,
        target_url: str,
        timeout: float = 20.0,
    ) -> Optional[str]:
        """Try CF Worker first, fall back to direct fetch."""
        if self.is_configured:
            result = await self.fetch(client, target_url, timeout)
            if result:
                return result

        # Direct fallback
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            }
            resp = await client.get(target_url, headers=headers, timeout=timeout, follow_redirects=True)
            if resp.status_code == 200:
                return resp.text
        except Exception:
            pass
        return None
