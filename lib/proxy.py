"""Shared proxy pool for HTTP request rotation.

Supports:
  - "direct" (no proxy)
  - BrightData DC/residential via env vars
  - Arbitrary HTTP proxy URLs via RMS_PROXY_URLS env var
  - Free public proxies from ProxyScrape (auto-tested)
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
