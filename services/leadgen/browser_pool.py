"""Persistent Chromium browser pool for Fargate detection workers.

Manages a single browser instance with a fixed pool of reusable contexts.
Designed for long-lived workers where launching a new browser per batch
is wasteful.
"""

import asyncio
from typing import Optional

from loguru import logger
from playwright.async_api import async_playwright, Browser, BrowserContext, Playwright

from services.leadgen.detector import get_random_user_agent


class BrowserPool:
    """Persistent Chromium browser with a pool of reusable contexts.

    Usage:
        pool = BrowserPool(pool_size=10)
        await pool.start()

        ctx = await pool.acquire_context()
        page = await ctx.new_page()
        # ... use page ...
        await page.close()
        await pool.release_context(ctx)

        await pool.close()
    """

    def __init__(self, pool_size: int = 10, headless: bool = True):
        self._pool_size = pool_size
        self._headless = headless
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._contexts: list[BrowserContext] = []
        self._context_queue: asyncio.Queue[BrowserContext] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._generation: int = 0
        self._closed = False

    @property
    def browser(self) -> Browser:
        return self._browser

    @property
    def generation(self) -> int:
        """Increments on each browser restart. Callers use this to avoid
        redundant restarts when multiple coroutines detect the same crash."""
        return self._generation

    @property
    def pool_size(self) -> int:
        return self._pool_size

    async def start(self) -> None:
        """Launch browser and create context pool."""
        self._playwright = await async_playwright().start()
        await self._launch_browser()
        logger.info(f"BrowserPool started: pool_size={self._pool_size}")

    async def _launch_browser(self) -> None:
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-sync",
                "--disable-translate",
                "--metrics-recording-only",
                "--mute-audio",
                "--no-first-run",
                "--safebrowsing-disable-auto-update",
            ],
        )

        self._contexts = []
        self._context_queue = asyncio.Queue()

        for _ in range(self._pool_size):
            ctx = await self._browser.new_context(
                user_agent=get_random_user_agent(),
                ignore_https_errors=True,
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                },
            )
            self._contexts.append(ctx)
            await self._context_queue.put(ctx)

        self._generation += 1

    async def acquire_context(self) -> BrowserContext:
        """Borrow a context from the pool. Blocks if all in use."""
        return await self._context_queue.get()

    async def release_context(self, ctx: BrowserContext) -> None:
        """Return a context to the pool. Closes any leftover pages."""
        for page in ctx.pages:
            try:
                await page.close()
            except Exception:
                pass
        await self._context_queue.put(ctx)

    async def restart_browser(self) -> None:
        """Restart browser after crash. Lock prevents thundering herd."""
        async with self._lock:
            logger.warning("Restarting browser...")
            for ctx in self._contexts:
                try:
                    await ctx.close()
                except Exception:
                    pass
            try:
                await self._browser.close()
            except Exception:
                pass

            await self._launch_browser()
            logger.info("Browser restarted successfully")

    async def close(self) -> None:
        """Shutdown browser and Playwright."""
        self._closed = True
        for ctx in self._contexts:
            try:
                await ctx.close()
            except Exception:
                pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        logger.info("BrowserPool closed")
