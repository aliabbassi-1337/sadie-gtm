"""Browser utilities for Playwright scraping.

Provides a managed browser pool for concurrent scraping.
"""

import asyncio
from typing import List, TypeVar, Generic, Callable, Awaitable, Any
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth
from pydantic import BaseModel


T = TypeVar('T')


class BrowserPool:
    """Manages a pool of browser contexts for concurrent scraping."""
    
    def __init__(
        self,
        concurrency: int = 3,
        headless: bool = True,
        user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    ):
        self.concurrency = concurrency
        self.headless = headless
        self.user_agent = user_agent
        self._browser: Browser = None
        self._contexts: List[BrowserContext] = []
        self._pages: List[Page] = []
    
    async def __aenter__(self):
        """Start browser and create context pool."""
        self._stealth = Stealth()
        self._playwright = await async_playwright().start()
        await self._stealth.apply_stealth_async(self._playwright)
        
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        
        for _ in range(self.concurrency):
            ctx = await self._browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 1280, "height": 800},
            )
            page = await ctx.new_page()
            self._contexts.append(ctx)
            self._pages.append(page)
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up browser resources."""
        for ctx in self._contexts:
            await ctx.close()
        await self._browser.close()
        await self._playwright.stop()
    
    @property
    def pages(self) -> List[Page]:
        """Get the list of pages for scraping."""
        return self._pages
    
    async def process_batch(
        self,
        items: List[Any],
        process_fn: Callable[[Page, Any], Awaitable[T]],
    ) -> List[T]:
        """Process a batch of items using the browser pool.
        
        Args:
            items: Items to process (will be chunked by concurrency)
            process_fn: Async function that takes (page, item) and returns result
            
        Returns:
            List of results from process_fn
        """
        all_results = []
        
        for batch_start in range(0, len(items), self.concurrency):
            batch = items[batch_start:batch_start + self.concurrency]
            
            tasks = [
                process_fn(self._pages[i], item)
                for i, item in enumerate(batch)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            all_results.extend(results)
        
        return all_results
