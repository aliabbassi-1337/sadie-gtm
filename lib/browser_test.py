"""Tests for Browser utilities."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from lib.browser import BrowserPool


class TestBrowserPool:
    """Tests for BrowserPool class."""
    
    @pytest.mark.asyncio
    async def test_creates_specified_number_of_contexts(self):
        """Should create the specified number of browser contexts."""
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_browser = AsyncMock()
            mock_context = AsyncMock()
            mock_page = AsyncMock()
            
            mock_playwright.return_value.start = AsyncMock(return_value=MagicMock(
                chromium=MagicMock(launch=AsyncMock(return_value=mock_browser))
            ))
            mock_browser.new_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=5)
                await pool.__aenter__()
                
                assert mock_browser.new_context.call_count == 5
                assert len(pool.pages) == 5
                
                await pool.__aexit__(None, None, None)
    
    @pytest.mark.asyncio
    async def test_pages_property_returns_pages(self):
        """Should return list of pages via pages property."""
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_browser = AsyncMock()
            mock_context = AsyncMock()
            mock_page = AsyncMock()
            
            mock_playwright.return_value.start = AsyncMock(return_value=MagicMock(
                chromium=MagicMock(launch=AsyncMock(return_value=mock_browser))
            ))
            mock_browser.new_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=3)
                await pool.__aenter__()
                
                pages = pool.pages
                assert len(pages) == 3
                
                await pool.__aexit__(None, None, None)


class TestBrowserPoolProcessBatch:
    """Tests for BrowserPool.process_batch() method."""
    
    @pytest.mark.asyncio
    async def test_processes_items_in_batches(self):
        """Should process items in batches of concurrency size."""
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_browser = AsyncMock()
            mock_context = AsyncMock()
            mock_page = AsyncMock()
            
            mock_playwright.return_value.start = AsyncMock(return_value=MagicMock(
                chromium=MagicMock(launch=AsyncMock(return_value=mock_browser))
            ))
            mock_browser.new_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=2)
                await pool.__aenter__()
                
                items = [1, 2, 3, 4, 5]
                results = []
                
                async def process_fn(page, item):
                    return item * 2
                
                results = await pool.process_batch(items, process_fn)
                
                assert results == [2, 4, 6, 8, 10]
                
                await pool.__aexit__(None, None, None)
    
    @pytest.mark.asyncio
    async def test_handles_exceptions_in_process_fn(self):
        """Should capture exceptions from process_fn in results."""
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_browser = AsyncMock()
            mock_context = AsyncMock()
            mock_page = AsyncMock()
            
            mock_playwright.return_value.start = AsyncMock(return_value=MagicMock(
                chromium=MagicMock(launch=AsyncMock(return_value=mock_browser))
            ))
            mock_browser.new_context = AsyncMock(return_value=mock_context)
            mock_context.new_page = AsyncMock(return_value=mock_page)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=2)
                await pool.__aenter__()
                
                items = [1, 2, 3]
                
                async def process_fn(page, item):
                    if item == 2:
                        raise ValueError("Test error")
                    return item * 2
                
                results = await pool.process_batch(items, process_fn)
                
                assert results[0] == 2
                assert isinstance(results[1], ValueError)
                assert results[2] == 6
                
                await pool.__aexit__(None, None, None)


@pytest.mark.online
class TestBrowserPoolIntegration:
    """Integration tests with real browser."""
    
    @pytest.mark.asyncio
    async def test_real_browser_pool(self):
        """Should create real browser pool and process items."""
        async with BrowserPool(concurrency=2) as pool:
            assert len(pool.pages) == 2
            
            async def get_title(page, url):
                await page.goto(url, timeout=10000)
                return await page.title()
            
            # Use a simple, fast-loading page
            results = await pool.process_batch(
                ["https://example.com"],
                get_title
            )
            
            assert len(results) == 1
            assert "Example" in results[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
