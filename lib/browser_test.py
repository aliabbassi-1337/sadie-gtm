"""Tests for Browser utilities."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from lib.browser import BrowserPool


class TestBrowserPool:
    """Tests for BrowserPool class."""
    
    @pytest.fixture
    def mock_playwright_setup(self):
        """Create properly mocked playwright setup."""
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_pw_instance = AsyncMock()
        
        mock_pw_instance.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_pw_instance.stop = AsyncMock()
        
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_browser.close = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()
        
        return mock_pw_instance, mock_browser, mock_context, mock_page
    
    @pytest.mark.asyncio
    async def test_creates_specified_number_of_contexts(self, mock_playwright_setup):
        """Should create the specified number of browser contexts."""
        mock_pw_instance, mock_browser, mock_context, mock_page = mock_playwright_setup
        
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_playwright.return_value.start = AsyncMock(return_value=mock_pw_instance)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=5)
                await pool.__aenter__()
                
                assert mock_browser.new_context.call_count == 5
                assert len(pool.pages) == 5
                
                await pool.__aexit__(None, None, None)
    
    @pytest.mark.asyncio
    async def test_pages_property_returns_pages(self, mock_playwright_setup):
        """Should return list of pages via pages property."""
        mock_pw_instance, mock_browser, mock_context, mock_page = mock_playwright_setup
        
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_playwright.return_value.start = AsyncMock(return_value=mock_pw_instance)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=3)
                await pool.__aenter__()
                
                pages = pool.pages
                assert len(pages) == 3
                
                await pool.__aexit__(None, None, None)


class TestBrowserPoolProcessBatch:
    """Tests for BrowserPool.process_batch() method."""
    
    @pytest.fixture
    def mock_playwright_setup(self):
        """Create properly mocked playwright setup."""
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_pw_instance = AsyncMock()
        
        mock_pw_instance.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_pw_instance.stop = AsyncMock()
        
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_browser.close = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_context.close = AsyncMock()
        
        return mock_pw_instance, mock_browser, mock_context, mock_page
    
    @pytest.mark.asyncio
    async def test_processes_items_in_batches(self, mock_playwright_setup):
        """Should process items in batches of concurrency size."""
        mock_pw_instance, mock_browser, mock_context, mock_page = mock_playwright_setup
        
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_playwright.return_value.start = AsyncMock(return_value=mock_pw_instance)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=2)
                await pool.__aenter__()
                
                items = [1, 2, 3, 4, 5]
                
                async def process_fn(page, item):
                    return item * 2
                
                # Use delay=0 for fast tests
                results = await pool.process_batch(items, process_fn, delay_between_batches=0)
                
                assert results == [2, 4, 6, 8, 10]
                
                await pool.__aexit__(None, None, None)
    
    @pytest.mark.asyncio
    async def test_handles_exceptions_in_process_fn(self, mock_playwright_setup):
        """Should capture exceptions from process_fn in results."""
        mock_pw_instance, mock_browser, mock_context, mock_page = mock_playwright_setup
        
        with patch('lib.browser.async_playwright') as mock_playwright:
            mock_playwright.return_value.start = AsyncMock(return_value=mock_pw_instance)
            
            with patch('lib.browser.Stealth') as mock_stealth:
                mock_stealth.return_value.apply_stealth_async = AsyncMock()
                
                pool = BrowserPool(concurrency=2)
                await pool.__aenter__()
                
                items = [1, 2, 3]
                
                async def process_fn(page, item):
                    if item == 2:
                        raise ValueError("Test error")
                    return item * 2
                
                results = await pool.process_batch(items, process_fn, delay_between_batches=0)
                
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
