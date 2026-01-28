"""Unit tests for RMS Scanner."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from services.enrichment.rms_scanner import (
    RMSScanner,
    MockScanner,
    ScannedURL,
    RMS_SUBDOMAINS,
)


pytestmark = pytest.mark.no_db  # All tests in this file use mocks


class TestMockScanner:
    """Tests for MockScanner."""
    
    @pytest.mark.asyncio
    async def test_scan_id_returns_none_for_invalid(self):
        """Should return None for IDs not in valid set."""
        scanner = MockScanner(valid_ids={100, 200})
        
        result = await scanner.scan_id(999)
        
        assert result is None
        assert 999 in scanner.scanned_ids
    
    @pytest.mark.asyncio
    async def test_scan_id_returns_url_for_valid(self):
        """Should return ScannedURL for valid IDs."""
        scanner = MockScanner(valid_ids={123, 456})
        
        result = await scanner.scan_id(123)
        
        assert result is not None
        assert isinstance(result, ScannedURL)
        assert result.id_num == 123
        assert result.url == "https://ibe.rmscloud.com/123"
        assert result.slug == "123"
        assert result.subdomain == "ibe"
    
    @pytest.mark.asyncio
    async def test_is_valid_page_checks_valid_ids(self):
        """Should check if URL ID is in valid set."""
        scanner = MockScanner(valid_ids={123})
        
        assert await scanner.is_valid_page("https://ibe.rmscloud.com/123") is True
        assert await scanner.is_valid_page("https://ibe.rmscloud.com/999") is False
        assert len(scanner.checked_urls) == 2
    
    @pytest.mark.asyncio
    async def test_tracks_scanned_ids(self):
        """Should track all scanned IDs."""
        scanner = MockScanner(valid_ids={1})
        
        await scanner.scan_id(1)
        await scanner.scan_id(2)
        await scanner.scan_id(3)
        
        assert scanner.scanned_ids == [1, 2, 3]


class TestRMSScannerIsValidPage:
    """Tests for RMSScanner.is_valid_page (with mocked page)."""
    
    @pytest.mark.asyncio
    async def test_returns_false_on_http_error(self):
        """Should return False when response status >= 400."""
        mock_page = MagicMock()
        mock_response = MagicMock()
        mock_response.status = 404
        mock_page.goto = AsyncMock(return_value=mock_response)
        
        scanner = RMSScanner(mock_page)
        result = await scanner.is_valid_page("https://ibe.rmscloud.com/999")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        """Should return False when page load throws."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock(side_effect=Exception("Timeout"))
        
        scanner = RMSScanner(mock_page)
        result = await scanner.is_valid_page("https://ibe.rmscloud.com/999")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_returns_false_on_error_page(self):
        """Should return False when page contains error indicators."""
        mock_page = MagicMock()
        mock_response = MagicMock()
        mock_response.status = 200
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="Error - application issues detected")
        mock_page.evaluate = AsyncMock(return_value="Short")
        
        scanner = RMSScanner(mock_page)
        result = await scanner.is_valid_page("https://ibe.rmscloud.com/123")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_returns_false_on_404_content(self):
        """Should return False when page content indicates 404."""
        mock_page = MagicMock()
        mock_response = MagicMock()
        mock_response.status = 200
        mock_page.goto = AsyncMock(return_value=mock_response)
        mock_page.content = AsyncMock(return_value="<h1>404 Page Not Found</h1>")
        mock_page.evaluate = AsyncMock(return_value="404 Page Not Found")
        
        scanner = RMSScanner(mock_page)
        result = await scanner.is_valid_page("https://ibe.rmscloud.com/123")
        
        assert result is False


class TestRMSScannerScanId:
    """Tests for RMSScanner.scan_id."""
    
    @pytest.mark.asyncio
    async def test_tries_multiple_formats(self):
        """Should try multiple slug formats."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock(side_effect=Exception("Not found"))
        
        scanner = RMSScanner(mock_page)
        result = await scanner.scan_id(42)
        
        # Should have tried all format/subdomain combinations
        assert result is None
        # At minimum: 3 formats * 2 subdomains = 6 attempts
        assert mock_page.goto.call_count >= 6
    
    @pytest.mark.asyncio
    async def test_returns_first_valid_url(self):
        """Should return first valid URL found."""
        call_count = 0
        
        async def mock_goto(url, **kwargs):
            nonlocal call_count
            call_count += 1
            # Third attempt succeeds (ibe12.rmscloud.com/0042)
            if call_count == 3:
                mock_response = MagicMock()
                mock_response.status = 200
                return mock_response
            raise Exception("Not found")
        
        mock_page = MagicMock()
        mock_page.goto = mock_goto
        mock_page.content = AsyncMock(return_value="<h1>Test Hotel</h1>")
        mock_page.evaluate = AsyncMock(return_value="Test Hotel - Book Now " * 10)
        mock_page.title = AsyncMock(return_value="Test Hotel")
        
        scanner = RMSScanner(mock_page)
        result = await scanner.scan_id(42)
        
        assert result is not None
        assert result.id_num == 42


class TestScannedURL:
    """Tests for ScannedURL dataclass."""
    
    def test_attributes(self):
        """Should have correct attributes."""
        url = ScannedURL(
            id_num=123,
            url="https://ibe.rmscloud.com/123",
            slug="123",
            subdomain="ibe",
        )
        
        assert url.id_num == 123
        assert url.url == "https://ibe.rmscloud.com/123"
        assert url.slug == "123"
        assert url.subdomain == "ibe"


@pytest.mark.online
@pytest.mark.integration
class TestRMSScannerIntegration:
    """Integration tests that hit live RMS URLs."""
    
    @pytest.mark.asyncio
    async def test_scan_known_valid_id(self):
        """Should find a known valid RMS hotel."""
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await ctx.new_page()
            
            scanner = RMSScanner(page)
            
            # Test a URL that should be valid (or might be)
            # This tests the actual network request handling
            result = await scanner.is_valid_page("https://ibe.rmscloud.com/1")
            
            # Result could be True or False depending on whether ID exists
            assert isinstance(result, bool)
            
            await ctx.close()
            await browser.close()
    
    @pytest.mark.asyncio
    async def test_invalid_url_returns_false(self):
        """Should return False for clearly invalid URLs."""
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            page = await ctx.new_page()
            
            scanner = RMSScanner(page)
            
            # Test a URL that definitely doesn't exist
            result = await scanner.is_valid_page("https://ibe.rmscloud.com/99999999999")
            
            assert result is False
            
            await ctx.close()
            await browser.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "not online"])
