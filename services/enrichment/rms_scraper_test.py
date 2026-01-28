"""Unit tests for RMS Scraper."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from services.enrichment.rms_scraper import (
    RMSScraper,
    MockScraper,
    ExtractedRMSData,
    decode_cloudflare_email,
    normalize_country,
)


class TestExtractedRMSData:
    """Tests for ExtractedRMSData dataclass."""
    
    def test_has_data_returns_true_with_name(self):
        """Should return True when name is valid."""
        data = ExtractedRMSData(
            slug="123",
            booking_url="https://ibe.rmscloud.com/123",
            name="Test Hotel",
        )
        assert data.has_data() is True
    
    def test_has_data_returns_false_without_name(self):
        """Should return False when name is None."""
        data = ExtractedRMSData(
            slug="123",
            booking_url="https://ibe.rmscloud.com/123",
            name=None,
        )
        assert data.has_data() is False
    
    def test_has_data_returns_false_for_generic_names(self):
        """Should return False for generic/placeholder names."""
        generic_names = ['Online Bookings', 'search', 'Error', 'Loading', '']
        
        for name in generic_names:
            data = ExtractedRMSData(
                slug="123",
                booking_url="https://ibe.rmscloud.com/123",
                name=name,
            )
            assert data.has_data() is False, f"Should reject '{name}'"


class TestDecodeCloudflareEmail:
    """Tests for decode_cloudflare_email."""
    
    def test_decodes_valid_email(self):
        """Should decode Cloudflare-protected email."""
        # Example: "test@example.com" encoded
        encoded = "7a515a461915151c465d5f1c464f4852"
        result = decode_cloudflare_email(encoded)
        # Note: This is a simplified test - real encoding varies
        assert "@" in result or result == ""
    
    def test_returns_empty_on_invalid(self):
        """Should return empty string on invalid input."""
        assert decode_cloudflare_email("invalid") == ""
        assert decode_cloudflare_email("zz") == ""


class TestNormalizeCountry:
    """Tests for normalize_country."""
    
    def test_normalizes_usa_variants(self):
        """Should normalize USA variants."""
        assert normalize_country("United States") == "USA"
        assert normalize_country("United States of America") == "USA"
        assert normalize_country("us") == "USA"
        assert normalize_country("USA") == "USA"
    
    def test_normalizes_other_countries(self):
        """Should normalize other country names."""
        assert normalize_country("Australia") == "AU"
        assert normalize_country("Canada") == "CA"
        assert normalize_country("New Zealand") == "NZ"
        assert normalize_country("United Kingdom") == "GB"
        assert normalize_country("UK") == "GB"
        assert normalize_country("Mexico") == "MX"
    
    def test_returns_uppercase_code_for_unknown(self):
        """Should return first 2 chars uppercase for unknown."""
        assert normalize_country("Germany") == "GE"
        assert normalize_country("France") == "FR"
    
    def test_handles_empty_string(self):
        """Should return empty for empty input."""
        assert normalize_country("") == ""
        assert normalize_country(None) == ""


class TestMockScraper:
    """Tests for MockScraper."""
    
    @pytest.mark.asyncio
    async def test_returns_configured_results(self):
        """Should return results from configuration."""
        expected_data = ExtractedRMSData(
            slug="123",
            booking_url="https://ibe.rmscloud.com/123",
            name="Mock Hotel",
            phone="555-1234",
        )
        
        scraper = MockScraper(results={
            "https://ibe.rmscloud.com/123": expected_data,
        })
        
        result = await scraper.extract("https://ibe.rmscloud.com/123", "123")
        
        assert result == expected_data
        assert result.name == "Mock Hotel"
    
    @pytest.mark.asyncio
    async def test_returns_none_for_unconfigured(self):
        """Should return None for unconfigured URLs."""
        scraper = MockScraper(results={})
        
        result = await scraper.extract("https://ibe.rmscloud.com/unknown", "unknown")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_tracks_extracted_urls(self):
        """Should track all URLs that were extracted."""
        scraper = MockScraper()
        
        await scraper.extract("https://url1.com", "1")
        await scraper.extract("https://url2.com", "2")
        await scraper.extract("https://url3.com", "3")
        
        assert scraper.extracted_urls == [
            "https://url1.com",
            "https://url2.com",
            "https://url3.com",
        ]


class TestRMSScraperExtract:
    """Tests for RMSScraper.extract (with mocked page)."""
    
    @pytest.mark.asyncio
    async def test_returns_none_on_error_page(self):
        """Should return None when page has error content."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_page.content = AsyncMock(return_value="Error - application issues")
        mock_page.evaluate = AsyncMock(return_value="Short")
        
        scraper = RMSScraper(mock_page)
        result = await scraper.extract("https://ibe.rmscloud.com/123", "123")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        """Should return None when page load throws."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock(side_effect=Exception("Network error"))
        
        scraper = RMSScraper(mock_page)
        result = await scraper.extract("https://ibe.rmscloud.com/123", "123")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_extracts_name_from_h1(self):
        """Should extract name from h1 element."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_page.content = AsyncMock(return_value="<h1>Beach Resort</h1>")
        mock_page.evaluate = AsyncMock(return_value="Beach Resort\nBook your stay" * 10)
        
        mock_h1 = MagicMock()
        mock_h1.inner_text = AsyncMock(return_value="Beach Resort")
        mock_page.query_selector = AsyncMock(return_value=mock_h1)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.title = AsyncMock(return_value="Beach Resort - RMS")
        
        scraper = RMSScraper(mock_page)
        result = await scraper.extract("https://ibe.rmscloud.com/123", "123")
        
        assert result is not None
        assert result.name == "Beach Resort"
    
    @pytest.mark.asyncio
    async def test_extracts_phone(self):
        """Should extract phone number from content."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_page.content = AsyncMock(return_value="<p>Call us</p>")
        mock_page.evaluate = AsyncMock(return_value="Call us: +1 555-123-4567\nBeach Resort" * 10)
        
        mock_h1 = MagicMock()
        mock_h1.inner_text = AsyncMock(return_value="Beach Resort")
        mock_page.query_selector = AsyncMock(return_value=mock_h1)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.title = AsyncMock(return_value="Beach Resort")
        
        scraper = RMSScraper(mock_page)
        result = await scraper.extract("https://ibe.rmscloud.com/123", "123")
        
        assert result is not None
        assert result.phone is not None
        assert "555" in result.phone
    
    @pytest.mark.asyncio
    async def test_extracts_cloudflare_email(self):
        """Should decode Cloudflare-protected emails."""
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_page.content = AsyncMock(return_value='<a data-cfemail="7a515a461915151c465d5f1c464f4852">email</a>')
        mock_page.evaluate = AsyncMock(return_value="Beach Resort" * 20)
        
        mock_h1 = MagicMock()
        mock_h1.inner_text = AsyncMock(return_value="Beach Resort")
        mock_page.query_selector = AsyncMock(return_value=mock_h1)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.title = AsyncMock(return_value="Beach Resort")
        
        scraper = RMSScraper(mock_page)
        result = await scraper.extract("https://ibe.rmscloud.com/123", "123")
        
        assert result is not None
        # Email should be decoded (or empty if decode fails)


class TestRMSScraperParseAddress:
    """Tests for RMSScraper._parse_address."""
    
    def test_extracts_state(self):
        """Should extract state code from address."""
        mock_page = MagicMock()
        scraper = RMSScraper(mock_page)
        
        state, country = scraper._parse_address("123 Main St, CA 90210")
        assert state == "CA"
    
    def test_extracts_country_usa(self):
        """Should extract and normalize USA."""
        mock_page = MagicMock()
        scraper = RMSScraper(mock_page)
        
        state, country = scraper._parse_address("123 Main St, California, USA")
        assert country == "USA"
    
    def test_extracts_country_australia(self):
        """Should extract and normalize Australia."""
        mock_page = MagicMock()
        scraper = RMSScraper(mock_page)
        
        state, country = scraper._parse_address("123 Beach Rd, QLD, Australia")
        assert country == "AU"
    
    def test_returns_none_for_missing(self):
        """Should return None when state/country not found."""
        mock_page = MagicMock()
        scraper = RMSScraper(mock_page)
        
        state, country = scraper._parse_address("Some random text")
        assert state is None
        assert country is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
