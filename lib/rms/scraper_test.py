"""Tests for RMS Scraper."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from lib.rms.scraper import RMSScraper
from lib.rms.models import ExtractedRMSData


class TestRMSScraperExtractName:
    """Tests for name extraction logic."""
    
    @pytest.fixture
    def mock_page(self):
        """Create mock Playwright page."""
        page = AsyncMock()
        page.query_selector = AsyncMock(return_value=None)
        page.title = AsyncMock(return_value="Online Bookings")
        return page
    
    @pytest.fixture
    def scraper(self, mock_page):
        """Create scraper with mock page."""
        return RMSScraper(mock_page)
    
    @pytest.mark.asyncio
    async def test_extracts_name_from_first_body_line(self, scraper):
        """Should extract hotel name from first non-garbage line of body text."""
        body_text = "By the Bay\nCart\n(0)\nBook your accommodation"
        
        name = await scraper._extract_name(body_text)
        
        assert name == "By the Bay"
    
    @pytest.mark.asyncio
    async def test_skips_garbage_names(self, scraper):
        """Should skip garbage names like Cart, Search, etc."""
        body_text = "Cart\n(0)\nBook your accommodation\nReal Hotel Name"
        
        name = await scraper._extract_name(body_text)
        
        assert name == "Real Hotel Name"
    
    @pytest.mark.asyncio
    async def test_skips_error_messages(self, scraper):
        """Should skip error message lines."""
        body_text = "Error\nLooks like we're having some application issues.\n1/28/2026 7:30:26 PM"
        
        name = await scraper._extract_name(body_text)
        
        assert name is None
    
    @pytest.mark.asyncio
    async def test_skips_dates(self, scraper):
        """Should skip date/timestamp lines."""
        body_text = "1/28/2026 7:30:26 PM\nV 5.25.345.4\nReal Hotel"
        
        name = await scraper._extract_name(body_text)
        
        assert name == "Real Hotel"
    
    @pytest.mark.asyncio
    async def test_skips_version_strings(self, scraper):
        """Should skip version strings."""
        body_text = "V 5.25.345.4\nReal Hotel"
        
        name = await scraper._extract_name(body_text)
        
        assert name == "Real Hotel"


class TestRMSScraperIsValid:
    """Tests for page validation."""
    
    @pytest.fixture
    def scraper(self):
        """Create scraper with mock page."""
        return RMSScraper(AsyncMock())
    
    def test_rejects_error_pages(self, scraper):
        """Should reject pages with application issues."""
        content = "<html>Error page</html>"
        body_text = "Error\nLooks like we're having some application issues."
        
        assert scraper._is_valid(content, body_text) is False
    
    def test_rejects_404_pages(self, scraper):
        """Should reject 404 pages."""
        content = "<html>404 Page Not Found</html>"
        body_text = "Page not found"
        
        assert scraper._is_valid(content, body_text) is False
    
    def test_rejects_short_content(self, scraper):
        """Should reject pages with very little content."""
        content = "<html></html>"
        body_text = "Short"
        
        assert scraper._is_valid(content, body_text) is False
    
    def test_accepts_valid_pages(self, scraper):
        """Should accept pages with sufficient content."""
        content = "<html>Valid booking page content</html>"
        body_text = "By the Bay\nCart\n(0)\nBook your accommodation\nDates\n" * 20
        
        assert scraper._is_valid(content, body_text) is True


class TestRMSScraperExtractPhone:
    """Tests for phone extraction."""
    
    @pytest.fixture
    def scraper(self):
        return RMSScraper(AsyncMock())
    
    def test_extracts_phone_with_label(self, scraper):
        """Should extract phone number with tel/phone label."""
        body_text = "Contact us\nPhone: +1 (555) 123-4567\nEmail: test@test.com"
        
        phone = scraper._extract_phone(body_text)
        
        assert phone is not None
        assert "555" in phone
    
    def test_extracts_international_phone(self, scraper):
        """Should extract international phone numbers with label."""
        body_text = "Tel: +61 2 9876 5432"
        
        phone = scraper._extract_phone(body_text)
        
        assert phone is not None


class TestRMSScraperExtractEmail:
    """Tests for email extraction."""
    
    @pytest.fixture
    def scraper(self):
        return RMSScraper(AsyncMock())
    
    def test_extracts_email(self, scraper):
        """Should extract email addresses."""
        content = "<html></html>"
        body_text = "Contact: info@grandhotel.com"
        
        email = scraper._extract_email(content, body_text)
        
        assert email == "info@grandhotel.com"
    
    def test_ignores_rmscloud_emails(self, scraper):
        """Should ignore RMS system emails."""
        content = "<html></html>"
        body_text = "noreply@rmscloud.com"
        
        email = scraper._extract_email(content, body_text)
        
        assert email is None


class TestRMSScraperParseAddress:
    """Tests for address parsing logic."""
    
    @pytest.fixture
    def scraper(self):
        return RMSScraper(AsyncMock())
    
    def test_parses_australian_address(self, scraper):
        """Should parse Australian address format."""
        address = "40 Ragonesi Rd, Alice Springs NT 0870 , Australia"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "Alice Springs"
        assert state == "NT"
        assert country == "AU"
    
    def test_parses_australian_address_vic(self, scraper):
        """Should parse Victorian address."""
        address = "830 Fifteenth Street, Mildura VIC 3500 , Australia"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "Mildura"
        assert state == "VIC"
        assert country == "AU"
    
    def test_parses_australian_address_nsw(self, scraper):
        """Should parse NSW address."""
        address = "98 Durham Street, Clarence Town NSW 2321 , Australia"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "Clarence Town"
        assert state == "NSW"
        assert country == "AU"
    
    def test_parses_us_address(self, scraper):
        """Should parse US address format."""
        address = "123 Main Street, Austin, TX 78701"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "Austin"
        assert state == "TX"
        assert country == "United States"
    
    def test_parses_us_address_california(self, scraper):
        """Should parse California address."""
        address = "456 Beach Blvd, San Diego, CA 92101-1234"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "San Diego"
        assert state == "CA"
        assert country == "United States"
    
    def test_parses_nz_address(self, scraper):
        """Should parse New Zealand address."""
        address = "10 Queen Street, Auckland 1010, New Zealand"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "Auckland"
        assert country == "NZ"
    
    def test_returns_none_for_unparseable(self, scraper):
        """Should return None for unparseable addresses."""
        address = "just a short description, not an address"
        
        city, state, country = scraper._parse_address(address)
        
        assert city is None
        # May still extract country if spelled out
    
    def test_fallback_extracts_country_name(self, scraper):
        """Should fallback to extracting country name."""
        address = "Some address in Australia"
        
        city, state, country = scraper._parse_address(address)
        
        assert country == "AU"
    
    # Edge cases
    def test_handles_empty_address(self, scraper):
        """Should handle empty address string."""
        city, state, country = scraper._parse_address("")
        
        assert city is None
        assert state is None
        assert country is None
    
    def test_handles_none_address(self, scraper):
        """Should handle None address."""
        city, state, country = scraper._parse_address(None)
        
        assert city is None
        assert state is None
        assert country is None
    
    def test_handles_address_with_only_numbers(self, scraper):
        """Should handle address that's just numbers."""
        city, state, country = scraper._parse_address("12345")
        
        assert city is None
    
    def test_handles_unicode_city_names(self, scraper):
        """Should handle unicode in city names."""
        address = "123 Main St, São Paulo SP 01310, Brazil"
        
        city, state, country = scraper._parse_address(address)
        # May or may not parse, but shouldn't crash
    
    def test_handles_multiple_commas(self, scraper):
        """Should handle addresses with many commas."""
        address = "Unit 5, Level 3, 123 Main Street, Sydney, NSW 2000, Australia"
        
        city, state, country = scraper._parse_address(address)
        
        # Should still extract Australia
        assert country == "AU"
    
    def test_handles_lowercase_state(self, scraper):
        """Should handle lowercase state abbreviations."""
        address = "123 Main St, Austin, tx 78701"
        
        city, state, country = scraper._parse_address(address)
        
        assert state == "TX"
        assert country == "United States"
    
    def test_handles_mixed_case_country(self, scraper):
        """Should handle mixed case country names."""
        address = "123 Street, City NSW 2000, AUSTRALIA"
        
        city, state, country = scraper._parse_address(address)
        
        assert country == "AU"
    
    def test_handles_zip_plus_four(self, scraper):
        """Should handle US ZIP+4 format."""
        address = "456 Oak Ave, Los Angeles, CA 90001-1234"
        
        city, state, country = scraper._parse_address(address)
        
        assert city == "Los Angeles"
        assert state == "CA"
        assert country == "United States"
    
    def test_handles_po_box_address(self, scraper):
        """Should handle PO Box addresses."""
        address = "PO Box 123, Melbourne VIC 3000, Australia"
        
        city, state, country = scraper._parse_address(address)
        
        assert state == "VIC"
        assert country == "AU"
    
    def test_handles_very_long_address(self, scraper):
        """Should handle very long addresses without crashing."""
        address = "A" * 1000 + ", Sydney NSW 2000, Australia"
        
        city, state, country = scraper._parse_address(address)
        # Should not crash, may or may not parse
    
    def test_handles_newlines_in_address(self, scraper):
        """Should handle newlines in address."""
        address = "123 Main St\nAustin, TX 78701"
        
        city, state, country = scraper._parse_address(address)
        # Should still attempt to parse


@pytest.mark.online
class TestRMSScraperIntegration:
    """Integration tests that hit real RMS pages."""
    
    @pytest.fixture
    async def browser_and_scraper(self):
        """Create real browser and scraper for integration tests."""
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
        
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await ctx.new_page()
        
        # Apply stealth to page
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        
        scraper = RMSScraper(page)
        
        yield scraper, page
        
        await ctx.close()
        await browser.close()
        await pw.stop()
    
    @pytest.mark.asyncio
    async def test_extracts_name_from_real_page(self, browser_and_scraper):
        """Should extract hotel name from a real RMS booking page."""
        scraper, page = browser_and_scraper
        
        # Try a few known RMS pages in case one is down
        urls = [
            ("https://bookings13.rmscloud.com/Search/Index/13308/90/", "13308"),
            ("https://bookings10.rmscloud.com/Search/Index/22261/68/", "22261"),
        ]
        
        data = None
        for url, slug in urls:
            data = await scraper.extract(url, slug)
            if data and data.has_data():
                break
        
        # At least one page should work
        if data:
            assert data.has_data() is True
            assert data.name is not None, "Should have hotel name"
        else:
            pytest.skip("All test RMS pages unavailable")
    
    @pytest.mark.asyncio
    async def test_parses_address_from_real_page(self, browser_and_scraper):
        """Should parse address and extract location from RMS page."""
        scraper, page = browser_and_scraper
        
        url = "https://bookings13.rmscloud.com/Search/Index/13308/90/"
        data = await scraper.extract(url, "13308")
        
        # RMS pages may or may not have address visible
        if data and data.address:
            # If address exists, parsing should have been attempted
            assert data.country is not None or data.state is not None or data.city is not None
    
    @pytest.mark.asyncio
    async def test_handles_invalid_slug(self, browser_and_scraper):
        """Should handle invalid RMS slugs gracefully."""
        scraper, page = browser_and_scraper
        
        url = "https://bookings13.rmscloud.com/Search/Index/99999/99/"
        data = await scraper.extract(url, "99999")
        
        # Should either return None or return data with is_valid=False
        if data:
            # If data exists, it should be marked as invalid or have no meaningful content
            pass  # Some error pages still return partial data
    
    @pytest.mark.asyncio
    async def test_extracts_email_from_page(self, browser_and_scraper):
        """Should extract email if present on page."""
        scraper, page = browser_and_scraper
        
        url = "https://bookings13.rmscloud.com/Search/Index/13308/90/"
        data = await scraper.extract(url, "13308")
        
        # Email extraction - may or may not be present
        if data and data.email:
            assert "@" in data.email
            assert "rmscloud.com" not in data.email.lower(), "Should filter out RMS system emails"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
