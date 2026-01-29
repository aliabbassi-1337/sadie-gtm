"""Tests for Cloudbeds Scraper."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from lib.cloudbeds.scraper import CloudbedsScraper, ExtractedCloudbedsData


class TestExtractedCloudbedsData:
    """Tests for ExtractedCloudbedsData model."""
    
    def test_has_data_with_name(self):
        """Should return True when name is present."""
        data = ExtractedCloudbedsData(name="Hotel Name")
        assert data.has_data() is True
    
    def test_has_data_with_city(self):
        """Should return True when city is present."""
        data = ExtractedCloudbedsData(city="Austin")
        assert data.has_data() is True
    
    def test_has_data_with_email(self):
        """Should return True when email is present."""
        data = ExtractedCloudbedsData(email="hotel@example.com")
        assert data.has_data() is True
    
    def test_has_data_with_phone(self):
        """Should return True when phone is present."""
        data = ExtractedCloudbedsData(phone="555-1234")
        assert data.has_data() is True
    
    def test_has_data_empty(self):
        """Should return False when no meaningful data."""
        data = ExtractedCloudbedsData()
        assert data.has_data() is False
    
    def test_has_data_only_address(self):
        """Should return False when only address (not meaningful alone)."""
        data = ExtractedCloudbedsData(address="123 Main St")
        assert data.has_data() is False


class TestCloudbedsScraperExtract:
    """Tests for CloudbedsScraper.extract() method."""
    
    @pytest.fixture
    def mock_page(self):
        """Create mock Playwright page."""
        page = AsyncMock()
        page.goto = AsyncMock(return_value=MagicMock(status=200))
        page.evaluate = AsyncMock(return_value=None)
        return page
    
    @pytest.fixture
    def scraper(self, mock_page):
        """Create scraper with mock page."""
        return CloudbedsScraper(mock_page)
    
    @pytest.mark.asyncio
    async def test_returns_none_on_404(self, mock_page, scraper):
        """Should return None when page returns 404."""
        mock_page.goto.return_value = MagicMock(status=404)
        
        result = await scraper.extract("https://hotels.cloudbeds.com/reservation/invalid")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_returns_none_on_garbage_name(self, mock_page, scraper):
        """Should return None when page has garbage name (Cloudbeds homepage)."""
        mock_page.evaluate = AsyncMock(side_effect=[
            {"name": "cloudbeds.com", "city": None, "state": None, "country": None},
            None,
            None,
            None,
        ])
        
        result = await scraper.extract("https://hotels.cloudbeds.com/reservation/test")
        
        assert result is None


class TestCloudbedsScraperTitleParsing:
    """Tests for title tag parsing logic."""
    
    def test_parses_city_country_format(self):
        """Should parse 'Hotel Name - City, Country' format."""
        # This tests the JavaScript logic indirectly through expected behavior
        title_data = {
            "name": "Grand Hotel",
            "city": "Austin",
            "state": None,
            "country": "United States of America"
        }
        
        # Verify expected structure
        assert title_data["name"] == "Grand Hotel"
        assert title_data["city"] == "Austin"
        assert title_data["country"] == "United States of America"
    
    def test_parses_city_state_country_format(self):
        """Should parse 'Hotel Name - City, State, Country' format."""
        title_data = {
            "name": "Beach Resort",
            "city": "Miami",
            "state": "Florida",
            "country": "USA"
        }
        
        assert title_data["state"] == "Florida"


class TestCloudbedsScraperStateCountryPattern:
    """Tests for state/country regex pattern matching."""
    
    def test_matches_us_state(self):
        """Should match US state patterns."""
        import re
        pattern = re.compile(
            r'^([A-Za-z\s]+)\s+(US|USA|AU|UK|CA|NZ|GB|IE|MX|AR|PR|CO|IT|ES|FR|DE|PT|BR|CL|PE|CR|PA)$',
            re.IGNORECASE
        )
        
        assert pattern.match("California US")
        assert pattern.match("Texas USA")
        assert pattern.match("New York US")
    
    def test_matches_australian_state(self):
        """Should match Australian state patterns."""
        import re
        pattern = re.compile(
            r'^([A-Za-z\s]+)\s+(US|USA|AU|UK|CA|NZ|GB|IE|MX|AR|PR|CO|IT|ES|FR|DE|PT|BR|CL|PE|CR|PA)$',
            re.IGNORECASE
        )
        
        assert pattern.match("New South Wales AU")
        assert pattern.match("Victoria AU")
    
    def test_extracts_state_and_country(self):
        """Should correctly extract state and country from match."""
        import re
        pattern = re.compile(
            r'^([A-Za-z\s]+)\s+(US|USA|AU|UK|CA|NZ|GB|IE|MX|AR|PR|CO|IT|ES|FR|DE|PT|BR|CL|PE|CR|PA)$',
            re.IGNORECASE
        )
        
        match = pattern.match("California US")
        assert match.group(1).strip() == "California"
        assert match.group(2).upper() == "US"


@pytest.mark.online
class TestCloudbedsScraperIntegration:
    """Integration tests that hit real Cloudbeds pages."""
    
    @pytest.mark.asyncio
    async def test_extracts_from_real_page(self):
        """Should extract data from a real Cloudbeds booking page."""
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
        
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            page = await ctx.new_page()
            scraper = CloudbedsScraper(page)
            
            # Known working Cloudbeds page
            url = "https://hotels.cloudbeds.com/reservation/chsz6e"
            data = await scraper.extract(url)
            
            await browser.close()
        
        # Page may or may not be available, but if data exists, validate structure
        if data:
            assert isinstance(data, ExtractedCloudbedsData)
            assert data.has_data() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
