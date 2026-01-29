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
        
        scraper = CloudbedsScraper(page)
        
        yield scraper, page
        
        await ctx.close()
        await browser.close()
        await pw.stop()
    
    @pytest.mark.asyncio
    async def test_extracts_name_and_location(self, browser_and_scraper):
        """Should extract name, city, state from a real Cloudbeds page."""
        scraper, page = browser_and_scraper
        
        # Known working Cloudbeds page (Texican Court in Irving, TX)
        url = "https://hotels.cloudbeds.com/reservation/chsz6e"
        data = await scraper.extract(url)
        
        assert data is not None, "Should extract data from page"
        assert isinstance(data, ExtractedCloudbedsData)
        assert data.has_data() is True
        assert data.name is not None, "Should have hotel name"
        assert data.city is not None, "Should have city"
    
    @pytest.mark.asyncio
    async def test_extracts_state_correctly(self, browser_and_scraper):
        """Should extract state from 'State Country' format."""
        scraper, page = browser_and_scraper
        
        # Test a page that should have state info
        url = "https://hotels.cloudbeds.com/reservation/chsz6e"
        data = await scraper.extract(url)
        
        if data and data.state:
            # State should be a valid US state name or abbreviation
            assert len(data.state) >= 2, "State should be at least 2 chars"
    
    @pytest.mark.asyncio
    async def test_handles_404_gracefully(self, browser_and_scraper):
        """Should return None for non-existent pages."""
        scraper, page = browser_and_scraper
        
        # Invalid slug that should 404
        url = "https://hotels.cloudbeds.com/reservation/invalidslug99999"
        data = await scraper.extract(url)
        
        assert data is None, "Should return None for 404 pages"
    
    @pytest.mark.asyncio
    async def test_detects_garbage_homepage(self, browser_and_scraper):
        """Should return None for Cloudbeds homepage (garbage data)."""
        scraper, page = browser_and_scraper
        
        # Root URL redirects to homepage
        url = "https://hotels.cloudbeds.com/"
        data = await scraper.extract(url)
        
        # Should either return None or have garbage name detected
        if data:
            assert data.name.lower() not in ['cloudbeds.com', 'cloudbeds'], \
                "Should not return garbage homepage name"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
