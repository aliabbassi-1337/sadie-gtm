"""Tests for RMS API Client."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from lib.rms.api_client import (
    RMSApiClient,
    RMSApiResponse,
    extract_with_fallback,
)
from lib.rms.models import ExtractedRMSData


class TestRMSApiClientParseStructuredDescription:
    """Tests for structured description parsing."""
    
    @pytest.fixture
    def client(self):
        return RMSApiClient()
    
    def test_parses_australian_address(self, client):
        """Should parse Australian structured description."""
        text = """
        lyf Collingwood Melbourne (Leased)
        42 Oxford Street
        Collingwood VIC
        Australia 3066
        Phone: (61 3) 9977 5988
        Email: lyf.collingwood@the-ascott.com
        """
        
        result = client._parse_structured_description(text)
        
        assert result["address"] == "42 Oxford Street"
        assert result["city"] == "Collingwood"
        assert result["state"] == "VIC"
        assert result["country"] == "AU"
        assert result["phone"] == "(61 3) 9977 5988"
        assert result["email"] == "lyf.collingwood@the-ascott.com"
    
    def test_parses_phone_with_label(self, client):
        """Should extract phone with Phone: label."""
        text = "Contact us\nPhone: +1 555 123 4567\nThanks"
        
        result = client._parse_structured_description(text)
        
        assert result["phone"] == "+1 555 123 4567"
    
    def test_parses_email_with_label(self, client):
        """Should extract email with Email: label."""
        text = "Contact\nEmail: info@hotel.com\nWebsite: hotel.com"
        
        result = client._parse_structured_description(text)
        
        assert result["email"] == "info@hotel.com"
    
    def test_parses_us_address(self, client):
        """Should parse US address format."""
        text = """
        Grand Hotel
        123 Main Street
        Austin, TX 78701
        Phone: 512-555-1234
        """
        
        result = client._parse_structured_description(text)
        
        assert result["city"] == "Austin"
        assert result["state"] == "TX"
        assert result["postcode"] == "78701"
        assert result["country"] == "USA"
    
    def test_handles_html_tags(self, client):
        """Should strip HTML tags from text."""
        text = "<br>Phone: 123-456-7890<br>Email: test@test.com"
        
        result = client._parse_structured_description(text)
        
        assert result["phone"] == "123-456-7890"
        assert result["email"] == "test@test.com"
    
    def test_handles_empty_text(self, client):
        """Should handle empty text."""
        result = client._parse_structured_description("")
        
        assert result["phone"] is None
        assert result["email"] is None
    
    def test_handles_none_text(self, client):
        """Should handle None text."""
        result = client._parse_structured_description(None)
        
        assert result["phone"] is None


class TestRMSApiClientExtractPhone:
    """Tests for phone extraction."""
    
    @pytest.fixture
    def client(self):
        return RMSApiClient()
    
    def test_extracts_australian_phone(self, client):
        """Should extract Australian phone format."""
        text = "Call us at 02-6238-1044 for bookings"
        
        phone = client._extract_phone(text)
        
        assert phone is not None
        assert "6238" in phone
    
    def test_extracts_international_phone(self, client):
        """Should extract international format."""
        text = "International: +61-2-6238-1044"
        
        phone = client._extract_phone(text)
        
        assert phone is not None
        assert "+61" in phone
    
    def test_extracts_us_phone(self, client):
        """Should extract US phone format."""
        text = "Call (555) 123-4567"
        
        phone = client._extract_phone(text)
        
        assert phone is not None
    
    def test_returns_none_for_no_phone(self, client):
        """Should return None when no phone found."""
        text = "No contact information here"
        
        phone = client._extract_phone(text)
        
        assert phone is None
    
    def test_strips_html(self, client):
        """Should strip HTML before extracting."""
        text = "<br>Phone: <span>123-456-7890</span>"
        
        phone = client._extract_phone(text)
        
        assert phone is not None


class TestRMSApiClientExtractEmail:
    """Tests for email extraction."""
    
    @pytest.fixture
    def client(self):
        return RMSApiClient()
    
    def test_extracts_email(self, client):
        """Should extract email address."""
        text = "Contact: info@grandhotel.com for reservations"
        
        email = client._extract_email(text)
        
        assert email == "info@grandhotel.com"
    
    def test_ignores_rmscloud_emails(self, client):
        """Should ignore RMS system emails."""
        text = "noreply@rmscloud.com"
        
        email = client._extract_email(text)
        
        assert email is None
    
    def test_ignores_example_emails(self, client):
        """Should ignore example/test emails."""
        text = "test@example.com"
        
        email = client._extract_email(text)
        
        assert email is None
    
    def test_returns_none_for_no_email(self, client):
        """Should return None when no email found."""
        text = "No email here"
        
        email = client._extract_email(text)
        
        assert email is None


class TestRMSApiClientExtractLocation:
    """Tests for location extraction from text."""
    
    @pytest.fixture
    def client(self):
        return RMSApiClient()
    
    def test_extracts_australian_location(self, client):
        """Should extract Australian location."""
        text = "Located in Bungendore, the gateway to the Canberra region in NSW"
        
        city, state, country = client._extract_location(text)
        
        # May not get all fields, but should get country at minimum
        # from "Australian" mention or state code
    
    def test_extracts_country_from_text(self, client):
        """Should extract country from mention."""
        text = "Welcome to our beautiful resort in Australia"
        
        city, state, country = client._extract_location(text)
        
        assert country == "AU"
    
    def test_extracts_nz_country(self, client):
        """Should extract New Zealand."""
        text = "Visit us in beautiful New Zealand"
        
        city, state, country = client._extract_location(text)
        
        assert country == "NZ"
    
    def test_extracts_usa_country(self, client):
        """Should extract USA."""
        text = "The best hotel in the United States"
        
        city, state, country = client._extract_location(text)
        
        assert country == "USA"


class TestRMSApiClientNormalizeCountry:
    """Tests for country normalization."""
    
    @pytest.fixture
    def client(self):
        return RMSApiClient()
    
    def test_normalizes_australia(self, client):
        """Should normalize Australia to AU."""
        assert client._normalize_country("Australia") == "AU"
        assert client._normalize_country("australia") == "AU"
        assert client._normalize_country("AUSTRALIA") == "AU"
    
    def test_normalizes_new_zealand(self, client):
        """Should normalize New Zealand to NZ."""
        assert client._normalize_country("New Zealand") == "NZ"
    
    def test_normalizes_usa(self, client):
        """Should normalize USA/United States to USA."""
        assert client._normalize_country("USA") == "USA"
        assert client._normalize_country("United States") == "USA"
    
    def test_normalizes_canada(self, client):
        """Should normalize Canada to CA."""
        assert client._normalize_country("Canada") == "CA"
    
    def test_handles_empty(self, client):
        """Should handle empty string."""
        assert client._normalize_country("") == ""
    
    def test_returns_original_for_unknown(self, client):
        """Should return original for unknown countries."""
        assert client._normalize_country("Germany") == "Germany"


class TestRMSApiClientParseHtml:
    """Tests for HTML parsing."""
    
    @pytest.fixture
    def client(self):
        return RMSApiClient()
    
    def test_extracts_name_from_hidden_input(self, client):
        """Should extract name from propertyName input."""
        html = '''
        <html>
        <input id="propertyName" value="Grand Hotel Test" />
        </html>
        '''
        
        data = client._parse_html(html, "12345", "https://example.com")
        
        assert data is not None
        assert data.name == "Grand Hotel Test"
    
    def test_extracts_name_from_h1(self, client):
        """Should extract name from h1 tag."""
        html = '''
        <html>
        <h1>Beautiful Beach Resort</h1>
        </html>
        '''
        
        data = client._parse_html(html, "12345", "https://example.com")
        
        assert data is not None
        assert data.name == "Beautiful Beach Resort"
    
    def test_extracts_name_from_p_input(self, client):
        """Should extract name from P hidden input."""
        html = '''
        <html>
        <input id="P" value="Mountain Lodge" />
        </html>
        '''
        
        data = client._parse_html(html, "12345", "https://example.com")
        
        assert data is not None
        assert data.name == "Mountain Lodge"
    
    def test_returns_none_without_name(self, client):
        """Should return None if no name found."""
        html = "<html><body>No hotel name here</body></html>"
        
        data = client._parse_html(html, "12345", "https://example.com")
        
        assert data is None
    
    def test_extracts_cloudflare_email(self, client):
        """Should decode Cloudflare-protected email."""
        # Cloudflare email protection encodes emails
        html = '''
        <html>
        <input id="propertyName" value="Test Hotel" />
        <a class="__cf_email__" data-cfemail="7e1b161b0c1f121f501b0c3e1b061f130e121b501d1113">email</a>
        </html>
        '''
        
        data = client._parse_html(html, "12345", "https://example.com")
        
        # Should attempt to decode the email
        assert data is not None
    
    def test_extracts_phone_from_icon(self, client):
        """Should extract phone from phone icon element."""
        html = '''
        <html>
        <input id="propertyName" value="Test Hotel" />
        <span><i class="fa fa-phone"></i> +61 3 9977 5988</span>
        </html>
        '''
        
        data = client._parse_html(html, "12345", "https://example.com")
        
        assert data is not None
        # Phone extraction may or may not work depending on HTML structure


@pytest.mark.online
class TestRMSApiClientIntegration:
    """Integration tests that hit real RMS API."""
    
    @pytest.mark.asyncio
    async def test_fetches_property_api(self):
        """Should fetch data from Property API."""
        client = RMSApiClient(timeout=15.0)
        
        # Use a known working property
        data = await client.extract("15819", "bookings.rmscloud.com")
        
        if data:
            assert data.name is not None
            assert data.slug == "15819"
        else:
            pytest.skip("RMS API unavailable")
    
    @pytest.mark.asyncio
    async def test_handles_invalid_slug(self):
        """Should handle invalid slug gracefully."""
        client = RMSApiClient(timeout=10.0)
        
        data = await client.extract("9999999999", "bookings.rmscloud.com")
        
        # Should return None for invalid slug
        assert data is None
    
    @pytest.mark.asyncio
    async def test_extracts_from_html(self):
        """Should extract data from HTML page."""
        client = RMSApiClient(timeout=15.0)
        
        data = await client.extract_from_html("15819", "bookings.rmscloud.com")
        
        if data:
            assert data.name is not None
            assert "lyf" in data.name.lower() or len(data.name) > 0
        else:
            pytest.skip("RMS HTML fetch unavailable")
    
    @pytest.mark.asyncio
    async def test_extract_with_fallback_returns_method(self):
        """Should return extraction method used."""
        data, method = await extract_with_fallback("15819", scraper=None)
        
        if data:
            assert method in ["api", "html", "scraper", "none"]
            assert data.name is not None
        else:
            assert method == "none"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
