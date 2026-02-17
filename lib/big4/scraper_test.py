"""Tests for BIG4 scraper."""

import json
import pytest

from lib.big4.models import Big4Park
from lib.big4.scraper import Big4Scraper


@pytest.mark.no_db
class TestExtractJsonLd:
    """Tests for _extract_json_ld."""

    @pytest.fixture
    def scraper(self):
        return Big4Scraper()

    def test_extracts_lodging_business(self, scraper):
        html = '<script type="application/ld+json">{"@type":"LodgingBusiness","name":"Test Park"}</script>'
        result = scraper._extract_json_ld(html)
        assert result is not None
        assert result["name"] == "Test Park"

    def test_extracts_from_list(self, scraper):
        data = [{"@type": "WebSite"}, {"@type": "LodgingBusiness", "name": "Found"}]
        html = f'<script type="application/ld+json">{json.dumps(data)}</script>'
        result = scraper._extract_json_ld(html)
        assert result["name"] == "Found"

    def test_extracts_from_graph(self, scraper):
        data = {"@graph": [{"@type": "WebSite"}, {"@type": "Hotel", "name": "Graph Hotel"}]}
        html = f'<script type="application/ld+json">{json.dumps(data)}</script>'
        result = scraper._extract_json_ld(html)
        assert result["name"] == "Graph Hotel"

    def test_extracts_from_rsc_streaming(self, scraper):
        """Should extract JSON-LD from Next.js RSC self.__next_f.push format."""
        ld_json = '{"@context":"http://schema.org","@type":"LodgingBusiness","name":"RSC Park","address":{"streetAddress":"1 Test St"}}'
        escaped = ld_json.replace('"', '\\"')
        html = f'<script>self.__next_f.push([1,"{escaped}"])</script>'
        result = scraper._extract_json_ld(html)
        assert result is not None
        assert result["name"] == "RSC Park"

    def test_returns_none_for_no_match(self, scraper):
        html = '<script type="application/ld+json">{"@type":"WebSite"}</script>'
        assert scraper._extract_json_ld(html) is None

    def test_returns_none_for_invalid_json(self, scraper):
        html = '<script type="application/ld+json">{invalid json}</script>'
        assert scraper._extract_json_ld(html) is None

    def test_returns_none_for_no_script(self, scraper):
        html = '<html><body>No JSON-LD here</body></html>'
        assert scraper._extract_json_ld(html) is None


@pytest.mark.no_db
class TestApplyJsonLd:
    """Tests for _apply_json_ld."""

    @pytest.fixture
    def scraper(self):
        return Big4Scraper()

    @pytest.fixture
    def park(self):
        return Big4Park(name="Original", slug="test", url_path="/test")

    def test_applies_name(self, scraper, park):
        scraper._apply_json_ld(park, {"name": "  New Name  "})
        assert park.name == "New Name"

    def test_applies_telephone(self, scraper, park):
        scraper._apply_json_ld(park, {"telephone": "02 1234 5678"})
        assert park.phone == "02 1234 5678"

    def test_applies_address(self, scraper, park):
        scraper._apply_json_ld(park, {
            "address": {
                "streetAddress": "1 Main St",
                "addressLocality": "Sydney",
                "addressRegion": "NSW",
                "postalCode": "2000",
            }
        })
        assert park.address == "1 Main St"
        assert park.city == "Sydney"
        assert park.state == "NSW"
        assert park.postcode == "2000"

    def test_applies_geo(self, scraper, park):
        scraper._apply_json_ld(park, {
            "geo": {"latitude": "-33.87", "longitude": "151.21"}
        })
        assert park.latitude == -33.87
        assert park.longitude == 151.21

    def test_skips_invalid_geo(self, scraper, park):
        scraper._apply_json_ld(park, {
            "geo": {"latitude": "not_a_number"}
        })
        assert park.latitude is None

    def test_applies_rating(self, scraper, park):
        scraper._apply_json_ld(park, {
            "aggregateRating": {"ratingValue": "4.5", "reviewCount": "123"}
        })
        assert park.rating == 4.5
        assert park.review_count == 123

    def test_applies_pets_allowed(self, scraper, park):
        scraper._apply_json_ld(park, {"petsAllowed": True})
        assert park.pets_allowed is True

    def test_truncates_description(self, scraper, park):
        scraper._apply_json_ld(park, {"description": "x" * 600})
        assert len(park.description) == 500


@pytest.mark.no_db
class TestExtractContactInfo:
    """Tests for _extract_contact_info."""

    @pytest.fixture
    def scraper(self):
        return Big4Scraper()

    def test_extracts_email(self, scraper):
        park = Big4Park(name="Test", slug="test", url_path="/test")
        html = '<a href="mailto:info@park.com.au">Email us</a>'
        scraper._extract_contact_info(park, html)
        assert park.email == "info@park.com.au"

    def test_does_not_overwrite_existing_email(self, scraper):
        park = Big4Park(name="Test", slug="test", url_path="/test", email="existing@park.com")
        html = '<a href="mailto:new@park.com">Email us</a>'
        scraper._extract_contact_info(park, html)
        assert park.email == "existing@park.com"

    def test_extracts_local_phone_over_1800(self, scraper):
        park = Big4Park(name="Test", slug="test", url_path="/test")
        html = '<a href="tel://1800123456">Call</a><a href="tel://0291234567">Local</a>'
        scraper._extract_contact_info(park, html)
        assert park.phone == "0291234567"

    def test_does_not_overwrite_existing_phone(self, scraper):
        park = Big4Park(name="Test", slug="test", url_path="/test", phone="0291234567")
        html = '<a href="tel://0399999999">Call</a>'
        scraper._extract_contact_info(park, html)
        assert park.phone == "0399999999"
