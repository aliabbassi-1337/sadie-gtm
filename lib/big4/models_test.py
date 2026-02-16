"""Tests for BIG4 models."""

import pytest

from lib.big4.models import Big4Park, Big4ScrapeResult


@pytest.mark.no_db
class TestBig4Park:
    """Tests for Big4Park model."""

    @pytest.fixture
    def park(self):
        return Big4Park(
            name="BIG4 Sydney Lakeside Holiday Park",
            slug="sydney-lakeside-holiday-park",
            url_path="/caravan-parks/nsw/greater-sydney/sydney-lakeside-holiday-park",
            state="NSW",
            region="Greater Sydney",
            phone="02 9913 7845",
            email="info@sydneylakeside.com.au",
            address="38 Lake Park Rd",
            city="Narrabeen",
            postcode="2101",
            latitude=-33.7185,
            longitude=151.2825,
        )

    def test_full_url(self, park):
        assert park.full_url == "https://www.big4.com.au/caravan-parks/nsw/greater-sydney/sydney-lakeside-holiday-park"

    def test_contact_url(self, park):
        assert park.contact_url == "https://www.big4.com.au/caravan-parks/nsw/greater-sydney/sydney-lakeside-holiday-park/contact"

    def test_external_id(self, park):
        assert park.external_id == "big4_sydney-lakeside-holiday-park"

    def test_has_location_true(self, park):
        assert park.has_location() is True

    def test_has_location_false_no_lat(self):
        park = Big4Park(name="Test", slug="test", url_path="/test")
        assert park.has_location() is False

    def test_has_location_false_partial(self):
        park = Big4Park(name="Test", slug="test", url_path="/test", latitude=-33.0)
        assert park.has_location() is False

    def test_to_insert_tuple(self, park):
        t = park.to_insert_tuple()
        assert t[0] == "BIG4 Sydney Lakeside Holiday Park"  # name
        assert t[1] == "big4_scrape"  # source
        assert t[2] == 1  # status
        assert t[6] == "Australia"  # country
        assert t[8] == "holiday_park"  # category
        assert t[9] == "big4_sydney-lakeside-holiday-park"  # external_id

    def test_optional_fields_default_none(self):
        park = Big4Park(name="Min Park", slug="min", url_path="/min")
        assert park.phone is None
        assert park.email is None
        assert park.latitude is None
        assert park.rating is None


@pytest.mark.no_db
class TestBig4ScrapeResult:
    """Tests for Big4ScrapeResult model."""

    def test_defaults(self):
        result = Big4ScrapeResult()
        assert result.parks_discovered == 0
        assert result.parks_scraped == 0
        assert result.parks_with_contact == 0
        assert result.parks_failed == 0
        assert result.parks == []

    def test_with_parks(self):
        park = Big4Park(name="Test", slug="test", url_path="/test")
        result = Big4ScrapeResult(parks_discovered=1, parks_scraped=1, parks=[park])
        assert len(result.parks) == 1
