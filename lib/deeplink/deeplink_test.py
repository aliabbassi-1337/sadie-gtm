"""Unit tests for deep-link URL generation. No DB needed."""

from datetime import date

import pytest

from lib.deeplink.engines.cloudbeds import CloudbedsBuilder
from lib.deeplink.engines.mews import MewsBuilder
from lib.deeplink.engines.rms import RmsBuilder
from lib.deeplink.engines.siteminder import SiteMinderBuilder
from services.deeplink.service import create_deeplink as generate_deeplink, detect_engine
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest

pytestmark = pytest.mark.no_db


# --- Engine Detection ---


class TestDetectEngine:
    def test_siteminder(self):
        assert detect_engine("https://direct-book.com/properties/foo") == "SiteMinder"

    def test_cloudbeds(self):
        assert detect_engine("https://hotels.cloudbeds.com/reservation/abc") == "Cloudbeds"

    def test_mews(self):
        url = "https://app.mews.com/distributor/cb6072cc-1e03-45cc-a6e8-ab0d00ea7979"
        assert detect_engine(url) == "Mews"

    def test_rms(self):
        assert detect_engine("https://bookings13.rmscloud.com/Search/Index/123/90/") == "RMS Cloud"

    def test_unknown(self):
        assert detect_engine("https://example.com/book") is None

    def test_rms_ibe_server(self):
        assert detect_engine("https://ibe12.rmscloud.com/12345") == "RMS Cloud"


# --- Slug Extraction ---


class TestSlugExtraction:
    def test_siteminder_slug(self):
        builder = SiteMinderBuilder()
        assert builder.extract_slug("https://direct-book.com/properties/thehindsheaddirect") == "thehindsheaddirect"

    def test_siteminder_slug_with_query(self):
        builder = SiteMinderBuilder()
        assert builder.extract_slug("https://direct-book.com/properties/hotelxyz?lang=en") == "hotelxyz"

    def test_siteminder_no_match(self):
        builder = SiteMinderBuilder()
        assert builder.extract_slug("https://direct-book.com/other/path") is None

    def test_cloudbeds_reservation(self):
        builder = CloudbedsBuilder()
        assert builder.extract_slug("https://hotels.cloudbeds.com/reservation/kypwgi") == "kypwgi"

    def test_cloudbeds_booking(self):
        builder = CloudbedsBuilder()
        assert builder.extract_slug("https://hotels.cloudbeds.com/booking/abcdef") == "abcdef"

    def test_cloudbeds_malformed_duplicate(self):
        builder = CloudbedsBuilder()
        url = "https://hotels.cloudbeds.com/reservation/hotels.cloudbeds.com/reservation/osbtup"
        assert builder.extract_slug(url) == "osbtup"

    def test_cloudbeds_rejects_domain_parts(self):
        builder = CloudbedsBuilder()
        assert builder.extract_slug("https://hotels.cloudbeds.com/reservation/hotels") is None

    def test_mews_uuid(self):
        builder = MewsBuilder()
        url = "https://app.mews.com/distributor/cb6072cc-1e03-45cc-a6e8-ab0d00ea7979"
        assert builder.extract_slug(url) == "cb6072cc-1e03-45cc-a6e8-ab0d00ea7979"

    def test_mews_no_match(self):
        builder = MewsBuilder()
        assert builder.extract_slug("https://app.mews.com/distributor/not-a-uuid") is None

    def test_rms_numeric(self):
        builder = RmsBuilder()
        assert builder.extract_slug("https://bookings13.rmscloud.com/Search/Index/13308/90/") == "13308"

    def test_rms_ibe_hex(self):
        builder = RmsBuilder()
        assert builder.extract_slug("https://ibe12.rmscloud.com/ABCDEF0123456789") == "ABCDEF0123456789"

    def test_rms_ibe_numeric(self):
        builder = RmsBuilder()
        assert builder.extract_slug("https://ibe13.rmscloud.com/12345") == "12345"


# --- URL Generation ---


def _make_request(url, **kwargs):
    defaults = dict(
        booking_url=url,
        checkin=date(2026, 3, 1),
        checkout=date(2026, 3, 3),
        adults=2,
        children=0,
        rooms=1,
    )
    defaults.update(kwargs)
    return DeepLinkRequest(**defaults)


class TestSiteMinderBuild:
    def test_basic(self):
        req = _make_request("https://direct-book.com/properties/thehindsheaddirect")
        result = generate_deeplink(req)
        assert result.engine_name == "SiteMinder"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert "checkInDate=2026-03-01" in result.url
        assert "checkOutDate=2026-03-03" in result.url
        assert "items%5B0%5D%5Badults%5D=2" in result.url
        assert "direct-book.com/properties/thehindsheaddirect" in result.url

    def test_with_promo(self):
        req = _make_request(
            "https://direct-book.com/properties/hotelxyz",
            promo_code="SUMMER",
        )
        result = generate_deeplink(req)
        assert "promoCode=SUMMER" in result.url


class TestCloudbedseBuild:
    def test_search_page_with_hash_params(self):
        req = _make_request("https://hotels.cloudbeds.com/reservation/kypwgi")
        result = generate_deeplink(req)
        assert result.engine_name == "Cloudbeds"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        # Hash params auto-trigger room search with dates pre-filled
        assert "/reservation/kypwgi#" in result.url
        assert "checkin=2026-03-01" in result.url
        assert "checkout=2026-03-03" in result.url
        assert "adults=2" in result.url
        assert "submit=1" in result.url


class TestMewsBuild:
    def test_basic(self):
        req = _make_request("https://app.mews.com/distributor/cb6072cc-1e03-45cc-a6e8-ab0d00ea7979")
        result = generate_deeplink(req)
        assert result.engine_name == "Mews"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert "mewsStart=2026-03-01" in result.url
        assert "mewsEnd=2026-03-03" in result.url
        assert "mewsAdultCount=2" in result.url

    def test_with_voucher(self):
        req = _make_request(
            "https://app.mews.com/distributor/cb6072cc-1e03-45cc-a6e8-ab0d00ea7979",
            promo_code="WINTER",
        )
        result = generate_deeplink(req)
        assert "mewsVoucherCode=WINTER" in result.url


class TestRmsBuild:
    def test_low_confidence(self):
        req = _make_request("https://bookings13.rmscloud.com/Search/Index/13308/90/")
        result = generate_deeplink(req)
        assert result.engine_name == "RMS Cloud"
        assert result.confidence == DeepLinkConfidence.LOW
        assert result.dates_prefilled is False
        assert "arrival=2026-03-01" in result.url
        assert "departure=2026-03-03" in result.url


class TestUnknownEngine:
    def test_returns_base_url(self):
        req = _make_request("https://example.com/book/hotel123")
        result = generate_deeplink(req)
        assert result.engine_name == "Unknown"
        assert result.confidence == DeepLinkConfidence.NONE
        assert result.dates_prefilled is False
        assert result.url == "https://example.com/book/hotel123"


# --- Validation ---


class TestValidation:
    def test_checkout_before_checkin(self):
        with pytest.raises(ValueError, match="checkout must be after checkin"):
            DeepLinkRequest(
                booking_url="https://example.com",
                checkin=date(2026, 3, 5),
                checkout=date(2026, 3, 1),
            )

    def test_checkout_equals_checkin(self):
        with pytest.raises(ValueError, match="checkout must be after checkin"):
            DeepLinkRequest(
                booking_url="https://example.com",
                checkin=date(2026, 3, 1),
                checkout=date(2026, 3, 1),
            )

    def test_zero_adults(self):
        with pytest.raises(ValueError, match="adults must be >= 1"):
            DeepLinkRequest(
                booking_url="https://example.com",
                checkin=date(2026, 3, 1),
                checkout=date(2026, 3, 3),
                adults=0,
            )

    def test_negative_children(self):
        with pytest.raises(ValueError, match="children must be >= 0"):
            DeepLinkRequest(
                booking_url="https://example.com",
                checkin=date(2026, 3, 1),
                checkout=date(2026, 3, 3),
                children=-1,
            )

    def test_zero_rooms(self):
        with pytest.raises(ValueError, match="rooms must be >= 1"):
            DeepLinkRequest(
                booking_url="https://example.com",
                checkin=date(2026, 3, 1),
                checkout=date(2026, 3, 3),
                rooms=0,
            )


# --- Edge Cases ---


class TestEdgeCases:
    def test_siteminder_bad_slug_returns_base_url(self):
        req = _make_request("https://direct-book.com/other/path")
        result = generate_deeplink(req)
        assert result.engine_name == "SiteMinder"
        assert result.confidence == DeepLinkConfidence.NONE
        assert result.dates_prefilled is False
        assert result.url == req.booking_url

    def test_cloudbeds_bad_slug_returns_base_url(self):
        req = _make_request("https://hotels.cloudbeds.com/other/path")
        result = generate_deeplink(req)
        assert result.confidence == DeepLinkConfidence.NONE
        assert result.dates_prefilled is False

    def test_mews_bad_uuid_returns_base_url(self):
        req = _make_request("https://app.mews.com/distributor/not-a-uuid")
        result = generate_deeplink(req)
        assert result.confidence == DeepLinkConfidence.NONE
        assert result.dates_prefilled is False

    def test_preserves_original_url(self):
        original = "https://direct-book.com/properties/test"
        req = _make_request(original)
        result = generate_deeplink(req)
        assert result.original_url == original
