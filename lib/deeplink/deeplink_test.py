"""Unit tests for deep-link URL generation. No DB needed."""

from datetime import date

import pytest

from lib.deeplink.models import DeepLinkConfidence
from services.deeplink.service import create_direct_link, create_proxy_session

pytestmark = pytest.mark.no_db


# --- Direct Link: ResNexus ---


class TestResNexusDirect:
    def test_basic(self):
        result = create_direct_link(
            engine="resnexus",
            property_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
        )
        assert result.engine_name == "ResNexus"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert "a1b2c3d4-e5f6-7890-abcd-ef1234567890" in result.url
        assert "startdate=03/01/2026" in result.url
        assert "nights=2" in result.url
        assert "adults=2" in result.url

    def test_single_night(self):
        result = create_direct_link(
            engine="resnexus",
            property_id="deadbeef-1234-5678-9abc-def012345678",
            checkin=date(2026, 6, 15),
            checkout=date(2026, 6, 16),
            adults=1,
        )
        assert "nights=1" in result.url
        assert "adults=1" in result.url

    def test_long_stay(self):
        result = create_direct_link(
            engine="resnexus",
            property_id="deadbeef-1234-5678-9abc-def012345678",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 15),
            adults=4,
        )
        assert "nights=14" in result.url
        assert "adults=4" in result.url


# --- Proxy Session: ResNexus ---


class TestResNexusProxy:
    def test_creates_session(self):
        result = create_proxy_session(
            engine="resnexus",
            property_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
            proxy_host="localhost:8000",
        )
        assert result.engine_name == "ResNexus"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert result.session_id is not None
        assert "/book/" in result.url
        assert result.session_id in result.url

    def test_session_is_retrievable(self):
        from services.deeplink.service import get_proxy_session

        result = create_proxy_session(
            engine="resnexus",
            property_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
            proxy_host="localhost:8000",
        )
        session = get_proxy_session(result.session_id)
        assert session is not None
        assert session["target_host"] == "resnexus.com"
        assert session["autobook"] is True
        assert session["autobook_engine"] == "resnexus"
        assert "a1b2c3d4-e5f6-7890-abcd-ef1234567890" in session["checkout_path"]
        assert "startdate=03/01/2026" in session["checkout_path"]
        assert "nights=2" in session["checkout_path"]
        assert "adults=2" in session["checkout_path"]

    def test_autobook_disabled(self):
        from services.deeplink.service import get_proxy_session

        result = create_proxy_session(
            engine="resnexus",
            property_id="deadbeef-1234-5678-9abc-def012345678",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
            autobook=False,
            proxy_host="localhost:8000",
        )
        session = get_proxy_session(result.session_id)
        assert session["autobook"] is False

    def test_proxy_url_with_host(self):
        result = create_proxy_session(
            engine="resnexus",
            property_id="deadbeef-1234-5678-9abc-def012345678",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
            proxy_host="myhost.example.com",
        )
        assert result.url.startswith("http://myhost.example.com/book/")

    def test_proxy_url_ngrok(self):
        result = create_proxy_session(
            engine="resnexus",
            property_id="deadbeef-1234-5678-9abc-def012345678",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
            proxy_host="abc123.ngrok.io",
        )
        assert result.url.startswith("https://abc123.ngrok.io/book/")


# --- Direct Link: Cloudbeds ---


class TestCloudbedseDirect:
    def test_basic(self):
        result = create_direct_link(
            engine="cloudbeds",
            property_id="kypwgi",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
        )
        assert result.engine_name == "Cloudbeds"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert "/reservation/kypwgi#" in result.url
        assert "checkin=2026-03-01" in result.url
        assert "checkout=2026-03-03" in result.url
        assert "adults=2" in result.url
        assert "submit=1" in result.url

    def test_with_rate_id(self):
        result = create_direct_link(
            engine="cloudbeds",
            property_id="kypwgi",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
            rate_id="560388",
        )
        assert "room_type_id=560388" in result.url


# --- Direct Link: SiteMinder ---


class TestSiteMinderDirect:
    def test_basic(self):
        result = create_direct_link(
            engine="siteminder",
            property_id="thehindsheaddirect",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
        )
        assert result.engine_name == "SiteMinder"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert "checkInDate=2026-03-01" in result.url
        assert "checkOutDate=2026-03-03" in result.url
        assert "direct-book.com/properties/thehindsheaddirect" in result.url


# --- Direct Link: Mews ---


class TestMewsDirect:
    def test_basic(self):
        result = create_direct_link(
            engine="mews",
            property_id="cb6072cc-1e03-45cc-a6e8-ab0d00ea7979",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
        )
        assert result.engine_name == "Mews"
        assert result.confidence == DeepLinkConfidence.HIGH
        assert result.dates_prefilled is True
        assert "mewsStart=2026-03-01" in result.url
        assert "mewsEnd=2026-03-03" in result.url


# --- Direct Link: RMS ---


class TestRmsDirect:
    def test_basic(self):
        result = create_direct_link(
            engine="rms",
            property_id="bookings13",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            adults=2,
        )
        assert result.engine_name == "RMS Cloud"
        assert result.confidence == DeepLinkConfidence.LOW
        assert result.dates_prefilled is True
        assert "bookings13.rmscloud.com" in result.url


# --- Unknown Engine ---


class TestUnknownEngine:
    def test_returns_empty_url(self):
        result = create_direct_link(
            engine="some_random_engine",
            property_id="abc",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
        )
        assert result.engine_name == "some_random_engine"
        assert result.confidence == DeepLinkConfidence.NONE
        assert result.dates_prefilled is False

    def test_proxy_unknown_engine(self):
        result = create_proxy_session(
            engine="some_random_engine",
            property_id="abc",
            checkin=date(2026, 3, 1),
            checkout=date(2026, 3, 3),
            proxy_host="localhost:8000",
        )
        assert result.confidence == DeepLinkConfidence.NONE


# --- Short Links ---


class TestShortLinks:
    def test_create_and_resolve(self):
        from services.deeplink.service import create_short_link, resolve_short_link

        code = create_short_link("https://example.com/foo")
        assert resolve_short_link(code) == "https://example.com/foo"

    def test_unknown_code(self):
        from services.deeplink.service import resolve_short_link

        assert resolve_short_link("nonexistent") is None
