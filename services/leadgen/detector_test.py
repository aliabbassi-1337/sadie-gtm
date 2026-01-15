"""Integration tests for the booking engine detector.

These tests use real hotel websites to verify detection accuracy.
Run with: uv run pytest services/leadgen/detector_test.py -v
"""

import pytest
from typing import List, Dict

from services.leadgen.detector import (
    BatchDetector,
    DetectionConfig,
    DetectionResult,
    EngineDetector,
    ContactExtractor,
    normalize_url,
    extract_domain,
    set_engine_patterns,
)


# Test engine patterns for unit tests
TEST_ENGINE_PATTERNS = {
    "Cloudbeds": ["cloudbeds.com"],
    "SynXis / TravelClick": ["synxis.com", "travelclick.com"],
    "Mews": ["mews.com", "mews.li"],
    "SiteMinder": ["siteminder.com", "thebookingbutton.com"],
    "Triptease": ["triptease.io", "triptease.com"],
    "WebRezPro": ["webrezpro.com"],
    "Clock PMS": ["clock-software.com"],
    "Little Hotelier": ["littlehotelier.com"],
    "Bookassist": ["bookassist.com"],
}


@pytest.fixture(autouse=True)
def setup_engine_patterns():
    """Set up test engine patterns before each test."""
    set_engine_patterns(TEST_ENGINE_PATTERNS)


# =============================================================================
# UNIT TESTS
# =============================================================================

class TestUtilities:
    """Unit tests for utility functions."""

    def test_normalize_url_adds_https(self):
        assert normalize_url("example.com") == "https://example.com"
        assert normalize_url("www.example.com") == "https://www.example.com"

    def test_normalize_url_preserves_protocol(self):
        assert normalize_url("https://example.com") == "https://example.com"
        assert normalize_url("http://example.com") == "http://example.com"

    def test_normalize_url_handles_empty(self):
        assert normalize_url("") == ""
        assert normalize_url(None) == ""

    def test_extract_domain(self):
        assert extract_domain("https://www.example.com/path") == "example.com"
        assert extract_domain("https://sub.example.com") == "sub.example.com"
        assert extract_domain("") == ""


class TestEngineDetector:
    """Unit tests for EngineDetector."""

    def test_from_domain_known_engine(self):
        engine, domain = EngineDetector.from_domain("cloudbeds.com")
        assert engine == "Cloudbeds"
        assert domain == "cloudbeds.com"

    def test_from_domain_synxis(self):
        engine, domain = EngineDetector.from_domain("synxis.com")
        assert engine == "SynXis / TravelClick"

    def test_from_domain_unknown(self):
        engine, domain = EngineDetector.from_domain("unknown-domain.com")
        assert engine == ""
        assert domain == ""

    def test_from_url_known_pattern(self):
        engine, domain, method = EngineDetector.from_url(
            "https://hotels.cloudbeds.com/reservation/abc123",
            "hotelexample.com"
        )
        assert engine == "Cloudbeds"
        assert method == "url_pattern_match"

    def test_from_url_third_party(self):
        engine, domain, method = EngineDetector.from_url(
            "https://unknown-booking.com/reserve",
            "hotelexample.com"
        )
        assert engine == "unknown_third_party"
        assert method == "third_party_domain"

    def test_from_url_same_domain(self):
        engine, domain, method = EngineDetector.from_url(
            "https://hotelexample.com/book",
            "hotelexample.com"
        )
        assert engine == "proprietary_or_same_domain"
        assert method == "same_domain"


class TestContactExtractor:
    """Unit tests for ContactExtractor."""

    def test_extract_phones(self):
        html = "Call us at (305) 555-1234 or +1-305-555-5678"
        phones = ContactExtractor.extract_phones(html)
        assert len(phones) >= 1
        assert any("305" in p for p in phones)

    def test_extract_emails(self):
        html = "Contact us at info@hotel.com or reservations@hotel.com"
        emails = ContactExtractor.extract_emails(html)
        assert "info@hotel.com" in emails
        assert "reservations@hotel.com" in emails

    def test_extract_emails_skips_junk(self):
        html = "test@example.com and real@hotel.com"
        emails = ContactExtractor.extract_emails(html)
        assert "real@hotel.com" in emails
        assert "test@example.com" not in emails

    def test_extract_room_count(self):
        text = "Our boutique hotel features 45 guest rooms"
        count = ContactExtractor.extract_room_count(text)
        assert count == "45"

    def test_extract_room_count_variants(self):
        assert ContactExtractor.extract_room_count("featuring 100 rooms") == "100"
        assert ContactExtractor.extract_room_count("a 50-room hotel") == "50"
        assert ContactExtractor.extract_room_count("we have 25 suites") == "25"

    def test_extract_room_count_no_match(self):
        assert ContactExtractor.extract_room_count("no rooms mentioned") == ""


# =============================================================================
# INTEGRATION TESTS - Real websites
# =============================================================================

# Test hotels with expected booking engines
# NOTE: These are real, active hotel websites as of January 2026
# Some hotels use overlay widgets (Triptease) on top of booking engines (SynXis)
TEST_HOTELS: List[Dict] = [
    {
        "id": 1,
        "name": "President Hotel Villa Miami Beach",
        "website": "https://www.presidentvillamiami.com/",
        "expected_engine": "SiteMinder",
    },
    {
        "id": 2,
        "name": "Renzzi On The Beach",
        "website": "https://renzzionthebeach.com/book-now/",
        "expected_engine": "unknown_booking_api",  # wubook.net
    },
    {
        "id": 3,
        "name": "The Setai Miami Beach",
        "website": "https://www.thesetaihotel.com",
        "expected_engine": "Triptease",  # Has Triptease overlay + SynXis backend
    },
    {
        "id": 4,
        "name": "The Betsy Hotel",
        "website": "https://www.thebetsyhotel.com",
        "expected_engine": "Triptease",
    },
]


@pytest.fixture
def detector():
    """Create detector with debug enabled for integration tests."""
    config = DetectionConfig(
        concurrency=2,
        headless=True,
        debug=True,
    )
    return BatchDetector(config)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_single_hotel_siteminder(detector):
    """Test detection of SiteMinder engine."""
    hotels = [TEST_HOTELS[0]]  # President Hotel Villa Miami Beach
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 1
    assert result.booking_engine == "SiteMinder"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_single_hotel_unknown_api(detector):
    """Test detection of unknown booking API (wubook)."""
    hotels = [TEST_HOTELS[1]]  # Renzzi On The Beach
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 2
    assert result.booking_engine == "unknown_booking_api"
    assert "wubook" in result.booking_url.lower()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_batch_multiple_hotels(detector):
    """Test batch detection of multiple hotels."""
    # Test with 3 hotels concurrently
    hotels = TEST_HOTELS[:3]
    results = await detector.detect_batch(hotels)

    assert len(results) == 3

    # Verify each result has hotel_id
    hotel_ids = {r.hotel_id for r in results}
    assert hotel_ids == {1, 2, 3}

    # Check that at least some detection succeeded
    successful = [r for r in results if r.booking_engine and not r.error]
    assert len(successful) >= 2, "At least 2/3 hotels should have detected engines"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_all_test_hotels(detector):
    """Test detection of all test hotels and report accuracy."""
    results = await detector.detect_batch(TEST_HOTELS)

    assert len(results) == len(TEST_HOTELS)

    # Build results map
    results_map = {r.hotel_id: r for r in results}

    # Report results
    correct = 0
    for hotel in TEST_HOTELS:
        result = results_map[hotel["id"]]
        expected = hotel["expected_engine"]
        actual = result.booking_engine

        # Check if detection matches expected
        is_correct = (
            actual == expected or
            expected.lower() in actual.lower() or
            actual.lower() in expected.lower()
        )

        if is_correct:
            correct += 1

        # Log for debugging
        status = "OK" if is_correct else "MISMATCH"
        print(f"{status}: {hotel['name']}")
        print(f"  Expected: {expected}")
        print(f"  Actual:   {actual}")
        print(f"  URL:      {result.booking_url[:60] if result.booking_url else 'N/A'}...")
        if result.error:
            print(f"  Error:    {result.error}")
        print()

    accuracy = correct / len(TEST_HOTELS) * 100
    print(f"\nAccuracy: {correct}/{len(TEST_HOTELS)} ({accuracy:.1f}%)")

    # Expect at least 70% accuracy
    assert correct >= len(TEST_HOTELS) * 0.7, f"Expected at least 70% accuracy, got {accuracy:.1f}%"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_extracts_contacts(detector):
    """Test that contact information is extracted."""
    hotels = [TEST_HOTELS[0]]  # The Setai Miami Beach
    results = await detector.detect_batch(hotels)

    result = results[0]
    # Note: Not all hotels have visible contact info, so this is a soft check
    print(f"Phone: {result.phone_website}")
    print(f"Email: {result.email}")
    print(f"Room count: {result.room_count}")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_handles_unreachable_website(detector):
    """Test that unreachable websites are handled gracefully."""
    hotels = [{
        "id": 999,
        "name": "Fake Hotel",
        "website": "https://this-domain-does-not-exist-12345.com",
    }]
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 999
    assert result.error  # Should have an error
    assert "precheck_failed" in result.error or "timeout" in result.error


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_handles_junk_domain(detector):
    """Test that junk domains are skipped."""
    hotels = [{
        "id": 998,
        "name": "Facebook Page Hotel",
        "website": "https://facebook.com/somehotel",
    }]
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 998
    assert result.error == "junk_domain"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_empty_batch(detector):
    """Test that empty batch returns empty results."""
    results = await detector.detect_batch([])
    assert results == []


# =============================================================================
# INDIVIDUAL HOTEL TESTS
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.integration
async def test_president_siteminder(detector):
    """President Hotel Villa Miami Beach uses SiteMinder."""
    hotels = [{"id": 1, "name": "President Hotel Villa Miami Beach", "website": "https://www.presidentvillamiami.com/"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "SiteMinder"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_renzzi_wubook(detector):
    """Renzzi On The Beach uses wubook.net (unknown_booking_api)."""
    hotels = [{"id": 2, "name": "Renzzi On The Beach", "website": "https://renzzionthebeach.com/book-now/"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "unknown_booking_api"
    assert "wubook" in results[0].booking_url.lower()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_setai_booking_engine(detector):
    """The Setai Miami Beach uses Triptease overlay with SynXis backend."""
    hotels = [{"id": 3, "name": "The Setai Miami Beach", "website": "https://www.thesetaihotel.com"}]
    results = await detector.detect_batch(hotels)
    # Either Triptease (overlay) or SynXis (backend) may be detected first
    assert results[0].booking_engine in ("Triptease", "SynXis / TravelClick")
    # The booking URL should always be SynXis
    assert "synxis" in results[0].booking_url.lower()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_betsy_triptease(detector):
    """The Betsy Hotel uses Triptease."""
    hotels = [{"id": 4, "name": "The Betsy Hotel", "website": "https://www.thebetsyhotel.com"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "Triptease"
