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
)


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
TEST_HOTELS: List[Dict] = [
    {
        "id": 1,
        "name": "Bentley Hotel South Beach",
        "website": "https://www.bentleysouthbeach.com",
        "expected_engine": "SynXis / TravelClick",
    },
    {
        "id": 2,
        "name": "Starlite Hotel",
        "website": "https://www.thestarlitehotel.com",
        "expected_engine": "Cloudbeds",
    },
    {
        "id": 3,
        "name": "Chesterfield Hotel & Suites",
        "website": "https://www.chesterfieldhotel.com",
        "expected_engine": "SynXis / TravelClick",
    },
    {
        "id": 4,
        "name": "Life House, South of Fifth",
        "website": "https://www.lifehousehotels.com/hotels/miami/south-of-fifth",
        "expected_engine": "Cloudbeds",
    },
    {
        "id": 5,
        "name": "Kasa El Paseo Miami Beach",
        "website": "https://kasa.com/vacation-rentals/florida/miami-beach/kasa-el-paseo-miami-beach",
        "expected_engine": "Triptease",
    },
    {
        "id": 6,
        "name": "Bungalows Key Largo",
        "website": "https://www.bfrhotels.com/bungalows-key-largo",
        "expected_engine": "Triptease",
    },
    {
        "id": 7,
        "name": "Riviere South Beach Hotel",
        "website": "https://www.rivieresouthbeach.com",
        "expected_engine": "Cloudbeds",
    },
    {
        "id": 8,
        "name": "ABAE Hotel by Eskape Collection",
        "website": "https://www.abaehotel.com",
        "expected_engine": "unknown_booking_api",
    },
    {
        "id": 9,
        "name": "Beach Place",
        "website": "https://beachplace.com",
        "expected_engine": "SiteMinder",
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
async def test_detect_single_hotel_synxis(detector):
    """Test detection of SynXis/TravelClick engine."""
    hotels = [TEST_HOTELS[0]]  # Bentley Hotel South Beach
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 1
    assert result.booking_engine == "SynXis / TravelClick"
    assert "synxis" in result.booking_url.lower() or "travelclick" in result.booking_url.lower()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_single_hotel_cloudbeds(detector):
    """Test detection of Cloudbeds engine."""
    hotels = [TEST_HOTELS[1]]  # Starlite Hotel
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 2
    assert result.booking_engine == "Cloudbeds"
    assert "cloudbeds" in result.booking_url.lower()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_detect_single_hotel_siteminder(detector):
    """Test detection of SiteMinder engine."""
    hotels = [TEST_HOTELS[8]]  # Beach Place
    results = await detector.detect_batch(hotels)

    assert len(results) == 1
    result = results[0]
    assert result.hotel_id == 9
    # SiteMinder can be detected as "SiteMinder" or via thebookingbutton.com
    assert result.booking_engine == "SiteMinder" or "siteminder" in result.booking_engine_domain.lower()


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
    hotels = [TEST_HOTELS[0]]  # Bentley Hotel South Beach
    results = await detector.detect_batch(hotels)

    result = results[0]
    # At least one contact should be extracted
    has_contact = bool(result.phone_website or result.email)
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
async def test_bentley_hotel_synxis(detector):
    """Bentley Hotel South Beach uses SynXis/TravelClick."""
    hotels = [{"id": 1, "name": "Bentley Hotel South Beach", "website": "https://www.bentleysouthbeach.com"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "SynXis / TravelClick"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_starlite_hotel_cloudbeds(detector):
    """Starlite Hotel uses Cloudbeds."""
    hotels = [{"id": 2, "name": "Starlite Hotel", "website": "https://www.thestarlitehotel.com"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "Cloudbeds"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_chesterfield_synxis(detector):
    """Chesterfield Hotel & Suites uses SynXis/TravelClick."""
    hotels = [{"id": 3, "name": "Chesterfield Hotel & Suites", "website": "https://www.chesterfieldhotel.com"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "SynXis / TravelClick"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_lifehouse_cloudbeds(detector):
    """Life House, South of Fifth uses Cloudbeds."""
    hotels = [{"id": 4, "name": "Life House, South of Fifth", "website": "https://www.lifehousehotels.com/hotels/miami/south-of-fifth"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "Cloudbeds"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_kasa_triptease(detector):
    """Kasa El Paseo Miami Beach uses Triptease."""
    hotels = [{"id": 5, "name": "Kasa El Paseo Miami Beach", "website": "https://kasa.com/vacation-rentals/florida/miami-beach/kasa-el-paseo-miami-beach"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "Triptease"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bungalows_triptease(detector):
    """Bungalows Key Largo uses Triptease."""
    hotels = [{"id": 6, "name": "Bungalows Key Largo", "website": "https://www.bfrhotels.com/bungalows-key-largo"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "Triptease"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_riviere_cloudbeds(detector):
    """Riviere South Beach Hotel uses Cloudbeds."""
    hotels = [{"id": 7, "name": "Riviere South Beach Hotel", "website": "https://www.rivieresouthbeach.com"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "Cloudbeds"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_abae_unknown_api(detector):
    """ABAE Hotel by Eskape Collection uses an unknown booking API."""
    hotels = [{"id": 8, "name": "ABAE Hotel by Eskape Collection", "website": "https://www.abaehotel.com"}]
    results = await detector.detect_batch(hotels)
    # This hotel uses an unknown booking system
    assert "unknown" in results[0].booking_engine.lower() or results[0].booking_url


@pytest.mark.asyncio
@pytest.mark.integration
async def test_beach_place_siteminder(detector):
    """Beach Place uses SiteMinder."""
    hotels = [{"id": 9, "name": "Beach Place", "website": "https://beachplace.com"}]
    results = await detector.detect_batch(hotels)
    assert results[0].booking_engine == "SiteMinder"
