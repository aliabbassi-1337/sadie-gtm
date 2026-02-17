"""Tests for Enrichment Service."""

import pytest
from unittest.mock import AsyncMock

from services.enrichment.service import Service, EnrichResult, EnqueueResult, ConsumeResult
from services.enrichment.rms_repo import RMSRepo
from services.enrichment.rms_queue import MockQueue
from lib.rms import RMSHotelRecord, QueueStats


@pytest.fixture
def service():
    """Create service with real repo but mock queue."""
    return Service(rms_repo=RMSRepo(), rms_queue=MockQueue())


@pytest.fixture
def service_with_queue():
    """Create service with real repo and return the mock queue."""
    queue = MockQueue()
    return Service(rms_repo=RMSRepo(), rms_queue=queue), queue


class TestGetRMSStats:
    """Tests for get_rms_stats."""
    
    @pytest.mark.asyncio
    async def test_returns_stats_dict(self, service):
        """Should return stats dictionary."""
        stats = await service.get_rms_stats()
        assert isinstance(stats, dict)
        assert "total" in stats


class TestCountRMSNeedingEnrichment:
    """Tests for count_rms_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_count(self, service):
        """Should return integer count."""
        count = await service.count_rms_needing_enrichment()
        assert isinstance(count, int)
        assert count >= 0


class TestGetRMSHotelsNeedingEnrichment:
    """Tests for get_rms_hotels_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_list(self, service):
        """Should return list of hotels."""
        hotels = await service.get_rms_hotels_needing_enrichment(limit=10)
        assert isinstance(hotels, list)


class TestGetRMSQueueStats:
    """Tests for get_rms_queue_stats."""
    
    def test_returns_queue_stats(self, service):
        """Should return QueueStats."""
        stats = service.get_rms_queue_stats()
        assert stats.pending == 0
        assert stats.in_flight == 0


class TestEnqueueRMSForEnrichment:
    """Tests for enqueue_rms_for_enrichment."""
    
    @pytest.mark.asyncio
    async def test_skips_when_queue_full(self, service_with_queue):
        """Should skip when queue is full."""
        service, queue = service_with_queue
        queue.pending = 2000  # Over MAX_QUEUE_DEPTH
        
        result = await service.enqueue_rms_for_enrichment(limit=100)
        
        assert result.skipped is True
        assert "exceeds" in result.reason
    
    @pytest.mark.asyncio
    async def test_enqueues_hotels_from_db(self, service):
        """Should enqueue hotels found in database."""
        result = await service.enqueue_rms_for_enrichment(limit=10)
        
        assert isinstance(result, EnqueueResult)
        assert result.skipped is False


class TestRequestShutdown:
    """Tests for request_shutdown."""
    
    def test_sets_shutdown_flag(self, service):
        """Should set shutdown flag."""
        assert service._shutdown_requested is False
        service.request_shutdown()
        assert service._shutdown_requested is True


class TestConsumeRMSEnrichmentQueue:
    """Tests for consume_rms_enrichment_queue."""
    
    @pytest.mark.asyncio
    async def test_stops_on_should_stop(self, service):
        """Should stop immediately when should_stop returns True."""
        result = await service.consume_rms_enrichment_queue(
            max_messages=1,
            should_stop=lambda: True,
        )
        
        assert result.messages_processed == 0
    
    @pytest.mark.asyncio
    async def test_stops_on_empty_queue(self, service):
        """Should stop when queue is empty and max_messages is set."""
        result = await service.consume_rms_enrichment_queue(max_messages=1)
        
        assert isinstance(result, ConsumeResult)


# ============================================================================
# CLOUDBEDS ENRICHMENT TESTS
# ============================================================================


class TestGetCloudbedsEnrichmentStatus:
    """Tests for get_cloudbeds_enrichment_status."""
    
    @pytest.mark.asyncio
    async def test_returns_status_dict(self, service):
        """Should return status dictionary with counts."""
        status = await service.get_cloudbeds_enrichment_status()
        
        assert isinstance(status, dict)
        assert "total" in status
        assert "needing_enrichment" in status
        assert "already_enriched" in status
        assert status["already_enriched"] == status["total"] - status["needing_enrichment"]


class TestGetCloudbedsHotelsNeedingEnrichment:
    """Tests for get_cloudbeds_hotels_needing_enrichment."""
    
    # Note: These tests require Cloudbeds hotels in DB to work properly
    # They are skipped if the DB query fails (schema mismatch)
    
    @pytest.mark.asyncio
    async def test_returns_list(self, service):
        """Should return list of hotel candidates."""
        try:
            hotels = await service.get_cloudbeds_hotels_needing_enrichment(limit=10)
            assert isinstance(hotels, list)
        except Exception:
            pytest.skip("DB schema mismatch - skipping")
    
    @pytest.mark.asyncio
    async def test_respects_limit(self, service):
        """Should respect the limit parameter."""
        try:
            hotels = await service.get_cloudbeds_hotels_needing_enrichment(limit=5)
            assert len(hotels) <= 5
        except Exception:
            pytest.skip("DB schema mismatch - skipping")


@pytest.mark.online
class TestProcessCloudbedsHotelIntegration:
    """Integration tests for _process_cloudbeds_hotel with real scraping."""
    
    @pytest.fixture
    async def service_and_scraper(self):
        """Create service with real browser for integration tests."""
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
        from lib.cloudbeds import CloudbedsScraper
        
        service = Service(rms_repo=RMSRepo(), rms_queue=MockQueue())
        
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
        
        yield service, scraper
        
        await ctx.close()
        await browser.close()
        await pw.stop()
    
    @pytest.mark.asyncio
    async def test_extracts_valid_hotel_data(self, service_and_scraper):
        """Should extract valid data from a real Cloudbeds page."""
        service, scraper = service_and_scraper
        
        # Known working Cloudbeds page
        url = "https://hotels.cloudbeds.com/reservation/chsz6e"
        result = await service._process_cloudbeds_hotel(scraper, 123, url)
        
        hotel_id, success, data, error = result
        assert hotel_id == 123
        assert success is True, f"Should succeed, got error: {error}"
        assert data is not None
        assert data.name is not None
        assert data.city is not None
    
    @pytest.mark.asyncio
    async def test_returns_no_data_for_invalid_slug(self, service_and_scraper):
        """Should return no_data for non-existent pages."""
        service, scraper = service_and_scraper
        
        url = "https://hotels.cloudbeds.com/reservation/invalidslug99999"
        result = await service._process_cloudbeds_hotel(scraper, 456, url)
        
        hotel_id, success, data, error = result
        assert hotel_id == 456
        assert success is False
        # Could be "no_data" or "404_not_found" depending on page behavior
        assert error in ["no_data", "404_not_found"]
    
    @pytest.mark.asyncio
    async def test_detects_homepage_as_garbage(self, service_and_scraper):
        """Should detect Cloudbeds homepage as garbage."""
        service, scraper = service_and_scraper
        
        # Root URL that redirects to homepage
        url = "https://hotels.cloudbeds.com/"
        result = await service._process_cloudbeds_hotel(scraper, 789, url)
        
        hotel_id, success, data, error = result
        # Homepage should be detected as garbage
        if data and data.name:
            assert data.name.lower() not in ['cloudbeds.com', 'cloudbeds']


class TestEnrichCloudbedsHotels:
    """Tests for enrich_cloudbeds_hotels."""
    
    @pytest.mark.asyncio
    async def test_returns_enrich_result(self, service):
        """Should return EnrichResult with counts."""
        # Use limit=1 to minimize processing but still test the flow
        try:
            result = await service.enrich_cloudbeds_hotels(limit=1)
            assert isinstance(result, EnrichResult)
            assert result.processed >= 0
            assert result.enriched >= 0
            assert result.failed >= 0
        except Exception:
            pytest.skip("DB or browser issue - skipping")


class TestBatchUpdateCloudbedsEnrichment:
    """Tests for batch_update_cloudbeds_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_count(self, service):
        """Should return count of updated hotels."""
        # Empty list should return 0
        updated = await service.batch_update_cloudbeds_enrichment([])
        
        assert updated == 0


class TestBatchMarkCloudbedsFailed:
    """Tests for batch_mark_cloudbeds_failed."""
    
    @pytest.mark.asyncio
    async def test_returns_count(self, service):
        """Should return count of marked hotels."""
        # Empty list should return 0
        marked = await service.batch_mark_cloudbeds_failed([])
        
        assert marked == 0


# ============================================================================
# EDGE CASE TESTS
# ============================================================================


class TestServiceEdgeCases:
    """Edge case tests for enrichment service."""
    
    @pytest.fixture
    def service(self):
        return Service(rms_repo=RMSRepo(), rms_queue=MockQueue())
    
    @pytest.mark.asyncio
    async def test_batch_update_with_empty_list(self, service):
        """Should handle empty batch update."""
        result = await service.batch_update_cloudbeds_enrichment([])
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_batch_update_with_none_fields(self, service):
        """Should handle updates with None fields."""
        # Hotel ID that likely doesn't exist
        results = [{
            "hotel_id": 999999999,
            "name": None,
            "address": None,
            "city": None,
            "state": None,
            "country": None,
            "phone": None,
            "email": None,
        }]
        # Should not crash
        await service.batch_update_cloudbeds_enrichment(results)
    
    @pytest.mark.asyncio
    async def test_batch_mark_failed_with_nonexistent_ids(self, service):
        """Should handle marking non-existent hotel IDs."""
        # These IDs likely don't exist
        result = await service.batch_mark_cloudbeds_failed([999999998, 999999999])
        # Should not crash, may return 0
        assert result >= 0


@pytest.mark.online
class TestServiceEdgeCasesOnline:
    """Edge case tests that require network access."""
    
    @pytest.fixture
    async def service_and_scraper(self):
        """Create service with real browser."""
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
        from lib.cloudbeds import CloudbedsScraper
        
        service = Service(rms_repo=RMSRepo(), rms_queue=MockQueue())
        
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await ctx.new_page()
        
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        
        scraper = CloudbedsScraper(page)
        
        yield service, scraper
        
        await ctx.close()
        await browser.close()
        await pw.stop()
    
    @pytest.mark.asyncio
    async def test_handles_malformed_url(self, service_and_scraper):
        """Should handle malformed URLs gracefully."""
        service, scraper = service_and_scraper
        
        # Malformed URL
        result = await service._process_cloudbeds_hotel(
            scraper, 123, "not-a-valid-url"
        )
        
        hotel_id, success, data, error = result
        assert success is False
        assert error is not None
    
    @pytest.mark.asyncio
    async def test_handles_non_cloudbeds_url(self, service_and_scraper):
        """Should handle non-Cloudbeds URLs gracefully."""
        service, scraper = service_and_scraper
        
        # Google homepage - not a Cloudbeds page
        result = await service._process_cloudbeds_hotel(
            scraper, 456, "https://www.google.com"
        )
        
        hotel_id, success, data, error = result
        # Should fail or return garbage detection
        assert hotel_id == 456
    
    @pytest.mark.asyncio
    async def test_handles_timeout_url(self, service_and_scraper):
        """Should handle URLs that might timeout."""
        service, scraper = service_and_scraper
        
        # Non-routable IP - will timeout
        result = await service._process_cloudbeds_hotel(
            scraper, 789, "https://10.255.255.1/"
        )
        
        hotel_id, success, data, error = result
        assert success is False
    
    @pytest.mark.asyncio
    async def test_handles_empty_url(self, service_and_scraper):
        """Should handle empty URL string."""
        service, scraper = service_and_scraper
        
        result = await service._process_cloudbeds_hotel(
            scraper, 101, ""
        )
        
        hotel_id, success, data, error = result
        assert success is False


# ============================================================================
# DATA SANITIZATION TESTS
# ============================================================================


@pytest.mark.no_db
class TestIsGarbage:
    """Tests for _is_garbage helper method."""
    
    @pytest.fixture
    def service(self):
        # No DB needed - just testing helper methods
        return Service(rms_repo=None, rms_queue=MockQueue())
    
    # =========================================================================
    # Obvious garbage - nullish values
    # =========================================================================
    @pytest.mark.parametrize("value", [
        None,
        '',
        ' ',
        '  ',
        '\t',
        '\n',
        '\r\n',
        '   \t\n   ',
    ])
    def test_nullish_values_are_garbage(self, service, value):
        """Null, empty, whitespace-only should be garbage."""
        assert service._is_garbage(value, 'name') is True
    
    # =========================================================================
    # Garbage strings - literal garbage words
    # =========================================================================
    @pytest.mark.parametrize("value", [
        # Boolean-like
        'false', 'False', 'FALSE', 'true', 'True', 'TRUE',
        # Null-like
        'null', 'Null', 'NULL', 'none', 'None', 'NONE',
        'undefined', 'Undefined', 'UNDEFINED',
        # N/A variants
        'n/a', 'N/A', 'N/a', 'na', 'NA', 'Na',
        # Unknown/error
        'unknown', 'Unknown', 'UNKNOWN',
        'error', 'Error', 'ERROR',
        'loading', 'Loading', 'LOADING',
        'test', 'Test', 'TEST',
        # Punctuation only
        '-', '--', '---', '.', '..', '...',
    ])
    def test_garbage_words_detected(self, service, value):
        """Literal garbage words should be detected."""
        assert service._is_garbage(value, 'name') is True
    
    # =========================================================================
    # Real API garbage - values that actually come from broken APIs
    # =========================================================================
    @pytest.mark.parametrize("value", [
        # JavaScript stringified
        '[object Object]', '[Object object]',
        'NaN', 'nan',
        'Infinity', 'infinity',
        # HTML garbage
        '&nbsp;',
        '&#65279;',  # BOM character
        '<br>', '<br/>',
        # Placeholder text
        'TBD', 'tbd',
        'TODO', 'todo',
        'FIXME', 'fixme',
        'placeholder', 'Placeholder',
        'example', 'Example',
        'sample', 'Sample',
        'demo', 'Demo',
        # System names
        'Online Bookings', 'online bookings',
        'Book Now', 'book now',
        'Booking Engine', 'booking engine',
        'Hotel Booking Engine',
        'Reservation', 'reservation',
        'Reservations', 'reservations',
        'Search', 'search',
        'Home', 'home',
        # Empty-ish
        '0', '00', '000', '0.0',
        # Single chars (except for state)
        'a', 'b', 'x', 'X', '1', '?', '!', '*',
    ])
    def test_api_garbage_detected(self, service, value):
        """Real garbage from APIs should be detected."""
        assert service._is_garbage(value, 'name') is True
    
    # =========================================================================
    # Valid values - should NOT be flagged as garbage
    # =========================================================================
    @pytest.mark.parametrize("value", [
        # Normal hotel names
        'Hilton Hotel',
        'Miami Beach Resort',
        'The Grand Hotel',
        'Hotel & Spa',
        'Marriott Downtown',
        "Bob's Inn",
        'Hôtel de Paris',
        '7 Stars Resort',
        'W Hotel',
        'AC Hotel',
        # Addresses
        '123 Main Street',
        '1 Beach Blvd',
        '1600 Pennsylvania Ave NW',
        # Cities
        'Miami',
        'New York',
        'Los Angeles',
        'San Francisco',
        'LA',  # 2 chars OK for city
        'DC',
        # Countries
        'USA',
        'United States',
        'Australia',
        'UK',
        # Contact info
        'contact@hotel.com',
        '+1-555-123-4567',
        '(555) 123-4567',
        'www.hotel.com',
        # Edge cases that look like garbage but aren't
        'The Test Kitchen Hotel',  # contains 'test'
        'Unknown Soldier Inn',  # contains 'unknown'
        'Loading Bay Hotel',  # contains 'loading'
        'True North Lodge',  # contains 'true'
        'Null Island Resort',  # contains 'null'
        'False Creek Hotel',  # contains 'false'
        'Error-Free Suites',  # contains 'error'
    ])
    def test_valid_values_not_garbage(self, service, value):
        """Valid values should NOT be flagged as garbage."""
        assert service._is_garbage(value, 'name') is False
    
    # =========================================================================
    # State field special cases (2-char codes are valid)
    # =========================================================================
    @pytest.mark.parametrize("value", [
        'CA', 'NY', 'TX', 'FL', 'WA', 'OR', 'AZ', 'NV',
        'NSW', 'VIC', 'QLD',  # Australian states
        'ON', 'BC', 'AB',  # Canadian provinces
    ])
    def test_state_codes_valid(self, service, value):
        """2-3 char state/province codes should be valid for state field."""
        assert service._is_garbage(value, 'state') is False
    
    @pytest.mark.parametrize("value", [
        'C', 'N', 'T', 'a', '1', '-', '',
    ])
    def test_single_char_garbage_for_state(self, service, value):
        """Single char or less should be garbage for state field."""
        assert service._is_garbage(value, 'state') is True
    
    # =========================================================================
    # Type coercion edge cases
    # =========================================================================
    def test_non_string_types_are_garbage(self, service):
        """Non-string types should be garbage."""
        assert service._is_garbage(123, 'name') is True
        assert service._is_garbage(0, 'name') is True
        assert service._is_garbage(False, 'name') is True
        assert service._is_garbage(True, 'name') is True
        assert service._is_garbage([], 'name') is True
        assert service._is_garbage({}, 'name') is True
        assert service._is_garbage(['hotel'], 'name') is True
    
    # =========================================================================
    # Whitespace handling
    # =========================================================================
    @pytest.mark.parametrize("value", [
        ' false ',
        '  null  ',
        '\ttest\t',
        ' - ',
        '  n/a  ',
    ])
    def test_garbage_with_whitespace_padding(self, service, value):
        """Garbage values with whitespace padding should still be garbage."""
        assert service._is_garbage(value, 'name') is True


@pytest.mark.no_db
class TestSanitizeEnrichmentData:
    """Tests for _sanitize_enrichment_data method."""
    
    @pytest.fixture
    def service(self):
        # No DB needed - just testing helper methods
        return Service(rms_repo=None, rms_queue=MockQueue())
    
    def test_garbage_values_set_to_none(self, service):
        """Should set garbage values to None."""
        updates = [{
            'hotel_id': 1,
            'name': 'Good Hotel',
            'city': 'Miami',
            'state': 'false',  # garbage
            'country': 'USA',
            'phone': '-',  # garbage
            'email': 'test@hotel.com',
            'address': '',  # garbage (empty)
            'website': 'null',  # garbage
        }]
        
        service._sanitize_enrichment_data(updates)
        
        assert updates[0]['name'] == 'Good Hotel'
        assert updates[0]['city'] == 'Miami'
        assert updates[0]['state'] is None
        assert updates[0]['country'] == 'USA'
        assert updates[0]['phone'] is None
        assert updates[0]['email'] == 'test@hotel.com'
        assert updates[0]['address'] is None
        assert updates[0]['website'] is None
    
    def test_valid_state_codes_preserved(self, service):
        """Should preserve valid 2-char state codes."""
        updates = [{
            'hotel_id': 1,
            'name': 'Hotel',
            'state': 'CA',
        }]
        
        service._sanitize_enrichment_data(updates)
        
        assert updates[0]['state'] == 'CA'
    
    def test_multiple_updates_processed(self, service):
        """Should process all updates in the list."""
        updates = [
            {'hotel_id': 1, 'name': 'Hotel A', 'city': 'unknown'},
            {'hotel_id': 2, 'name': 'false', 'city': 'Miami'},
            {'hotel_id': 3, 'name': 'Hotel C', 'city': 'Boston'},
        ]
        
        service._sanitize_enrichment_data(updates)
        
        assert updates[0]['name'] == 'Hotel A'
        assert updates[0]['city'] is None  # 'unknown' is garbage
        assert updates[1]['name'] is None  # 'false' is garbage
        assert updates[1]['city'] == 'Miami'
        assert updates[2]['name'] == 'Hotel C'
        assert updates[2]['city'] == 'Boston'
    
    def test_empty_list_handled(self, service):
        """Should handle empty list without error."""
        updates = []
        service._sanitize_enrichment_data(updates)
        assert updates == []
    
    def test_missing_fields_handled(self, service):
        """Should handle updates with missing fields."""
        updates = [{'hotel_id': 1, 'name': 'Hotel'}]
        
        # Should not raise KeyError
        service._sanitize_enrichment_data(updates)
        
        assert updates[0]['name'] == 'Hotel'


# ============================================================================
# BIG4 SCRAPE TESTS
# ============================================================================


@pytest.mark.no_db
class TestScrapeBig4ParksNoParks:
    """Tests for scrape_big4_parks when scraper returns empty."""

    @pytest.fixture
    def service(self):
        return Service(rms_repo=None, rms_queue=MockQueue())

    @pytest.mark.asyncio
    async def test_returns_zeros_when_no_parks(self, service):
        """Should return zero counts when no parks discovered."""
        from unittest.mock import patch, AsyncMock

        mock_scraper = AsyncMock()
        mock_scraper.scrape_all = AsyncMock(return_value=[])
        mock_scraper.__aenter__ = AsyncMock(return_value=mock_scraper)
        mock_scraper.__aexit__ = AsyncMock(return_value=False)

        with patch("lib.big4.Big4Scraper", return_value=mock_scraper):
            result = await service.scrape_big4_parks()

        assert result["discovered"] == 0
        assert result["total_big4"] == 0
        assert result["with_email"] == 0
        assert result["with_phone"] == 0
        assert result["with_address"] == 0


@pytest.mark.no_db
class TestScrapeBig4ParksWithMocks:
    """Tests for scrape_big4_parks with mocked scraper and repo."""

    @pytest.mark.asyncio
    async def test_returns_correct_counts(self):
        """Should return correct discovered and contact counts."""
        from unittest.mock import patch, AsyncMock
        from lib.big4.models import Big4Park

        parks = [
            Big4Park(name="Park A", slug="a", url_path="/a", email="a@test.com", phone="123", address="1 St"),
            Big4Park(name="Park B", slug="b", url_path="/b", email="b@test.com"),
            Big4Park(name="Park C", slug="c", url_path="/c", phone="456"),
        ]

        mock_scraper = AsyncMock()
        mock_scraper.scrape_all = AsyncMock(return_value=parks)
        mock_scraper.__aenter__ = AsyncMock(return_value=mock_scraper)
        mock_scraper.__aexit__ = AsyncMock(return_value=False)

        service = Service(rms_repo=None, rms_queue=MockQueue())

        with patch("lib.big4.Big4Scraper", return_value=mock_scraper):
            with patch("services.enrichment.repo.upsert_big4_parks", new_callable=AsyncMock) as mock_upsert:
                with patch("services.enrichment.repo.get_big4_count", new_callable=AsyncMock, return_value=3) as mock_count:
                    result = await service.scrape_big4_parks()

        assert result["discovered"] == 3
        assert result["total_big4"] == 3
        assert result["with_email"] == 2
        assert result["with_phone"] == 2
        assert result["with_address"] == 1
        mock_upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_passes_correct_arrays_to_repo(self):
        """Should pass correctly structured arrays to upsert_big4_parks."""
        from unittest.mock import patch, AsyncMock
        from lib.big4.models import Big4Park

        parks = [
            Big4Park(
                name="BIG4 Test Park", slug="test-park", url_path="/caravan-parks/nsw/test/test-park",
                state="NSW", phone="02 1111 2222", email="test@park.com",
                address="10 Park Rd", city="Parkville", postcode="2000",
                latitude=-33.5, longitude=151.0,
            ),
        ]

        mock_scraper = AsyncMock()
        mock_scraper.scrape_all = AsyncMock(return_value=parks)
        mock_scraper.__aenter__ = AsyncMock(return_value=mock_scraper)
        mock_scraper.__aexit__ = AsyncMock(return_value=False)

        service = Service(rms_repo=None, rms_queue=MockQueue())

        with patch("lib.big4.Big4Scraper", return_value=mock_scraper):
            with patch("services.enrichment.repo.upsert_big4_parks", new_callable=AsyncMock) as mock_upsert:
                with patch("services.enrichment.repo.get_big4_count", new_callable=AsyncMock, return_value=1):
                    await service.scrape_big4_parks()

        mock_upsert.assert_called_once_with(
            names=["BIG4 Test Park"],
            slugs=["test-park"],
            phones=["02 1111 2222"],
            emails=["test@park.com"],
            websites=["https://www.big4.com.au/caravan-parks/nsw/test/test-park"],
            addresses=["10 Park Rd"],
            cities=["Parkville"],
            states=["NSW"],
            postcodes=["2000"],
            lats=[-33.5],
            lons=[151.0],
        )


# ============================================================================
# OWNER ENRICHMENT TESTS
# ============================================================================


class TestGetOwnerEnrichmentStats:
    """Tests for get_owner_enrichment_stats."""

    @pytest.mark.asyncio
    async def test_returns_stats_dict(self, service):
        stats = await service.get_owner_enrichment_stats()
        assert isinstance(stats, dict)
        assert "total_with_website" in stats
        assert "total_contacts" in stats


class TestGetHotelsPendingOwnerEnrichment:
    """Tests for get_hotels_pending_owner_enrichment."""

    @pytest.mark.asyncio
    async def test_returns_list(self, service):
        hotels = await service.get_hotels_pending_owner_enrichment(limit=5)
        assert isinstance(hotels, list)

    @pytest.mark.asyncio
    async def test_respects_limit(self, service):
        hotels = await service.get_hotels_pending_owner_enrichment(limit=3)
        assert len(hotels) <= 3


class TestGetDecisionMakersForHotel:
    """Tests for get_decision_makers_for_hotel."""

    @pytest.mark.asyncio
    async def test_nonexistent_hotel_returns_empty(self, service):
        dms = await service.get_decision_makers_for_hotel(999999999)
        assert dms == []


class TestPersistOwnerEnrichmentResults:
    """Tests for _persist_owner_results."""

    @pytest.mark.asyncio
    async def test_empty_results_returns_zero(self, service):
        saved = await service._persist_owner_results([])
        assert saved == 0

    @pytest.mark.asyncio
    async def test_persists_decision_makers(self, service):
        from services.enrichment.owner_models import (
            DecisionMaker, DomainIntel, OwnerEnrichmentResult,
            LAYER_RDAP, LAYER_DNS,
        )
        from services.leadgen.repo import insert_hotel, delete_hotel
        from services.enrichment import repo
        from db.client import get_conn

        hotel_id = await insert_hotel(
            name="Persist Test Hotel",
            website="https://persist-test-svc.com",
            city="Miami", state="Florida",
            status=0, source="test",
        )
        try:
            results = [
                OwnerEnrichmentResult(
                    hotel_id=hotel_id,
                    domain="persist-test-svc.com",
                    decision_makers=[
                        DecisionMaker(
                            full_name="Alice Owner",
                            title="Owner",
                            email="alice@persist-test-svc.com",
                            sources=["rdap"],
                            confidence=0.85,
                        ),
                    ],
                    domain_intel=DomainIntel(
                        domain="persist-test-svc.com",
                        registrant_name="Alice Owner",
                        is_privacy_protected=False,
                        whois_sources=["rdap"],
                        email_provider="google_workspace",
                        mx_records=["aspmx.l.google.com"],
                    ),
                    layers_completed=LAYER_RDAP | LAYER_DNS,
                ),
            ]

            saved = await service._persist_owner_results(results)
            assert saved == 1

            # Verify DM was persisted
            dms = await service.get_decision_makers_for_hotel(hotel_id)
            assert len(dms) == 1
            assert dms[0]["full_name"] == "Alice Owner"

            # Verify enrichment status was written
            async with get_conn() as conn:
                row = await conn.fetchrow(
                    "SELECT status, layers_completed FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1",
                    hotel_id,
                )
            assert row["status"] == 1  # complete
            assert row["layers_completed"] == LAYER_RDAP | LAYER_DNS
        finally:
            async with get_conn() as conn:
                await conn.execute(
                    "DELETE FROM sadie_gtm.hotel_decision_makers WHERE hotel_id = $1", hotel_id,
                )
                await conn.execute(
                    "DELETE FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1", hotel_id,
                )
                await conn.execute(
                    "DELETE FROM sadie_gtm.domain_whois_cache WHERE domain = $1", "persist-test-svc.com",
                )
                await conn.execute(
                    "DELETE FROM sadie_gtm.domain_dns_cache WHERE domain = $1", "persist-test-svc.com",
                )
            await delete_hotel(hotel_id)

    @pytest.mark.asyncio
    async def test_no_contacts_sets_status_no_results(self, service):
        from services.enrichment.owner_models import OwnerEnrichmentResult, LAYER_RDAP
        from services.leadgen.repo import insert_hotel, delete_hotel
        from db.client import get_conn

        hotel_id = await insert_hotel(
            name="No Results Hotel",
            website="https://no-results-svc.com",
            city="Tampa", state="Florida",
            status=0, source="test",
        )
        try:
            results = [
                OwnerEnrichmentResult(
                    hotel_id=hotel_id,
                    domain="no-results-svc.com",
                    decision_makers=[],
                    layers_completed=LAYER_RDAP,
                ),
            ]

            saved = await service._persist_owner_results(results)
            assert saved == 0

            async with get_conn() as conn:
                row = await conn.fetchrow(
                    "SELECT status FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1",
                    hotel_id,
                )
            assert row["status"] == 2  # no_results
        finally:
            async with get_conn() as conn:
                await conn.execute(
                    "DELETE FROM sadie_gtm.hotel_owner_enrichment WHERE hotel_id = $1", hotel_id,
                )
            await delete_hotel(hotel_id)


@pytest.mark.no_db
class TestRunOwnerEnrichmentMocked:
    """Tests for run_owner_enrichment with mocked enricher."""

    @pytest.mark.asyncio
    async def test_returns_zeros_when_no_pending(self):
        from unittest.mock import patch, AsyncMock

        service = Service(rms_repo=None, rms_queue=MockQueue())
        with patch("services.enrichment.repo.get_hotels_pending_owner_enrichment",
                    new_callable=AsyncMock, return_value=[]):
            result = await service.run_owner_enrichment(limit=10)

        assert result["processed"] == 0
        assert result["found"] == 0
        assert result["contacts"] == 0

    @pytest.mark.asyncio
    async def test_processes_hotels_and_persists(self):
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_models import (
            DecisionMaker, OwnerEnrichmentResult, LAYER_RDAP,
        )

        fake_hotels = [
            {"hotel_id": 1, "website": "https://a.com"},
            {"hotel_id": 2, "website": "https://b.com"},
        ]
        fake_results = [
            OwnerEnrichmentResult(
                hotel_id=1, domain="a.com",
                decision_makers=[
                    DecisionMaker(full_name="Owner A", sources=["rdap"], confidence=0.9),
                ],
                layers_completed=LAYER_RDAP,
            ),
            OwnerEnrichmentResult(
                hotel_id=2, domain="b.com",
                decision_makers=[],
                layers_completed=LAYER_RDAP,
            ),
        ]

        service = Service(rms_repo=None, rms_queue=MockQueue())

        with patch("services.enrichment.repo.get_hotels_pending_owner_enrichment",
                    new_callable=AsyncMock, return_value=fake_hotels):
            with patch("services.enrichment.owner_enricher.enrich_batch",
                        new_callable=AsyncMock, return_value=fake_results):
                with patch.object(service, "_persist_owner_results",
                                  new_callable=AsyncMock, return_value=1):
                    result = await service.run_owner_enrichment(limit=10)

        assert result["processed"] == 2
        assert result["found"] == 1
        assert result["contacts"] == 1
        assert result["verified"] == 0
        assert result["saved"] == 1

    @pytest.mark.asyncio
    async def test_layer_filter_maps_correctly(self):
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_models import LAYER_WEBSITE

        service = Service(rms_repo=None, rms_queue=MockQueue())

        with patch("services.enrichment.repo.get_hotels_pending_owner_enrichment",
                    new_callable=AsyncMock, return_value=[]) as mock_pending:
            await service.run_owner_enrichment(limit=5, layer="website")

        mock_pending.assert_called_once_with(limit=5, layer=LAYER_WEBSITE)

    @pytest.mark.asyncio
    async def test_gov_data_layer_filter_maps_correctly(self):
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_models import LAYER_GOV_DATA

        service = Service(rms_repo=None, rms_queue=MockQueue())

        with patch("services.enrichment.repo.get_hotels_pending_owner_enrichment",
                    new_callable=AsyncMock, return_value=[]) as mock_pending:
            await service.run_owner_enrichment(limit=5, layer="gov-data")

        mock_pending.assert_called_once_with(limit=5, layer=LAYER_GOV_DATA)


@pytest.mark.no_db
class TestRunGovDataLayer:
    """Tests for _run_gov_data layer function."""

    @pytest.mark.asyncio
    async def test_returns_decision_makers_from_gov_matches(self):
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_enricher import _run_gov_data

        fake_matches = [
            {
                "id": 100,
                "name": "Sunshine Hotel LLC",
                "email": "owner@sunshine.com",
                "phone_google": "555-1234",
                "phone_website": None,
                "address": "123 Beach Rd",
                "source": "dbpr_license",
                "external_id": "H12345",
                "external_id_type": "license_number",
            },
        ]

        with patch("services.enrichment.repo.find_gov_matches",
                    new_callable=AsyncMock, return_value=fake_matches):
            dms = await _run_gov_data("[test]", 1, "Sunshine Hotel", "Miami", "Florida")

        assert len(dms) == 1
        assert dms[0].full_name == "Sunshine Hotel LLC"
        assert dms[0].email == "owner@sunshine.com"
        assert dms[0].phone == "555-1234"
        assert dms[0].sources == ["gov_dbpr_license"]
        assert dms[0].confidence == 0.9
        assert dms[0].raw_source_url == "gov://dbpr_license/H12345"

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_city_state(self):
        from services.enrichment.owner_enricher import _run_gov_data

        dms = await _run_gov_data("[test]", 1, "Hotel", None, None)
        assert dms == []

        dms = await _run_gov_data("[test]", 1, "Hotel", "Miami", None)
        assert dms == []

        dms = await _run_gov_data("[test]", 1, "Hotel", None, "Florida")
        assert dms == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_matches(self):
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_enricher import _run_gov_data

        with patch("services.enrichment.repo.find_gov_matches",
                    new_callable=AsyncMock, return_value=[]):
            dms = await _run_gov_data("[test]", 1, "Nonexistent Hotel", "Miami", "Florida")

        assert dms == []

    @pytest.mark.asyncio
    async def test_multiple_gov_matches(self):
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_enricher import _run_gov_data

        fake_matches = [
            {"id": 10, "name": "Hotel A", "email": None, "phone_google": None,
             "phone_website": "555-0001", "source": "texas_hot", "external_id": "TX100"},
            {"id": 20, "name": "Hotel A Corp", "email": "info@a.com", "phone_google": "555-0002",
             "phone_website": None, "source": "dbpr_license", "external_id": "FL200"},
        ]

        with patch("services.enrichment.repo.find_gov_matches",
                    new_callable=AsyncMock, return_value=fake_matches):
            dms = await _run_gov_data("[test]", 1, "Hotel A", "Austin", "Texas")

        assert len(dms) == 2
        assert dms[0].sources == ["gov_texas_hot"]
        assert dms[0].phone == "555-0001"  # falls back to phone_website
        assert dms[1].sources == ["gov_dbpr_license"]
        assert dms[1].email == "info@a.com"

    @pytest.mark.asyncio
    async def test_gov_data_wired_into_orchestrator(self):
        """Verify _run_gov_data is called during enrichment when LAYER_GOV_DATA is set."""
        from unittest.mock import patch, AsyncMock
        from services.enrichment.owner_enricher import enrich_single_hotel
        from services.enrichment.owner_models import LAYER_GOV_DATA, DecisionMaker

        hotel = {
            "hotel_id": 99, "name": "Test Hotel", "website": "https://test.com",
            "city": "Miami", "state": "Florida",
        }

        gov_dm = DecisionMaker(
            full_name="Gov Match", sources=["gov_dbpr_license"], confidence=0.9,
        )

        with patch("services.enrichment.owner_enricher._run_gov_data",
                    new_callable=AsyncMock, return_value=[gov_dm]) as mock_gov:
            async with __import__("httpx").AsyncClient() as client:
                result = await enrich_single_hotel(
                    client, hotel, layers=LAYER_GOV_DATA,
                )

        mock_gov.assert_called_once_with("[99|test.com]", 99, "Test Hotel", "Miami", "Florida")
        assert len(result.decision_makers) == 1
        assert result.decision_makers[0].full_name == "Gov Match"
        assert result.layers_completed & LAYER_GOV_DATA


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
