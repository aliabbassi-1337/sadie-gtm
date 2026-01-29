"""Tests for Enrichment Service."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import BaseModel

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


# Mock for ExtractedCloudbedsData
class MockCloudbedsData:
    def __init__(self, name=None, city=None, state=None, country=None, 
                 address=None, phone=None, email=None):
        self.name = name
        self.city = city
        self.state = state
        self.country = country
        self.address = address
        self.phone = phone
        self.email = email


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
    
    @pytest.mark.asyncio
    async def test_returns_list(self, service):
        """Should return list of hotel candidates."""
        hotels = await service.get_cloudbeds_hotels_needing_enrichment(limit=10)
        
        assert isinstance(hotels, list)
    
    @pytest.mark.asyncio
    async def test_respects_limit(self, service):
        """Should respect the limit parameter."""
        hotels = await service.get_cloudbeds_hotels_needing_enrichment(limit=5)
        
        assert len(hotels) <= 5


class TestProcessCloudbedsHotel:
    """Tests for _process_cloudbeds_hotel (garbage detection)."""
    
    @pytest.fixture
    def service(self):
        return Service(rms_repo=RMSRepo(), rms_queue=MockQueue())
    
    @pytest.mark.asyncio
    async def test_returns_no_data_when_extract_fails(self, service):
        """Should return no_data error when scraper returns None."""
        mock_scraper = AsyncMock()
        mock_scraper.extract = AsyncMock(return_value=None)
        
        result = await service._process_cloudbeds_hotel(mock_scraper, 123, "https://example.com")
        
        hotel_id, success, data, error = result
        assert hotel_id == 123
        assert success is False
        assert data is None
        assert error == "no_data"
    
    @pytest.mark.asyncio
    async def test_detects_cloudbeds_homepage_garbage(self, service):
        """Should detect Cloudbeds homepage as garbage."""
        mock_scraper = AsyncMock()
        mock_scraper.extract = AsyncMock(return_value=MockCloudbedsData(
            name="cloudbeds.com",
            city="Some City"
        ))
        
        result = await service._process_cloudbeds_hotel(mock_scraper, 123, "https://example.com")
        
        hotel_id, success, data, error = result
        assert success is False
        assert error == "404_not_found"
    
    @pytest.mark.asyncio
    async def test_detects_book_now_garbage(self, service):
        """Should detect 'Book Now' as garbage name."""
        mock_scraper = AsyncMock()
        mock_scraper.extract = AsyncMock(return_value=MockCloudbedsData(
            name="Book Now",
            city="Austin"
        ))
        
        result = await service._process_cloudbeds_hotel(mock_scraper, 123, "https://example.com")
        
        hotel_id, success, data, error = result
        assert success is False
        assert error == "404_not_found"
    
    @pytest.mark.asyncio
    async def test_detects_portuguese_garbage(self, service):
        """Should detect Portuguese error page as garbage."""
        mock_scraper = AsyncMock()
        mock_scraper.extract = AsyncMock(return_value=MockCloudbedsData(
            name="Some Hotel",
            city="Soluções Online para Hotéis"
        ))
        
        result = await service._process_cloudbeds_hotel(mock_scraper, 123, "https://example.com")
        
        hotel_id, success, data, error = result
        assert success is False
        assert error == "404_not_found"
    
    @pytest.mark.asyncio
    async def test_returns_valid_data(self, service):
        """Should return success for valid hotel data."""
        mock_scraper = AsyncMock()
        mock_scraper.extract = AsyncMock(return_value=MockCloudbedsData(
            name="Grand Hotel Austin",
            city="Austin",
            state="Texas",
            country="USA"
        ))
        
        result = await service._process_cloudbeds_hotel(mock_scraper, 123, "https://example.com")
        
        hotel_id, success, data, error = result
        assert hotel_id == 123
        assert success is True
        assert data.name == "Grand Hotel Austin"
        assert data.city == "Austin"
        assert error is None
    
    @pytest.mark.asyncio
    async def test_handles_exceptions(self, service):
        """Should handle exceptions gracefully."""
        mock_scraper = AsyncMock()
        mock_scraper.extract = AsyncMock(side_effect=Exception("Network error"))
        
        result = await service._process_cloudbeds_hotel(mock_scraper, 123, "https://example.com")
        
        hotel_id, success, data, error = result
        assert hotel_id == 123
        assert success is False
        assert data is None
        assert "Network error" in error


class TestEnrichCloudbedsHotels:
    """Tests for enrich_cloudbeds_hotels."""
    
    @pytest.mark.asyncio
    async def test_returns_zero_when_no_hotels(self, service):
        """Should return zero counts when no hotels need enrichment."""
        with patch('services.enrichment.service.repo') as mock_repo:
            mock_repo.get_cloudbeds_hotels_needing_enrichment = AsyncMock(return_value=[])
            
            result = await service.enrich_cloudbeds_hotels(limit=10)
            
            assert isinstance(result, EnrichResult)
            assert result.processed == 0
            assert result.enriched == 0
            assert result.failed == 0


class TestBatchUpdateCloudbedsEnrichment:
    """Tests for batch_update_cloudbeds_enrichment."""
    
    @pytest.mark.asyncio
    async def test_calls_repo(self, service):
        """Should call repo method with results."""
        with patch('services.enrichment.service.repo') as mock_repo:
            mock_repo.batch_update_cloudbeds_enrichment = AsyncMock(return_value=5)
            
            results = [{"hotel_id": 1, "name": "Test"}]
            updated = await service.batch_update_cloudbeds_enrichment(results)
            
            assert updated == 5
            mock_repo.batch_update_cloudbeds_enrichment.assert_called_once_with(results)


class TestBatchMarkCloudbedsFailed:
    """Tests for batch_mark_cloudbeds_failed."""
    
    @pytest.mark.asyncio
    async def test_calls_repo(self, service):
        """Should call repo method with hotel IDs."""
        with patch('services.enrichment.service.repo') as mock_repo:
            mock_repo.batch_set_last_enrichment_attempt = AsyncMock(return_value=3)
            
            marked = await service.batch_mark_cloudbeds_failed([1, 2, 3])
            
            assert marked == 3
            mock_repo.batch_set_last_enrichment_attempt.assert_called_once_with([1, 2, 3])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
