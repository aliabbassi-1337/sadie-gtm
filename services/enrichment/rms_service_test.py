"""Tests for RMS Enrichment Service against local database."""

import pytest

from services.enrichment.rms_service import RMSEnrichmentService
from services.enrichment.rms_repo import RMSRepo, RMSHotelRecord
from services.enrichment.rms_queue import MockQueue


@pytest.fixture
async def service():
    """Create service with real repo but mock queue."""
    return RMSEnrichmentService(repo=RMSRepo(), queue=MockQueue())


@pytest.fixture
async def service_with_queue():
    """Create service with real repo and return the mock queue."""
    queue = MockQueue()
    return RMSEnrichmentService(repo=RMSRepo(), queue=queue), queue


class TestGetStats:
    """Tests for get_stats."""
    
    @pytest.mark.asyncio
    async def test_returns_stats_dict(self, service):
        """Should return statistics from database."""
        stats = await service.get_stats()
        
        assert isinstance(stats, dict)
        assert "total" in stats
        assert isinstance(stats["total"], int)


class TestCountNeedingEnrichment:
    """Tests for count_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_count(self, service):
        """Should return count of hotels needing enrichment."""
        count = await service.count_needing_enrichment()
        
        assert isinstance(count, int)
        assert count >= 0


class TestGetHotelsNeedingEnrichment:
    """Tests for get_hotels_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_list(self, service):
        """Should return list of hotels needing enrichment."""
        hotels = await service.get_hotels_needing_enrichment(limit=10)
        
        assert isinstance(hotels, list)
        for hotel in hotels:
            assert isinstance(hotel, RMSHotelRecord)


class TestGetQueueStats:
    """Tests for get_queue_stats (uses mock queue)."""
    
    @pytest.mark.asyncio
    async def test_returns_queue_stats(self, service_with_queue):
        """Should return queue statistics."""
        service, queue = service_with_queue
        queue.pending = 10
        queue.in_flight = 5
        
        stats = service.get_queue_stats()
        
        assert stats.pending == 10
        assert stats.in_flight == 5


class TestEnqueueForEnrichment:
    """Tests for enqueue_for_enrichment."""
    
    @pytest.mark.asyncio
    async def test_skips_when_queue_full(self, service_with_queue):
        """Should skip when queue depth exceeds threshold."""
        service, queue = service_with_queue
        queue.pending = 2000  # Over MAX_QUEUE_DEPTH
        
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is True
        assert "exceeds" in result.reason
        assert result.enqueued == 0
    
    @pytest.mark.asyncio
    async def test_enqueues_hotels_from_db(self, service_with_queue):
        """Should find and enqueue hotels from real database."""
        service, queue = service_with_queue
        
        result = await service.enqueue_for_enrichment(limit=10)
        
        # Result depends on database state
        assert result.skipped is False
        assert result.total_found >= 0
        assert result.enqueued == result.total_found


class TestRequestShutdown:
    """Tests for request_shutdown."""
    
    @pytest.mark.asyncio
    async def test_sets_shutdown_flag(self, service):
        """Should set shutdown flag."""
        assert service._shutdown_requested is False
        service.request_shutdown()
        assert service._shutdown_requested is True


class TestConsumeEnrichmentQueue:
    """Tests for consume_enrichment_queue."""
    
    @pytest.mark.asyncio
    async def test_stops_on_should_stop(self, service):
        """Should stop immediately when should_stop returns True."""
        result = await service.consume_enrichment_queue(
            concurrency=1,
            should_stop=lambda: True,
        )
        
        assert result.messages_processed == 0
        assert result.hotels_processed == 0
    
    @pytest.mark.asyncio
    async def test_stops_on_empty_queue(self, service_with_queue):
        """Should stop when queue is empty and max_messages is set."""
        service, queue = service_with_queue
        # Queue is empty by default
        
        result = await service.consume_enrichment_queue(
            concurrency=1,
            max_messages=1,  # Stop after trying to get 1 message
        )
        
        assert result.messages_processed == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
