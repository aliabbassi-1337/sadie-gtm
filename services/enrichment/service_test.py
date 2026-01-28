"""Tests for Enrichment Service."""

import pytest

from services.enrichment.service import Service, EnrichResult, EnqueueResult, ConsumeResult, RMSRepo, MockQueue


@pytest.fixture
async def service():
    """Create service with real repo but mock queue."""
    return Service(rms_repo=RMSRepo(), rms_queue=MockQueue())


@pytest.fixture
async def service_with_queue():
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
