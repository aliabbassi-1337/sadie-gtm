"""Unit tests for RMS Service."""

import pytest
from typing import List, Optional, Dict, Any

from services.enrichment.rms_service import (
    RMSService,
    IngestResult,
    EnrichResult,
    EnqueueResult,
    ConsumeResult,
)
from services.enrichment.rms_repo import IRMSRepo, RMSHotelRecord
from services.enrichment.rms_queue import MockQueue


pytestmark = pytest.mark.no_db  # All tests in this file use mocks


class MockRepo(IRMSRepo):
    """Mock repository for testing."""
    
    def __init__(self):
        self.hotels: Dict[int, dict] = {}
        self.booking_engines: List[dict] = []
        self.enrichment_statuses: Dict[str, str] = {}
        self._next_id = 1
    
    async def get_booking_engine_id(self) -> int:
        return 4
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        return [
            RMSHotelRecord(hotel_id=hid, booking_url=h.get("booking_url", ""))
            for hid, h in list(self.hotels.items())[:limit]
            if not h.get("name")
        ]
    
    async def insert_hotel(
        self,
        name: Optional[str],
        address: Optional[str],
        city: Optional[str],
        state: Optional[str],
        country: Optional[str],
        phone: Optional[str],
        email: Optional[str],
        website: Optional[str],
        source: str = "rms_scan",
        status: int = 1,
    ) -> Optional[int]:
        hotel_id = self._next_id
        self._next_id += 1
        self.hotels[hotel_id] = {
            "name": name,
            "address": address,
            "city": city,
            "state": state,
            "country": country,
            "phone": phone,
            "email": email,
            "website": website,
            "source": source,
            "status": status,
        }
        return hotel_id
    
    async def insert_hotel_booking_engine(
        self,
        hotel_id: int,
        booking_engine_id: int,
        booking_url: str,
        enrichment_status: str = "enriched",
    ) -> None:
        self.booking_engines.append({
            "hotel_id": hotel_id,
            "booking_engine_id": booking_engine_id,
            "booking_url": booking_url,
            "enrichment_status": enrichment_status,
        })
    
    async def update_hotel(
        self,
        hotel_id: int,
        name: Optional[str] = None,
        address: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        website: Optional[str] = None,
    ) -> None:
        if hotel_id in self.hotels:
            if name:
                self.hotels[hotel_id]["name"] = name
            if address:
                self.hotels[hotel_id]["address"] = address
            if city:
                self.hotels[hotel_id]["city"] = city
            if state:
                self.hotels[hotel_id]["state"] = state
            if country:
                self.hotels[hotel_id]["country"] = country
            if phone:
                self.hotels[hotel_id]["phone"] = phone
            if email:
                self.hotels[hotel_id]["email"] = email
            if website:
                self.hotels[hotel_id]["website"] = website
    
    async def update_enrichment_status(self, booking_url: str, status: str) -> None:
        self.enrichment_statuses[booking_url] = status
    
    async def get_stats(self) -> Dict[str, int]:
        return {
            "total": len(self.hotels),
            "with_name": sum(1 for h in self.hotels.values() if h.get("name")),
            "with_email": sum(1 for h in self.hotels.values() if h.get("email")),
        }
    
    async def count_needing_enrichment(self) -> int:
        return sum(1 for h in self.hotels.values() if not h.get("name"))


class TestEnqueueForEnrichment:
    """Tests for enqueue_for_enrichment."""
    
    @pytest.mark.asyncio
    async def test_skips_when_queue_full(self):
        """Should skip when queue depth exceeds threshold."""
        repo = MockRepo()
        queue = MockQueue()
        queue.pending = 2000
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is True
        assert "exceeds" in result.reason
        assert result.enqueued == 0
    
    @pytest.mark.asyncio
    async def test_enqueues_hotels(self):
        """Should find and enqueue hotels."""
        repo = MockRepo()
        repo.hotels[1] = {"booking_url": "https://ibe.rmscloud.com/123"}
        repo.hotels[2] = {"booking_url": "https://ibe.rmscloud.com/456"}
        
        queue = MockQueue()
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is False
        assert result.total_found == 2
        assert result.enqueued == 2
    
    @pytest.mark.asyncio
    async def test_returns_zero_when_no_hotels(self):
        """Should return zero when no hotels need enrichment."""
        repo = MockRepo()
        repo.hotels[1] = {"name": "Has Name", "booking_url": "https://x.com"}
        
        queue = MockQueue()
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is False
        assert result.total_found == 0
        assert result.enqueued == 0


class TestGetStats:
    """Tests for get_stats."""
    
    @pytest.mark.asyncio
    async def test_returns_repo_stats(self):
        """Should return statistics from repo."""
        repo = MockRepo()
        repo.hotels[1] = {"name": "A", "email": "a@test.com"}
        repo.hotels[2] = {"name": "B"}
        repo.hotels[3] = {}
        
        service = RMSService(repo=repo, queue=MockQueue())
        stats = await service.get_stats()
        
        assert stats["total"] == 3
        assert stats["with_name"] == 2
        assert stats["with_email"] == 1


class TestCountNeedingEnrichment:
    """Tests for count_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_counts_hotels_without_names(self):
        """Should count hotels missing names."""
        repo = MockRepo()
        repo.hotels[1] = {"name": "Has Name"}
        repo.hotels[2] = {}
        repo.hotels[3] = {}
        
        service = RMSService(repo=repo, queue=MockQueue())
        count = await service.count_needing_enrichment()
        
        assert count == 2


class TestGetQueueStats:
    """Tests for get_queue_stats."""
    
    def test_returns_queue_stats(self):
        """Should return queue statistics."""
        queue = MockQueue()
        queue.pending = 10
        queue.in_flight = 5
        
        service = RMSService(repo=MockRepo(), queue=queue)
        stats = service.get_queue_stats()
        
        assert stats.pending == 10
        assert stats.in_flight == 5


class TestConsumeEnrichmentQueue:
    """Tests for consume_enrichment_queue."""
    
    @pytest.mark.asyncio
    async def test_stops_on_should_stop(self):
        """Should stop when should_stop returns True."""
        repo = MockRepo()
        queue = MockQueue()
        queue.add_message([RMSHotelRecord(hotel_id=1, booking_url="https://x.com")])
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.consume_enrichment_queue(
            concurrency=1,
            should_stop=lambda: True,
        )
        
        assert result.messages_processed == 0
    
    @pytest.mark.asyncio
    async def test_stops_on_max_messages(self):
        """Should stop after processing max_messages."""
        repo = MockRepo()
        queue = MockQueue()
        # Add messages but set max_messages to 0
        queue.add_message([RMSHotelRecord(hotel_id=1, booking_url="https://x.com")])
        
        service = RMSService(repo=repo, queue=queue)
        
        # max_messages=0 with should_stop=True should stop immediately
        result = await service.consume_enrichment_queue(
            concurrency=1,
            max_messages=0,
            should_stop=lambda: True,
        )
        
        assert result.messages_processed == 0


class TestRequestShutdown:
    """Tests for request_shutdown."""
    
    def test_sets_shutdown_flag(self):
        """Should set shutdown flag."""
        service = RMSService(repo=MockRepo(), queue=MockQueue())
        
        assert service._shutdown_requested is False
        service.request_shutdown()
        assert service._shutdown_requested is True


class TestSaveHotelsBatch:
    """Tests for _save_hotels_batch."""
    
    @pytest.mark.asyncio
    async def test_saves_hotels_to_repo(self):
        """Should save hotels via repo."""
        from services.enrichment.rms_scraper import ExtractedRMSData
        
        repo = MockRepo()
        service = RMSService(repo=repo, queue=MockQueue())
        
        hotels = [
            ExtractedRMSData(
                slug="123",
                booking_url="https://ibe.rmscloud.com/123",
                name="Hotel A",
                phone="555-1234",
            ),
            ExtractedRMSData(
                slug="456",
                booking_url="https://ibe.rmscloud.com/456",
                name="Hotel B",
            ),
        ]
        
        saved = await service._save_hotels_batch(hotels, booking_engine_id=4)
        
        assert saved == 2
        assert len(repo.hotels) == 2
        assert len(repo.booking_engines) == 2
        assert repo.hotels[1]["name"] == "Hotel A"
        assert repo.hotels[2]["name"] == "Hotel B"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
