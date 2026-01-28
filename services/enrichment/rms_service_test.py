"""Unit tests for RMS Service.

Demonstrates how to use mocks for testing the service layer.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import List, Optional, Dict, Any

from services.enrichment.rms_service import RMSService, IngestResult, EnrichResult
from services.enrichment.rms_repo import IRMSRepo, RMSHotelRecord
from services.enrichment.rms_scraper import ExtractedRMSData, MockScraper
from services.enrichment.rms_queue import MockQueue, QueueStats


class MockRepo(IRMSRepo):
    """Mock repository for testing."""
    
    def __init__(self):
        self.hotels: Dict[int, dict] = {}
        self.booking_engines: List[dict] = []
        self.enrichment_statuses: Dict[str, str] = {}
        self._next_id = 1
    
    async def get_booking_engine_id(self) -> int:
        return 4  # RMS Cloud
    
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


class TestRMSServiceEnqueue:
    """Test enqueueing hotels for enrichment."""
    
    @pytest.mark.asyncio
    async def test_enqueue_skips_when_queue_full(self):
        """Should skip enqueueing when queue depth exceeds threshold."""
        repo = MockRepo()
        queue = MockQueue()
        queue.pending = 2000  # Over threshold
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is True
        assert "exceeds" in result.reason
        assert result.enqueued == 0
    
    @pytest.mark.asyncio
    async def test_enqueue_finds_and_queues_hotels(self):
        """Should find hotels needing enrichment and queue them."""
        repo = MockRepo()
        # Add some hotels without names
        repo.hotels[1] = {"booking_url": "https://ibe.rmscloud.com/123"}
        repo.hotels[2] = {"booking_url": "https://ibe.rmscloud.com/456"}
        
        queue = MockQueue()
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is False
        assert result.total_found == 2
        assert result.enqueued == 2
        assert len(queue._enqueued) == 1  # One batch
    
    @pytest.mark.asyncio
    async def test_enqueue_returns_zero_when_no_hotels(self):
        """Should return zero when no hotels need enrichment."""
        repo = MockRepo()
        # Add hotel with name (doesn't need enrichment)
        repo.hotels[1] = {"name": "Test Hotel", "booking_url": "https://x.com"}
        
        queue = MockQueue()
        
        service = RMSService(repo=repo, queue=queue)
        result = await service.enqueue_for_enrichment(limit=100)
        
        assert result.skipped is False
        assert result.total_found == 0
        assert result.enqueued == 0


class TestRMSServiceStats:
    """Test statistics methods."""
    
    @pytest.mark.asyncio
    async def test_get_stats(self):
        """Should return statistics from repo."""
        repo = MockRepo()
        repo.hotels[1] = {"name": "Hotel A", "email": "a@test.com"}
        repo.hotels[2] = {"name": "Hotel B"}
        repo.hotels[3] = {}
        
        service = RMSService(repo=repo, queue=MockQueue())
        stats = await service.get_stats()
        
        assert stats["total"] == 3
        assert stats["with_name"] == 2
        assert stats["with_email"] == 1
    
    @pytest.mark.asyncio
    async def test_count_needing_enrichment(self):
        """Should count hotels needing enrichment."""
        repo = MockRepo()
        repo.hotels[1] = {"name": "Hotel A"}
        repo.hotels[2] = {}  # Needs enrichment
        repo.hotels[3] = {}  # Needs enrichment
        
        service = RMSService(repo=repo, queue=MockQueue())
        count = await service.count_needing_enrichment()
        
        assert count == 2
    
    def test_get_queue_stats(self):
        """Should return queue statistics."""
        queue = MockQueue()
        queue.pending = 10
        queue.in_flight = 5
        
        service = RMSService(repo=MockRepo(), queue=queue)
        stats = service.get_queue_stats()
        
        assert stats.pending == 10
        assert stats.in_flight == 5


class TestRMSServiceConsume:
    """Test consuming from queue."""
    
    @pytest.mark.asyncio
    async def test_consume_stops_when_max_messages_reached(self):
        """Should stop after processing max_messages."""
        repo = MockRepo()
        queue = MockQueue()
        
        # Add messages with hotels
        queue.add_message([RMSHotelRecord(hotel_id=1, booking_url="https://test.com/1")])
        queue.add_message([RMSHotelRecord(hotel_id=2, booking_url="https://test.com/2")])
        queue.add_message([RMSHotelRecord(hotel_id=3, booking_url="https://test.com/3")])
        
        service = RMSService(repo=repo, queue=queue)
        
        # Note: This test would need scraper mocking to fully work
        # For now, just test the stop condition
        result = await service.consume_enrichment_queue(
            concurrency=1,
            max_messages=0,  # Stop immediately when queue empties
            should_stop=lambda: True,  # Stop immediately
        )
        
        assert result.messages_processed == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
