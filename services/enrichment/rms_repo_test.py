"""Tests for RMS Repository against local database."""

import pytest

from services.enrichment.rms_repo import RMSRepo, RMSHotelRecord


@pytest.fixture
async def repo():
    """Create repo instance."""
    return RMSRepo()


class TestRMSRepoGetBookingEngineId:
    """Tests for get_booking_engine_id."""
    
    @pytest.mark.asyncio
    async def test_returns_rms_cloud_id(self, repo):
        """Should return RMS Cloud booking engine ID from database."""
        result = await repo.get_booking_engine_id()
        
        assert isinstance(result, int)
        assert result > 0


class TestRMSRepoGetHotelsNeedingEnrichment:
    """Tests for get_hotels_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_list(self, repo):
        """Should return a list of RMSHotelRecord."""
        result = await repo.get_hotels_needing_enrichment(limit=10)
        
        assert isinstance(result, list)
        for record in result:
            assert isinstance(record, RMSHotelRecord)
            assert record.hotel_id > 0
            assert record.booking_url


class TestRMSRepoGetStats:
    """Tests for get_stats."""
    
    @pytest.mark.asyncio
    async def test_returns_stats_dict(self, repo):
        """Should return statistics dictionary."""
        result = await repo.get_stats()
        
        assert isinstance(result, dict)
        assert "total" in result
        assert isinstance(result["total"], int)


class TestRMSRepoCountNeedingEnrichment:
    """Tests for count_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_count(self, repo):
        """Should return count of hotels needing enrichment."""
        result = await repo.count_needing_enrichment()
        
        assert isinstance(result, int)
        assert result >= 0


class TestRMSRepoInsertAndUpdate:
    """Tests for insert and update operations."""
    
    @pytest.mark.asyncio
    async def test_insert_hotel_returns_id(self, repo):
        """Should insert hotel and return ID."""
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        
        result = await repo.insert_hotel(
            name=f"Test Hotel RMS {unique_id}",
            address="123 Test St",
            city="Test City",
            state="TS",
            country="USA",
            phone="555-1234",
            email="test@rmstest.com",
            website="https://testhotel.com",
            external_id=f"test_{unique_id}",
            source="rms_test",
            status=1,
        )
        
        assert result is not None
        assert isinstance(result, int)
        assert result > 0
    
    @pytest.mark.asyncio
    async def test_insert_booking_engine_relation(self, repo):
        """Should insert hotel booking engine relation."""
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        
        hotel_id = await repo.insert_hotel(
            name=f"Test Hotel BE {unique_id}",
            address=None,
            city=None,
            state=None,
            country=None,
            phone=None,
            email=None,
            website=None,
            external_id=f"be_{unique_id}",
            source="rms_test",
            status=1,
        )
        
        booking_engine_id = await repo.get_booking_engine_id()
        
        await repo.insert_hotel_booking_engine(
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            booking_url=f"https://ibe.rmscloud.com/test{unique_id}",
            enrichment_status="enriched",
        )
    
    @pytest.mark.asyncio
    async def test_update_hotel(self, repo):
        """Should update hotel fields."""
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        
        hotel_id = await repo.insert_hotel(
            name=f"Test Hotel Update {unique_id}",
            address=None,
            city=None,
            state=None,
            country=None,
            phone=None,
            email=None,
            website=None,
            external_id=f"update_{unique_id}",
            source="rms_test",
            status=1,
        )
        
        await repo.update_hotel(
            hotel_id=hotel_id,
            name=f"Updated Hotel {unique_id}",
            email="updated@test.com",
            phone="555-9999",
        )
    
    @pytest.mark.asyncio
    async def test_update_enrichment_status(self, repo):
        """Should update enrichment status."""
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        
        hotel_id = await repo.insert_hotel(
            name=f"Test Hotel Status {unique_id}",
            address=None,
            city=None,
            state=None,
            country=None,
            phone=None,
            email=None,
            website=None,
            external_id=f"status_{unique_id}",
            source="rms_test",
            status=1,
        )
        
        booking_engine_id = await repo.get_booking_engine_id()
        booking_url = f"https://ibe.rmscloud.com/status{unique_id}"
        
        await repo.insert_hotel_booking_engine(
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            booking_url=booking_url,
            enrichment_status="pending",
        )
        
        await repo.update_enrichment_status(
            booking_url=booking_url,
            status="enriched",
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
