"""Unit tests for RMS Repository."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.enrichment.rms_repo import RMSRepo, RMSHotelRecord


pytestmark = pytest.mark.no_db  # All tests in this file use mocks, no real DB


class TestRMSRepoGetBookingEngineId:
    """Tests for get_booking_engine_id."""
    
    @pytest.mark.asyncio
    async def test_returns_id_when_found(self):
        """Should return booking engine ID from database."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.get_rms_booking_engine_id = AsyncMock(return_value={"id": 4})
                
                repo = RMSRepo()
                result = await repo.get_booking_engine_id()
                
                assert result == 4
    
    @pytest.mark.asyncio
    async def test_raises_when_not_found(self):
        """Should raise ValueError when booking engine not found."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.get_rms_booking_engine_id = AsyncMock(return_value=None)
                
                repo = RMSRepo()
                
                with pytest.raises(ValueError, match="not found"):
                    await repo.get_booking_engine_id()


class TestRMSRepoGetHotelsNeedingEnrichment:
    """Tests for get_hotels_needing_enrichment."""
    
    @pytest.mark.asyncio
    async def test_returns_hotel_records(self):
        """Should return list of RMSHotelRecord."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.get_rms_hotels_needing_enrichment = AsyncMock(return_value=[
                    {"hotel_id": 1, "booking_url": "https://ibe.rmscloud.com/123"},
                    {"hotel_id": 2, "booking_url": "https://ibe.rmscloud.com/456"},
                ])
                
                repo = RMSRepo()
                result = await repo.get_hotels_needing_enrichment(limit=100)
                
                assert len(result) == 2
                assert isinstance(result[0], RMSHotelRecord)
                assert result[0].hotel_id == 1
                assert result[0].booking_url == "https://ibe.rmscloud.com/123"
    
    @pytest.mark.asyncio
    async def test_returns_empty_list_when_none(self):
        """Should return empty list when no hotels need enrichment."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.get_rms_hotels_needing_enrichment = AsyncMock(return_value=[])
                
                repo = RMSRepo()
                result = await repo.get_hotels_needing_enrichment(limit=100)
                
                assert result == []


class TestRMSRepoInsertHotel:
    """Tests for insert_hotel."""
    
    @pytest.mark.asyncio
    async def test_inserts_and_returns_id(self):
        """Should insert hotel and return the new ID."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.insert_rms_hotel = AsyncMock(return_value=42)
                
                repo = RMSRepo()
                result = await repo.insert_hotel(
                    name="Test Hotel",
                    address="123 Main St",
                    city="Test City",
                    state="TS",
                    country="USA",
                    phone="555-1234",
                    email="test@hotel.com",
                    website="https://testhotel.com",
                )
                
                assert result == 42
                mock_queries.insert_rms_hotel.assert_called_once()


class TestRMSRepoUpdateHotel:
    """Tests for update_hotel."""
    
    @pytest.mark.asyncio
    async def test_updates_hotel(self):
        """Should call update query with correct params."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.update_rms_hotel = AsyncMock()
                
                repo = RMSRepo()
                await repo.update_hotel(
                    hotel_id=1,
                    name="Updated Name",
                    email="new@email.com",
                )
                
                mock_queries.update_rms_hotel.assert_called_once()
                call_kwargs = mock_queries.update_rms_hotel.call_args[1]
                assert call_kwargs["hotel_id"] == 1
                assert call_kwargs["name"] == "Updated Name"
                assert call_kwargs["email"] == "new@email.com"


class TestRMSRepoGetStats:
    """Tests for get_stats."""
    
    @pytest.mark.asyncio
    async def test_returns_stats_dict(self):
        """Should return statistics dictionary."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.get_rms_stats = AsyncMock(return_value={
                    "total": 100,
                    "with_name": 80,
                    "with_email": 50,
                })
                
                repo = RMSRepo()
                result = await repo.get_stats()
                
                assert result["total"] == 100
                assert result["with_name"] == 80
                assert result["with_email"] == 50
    
    @pytest.mark.asyncio
    async def test_returns_defaults_when_none(self):
        """Should return default stats when query returns None."""
        with patch("services.enrichment.rms_repo.get_conn") as mock_conn:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = MagicMock()
            mock_conn.return_value = mock_context
            
            with patch("services.enrichment.rms_repo.queries") as mock_queries:
                mock_queries.get_rms_stats = AsyncMock(return_value=None)
                
                repo = RMSRepo()
                result = await repo.get_stats()
                
                assert result["total"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
