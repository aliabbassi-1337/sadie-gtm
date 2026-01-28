"""Tests for RMS Ingestor."""

import pytest

from services.ingestor.ingestors.rms import RMSIngestor, RMSIngestResult
from services.rms import RMSRepo, ExtractedRMSData


@pytest.fixture
async def ingestor():
    """Create ingestor with real repo."""
    return RMSIngestor(repo=RMSRepo())


@pytest.mark.no_db
class TestRMSIngestorInit:
    """Tests for RMSIngestor initialization (no DB needed)."""
    
    def test_has_source_name(self):
        """Should have correct source name."""
        ingestor = RMSIngestor()
        assert ingestor.source_name == "rms_scan"
    
    def test_has_external_id_type(self):
        """Should have correct external_id_type."""
        ingestor = RMSIngestor()
        assert ingestor.external_id_type == "rms_slug"


@pytest.mark.no_db
class TestRMSIngestorShutdown:
    """Tests for shutdown handling (no DB needed)."""
    
    def test_request_shutdown(self):
        """Should set shutdown flag."""
        ingestor = RMSIngestor()
        assert ingestor._shutdown_requested is False
        ingestor.request_shutdown()
        assert ingestor._shutdown_requested is True


class TestRMSIngestorSaveBatch:
    """Tests for _save_batch."""
    
    @pytest.mark.asyncio
    async def test_saves_hotels_to_db(self, ingestor):
        """Should save hotels to database."""
        import uuid
        
        unique_id = str(uuid.uuid4())[:8]
        
        hotels = [
            ExtractedRMSData(
                slug=f"ingest_test_{unique_id}",
                booking_url=f"https://ibe.rmscloud.com/ingest_test_{unique_id}",
                name=f"Ingestor Test Hotel {unique_id}",
                phone="555-1234",
            ),
        ]
        
        booking_engine_id = await ingestor._repo.get_booking_engine_id()
        saved = await ingestor._save_batch(hotels, booking_engine_id)
        
        assert saved == 1


@pytest.mark.online
@pytest.mark.integration
class TestRMSIngestorIntegration:
    """Integration tests that hit live RMS URLs."""
    
    @pytest.mark.asyncio
    async def test_ingest_dry_run(self, ingestor):
        """Should scan IDs in dry run mode."""
        # Small range for testing
        result = await ingestor.ingest(
            start_id=1,
            end_id=3,
            concurrency=2,
            dry_run=True,
        )
        
        assert isinstance(result, RMSIngestResult)
        assert result.total_scanned == 2
        assert result.hotels_saved == 0  # Dry run


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "not online"])
