"""Tests for ingestor service."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from services.ingestor.service import Service, IService
from services.ingestor.models.base import IngestStats
from services.ingestor.models.dbpr import LICENSE_TYPES


class TestService:
    """Tests for Service class."""

    @pytest.mark.no_db
    def test_implements_interface(self):
        """Service implements IService interface."""
        service = Service()
        assert isinstance(service, IService)

    @pytest.mark.no_db
    def test_list_sources(self):
        """List registered sources."""
        service = Service()
        sources = service.list_sources()

        assert "dbpr" in sources
        assert "texas" in sources

    @pytest.mark.no_db
    def test_get_dbpr_license_types(self):
        """Get DBPR license types."""
        service = Service()
        types = service.get_dbpr_license_types()

        assert types == LICENSE_TYPES
        assert types is not LICENSE_TYPES  # Should be a copy

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_creates_ingestor(self):
        """Ingest creates and runs ingestor."""
        service = Service()

        with patch("services.ingestor.registry.get_ingestor") as mock_get:
            mock_ingestor_cls = MagicMock()
            mock_ingestor = AsyncMock()
            mock_ingestor.ingest = AsyncMock(return_value=([], IngestStats()))
            mock_ingestor_cls.return_value = mock_ingestor
            mock_get.return_value = mock_ingestor_cls

            records, stats = await service.ingest("test_source")

            mock_get.assert_called_once_with("test_source")
            mock_ingestor.ingest.assert_called_once()

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_passes_kwargs(self):
        """Ingest passes kwargs to ingestor constructor."""
        service = Service()

        with patch("services.ingestor.registry.get_ingestor") as mock_get:
            mock_ingestor_cls = MagicMock()
            mock_ingestor = AsyncMock()
            mock_ingestor.ingest = AsyncMock(return_value=([], IngestStats()))
            mock_ingestor_cls.return_value = mock_ingestor
            mock_get.return_value = mock_ingestor_cls

            await service.ingest(
                "test_source",
                new_only=True,
                quarter="Q3",
            )

            mock_ingestor_cls.assert_called_once_with(new_only=True, quarter="Q3")

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_passes_filters(self):
        """Ingest passes filters to ingest method."""
        service = Service()

        with patch("services.ingestor.registry.get_ingestor") as mock_get:
            mock_ingestor_cls = MagicMock()
            mock_ingestor = AsyncMock()
            mock_ingestor.ingest = AsyncMock(return_value=([], IngestStats()))
            mock_ingestor_cls.return_value = mock_ingestor
            mock_get.return_value = mock_ingestor_cls

            filters = {"counties": ["Miami-Dade"]}
            await service.ingest("test_source", filters=filters)

            mock_ingestor.ingest.assert_called_once_with(filters=filters)

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_dbpr_creates_dbpr_ingestor(self):
        """ingest_dbpr creates DBPRIngestor."""
        service = Service()

        with patch(
            "services.ingestor.service.DBPRIngestor"
        ) as mock_ingestor_cls:
            mock_ingestor = AsyncMock()
            mock_ingestor.ingest = AsyncMock(
                return_value=([], IngestStats())
            )
            mock_ingestor_cls.return_value = mock_ingestor

            await service.ingest_dbpr(
                counties=["Miami-Dade"],
                license_types=["Hotel"],
                new_only=True,
            )

            mock_ingestor_cls.assert_called_once_with(new_only=True)
            mock_ingestor.ingest.assert_called_once()

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_texas_creates_texas_ingestor(self):
        """ingest_texas creates TexasIngestor."""
        service = Service()

        with patch(
            "services.ingestor.service.TexasIngestor"
        ) as mock_ingestor_cls:
            mock_ingestor = AsyncMock()
            mock_ingestor.ingest = AsyncMock(
                return_value=([], IngestStats())
            )
            mock_ingestor_cls.return_value = mock_ingestor

            await service.ingest_texas(quarter="HOT 25 Q3")

            mock_ingestor_cls.assert_called_once_with(quarter="HOT 25 Q3")
            mock_ingestor.ingest.assert_called_once()
