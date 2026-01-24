"""Tests for base ingestor class."""

import pytest
from typing import List, AsyncIterator, Tuple
from unittest.mock import AsyncMock, patch

from services.ingestor.base import BaseIngestor
from services.ingestor.models.base import BaseRecord, IngestStats


class ConcreteIngestor(BaseIngestor[BaseRecord]):
    """Concrete implementation for testing."""

    source_name = "test"
    external_id_type = "test_id"

    def __init__(self, records: List[BaseRecord] = None):
        self._records = records or []
        self._fetch_called = False
        self._parse_called = False

    async def fetch(self) -> AsyncIterator[Tuple[str, bytes]]:
        self._fetch_called = True
        yield "test.csv", b"test data"

    def parse(self, data: bytes, filename: str = "") -> List[BaseRecord]:
        self._parse_called = True
        return self._records


class TestBaseIngestor:
    """Tests for BaseIngestor abstract class."""

    @pytest.mark.no_db
    def test_validate_passes_valid_record(self):
        """Validate passes record with name and external_id."""
        ingestor = ConcreteIngestor()
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test Hotel",
            source="test",
        )

        result = ingestor.validate(record)
        assert result == record

    @pytest.mark.no_db
    def test_validate_rejects_missing_name(self):
        """Validate rejects record without name."""
        ingestor = ConcreteIngestor()
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="",
            source="test",
        )

        result = ingestor.validate(record)
        assert result is None

    @pytest.mark.no_db
    def test_validate_rejects_missing_external_id(self):
        """Validate rejects record without external_id."""
        ingestor = ConcreteIngestor()
        record = BaseRecord(
            external_id="",
            external_id_type="test",
            name="Test",
            source="test",
        )

        result = ingestor.validate(record)
        assert result is None

    @pytest.mark.no_db
    def test_transform_returns_record(self):
        """Transform returns record unchanged by default."""
        ingestor = ConcreteIngestor()
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test",
            source="test",
        )

        result = ingestor.transform(record)
        assert result == record

    @pytest.mark.no_db
    def test_deduplicate_removes_duplicates(self):
        """Deduplicate removes records with same external_id."""
        ingestor = ConcreteIngestor()
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s"),
            BaseRecord(external_id="2", external_id_type="t", name="B", source="s"),
            BaseRecord(external_id="1", external_id_type="t", name="A2", source="s"),
        ]

        result = ingestor.deduplicate(records)

        assert len(result) == 2
        ids = {r.external_id for r in result}
        assert ids == {"1", "2"}

    @pytest.mark.no_db
    def test_apply_filters_by_county(self):
        """Apply county filter."""
        ingestor = ConcreteIngestor()
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s", county="Miami-Dade"),
            BaseRecord(external_id="2", external_id_type="t", name="B", source="s", county="Orange"),
            BaseRecord(external_id="3", external_id_type="t", name="C", source="s", county="Broward"),
        ]

        result = ingestor._apply_filters(records, {"counties": ["Miami-Dade", "Broward"]})

        assert len(result) == 2
        counties = {r.county for r in result}
        assert counties == {"Miami-Dade", "Broward"}

    @pytest.mark.no_db
    def test_apply_filters_by_state(self):
        """Apply state filter."""
        ingestor = ConcreteIngestor()
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s", state="FL"),
            BaseRecord(external_id="2", external_id_type="t", name="B", source="s", state="TX"),
            BaseRecord(external_id="3", external_id_type="t", name="C", source="s", state="FL"),
        ]

        result = ingestor._apply_filters(records, {"states": ["FL"]})

        assert len(result) == 2
        assert all(r.state == "FL" for r in result)

    @pytest.mark.no_db
    def test_apply_filters_by_category(self):
        """Apply category filter."""
        ingestor = ConcreteIngestor()
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s", category="hotel"),
            BaseRecord(external_id="2", external_id_type="t", name="B", source="s", category="motel"),
            BaseRecord(external_id="3", external_id_type="t", name="C", source="s", category="hotel"),
        ]

        result = ingestor._apply_filters(records, {"categories": ["hotel"]})

        assert len(result) == 2
        assert all(r.category == "hotel" for r in result)

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_calls_fetch_and_parse(self):
        """Ingest calls fetch and parse methods."""
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s"),
        ]
        ingestor = ConcreteIngestor(records)

        with patch.object(ingestor, "_batch_save", new_callable=AsyncMock) as mock_save:
            mock_save.return_value = 1

            result_records, stats = await ingestor.ingest()

            assert ingestor._fetch_called
            assert ingestor._parse_called
            assert len(result_records) == 1
            assert stats.records_parsed == 1

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_saves_records(self):
        """Ingest saves records to database."""
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s"),
        ]
        ingestor = ConcreteIngestor(records)

        with patch.object(ingestor, "_batch_save", new_callable=AsyncMock) as mock_save:
            mock_save.return_value = 1
            result_records, stats = await ingestor.ingest()

            mock_save.assert_called_once()
            assert stats.records_saved == 1

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_applies_filters(self):
        """Ingest applies filters to records."""
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s", county="Miami-Dade"),
            BaseRecord(external_id="2", external_id_type="t", name="B", source="s", county="Orange"),
        ]
        ingestor = ConcreteIngestor(records)

        with patch.object(ingestor, "_batch_save", new_callable=AsyncMock) as mock_save:
            mock_save.return_value = 1
            result_records, stats = await ingestor.ingest(
                filters={"counties": ["Miami-Dade"]},
            )

        assert len(result_records) == 1
        assert result_records[0].county == "Miami-Dade"

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_ingest_tracks_stats(self):
        """Ingest tracks statistics correctly."""
        records = [
            BaseRecord(external_id="1", external_id_type="t", name="A", source="s"),
            BaseRecord(external_id="2", external_id_type="t", name="B", source="s"),
            BaseRecord(external_id="1", external_id_type="t", name="A2", source="s"),  # Duplicate
        ]
        ingestor = ConcreteIngestor(records)

        with patch.object(ingestor, "_batch_save", new_callable=AsyncMock) as mock_save:
            mock_save.return_value = 2
            result_records, stats = await ingestor.ingest()

        assert stats.files_processed == 1
        assert stats.records_parsed == 3
        assert stats.duplicates_skipped == 1
