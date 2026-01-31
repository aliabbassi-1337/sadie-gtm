"""Tests for base models."""

import pytest
from services.ingestor.models.base import BaseRecord, IngestStats


class TestBaseRecord:
    """Tests for BaseRecord model."""

    @pytest.mark.no_db
    def test_create_with_required_fields(self):
        """Create record with only required fields."""
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test_source",
            name="Test Hotel",
            source="test",
        )

        assert record.external_id == "test-123"
        assert record.external_id_type == "test_source"
        assert record.name == "Test Hotel"
        assert record.source == "test"

    @pytest.mark.no_db
    def test_optional_fields_default_none(self):
        """Optional fields default to None."""
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test",
            source="test",
        )

        assert record.address is None
        assert record.city is None
        assert record.state is None
        assert record.zip_code is None
        assert record.county is None
        assert record.phone is None
        assert record.category is None
        assert record.room_count is None

    @pytest.mark.no_db
    def test_country_defaults_to_usa(self):
        """Country defaults to USA."""
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test",
            source="test",
        )

        assert record.country == "United States"

    @pytest.mark.no_db
    def test_raw_defaults_to_empty_dict(self):
        """Raw field defaults to empty dict."""
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test",
            source="test",
        )

        assert record.raw == {}

    @pytest.mark.no_db
    def test_to_db_tuple(self):
        """Convert record to database tuple."""
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test Hotel",
            source="test_source",
            address="123 Main St",
            city="Houston",
            state="TX",
            country="United States",
            phone="555-1234",
            category="hotel",
            lat=29.7604,
            lon=-95.3698,
        )

        result = record.to_db_tuple()

        assert result == (
            "Test Hotel",      # name
            "test_source",     # source
            0,                 # status (HOTEL_STATUS_PENDING)
            "123 Main St",     # address
            "Houston",         # city
            "TX",              # state
            "United States",   # country
            "555-1234",        # phone
            "hotel",           # category
            "test-123",        # external_id
            29.7604,           # lat
            -95.3698,          # lon
        )

    @pytest.mark.no_db
    def test_to_db_tuple_with_none_values(self):
        """Convert record with None values to database tuple."""
        record = BaseRecord(
            external_id="test-123",
            external_id_type="test",
            name="Test Hotel",
            source="test_source",
        )

        result = record.to_db_tuple()

        assert result == (
            "Test Hotel",
            "test_source",
            0,
            None,
            None,
            None,
            "United States",
            None,
            None,
            "test-123",
            None,  # lat
            None,  # lon
        )


class TestIngestStats:
    """Tests for IngestStats model."""

    @pytest.mark.no_db
    def test_defaults_to_zero(self):
        """All fields default to zero."""
        stats = IngestStats()

        assert stats.files_processed == 0
        assert stats.records_parsed == 0
        assert stats.records_saved == 0
        assert stats.duplicates_skipped == 0
        assert stats.errors == 0

    @pytest.mark.no_db
    def test_to_dict(self):
        """Convert stats to dictionary."""
        stats = IngestStats(
            files_processed=5,
            records_parsed=100,
            records_saved=90,
            duplicates_skipped=10,
            errors=2,
        )

        result = stats.to_dict()

        assert result["files_processed"] == 5
        assert result["files_downloaded"] == 5  # Alias
        assert result["records_parsed"] == 100
        assert result["records_saved"] == 90
        assert result["duplicates_skipped"] == 10
        assert result["errors"] == 2

    @pytest.mark.no_db
    def test_to_dict_empty_stats(self):
        """Convert empty stats to dictionary."""
        stats = IngestStats()
        result = stats.to_dict()

        assert result["files_processed"] == 0
        assert result["records_parsed"] == 0
