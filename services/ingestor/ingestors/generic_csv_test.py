"""Tests for generic CSV ingestor."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from services.ingestor.ingestors.generic_csv import (
    GenericCSVIngestor,
    _transform_value,
    create_ingestor_from_config,
)
from services.ingestor.config import CSVIngestorConfig, ColumnMapping


class TestTransformValue:
    """Tests for value transformation function."""

    @pytest.mark.no_db
    def test_transform_int(self):
        """Transform to integer."""
        assert _transform_value("123", "int") == 123
        assert _transform_value("", "int") is None
        assert _transform_value("abc", "int") is None

    @pytest.mark.no_db
    def test_transform_float(self):
        """Transform to float."""
        assert _transform_value("123.45", "float") == 123.45
        assert _transform_value("", "float") is None
        assert _transform_value("abc", "float") is None

    @pytest.mark.no_db
    def test_transform_phone(self):
        """Transform phone number."""
        assert _transform_value("5551234567", "phone") == "555-123-4567"
        assert _transform_value("555123", "phone") == "555123"  # Not 10 digits
        assert _transform_value("", "phone") is None

    @pytest.mark.no_db
    def test_transform_lower(self):
        """Transform to lowercase."""
        assert _transform_value("HELLO", "lower") == "hello"

    @pytest.mark.no_db
    def test_transform_upper(self):
        """Transform to uppercase."""
        assert _transform_value("hello", "upper") == "HELLO"

    @pytest.mark.no_db
    def test_transform_strip(self):
        """Transform strips whitespace."""
        assert _transform_value("  hello  ", "strip") == "hello"

    @pytest.mark.no_db
    def test_transform_none(self):
        """No transform returns original value."""
        assert _transform_value("hello", None) == "hello"

    @pytest.mark.no_db
    def test_transform_unknown(self):
        """Unknown transform returns original value."""
        assert _transform_value("hello", "unknown") == "hello"


class TestGenericCSVIngestor:
    """Tests for GenericCSVIngestor."""

    @pytest.mark.no_db
    def test_init_with_local_config(self):
        """Initialize with local source config."""
        with TemporaryDirectory() as tmpdir:
            config = CSVIngestorConfig(
                name="test_source",
                external_id_type="test_license",
                source_type="local",
                local_path=tmpdir,
                columns=[
                    ColumnMapping(column="ID", field="external_id"),
                    ColumnMapping(column="NAME", field="name"),
                ],
                external_id_columns=["ID"],
            )

            ingestor = GenericCSVIngestor(config)

            assert ingestor.source_name == "test_source"
            assert ingestor.external_id_type == "test_license"

    @pytest.mark.no_db
    def test_parse_with_header(self):
        """Parse CSV with header row."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            has_header=True,
            columns=[
                ColumnMapping(column="LICENSE_NO", field="external_id"),
                ColumnMapping(column="HOTEL_NAME", field="name"),
                ColumnMapping(column="CITY", field="city"),
            ],
            external_id_columns=["external_id"],
        )

        ingestor = GenericCSVIngestor(config)

        csv_content = b"LICENSE_NO,HOTEL_NAME,CITY\nL001,Test Hotel,Miami\nL002,Another Hotel,Orlando"
        records = ingestor.parse(csv_content)

        assert len(records) == 2
        assert records[0].external_id == "L001"
        assert records[0].name == "Test Hotel"
        assert records[0].city == "Miami"

    @pytest.mark.no_db
    def test_parse_without_header(self):
        """Parse CSV without header row."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            has_header=False,
            columns=[
                ColumnMapping(column=0, field="external_id"),
                ColumnMapping(column=1, field="name"),
                ColumnMapping(column=2, field="city"),
            ],
            external_id_columns=["external_id"],
        )

        ingestor = GenericCSVIngestor(config)

        csv_content = b"L001,Test Hotel,Miami\nL002,Another Hotel,Orlando"
        records = ingestor.parse(csv_content)

        assert len(records) == 2
        assert records[0].external_id == "L001"
        assert records[0].name == "Test Hotel"

    @pytest.mark.no_db
    def test_parse_with_transform(self):
        """Parse CSV with column transforms."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            has_header=True,
            columns=[
                ColumnMapping(column="ID", field="external_id"),
                ColumnMapping(column="NAME", field="name"),
                ColumnMapping(column="PHONE", field="phone", transform="phone"),
                ColumnMapping(column="ROOMS", field="room_count", transform="int"),
            ],
            external_id_columns=["external_id"],
        )

        ingestor = GenericCSVIngestor(config)

        csv_content = b"ID,NAME,PHONE,ROOMS\nL001,Test Hotel,5551234567,100"
        records = ingestor.parse(csv_content)

        assert len(records) == 1
        assert records[0].phone == "555-123-4567"
        assert records[0].room_count == 100

    @pytest.mark.no_db
    def test_parse_with_state_filter(self):
        """Parse CSV with state filter."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            has_header=True,
            columns=[
                ColumnMapping(column="ID", field="external_id"),
                ColumnMapping(column="NAME", field="name"),
                ColumnMapping(column="STATE", field="state"),
            ],
            external_id_columns=["external_id"],
            state_filter="FL",
        )

        ingestor = GenericCSVIngestor(config)

        csv_content = b"ID,NAME,STATE\nL001,Hotel A,FL\nL002,Hotel B,TX\nL003,Hotel C,FL"
        records = ingestor.parse(csv_content)

        assert len(records) == 2
        assert all(r.state == "FL" for r in records)

    @pytest.mark.no_db
    def test_parse_with_defaults(self):
        """Parse CSV applies default values."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            has_header=True,
            columns=[
                ColumnMapping(column="ID", field="external_id"),
                ColumnMapping(column="NAME", field="name"),
            ],
            external_id_columns=["external_id"],
            default_category="hotel",
            default_country="USA",
        )

        ingestor = GenericCSVIngestor(config)

        csv_content = b"ID,NAME\nL001,Test Hotel"
        records = ingestor.parse(csv_content)

        assert len(records) == 1
        assert records[0].category == "hotel"
        assert records[0].country == "USA"

    @pytest.mark.no_db
    def test_parse_composite_external_id(self):
        """Parse CSV with composite external ID."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            has_header=True,
            columns=[
                ColumnMapping(column="TAXPAYER", field="taxpayer"),
                ColumnMapping(column="LOCATION", field="location"),
                ColumnMapping(column="NAME", field="name"),
            ],
            external_id_columns=["taxpayer", "location"],
            external_id_separator=":",
        )

        ingestor = GenericCSVIngestor(config)

        csv_content = b"TAXPAYER,LOCATION,NAME\n12345,001,Test Hotel"
        records = ingestor.parse(csv_content)

        assert len(records) == 1
        assert records[0].external_id == "12345:001"


class TestCreateIngestorFromConfig:
    """Tests for factory function."""

    @pytest.mark.no_db
    def test_creates_ingestor(self):
        """Create ingestor from config."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="local",
            local_path="/tmp",
            columns=[],
            external_id_columns=["ID"],
        )

        ingestor = create_ingestor_from_config(config)

        assert isinstance(ingestor, GenericCSVIngestor)
        assert ingestor.config == config
