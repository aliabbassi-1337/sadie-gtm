"""Tests for ingestor configuration models."""

import pytest
from services.ingestor.config import (
    ColumnMapping,
    CSVIngestorConfig,
    IngestorConfig,
)


class TestColumnMapping:
    """Tests for ColumnMapping model."""

    @pytest.mark.no_db
    def test_create_with_string_column(self):
        """Create mapping with string column name."""
        mapping = ColumnMapping(
            column="LICENSE_NO",
            field="external_id",
        )

        assert mapping.column == "LICENSE_NO"
        assert mapping.field == "external_id"
        assert mapping.transform is None
        assert mapping.default is None

    @pytest.mark.no_db
    def test_create_with_int_column(self):
        """Create mapping with integer column index."""
        mapping = ColumnMapping(
            column=0,
            field="taxpayer_number",
        )

        assert mapping.column == 0
        assert mapping.field == "taxpayer_number"

    @pytest.mark.no_db
    def test_create_with_transform(self):
        """Create mapping with transform."""
        mapping = ColumnMapping(
            column="PHONE",
            field="phone",
            transform="phone",
        )

        assert mapping.transform == "phone"

    @pytest.mark.no_db
    def test_create_with_default(self):
        """Create mapping with default value."""
        mapping = ColumnMapping(
            column="COUNTRY",
            field="country",
            default="United States",
        )

        assert mapping.default == "United States"


class TestCSVIngestorConfig:
    """Tests for CSVIngestorConfig model."""

    @pytest.mark.no_db
    def test_create_http_config(self):
        """Create config for HTTP source."""
        config = CSVIngestorConfig(
            name="test_source",
            external_id_type="test_license",
            source_type="http",
            urls=["https://example.com/data.csv"],
            columns=[
                ColumnMapping(column="ID", field="external_id"),
                ColumnMapping(column="NAME", field="name"),
            ],
            external_id_columns=["ID"],
        )

        assert config.name == "test_source"
        assert config.external_id_type == "test_license"
        assert config.source_type == "http"
        assert len(config.urls) == 1
        assert len(config.columns) == 2

    @pytest.mark.no_db
    def test_create_s3_config(self):
        """Create config for S3 source."""
        config = CSVIngestorConfig(
            name="s3_source",
            external_id_type="s3_license",
            source_type="s3",
            s3_bucket="my-bucket",
            s3_prefix="data/",
            columns=[
                ColumnMapping(column="ID", field="external_id"),
            ],
            external_id_columns=["ID"],
        )

        assert config.source_type == "s3"
        assert config.s3_bucket == "my-bucket"
        assert config.s3_prefix == "data/"

    @pytest.mark.no_db
    def test_create_local_config(self):
        """Create config for local source."""
        config = CSVIngestorConfig(
            name="local_source",
            external_id_type="local_license",
            source_type="local",
            local_path="/data/files",
            local_pattern="*.csv",
            columns=[
                ColumnMapping(column="ID", field="external_id"),
            ],
            external_id_columns=["ID"],
        )

        assert config.source_type == "local"
        assert config.local_path == "/data/files"
        assert config.local_pattern == "*.csv"

    @pytest.mark.no_db
    def test_source_name_uses_name_by_default(self):
        """Source name defaults to config name."""
        config = CSVIngestorConfig(
            name="my_source",
            external_id_type="test",
            source_type="local",
            local_path="/data",
            columns=[],
            external_id_columns=["ID"],
        )

        assert config.source_name == "my_source"

    @pytest.mark.no_db
    def test_source_name_uses_default_source(self):
        """Source name uses default_source when set."""
        config = CSVIngestorConfig(
            name="my_source",
            external_id_type="test",
            source_type="local",
            local_path="/data",
            columns=[],
            external_id_columns=["ID"],
            default_source="custom_source",
        )

        assert config.source_name == "custom_source"

    @pytest.mark.no_db
    def test_default_values(self):
        """Config has sensible defaults."""
        config = CSVIngestorConfig(
            name="test",
            external_id_type="test",
            source_type="http",
            columns=[],
            external_id_columns=["ID"],
        )

        assert config.has_header is True
        assert config.encoding == "utf-8"
        assert config.delimiter == ","
        assert config.quotechar == '"'
        assert config.external_id_separator == ":"
        assert config.default_country == "United States"
        assert config.use_cache is True


class TestIngestorConfig:
    """Tests for IngestorConfig model."""

    @pytest.mark.no_db
    def test_create_empty_config(self):
        """Create config with defaults."""
        config = IngestorConfig()

        assert config.counties is None
        assert config.states is None
        assert config.categories is None
        assert config.batch_size == 500
        assert config.options == {}

    @pytest.mark.no_db
    def test_create_with_filters(self):
        """Create config with filters."""
        config = IngestorConfig(
            counties=["Miami-Dade", "Broward"],
            states=["FL"],
            categories=["hotel", "motel"],
        )

        assert config.counties == ["Miami-Dade", "Broward"]
        assert config.states == ["FL"]
        assert config.categories == ["hotel", "motel"]

    @pytest.mark.no_db
    def test_create_with_options(self):
        """Create config with custom options."""
        config = IngestorConfig(
            options={"new_only": True, "quarter": "Q3 2025"},
        )

        assert config.options["new_only"] is True
        assert config.options["quarter"] == "Q3 2025"
