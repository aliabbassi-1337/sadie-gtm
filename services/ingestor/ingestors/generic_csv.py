"""
Generic CSV Ingestor - Configuration-driven CSV parser.

This allows creating new ingestors with zero code - just configuration.
"""

import csv
import io
from pathlib import Path
from typing import List, Optional, AsyncIterator, Tuple, Any

from loguru import logger

from services.ingestor.base import BaseIngestor
from services.ingestor.registry import register
from services.ingestor.models.base import BaseRecord, IngestStats
from services.ingestor.config import CSVIngestorConfig, ColumnMapping
from services.ingestor.sources.http import HTTPSource
from services.ingestor.sources.s3 import S3Source
from services.ingestor.sources.local import LocalSource


def _transform_value(value: str, transform: Optional[str]) -> Any:
    """Apply a transform to a value."""
    if not transform:
        return value

    # Handle empty values for each transform type
    if not value:
        if transform in ("int", "float", "phone"):
            return None
        return value

    if transform == "int":
        try:
            return int(value)
        except ValueError:
            return None
    elif transform == "float":
        try:
            return float(value)
        except ValueError:
            return None
    elif transform == "phone":
        # Format 10-digit phone
        digits = "".join(c for c in value if c.isdigit())
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        return value if value else None
    elif transform == "lower":
        return value.lower()
    elif transform == "upper":
        return value.upper()
    elif transform == "strip":
        return value.strip()
    else:
        return value


class GenericCSVIngestor(BaseIngestor[BaseRecord]):
    """
    Configuration-driven CSV ingestor.

    Usage:
        config = CSVIngestorConfig(
            name="new_state",
            external_id_type="new_state_license",
            source_type="s3",
            s3_bucket="my-bucket",
            s3_prefix="data/",
            columns=[
                ColumnMapping(column="LICENSE_NO", field="external_id"),
                ColumnMapping(column="NAME", field="name"),
                ColumnMapping(column="PHONE", field="phone", transform="phone"),
            ],
            external_id_columns=["LICENSE_NO"],
        )

        ingestor = GenericCSVIngestor(config)
        records, stats = await ingestor.ingest()
    """

    def __init__(self, config: CSVIngestorConfig):
        """
        Initialize generic CSV ingestor.

        Args:
            config: CSV ingestor configuration
        """
        self.config = config
        self._source = self._create_source()

    @property
    def source_name(self) -> str:
        return self.config.source_name

    @property
    def external_id_type(self) -> str:
        return self.config.external_id_type

    def _create_source(self):
        """Create appropriate source handler based on config."""
        cache_dir = self.config.cache_dir

        if self.config.source_type == "http":
            return HTTPSource(
                urls=self.config.urls,
                timeout=self.config.http_timeout,
                cache_dir=cache_dir,
                use_cache=self.config.use_cache,
            )
        elif self.config.source_type == "s3":
            return S3Source(
                bucket=self.config.s3_bucket,
                prefix=self.config.s3_prefix or "",
                cache_dir=cache_dir,
                use_cache=self.config.use_cache,
            )
        elif self.config.source_type == "local":
            return LocalSource(
                path=self.config.local_path,
                encoding=self.config.encoding,
            )
        else:
            raise ValueError(f"Unknown source type: {self.config.source_type}")

    async def fetch(self) -> AsyncIterator[Tuple[str, bytes]]:
        """Fetch CSV files from configured source."""
        pattern = "*.csv"
        if self.config.source_type == "s3":
            pattern = self.config.s3_pattern
        elif self.config.source_type == "local":
            pattern = self.config.local_pattern

        async for filename, content in self._source.fetch_all(pattern):
            yield filename, content

    def parse(self, data: bytes, filename: str = "") -> List[BaseRecord]:
        """Parse CSV data into records based on configuration."""
        records = []

        # Decode
        try:
            text = data.decode(self.config.encoding)
        except UnicodeDecodeError:
            text = data.decode("latin-1")

        if self.config.has_header:
            reader = csv.DictReader(
                io.StringIO(text),
                delimiter=self.config.delimiter,
                quotechar=self.config.quotechar,
            )
            for row in reader:
                record = self._parse_dict_row(row)
                if record:
                    records.append(record)
        else:
            reader = csv.reader(
                io.StringIO(text),
                delimiter=self.config.delimiter,
                quotechar=self.config.quotechar,
            )
            for row in reader:
                record = self._parse_list_row(row)
                if record:
                    records.append(record)

        return records

    def _parse_dict_row(self, row: dict) -> Optional[BaseRecord]:
        """Parse a row with header columns."""
        data = {}

        for mapping in self.config.columns:
            col_name = mapping.column
            if isinstance(col_name, int):
                # Shouldn't happen with DictReader but handle anyway
                continue

            raw_value = row.get(col_name, "").strip()
            if not raw_value and mapping.default is not None:
                raw_value = str(mapping.default)

            value = _transform_value(raw_value, mapping.transform)
            data[mapping.field] = value

        return self._build_record(data, row)

    def _parse_list_row(self, row: List[str]) -> Optional[BaseRecord]:
        """Parse a row without headers (by index)."""
        data = {}

        for mapping in self.config.columns:
            col_idx = mapping.column
            if isinstance(col_idx, str):
                # Try to convert string to int for index-based parsing
                try:
                    col_idx = int(col_idx)
                except ValueError:
                    continue

            if col_idx < 0 or col_idx >= len(row):
                raw_value = ""
            else:
                raw_value = row[col_idx].strip().strip('"')

            if not raw_value and mapping.default is not None:
                raw_value = str(mapping.default)

            value = _transform_value(raw_value, mapping.transform)
            data[mapping.field] = value

        return self._build_record(data, dict(enumerate(row)))

    def _build_record(self, data: dict, raw: dict) -> Optional[BaseRecord]:
        """Build a BaseRecord from parsed data."""
        # Build external ID from configured columns
        external_id_parts = []
        for col in self.config.external_id_columns:
            part = data.get(col, "")
            if part:
                external_id_parts.append(str(part))

        if not external_id_parts:
            return None

        external_id = self.config.external_id_separator.join(external_id_parts)

        # Get required fields
        name = data.get("name", "")
        if not name:
            return None

        # Apply state filter
        state = data.get("state", "")
        if self.config.state_filter and state != self.config.state_filter:
            return None

        # Build record
        return BaseRecord(
            external_id=external_id,
            external_id_type=self.config.external_id_type,
            name=name,
            address=data.get("address"),
            city=data.get("city"),
            state=state or None,
            zip_code=data.get("zip_code"),
            county=data.get("county"),
            country=data.get("country", self.config.default_country),
            phone=data.get("phone"),
            category=data.get("category", self.config.default_category),
            source=self.config.source_name,
            room_count=data.get("room_count"),
            raw=raw,
        )


def create_ingestor_from_config(config: CSVIngestorConfig) -> GenericCSVIngestor:
    """Factory function to create an ingestor from configuration."""
    return GenericCSVIngestor(config)
