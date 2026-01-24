"""
Ingestor configuration models.
"""

from typing import Optional, List, Literal, Callable, Any, Union
from pydantic import BaseModel, Field


class ColumnMapping(BaseModel):
    """Maps a CSV column to a record field."""

    # Column identifier - either name (for header CSVs) or index (for no-header CSVs)
    column: Union[str, int]

    # Target field name on the record
    field: str

    # Optional transform function name
    transform: Optional[str] = None  # "phone", "int", "float", "lower", "upper", "strip"

    # Default value if column is empty
    default: Optional[Any] = None


class CSVIngestorConfig(BaseModel):
    """
    Configuration for a CSV-based ingestor.

    This allows creating new ingestors with zero code - just configuration.
    """

    # Identification
    name: str = Field(..., description="Unique name for this ingestor")
    external_id_type: str = Field(..., description="Type for external_id deduplication")

    # Data source
    source_type: Literal["http", "s3", "local"] = Field(
        ..., description="Where to fetch data from"
    )

    # HTTP source options
    urls: List[str] = Field(default_factory=list, description="URLs to download")
    http_timeout: float = Field(default=120.0, description="HTTP timeout in seconds")

    # S3 source options
    s3_bucket: Optional[str] = Field(default=None, description="S3 bucket name")
    s3_prefix: Optional[str] = Field(default=None, description="S3 key prefix")
    s3_pattern: str = Field(default="*.csv", description="File pattern to match")

    # Local source options
    local_path: Optional[str] = Field(default=None, description="Local directory path")
    local_pattern: str = Field(default="*.csv", description="File pattern to match")

    # CSV parsing options
    has_header: bool = Field(default=True, description="Whether CSV has header row")
    encoding: str = Field(default="utf-8", description="File encoding")
    delimiter: str = Field(default=",", description="Column delimiter")
    quotechar: str = Field(default='"', description="Quote character")

    # Column mappings
    columns: List[ColumnMapping] = Field(
        default_factory=list, description="Column to field mappings"
    )

    # External ID construction
    external_id_columns: List[str] = Field(
        ..., description="Fields to join for external_id"
    )
    external_id_separator: str = Field(
        default=":", description="Separator for external_id parts"
    )

    # Filtering
    state_filter: Optional[str] = Field(
        default=None, description="Only include records from this state"
    )

    # Default values
    default_category: Optional[str] = Field(
        default=None, description="Default category for all records"
    )
    default_country: str = Field(default="USA", description="Default country")
    default_source: Optional[str] = Field(
        default=None, description="Source name (defaults to config name)"
    )

    # Caching
    cache_dir: Optional[str] = Field(
        default=None, description="Directory for caching downloaded files"
    )
    use_cache: bool = Field(default=True, description="Whether to use cached files")

    @property
    def source_name(self) -> str:
        """Get the source name for database records."""
        return self.default_source or self.name


class IngestorConfig(BaseModel):
    """
    Base configuration for any ingestor.

    Used for runtime configuration that doesn't define the ingestor itself.
    """

    # Filtering options
    counties: Optional[List[str]] = None
    states: Optional[List[str]] = None
    categories: Optional[List[str]] = None

    # Processing options
    batch_size: int = 500

    # Source-specific options (passed to ingestor)
    options: dict = Field(default_factory=dict)
