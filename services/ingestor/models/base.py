"""
Base models for all ingestors.
"""

from typing import Optional
from pydantic import BaseModel, Field


class BaseRecord(BaseModel):
    """Base model for all ingested records."""

    # Required identifiers
    external_id: str
    external_id_type: str
    name: str

    # Location
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    county: Optional[str] = None
    country: str = "USA"

    # Contact
    phone: Optional[str] = None
    website: Optional[str] = None

    # Classification
    category: Optional[str] = None
    source: str

    # Room count (if available from source)
    room_count: Optional[int] = None

    # Raw data for debugging
    raw: dict = Field(default_factory=dict)

    def to_db_tuple(self) -> tuple:
        """
        Convert to tuple for batch insert.
        Format: (name, source, status, address, city, state, country, phone, category, external_id)
        """
        return (
            self.name,
            self.source,
            0,  # HOTEL_STATUS_PENDING
            self.address,
            self.city,
            self.state,
            self.country,
            self.phone,
            self.category,
            self.external_id,
        )


class IngestStats(BaseModel):
    """Statistics from an ingestion run."""

    files_processed: int = 0
    records_parsed: int = 0
    records_saved: int = 0
    duplicates_skipped: int = 0
    errors: int = 0

    def to_dict(self) -> dict:
        """Convert to dict for API responses."""
        return {
            "files_processed": self.files_processed,
            "files_downloaded": self.files_processed,  # Alias for backwards compat
            "records_parsed": self.records_parsed,
            "records_saved": self.records_saved,
            "duplicates_skipped": self.duplicates_skipped,
            "errors": self.errors,
        }
