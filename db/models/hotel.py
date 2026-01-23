from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class Hotel(BaseModel):
    """Hotel model matching the database schema."""

    id: int
    name: str
    external_id: Optional[str] = None  # External ID for deduplication
    external_id_type: Optional[str] = None  # Type of external ID (google_place, texas_hot, dbpr_license)
    website: Optional[str] = None

    # Contact
    phone_google: Optional[str] = None
    phone_website: Optional[str] = None
    email: Optional[str] = None

    # Location
    city: Optional[str] = None
    state: Optional[str] = None
    country: str = "USA"
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Ratings
    rating: Optional[float] = None
    review_count: Optional[int] = None

    # Pipeline
    status: int = 0

    # Metadata
    source: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
