from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, ConfigDict


class ExistingCustomer(BaseModel):
    """Existing Sadie customer model matching the database schema."""

    id: int
    name: str
    sadie_hotel_id: Optional[str] = None

    # Location
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: str = "USA"
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Status
    status: str = "active"
    go_live_date: Optional[date] = None

    # Metadata
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)
