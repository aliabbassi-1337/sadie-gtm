from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class HotelBookingEngine(BaseModel):
    """Hotel booking engine detection model matching the database schema."""

    hotel_id: int
    booking_engine_id: Optional[int] = None

    # Detection metadata
    booking_url: Optional[str] = None
    detection_method: Optional[str] = None

    # Timestamps
    detected_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
