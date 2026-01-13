from datetime import datetime
from typing import Optional
from decimal import Decimal
from pydantic import BaseModel, ConfigDict


class HotelRoomCount(BaseModel):
    """Hotel room count enrichment model matching the database schema."""

    id: int
    hotel_id: int
    room_count: int
    source: Optional[str] = None
    confidence: Optional[Decimal] = None
    enriched_at: datetime

    model_config = ConfigDict(from_attributes=True)
