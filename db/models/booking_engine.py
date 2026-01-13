from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, ConfigDict


class BookingEngine(BaseModel):
    """Booking engine reference model matching the database schema."""

    id: int
    name: str
    domains: Optional[List[str]] = None
    tier: int = 1
    is_active: bool = True

    model_config = ConfigDict(from_attributes=True)
