"""RMS Pydantic models."""

from typing import Optional, List

from pydantic import BaseModel


class ScannedURL(BaseModel):
    """Result of a successful URL scan."""
    id_num: int
    url: str
    slug: str
    subdomain: str


class ExtractedRMSData(BaseModel):
    """Data extracted from RMS booking page."""
    slug: str
    booking_url: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    def has_data(self) -> bool:
        return bool(self.name and self.name.lower() not in ['online bookings', 'search', 'error', 'loading', ''])


class RMSHotelRecord(BaseModel):
    """RMS hotel record from database."""
    hotel_id: int
    booking_url: str


class QueueStats(BaseModel):
    """Queue statistics."""
    pending: int
    in_flight: int


class QueueMessage(BaseModel):
    """Message from queue."""
    receipt_handle: str
    hotels: List[RMSHotelRecord]
