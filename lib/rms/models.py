"""RMS Pydantic models."""

from typing import Optional, List

from pydantic import BaseModel


class ScannedURL(BaseModel):
    """Result of a successful URL scan."""
    id_num: int
    url: str
    slug: str
    subdomain: str
    name: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "id": self.id_num,
            "url": self.url,
            "slug": self.slug,
            "subdomain": self.subdomain,
            "name": self.name,
            "booking_url": self.url,
            "address": self.address,
            "phone": self.phone,
            "email": self.email,
        }


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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    def has_data(self) -> bool:
        return bool(self.name and self.name.lower() not in ['online bookings', 'search', 'error', 'loading', ''])
    
    def has_location(self) -> bool:
        return self.latitude is not None and self.longitude is not None


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
