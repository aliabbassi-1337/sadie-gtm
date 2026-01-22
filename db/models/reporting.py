"""Pydantic models for reporting service."""

from typing import Optional, List
from decimal import Decimal
from pydantic import BaseModel, ConfigDict


class HotelLead(BaseModel):
    """Hotel lead model for Excel export."""

    id: int
    hotel_name: str
    category: Optional[str] = None
    website: Optional[str] = None

    # Contact
    phone_google: Optional[str] = None
    phone_website: Optional[str] = None
    email: Optional[str] = None

    # Location
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: str = "USA"

    # Ratings
    rating: Optional[float] = None
    review_count: Optional[int] = None

    # Booking engine
    booking_engine_name: Optional[str] = None
    booking_engine_tier: Optional[int] = None

    # Enrichment data
    room_count: Optional[int] = None
    nearest_customer_name: Optional[str] = None
    nearest_customer_distance_km: Optional[Decimal] = None

    model_config = ConfigDict(from_attributes=True)


class CityStats(BaseModel):
    """City-level analytics stats."""

    total_scraped: int = 0
    with_website: int = 0
    booking_found: int = 0
    with_phone: int = 0
    with_email: int = 0
    tier_1_count: int = 0
    tier_2_count: int = 0

    model_config = ConfigDict(from_attributes=True)


class EngineCount(BaseModel):
    """Booking engine usage count."""

    engine_name: str
    hotel_count: int

    model_config = ConfigDict(from_attributes=True)


class DetectionFunnel(BaseModel):
    """Comprehensive detection funnel metrics."""

    total_hotels: int = 0
    with_website: int = 0
    launched: int = 0
    detection_attempted: int = 0
    engine_found: int = 0
    ota_found: int = 0  # Hotels using OTAs (Booking.com, Expedia, etc.)
    no_engine_found: int = 0
    pending_detection: int = 0
    # Failure breakdown
    http_403: int = 0  # Bot protection
    http_429: int = 0  # Rate limited
    junk_url: int = 0  # Junk booking URL
    junk_domain: int = 0  # Junk domain
    non_hotel_name: int = 0  # Non-hotel business
    timeout_err: int = 0  # Timeout
    server_5xx: int = 0  # Server errors
    browser_err: int = 0  # Browser exceptions

    model_config = ConfigDict(from_attributes=True)

    @property
    def detection_rate(self) -> float:
        """Detection rate as percentage of attempted."""
        if self.detection_attempted == 0:
            return 0.0
        return 100 * self.engine_found / self.detection_attempted

    @property
    def website_rate(self) -> float:
        """Percentage of hotels with websites."""
        if self.total_hotels == 0:
            return 0.0
        return 100 * self.with_website / self.total_hotels

    @property
    def launch_rate(self) -> float:
        """Percentage of hotels launched (of those with engines)."""
        if self.engine_found == 0:
            return 0.0
        return 100 * self.launched / self.engine_found


class ReportStats(BaseModel):
    """Complete stats for a city/state report."""

    location_name: str  # City name or State name
    stats: CityStats
    top_engines: List[EngineCount]
    funnel: Optional[DetectionFunnel] = None


class LaunchableHotel(BaseModel):
    """Hotel ready to be launched (fully enriched)."""

    id: int
    hotel_name: str
    website: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    booking_engine_name: Optional[str] = None
    booking_engine_tier: Optional[int] = None
    room_count: Optional[int] = None
    nearest_customer_name: Optional[str] = None
    nearest_customer_distance_km: Optional[Decimal] = None

    model_config = ConfigDict(from_attributes=True)
