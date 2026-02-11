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
    country: Optional[str] = None

    # Ratings
    rating: Optional[float] = None
    review_count: Optional[int] = None

    # Booking engine
    booking_engine_name: Optional[str] = None
    booking_engine_tier: Optional[int] = None
    booking_url: Optional[str] = None
    engine_property_id: Optional[str] = None

    # Enrichment data
    room_count: Optional[int] = None
    room_count_source: Optional[str] = None
    room_count_confidence: Optional[Decimal] = None
    nearest_customer_name: Optional[str] = None
    nearest_customer_distance_km: Optional[Decimal] = None

    # Active status
    is_active: Optional[bool] = None

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


class ReportStats(BaseModel):
    """Complete stats for a city/state report."""

    location_name: str  # City name or State name
    stats: CityStats
    top_engines: List[EngineCount]
    funnel: Optional[dict] = None


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


class EnrichmentStats(BaseModel):
    """Enrichment statistics per booking engine."""

    engine_name: str
    total_hotels: int = 0

    # Status breakdown
    live: int = 0
    pending: int = 0
    error: int = 0

    # Data completeness
    has_name: int = 0
    has_email: int = 0
    has_phone: int = 0
    has_contact: int = 0
    has_city: int = 0
    has_state: int = 0
    has_country: int = 0
    has_website: int = 0
    has_address: int = 0
    has_coordinates: int = 0

    # Enrichment status
    has_booking_engine: int = 0
    has_room_count: int = 0

    model_config = ConfigDict(from_attributes=True)
