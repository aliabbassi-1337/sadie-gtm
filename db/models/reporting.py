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
    booking_url: Optional[str] = None
    engine_property_id: Optional[str] = None

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
