"""Repository for leadgen service database operations."""

from typing import Optional, List
from db.client import queries, get_conn
from db.models.hotel import Hotel
from db.models.booking_engine import BookingEngine


async def get_hotel_by_id(hotel_id: int) -> Optional[Hotel]:
    """Get hotel by ID with location coordinates."""
    async with get_conn() as conn:
        result = await queries.get_hotel_by_id(conn, hotel_id=hotel_id)
        if result:
            return Hotel.model_validate(dict(result))
        return None


async def insert_hotel(
    name: str,
    website: Optional[str] = None,
    phone_google: Optional[str] = None,
    phone_website: Optional[str] = None,
    email: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: str = "USA",
    rating: Optional[float] = None,
    review_count: Optional[int] = None,
    status: int = 0,
    source: Optional[str] = None,
) -> int:
    """Insert a new hotel and return the ID."""
    async with get_conn() as conn:
        result = await queries.insert_hotel(
            conn,
            name=name,
            website=website,
            phone_google=phone_google,
            phone_website=phone_website,
            email=email,
            latitude=latitude,
            longitude=longitude,
            address=address,
            city=city,
            state=state,
            country=country,
            rating=rating,
            review_count=review_count,
            status=status,
            source=source,
        )
        return result


async def delete_hotel(hotel_id: int) -> None:
    """Delete a hotel by ID."""
    async with get_conn() as conn:
        await queries.delete_hotel(conn, hotel_id=hotel_id)


async def get_hotels_pending_detection(limit: int = 100) -> List[Hotel]:
    """Get hotels that need booking engine detection.

    Criteria:
    - status = 0 (scraped)
    - website is not null
    - not a big chain (Marriott, Hilton, IHG, Hyatt, Wyndham, etc.)
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_detection(conn, limit=limit)
        return [Hotel.model_validate(dict(row)) for row in results]


async def claim_hotels_for_detection(limit: int = 100) -> List[Hotel]:
    """Atomically claim hotels for processing (multi-worker safe).

    Uses FOR UPDATE SKIP LOCKED so multiple workers can run concurrently
    without grabbing the same hotels. Sets status=10 (processing).

    Returns list of claimed hotels.
    """
    async with get_conn() as conn:
        results = await queries.claim_hotels_for_detection(conn, limit=limit)
        return [Hotel.model_validate(dict(row)) for row in results]


async def reset_stale_processing_hotels() -> None:
    """Reset hotels stuck in processing state (status=10) for > 30 min.

    Run this periodically to recover from crashed workers.
    """
    async with get_conn() as conn:
        await queries.reset_stale_processing_hotels(conn)


async def update_hotel_status(
    hotel_id: int,
    status: int,
    phone_website: Optional[str] = None,
    email: Optional[str] = None,
) -> None:
    """Update hotel status after detection.

    Status values:
    - 1 = detected (booking engine found)
    - 99 = no_booking_engine (dead end)
    """
    async with get_conn() as conn:
        await queries.update_hotel_status(
            conn,
            hotel_id=hotel_id,
            status=status,
            phone_website=phone_website,
            email=email,
        )


async def get_booking_engine_by_name(name: str) -> Optional[BookingEngine]:
    """Get booking engine by name."""
    async with get_conn() as conn:
        result = await queries.get_booking_engine_by_name(conn, name=name)
        if result:
            return BookingEngine.model_validate(dict(result))
        return None


async def get_all_booking_engines() -> List[BookingEngine]:
    """Get all active booking engines with domain patterns.

    Returns list of BookingEngine models for pattern matching.
    """
    async with get_conn() as conn:
        results = await queries.get_all_booking_engines(conn)
        return [BookingEngine.model_validate(dict(row)) for row in results]


async def insert_booking_engine(
    name: str,
    domains: Optional[List[str]] = None,
    tier: int = 2,
) -> int:
    """Insert a new booking engine and return the ID.

    tier=2 means unknown/discovered engine.
    """
    async with get_conn() as conn:
        result = await queries.insert_booking_engine(
            conn,
            name=name,
            domains=domains,
            tier=tier,
        )
        return result


async def insert_hotel_booking_engine(
    hotel_id: int,
    booking_engine_id: int,
    booking_url: Optional[str] = None,
    detection_method: Optional[str] = None,
) -> None:
    """Link hotel to detected booking engine."""
    async with get_conn() as conn:
        await queries.insert_hotel_booking_engine(
            conn,
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            booking_url=booking_url,
            detection_method=detection_method,
        )
