"""Repository for leadgen service database operations."""

from typing import Optional, List
from db.client import queries, get_conn
from db.models.hotel import Hotel
from db.models.booking_engine import BookingEngine

BATCH_SIZE = 50


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


async def update_hotel_status(
    hotel_id: int,
    status: int,
    phone_website: Optional[str] = None,
    email: Optional[str] = None,
) -> None:
    """Update hotel status (used for rejection statuses).

    Status values:
    - -2 = location_mismatch (rejected)
    - -1 = no_booking_engine (rejected)
    """
    async with get_conn() as conn:
        await queries.update_hotel_status(
            conn,
            hotel_id=hotel_id,
            status=status,
            phone_website=phone_website,
            email=email,
        )


async def update_hotel_contact_info(
    hotel_id: int,
    phone_website: Optional[str] = None,
    email: Optional[str] = None,
) -> None:
    """Update hotel contact info without changing status."""
    async with get_conn() as conn:
        await queries.update_hotel_contact_info(
            conn,
            hotel_id=hotel_id,
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
    booking_engine_id: Optional[int] = None,
    booking_url: Optional[str] = None,
    detection_method: Optional[str] = None,
    status: int = 1,
) -> None:
    """Link hotel to detected booking engine.

    status: -1=failed (non-retriable), 1=success (default)
    """
    async with get_conn() as conn:
        await queries.insert_hotel_booking_engine(
            conn,
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            booking_url=booking_url,
            detection_method=detection_method,
            status=status,
        )


async def get_hotels_by_ids(hotel_ids: List[int]) -> List[Hotel]:
    """Get hotels by list of IDs.

    Used by worker to fetch batch of hotels from SQS message.
    """
    if not hotel_ids:
        return []
    async with get_conn() as conn:
        results = await queries.get_hotels_by_ids(conn, hotel_ids=hotel_ids)
        return [Hotel.model_validate(dict(row)) for row in results]


async def update_hotels_status_batch(hotel_ids: List[int], status: int) -> None:
    """Update status for multiple hotels at once.

    Note: With simplified status system, this is only used for batch rejections.
    Detection/enrichment progress is tracked by presence of records.
    """
    if not hotel_ids:
        return
    async with get_conn() as conn:
        await queries.update_hotels_status_batch(conn, hotel_ids=hotel_ids, status=status)
async def insert_hotels_bulk(hotels: List[dict]) -> int:
    """
    Insert multiple hotels in batches of 50 and return count of inserted/updated rows.

    Each hotel dict should have keys matching insert_hotel parameters:
    name, website, phone_google, latitude, longitude, address, city, state,
    country, rating, review_count, status, source

    Uses individual inserts with ON CONFLICT (upsert) to handle duplicates.
    """
    if not hotels:
        return 0

    count = 0

    # Process in batches of 50
    for i in range(0, len(hotels), BATCH_SIZE):
        batch = hotels[i:i + BATCH_SIZE]

        async with get_conn() as conn:
            for hotel in batch:
                try:
                    await queries.insert_hotel(
                        conn,
                        name=hotel.get("name"),
                        website=hotel.get("website"),
                        phone_google=hotel.get("phone_google"),
                        phone_website=hotel.get("phone_website"),
                        email=hotel.get("email"),
                        latitude=hotel.get("latitude"),
                        longitude=hotel.get("longitude"),
                        address=hotel.get("address"),
                        city=hotel.get("city"),
                        state=hotel.get("state"),
                        country=hotel.get("country", "USA"),
                        rating=hotel.get("rating"),
                        review_count=hotel.get("review_count"),
                        status=hotel.get("status", 0),
                        source=hotel.get("source"),
                    )
                    count += 1
                except Exception:
                    # Skip individual failures (e.g., constraint violations)
                    continue

    return count


async def insert_detection_error(
    hotel_id: int,
    error_type: str,
    error_message: Optional[str] = None,
    detected_location: Optional[str] = None,
) -> None:
    """Log a detection error for debugging."""
    async with get_conn() as conn:
        await queries.insert_detection_error(
            conn,
            hotel_id=hotel_id,
            error_type=error_type,
            error_message=error_message,
            detected_location=detected_location,
        )


# =============================================================================
# SCRAPE TARGET CITIES
# =============================================================================

async def get_target_cities_by_state(state: str, limit: int = 100) -> List[dict]:
    """Get all target cities for a state."""
    async with get_conn() as conn:
        results = await queries.get_target_cities_by_state(conn, state=state, limit=limit)
        return [dict(row) for row in results] if results else []


async def get_target_city(name: str, state: str) -> Optional[dict]:
    """Get a specific target city by name and state."""
    async with get_conn() as conn:
        result = await queries.get_target_city(conn, name=name, state=state)
        return dict(result) if result else None


async def insert_target_city(
    name: str,
    state: str,
    lat: float,
    lng: float,
    radius_km: float = 12.0,
    population: Optional[int] = None,
    display_name: Optional[str] = None,
    source: str = "nominatim",
) -> int:
    """Insert or update a target city. Returns the city ID."""
    async with get_conn() as conn:
        result = await queries.insert_target_city(
            conn,
            name=name,
            state=state,
            lat=lat,
            lng=lng,
            radius_km=radius_km,
            population=population,
            display_name=display_name,
            source=source,
        )
        return result


async def delete_target_city(name: str, state: str) -> None:
    """Delete a target city."""
    async with get_conn() as conn:
        await queries.delete_target_city(conn, name=name, state=state)


async def count_target_cities_by_state(state: str) -> int:
    """Count target cities for a state."""
    async with get_conn() as conn:
        result = await queries.count_target_cities_by_state(conn, state=state)
        return result or 0
