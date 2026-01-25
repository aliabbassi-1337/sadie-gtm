"""Repository for enrichment service database operations."""

from typing import Optional, List, Dict, Any
from decimal import Decimal
from db.client import queries, get_conn
from db.models.hotel import Hotel
from db.models.hotel_room_count import HotelRoomCount
from db.models.existing_customer import ExistingCustomer
from db.models.hotel_customer_proximity import HotelCustomerProximity


async def get_hotels_pending_enrichment(limit: int = 100) -> List[Hotel]:
    """Get hotels that need room count enrichment (read-only, for status display).

    Criteria:
    - has website
    - not already in hotel_room_count table
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_enrichment(conn, limit=limit)
        return [Hotel.model_validate(dict(row)) for row in results]


async def claim_hotels_for_enrichment(limit: int = 100) -> List[Hotel]:
    """Atomically claim hotels for enrichment (multi-worker safe).

    Inserts status=-1 (processing) records into hotel_room_count.
    Uses ON CONFLICT DO NOTHING so only one worker claims each hotel.

    Args:
        limit: Max hotels to claim

    Returns list of successfully claimed hotels.
    """
    async with get_conn() as conn:
        results = await queries.claim_hotels_for_enrichment(conn, limit=limit)
        return [Hotel.model_validate(dict(row)) for row in results]


async def reset_stale_enrichment_claims() -> None:
    """Reset claims stuck in processing state (status=-1) for > 30 min.

    Run this periodically to recover from crashed workers.
    """
    async with get_conn() as conn:
        await queries.reset_stale_enrichment_claims(conn)


async def get_pending_enrichment_count() -> int:
    """Count hotels waiting for enrichment (has website, not yet in hotel_room_count)."""
    async with get_conn() as conn:
        result = await queries.get_pending_enrichment_count(conn)
        return result["count"] if result else 0


async def insert_room_count(
    hotel_id: int,
    room_count: Optional[int],
    source: Optional[str] = None,
    confidence: Optional[Decimal] = None,
    status: int = 0,
) -> int:
    """Insert room count for a hotel.

    Args:
        hotel_id: The hotel ID
        room_count: The room count (can be None for failed enrichment)
        source: Source of the data (regex, groq, etc)
        confidence: Confidence score
        status: 0=failed, 1=success

    Returns the hotel_room_count ID.
    """
    async with get_conn() as conn:
        result = await queries.insert_room_count(
            conn,
            hotel_id=hotel_id,
            room_count=room_count,
            source=source,
            confidence=confidence,
            status=status,
        )
        return result


async def get_room_count_by_hotel_id(hotel_id: int) -> Optional[HotelRoomCount]:
    """Get room count for a specific hotel."""
    async with get_conn() as conn:
        result = await queries.get_room_count_by_hotel_id(conn, hotel_id=hotel_id)
        if result:
            return HotelRoomCount.model_validate(dict(result))
        return None


async def delete_room_count(hotel_id: int) -> None:
    """Delete room count for a hotel (for testing)."""
    async with get_conn() as conn:
        await queries.delete_room_count(conn, hotel_id=hotel_id)


# ============================================================================
# CUSTOMER PROXIMITY FUNCTIONS
# ============================================================================


async def get_hotels_pending_proximity(limit: int = 100) -> List[Hotel]:
    """Get hotels that need customer proximity calculation.

    Criteria:
    - has location
    - not already in hotel_customer_proximity table
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_proximity(conn, limit=limit)
        return [Hotel.model_validate(dict(row)) for row in results]


async def get_all_existing_customers() -> List[ExistingCustomer]:
    """Get all existing customers with location for proximity calculation."""
    async with get_conn() as conn:
        results = await queries.get_all_existing_customers(conn)
        return [ExistingCustomer.model_validate(dict(row)) for row in results]


async def find_nearest_customer(
    hotel_id: int,
    max_distance_km: float = 100.0,
) -> Optional[Dict[str, Any]]:
    """Find the nearest existing customer to a hotel within max_distance_km.

    Uses PostGIS ST_DWithin for efficient spatial query.

    Returns dict with existing_customer_id, customer_name, distance_km
    or None if no customer found within range.
    """
    # Convert km to meters for ST_DWithin
    max_distance_meters = max_distance_km * 1000

    async with get_conn() as conn:
        result = await queries.find_nearest_customer(
            conn,
            hotel_id=hotel_id,
            max_distance_meters=max_distance_meters,
        )
        if result:
            return dict(result)
        return None


async def insert_customer_proximity(
    hotel_id: int,
    existing_customer_id: int,
    distance_km: Decimal,
) -> int:
    """Insert customer proximity for a hotel.

    Returns the hotel_customer_proximity ID.
    """
    async with get_conn() as conn:
        result = await queries.insert_customer_proximity(
            conn,
            hotel_id=hotel_id,
            existing_customer_id=existing_customer_id,
            distance_km=distance_km,
        )
        return result


async def insert_customer_proximity_none(hotel_id: int) -> int:
    """Insert record marking hotel as processed with no nearby customer (NULL values).

    Returns the hotel_customer_proximity ID.
    """
    async with get_conn() as conn:
        result = await queries.insert_customer_proximity_none(
            conn,
            hotel_id=hotel_id,
        )
        return result


async def get_customer_proximity_by_hotel_id(hotel_id: int) -> Optional[Dict[str, Any]]:
    """Get customer proximity for a specific hotel.

    Returns dict with proximity info and customer name, or None if not found.
    """
    async with get_conn() as conn:
        result = await queries.get_customer_proximity_by_hotel_id(
            conn, hotel_id=hotel_id
        )
        if result:
            return dict(result)
        return None


async def delete_customer_proximity(hotel_id: int) -> None:
    """Delete customer proximity for a hotel (for testing)."""
    async with get_conn() as conn:
        await queries.delete_customer_proximity(conn, hotel_id=hotel_id)


async def get_pending_proximity_count() -> int:
    """Count hotels waiting for proximity calculation."""
    async with get_conn() as conn:
        result = await queries.get_pending_proximity_count(conn)
        return result["count"] if result else 0


# ============================================================================
# WEBSITE ENRICHMENT FUNCTIONS
# ============================================================================


async def get_hotels_pending_website_enrichment(limit: int = 100) -> List[Dict[str, Any]]:
    """Get hotels that need website enrichment (read-only, for status display).

    Criteria:
    - no website
    - has name and city
    - not already in hotel_website_enrichment table
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_website_enrichment(conn, limit=limit)
        return [dict(row) for row in results]


async def claim_hotels_for_website_enrichment(
    limit: int = 100,
    source_filter: Optional[str] = None,
    state_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Atomically claim hotels for website enrichment (multi-worker safe).

    Inserts status=-1 (processing) records into hotel_website_enrichment.
    Uses ON CONFLICT DO NOTHING so only one worker claims each hotel.

    Args:
        limit: Max hotels to claim
        source_filter: Filter by source (e.g., 'dbpr%')
        state_filter: Filter by state (e.g., 'FL')

    Returns list of successfully claimed hotels.
    """
    async with get_conn() as conn:
        if source_filter or state_filter:
            results = await queries.claim_hotels_for_website_enrichment_filtered(
                conn,
                limit=limit,
                source_filter=f"%{source_filter}%" if source_filter else None,
                state_filter=state_filter,
            )
        else:
            results = await queries.claim_hotels_for_website_enrichment(conn, limit=limit)
        return [dict(row) for row in results]


async def reset_stale_website_enrichment_claims() -> None:
    """Reset claims stuck in processing state (status=-1) for > 30 min.

    Run this periodically to recover from crashed workers.
    """
    async with get_conn() as conn:
        await queries.reset_stale_website_enrichment_claims(conn)


async def get_pending_website_enrichment_count() -> int:
    """Count hotels waiting for website enrichment."""
    async with get_conn() as conn:
        result = await queries.get_pending_website_enrichment_count(conn)
        return result["count"] if result else 0


async def update_hotel_website(hotel_id: int, website: str) -> None:
    """Update hotel with enriched website."""
    async with get_conn() as conn:
        await queries.update_hotel_website(conn, hotel_id=hotel_id, website=website)


async def update_hotel_location_point_if_null(hotel_id: int, lat: float, lng: float) -> None:
    """Update hotel location from lat/lng coordinates ONLY if location is currently NULL."""
    async with get_conn() as conn:
        await queries.update_hotel_location_point_if_null(conn, hotel_id=hotel_id, lat=lat, lng=lng)


async def update_website_enrichment_status(
    hotel_id: int,
    status: int,
    source: Optional[str] = None,
) -> None:
    """Update website enrichment status after processing.

    Args:
        hotel_id: The hotel ID
        status: 0=failed, 1=success
        source: Source of enrichment (serper, manual, etc)
    """
    async with get_conn() as conn:
        await queries.update_website_enrichment_status(
            conn,
            hotel_id=hotel_id,
            status=status,
            source=source,
        )


async def get_website_enrichment_stats(source_prefix: Optional[str] = None) -> Dict[str, int]:
    """Get stats for website enrichment progress.

    Returns dict with total, with_website, without_website, enriched_success, enriched_failed counts.
    """
    async with get_conn() as conn:
        result = await queries.get_website_enrichment_stats(
            conn,
            source_prefix=f"{source_prefix}%" if source_prefix else None,
        )
        return dict(result) if result else {
            "total": 0,
            "with_website": 0,
            "without_website": 0,
            "enriched_success": 0,
            "enriched_failed": 0,
            "in_progress": 0,
        }


# ============================================================================
# LOCATION-ONLY ENRICHMENT FUNCTIONS (for hotels with website but no location)
# ============================================================================


async def get_hotels_pending_location_from_places(
    limit: int = 100,
    source_filter: Optional[str] = None,
    state_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get hotels that have website but no location (need Serper Places lookup).

    Criteria:
    - has website
    - has name and city
    - no location

    Args:
        limit: Max hotels to return
        source_filter: Filter by source (e.g., 'texas_hot')
        state_filter: Filter by state (e.g., 'TX')

    Returns list of hotels needing location enrichment.
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_location_from_places(
            conn,
            limit=limit,
            source_filter=f"%{source_filter}%" if source_filter else None,
            state_filter=state_filter,
        )
        return [dict(row) for row in results]


async def get_pending_location_from_places_count(
    source_filter: Optional[str] = None,
    state_filter: Optional[str] = None,
) -> int:
    """Count hotels that have website but no location."""
    async with get_conn() as conn:
        result = await queries.get_pending_location_from_places_count(
            conn,
            source_filter=f"%{source_filter}%" if source_filter else None,
            state_filter=state_filter,
        )
        return result["count"] if result else 0


# ============================================================================
# COORDINATE ENRICHMENT FUNCTIONS (for parcel data with coords but no name/website)
# ============================================================================


async def get_hotels_pending_coordinate_enrichment(
    limit: int = 100,
    sources: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Get hotels with coordinates but no website (parcel data needing Places API lookup).

    Criteria:
    - has location (coordinates)
    - no website
    - optionally filter by source names

    Args:
        limit: Max hotels to return
        sources: Optional list of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_coordinate_enrichment(
            conn,
            limit=limit,
            sources=sources,
        )
        return [dict(row) for row in results]


async def get_pending_coordinate_enrichment_count(
    sources: Optional[List[str]] = None,
) -> int:
    """Count hotels needing coordinate-based enrichment.

    Args:
        sources: Optional list of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
    """
    async with get_conn() as conn:
        result = await queries.get_pending_coordinate_enrichment_count(
            conn,
            sources=sources,
        )
        return result["count"] if result else 0


async def update_hotel_from_places(
    hotel_id: int,
    name: Optional[str] = None,
    website: Optional[str] = None,
    phone: Optional[str] = None,
    rating: Optional[float] = None,
    address: Optional[str] = None,
) -> None:
    """Update hotel with data from Places API."""
    async with get_conn() as conn:
        await queries.update_hotel_from_places(
            conn,
            hotel_id=hotel_id,
            name=name,
            website=website,
            phone=phone,
            rating=rating,
            address=address,
        )
