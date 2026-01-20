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


async def get_hotels_without_websites(
    limit: int = 100,
    source_filter: Optional[str] = None,
    state_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get hotels that need website enrichment.

    Args:
        limit: Max hotels to return
        source_filter: Filter by source (e.g., 'dbpr')
        state_filter: Filter by state (e.g., 'FL')

    Returns list of hotel dicts with id, name, city, state, address.
    """
    async with get_conn() as conn:
        query = """
            SELECT id, name, city, state, address
            FROM sadie_gtm.hotels
            WHERE website IS NULL
            AND city IS NOT NULL
            AND name IS NOT NULL
        """
        params = []

        if source_filter:
            query += f" AND source LIKE ${len(params) + 1}"
            params.append(f"%{source_filter}%")

        if state_filter:
            query += f" AND state = ${len(params) + 1}"
            params.append(state_filter)

        query += f" ORDER BY created_at DESC LIMIT ${len(params) + 1}"
        params.append(limit)

        rows = await conn.fetch(query, *params)
        return [dict(r) for r in rows]


async def update_hotel_website(hotel_id: int, website: str) -> bool:
    """Update hotel with enriched website.

    Returns True if updated.
    """
    async with get_conn() as conn:
        result = await conn.execute(
            "UPDATE sadie_gtm.hotels SET website = $1 WHERE id = $2",
            website, hotel_id
        )
        return result == "UPDATE 1"


async def get_website_enrichment_stats(source_prefix: str = "dbpr") -> Dict[str, int]:
    """Get stats for hotels needing website enrichment.

    Returns dict with total, with_website, without_website counts.
    """
    async with get_conn() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total,
                COUNT(website) as with_website,
                COUNT(*) - COUNT(website) as without_website
            FROM sadie_gtm.hotels
            WHERE source LIKE $1
            """,
            f"{source_prefix}%"
        )
        return dict(row) if row else {"total": 0, "with_website": 0, "without_website": 0}
