"""Repository for enrichment service database operations."""

from typing import Optional, List, Dict, Any
from decimal import Decimal
from pydantic import BaseModel
from db.client import queries, get_conn, get_transaction
from db.models.hotel import Hotel
from db.models.hotel_room_count import HotelRoomCount
from db.models.existing_customer import ExistingCustomer
from db.models.hotel_customer_proximity import HotelCustomerProximity
from db.queries import enrichment_batch as batch_sql


async def get_hotels_pending_enrichment(
    limit: int = 100,
    state: Optional[str] = None,
    country: Optional[str] = None,
) -> List[Hotel]:
    """Get hotels that need room count enrichment (read-only, for status display).

    Criteria:
    - not already in hotel_room_count table
    - optionally filtered by state and/or country
    """
    async with get_conn() as conn:
        results = await queries.get_hotels_pending_enrichment(
            conn, limit=limit, state=state, country=country
        )
        return [Hotel.model_validate(dict(row)) for row in results]


async def claim_hotels_for_enrichment(
    limit: int = 100,
    state: Optional[str] = None,
    country: Optional[str] = None,
) -> List[Hotel]:
    """Atomically claim hotels for enrichment (multi-worker safe).

    Inserts status=-1 (processing) records into hotel_room_count.
    Uses ON CONFLICT DO NOTHING so only one worker claims each hotel.

    Args:
        limit: Max hotels to claim
        state: Optional state filter (e.g., "California")
        country: Optional country filter (e.g., "United States")

    Returns list of successfully claimed hotels.
    """
    async with get_conn() as conn:
        results = await queries.claim_hotels_for_enrichment(
            conn, limit=limit, state=state, country=country
        )
        return [Hotel.model_validate(dict(row)) for row in results]


async def reset_stale_enrichment_claims() -> None:
    """Reset claims stuck in processing state (status=-1) for > 30 min.

    Run this periodically to recover from crashed workers.
    """
    async with get_conn() as conn:
        await queries.reset_stale_enrichment_claims(conn)


async def get_pending_enrichment_count(
    state: Optional[str] = None,
    country: Optional[str] = None,
) -> int:
    """Count hotels waiting for enrichment (not yet in hotel_room_count).

    Args:
        state: Optional state filter (e.g., "California")
        country: Optional country filter (e.g., "United States")
    """
    async with get_conn() as conn:
        result = await queries.get_pending_enrichment_count(
            conn, state=state, country=country
        )
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
    """Update hotel with enriched website and advance pipeline stage."""
    async with get_conn() as conn:
        await queries.update_hotel_website(conn, hotel_id=hotel_id, website=website)
        await queries.advance_to_has_website(conn, hotel_id=hotel_id)


async def update_hotel_website_only(hotel_id: int, website: str) -> None:
    """Update hotel website without advancing pipeline stage.

    Used by room count enricher when it discovers a website via LLM.
    These hotels are already launched, so we don't need to advance_to_has_website.
    """
    async with get_conn() as conn:
        await queries.update_hotel_website(conn, hotel_id=hotel_id, website=website)


async def update_hotel_contact_info(
    hotel_id: int,
    phone: Optional[str] = None,
    email: Optional[str] = None,
) -> None:
    """Update hotel contact info (phone_website, email) without changing status.

    Uses COALESCE so only fills in missing values — won't overwrite existing data.
    """
    async with get_conn() as conn:
        await queries.update_hotel_contact_info(
            conn, hotel_id=hotel_id, phone_website=phone, email=email
        )


async def update_hotel_location_point_if_null(hotel_id: int, lat: float, lng: float) -> None:
    """Update hotel location from lat/lng coordinates ONLY if location is currently NULL."""
    async with get_conn() as conn:
        await queries.update_hotel_location_point_if_null(conn, hotel_id=hotel_id, lat=lat, lng=lng)
        await queries.advance_to_has_location(conn, hotel_id=hotel_id)


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


# ============================================================================
# BOOKING PAGE ENRICHMENT FUNCTIONS (name + address from booking URLs)
# ============================================================================


async def get_hotels_needing_booking_page_enrichment(limit: int = 1000) -> List["HotelEnrichmentCandidate"]:
    """Get hotels with booking URLs needing name or address enrichment.
    
    Criteria:
    - has booking URL
    - missing name (null/empty/Unknown) OR missing city/state
    """
    # Import here to avoid circular import
    from services.enrichment.service import HotelEnrichmentCandidate
    
    async with get_conn() as conn:
        results = await queries.get_hotels_needing_enrichment(
            conn, enrich_type="both", limit=limit
        )
        return [HotelEnrichmentCandidate.model_validate(dict(row)) for row in results]


async def get_hotel_by_id(hotel_id: int) -> Optional[Hotel]:
    """Get hotel by ID for checking current enrichment state."""
    async with get_conn() as conn:
        result = await queries.get_hotel_by_id(conn, hotel_id=hotel_id)
        return Hotel.model_validate(dict(result)) if result else None


async def update_hotel_name(hotel_id: int, name: str) -> None:
    """Update hotel name."""
    async with get_conn() as conn:
        await queries.update_hotel_name(conn, hotel_id=hotel_id, name=name)


async def update_hotel_name_and_location(
    hotel_id: int,
    name: Optional[str] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
) -> None:
    """Update hotel name and/or location. Uses COALESCE to preserve existing values."""
    async with get_conn() as conn:
        await queries.update_hotel_name_and_location(
            conn,
            hotel_id=hotel_id,
            name=name,
            address=address,
            city=city,
            state=state,
            country=country,
            phone=phone,
            email=email,
        )


# ============================================================================
# GEOCODING FUNCTIONS (Serper Places enrichment)
# ============================================================================


class HotelGeocodingCandidate(BaseModel):
    """Hotel needing geocoding via Serper Places."""
    id: int
    name: str
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    source: Optional[str] = None
    booking_url: Optional[str] = None
    engine_name: Optional[str] = None


async def get_hotels_needing_geocoding(
    limit: int = 1000,
    source: Optional[str] = None,
    engine: Optional[str] = None,
    country: Optional[str] = None,
) -> List[HotelGeocodingCandidate]:
    """Get hotels with names but missing location data for Serper Places geocoding.
    
    Args:
        limit: Max hotels to return
        source: Optional source filter (e.g., 'cloudbeds_crawl')
        engine: Optional booking engine filter (e.g., 'Cloudbeds', 'RMS Cloud')
                When specified, filters to that engine only.
                When None, excludes Cloudbeds by default.
        country: Optional country filter (e.g., 'United States')
    """
    async with get_conn() as conn:
        source_pattern = f"%{source}%" if source else None
        results = await queries.get_hotels_needing_geocoding(
            conn, limit=limit, source=source_pattern, engine=engine, country=country
        )
        return [HotelGeocodingCandidate.model_validate(dict(row)) for row in results]


async def get_hotels_needing_geocoding_count(
    source: Optional[str] = None,
    engine: Optional[str] = None,
    country: Optional[str] = None,
) -> int:
    """Count hotels needing geocoding."""
    async with get_conn() as conn:
        source_pattern = f"%{source}%" if source else None
        result = await queries.get_hotels_needing_geocoding_count(
            conn, source=source_pattern, engine=engine, country=country
        )
        return result["count"] if result else 0


async def update_hotel_geocoding(
    hotel_id: int,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
) -> None:
    """Update hotel with geocoding results from Serper Places."""
    async with get_conn() as conn:
        await queries.update_hotel_geocoding(
            conn,
            hotel_id=hotel_id,
            address=address,
            city=city,
            state=state,
            country=country,
            latitude=latitude,
            longitude=longitude,
            phone=phone,
            email=email,
        )


async def batch_update_hotel_geocoding(
    updates: List[Dict],
) -> int:
    """Batch update hotels with geocoding results using bulk UPDATE.
    
    Uses PostgreSQL unnest for efficient single-query batch update.
    Returns count of updated hotels.
    """
    if not updates:
        return 0
    
    # Prepare arrays for unnest
    hotel_ids = []
    addresses = []
    cities = []
    states = []
    countries = []
    latitudes = []
    longitudes = []
    phones = []
    emails = []
    
    for u in updates:
        hotel_ids.append(u["hotel_id"])
        addresses.append(u.get("address"))
        cities.append(u.get("city"))
        states.append(u.get("state"))
        countries.append(u.get("country"))
        latitudes.append(u.get("latitude"))
        longitudes.append(u.get("longitude"))
        phones.append(u.get("phone"))
        emails.append(u.get("email"))
    
    async with get_conn() as conn:
        result = await conn.execute(
            batch_sql.BATCH_UPDATE_GEOCODING,
            hotel_ids,
            addresses,
            cities,
            states,
            countries,
            latitudes,
            longitudes,
            phones,
            emails,
        )
        # Parse "UPDATE N" to get count
        count = int(result.split()[-1]) if result else len(updates)
    
    return count


# ============================================================================
# CLOUDBEDS ENRICHMENT
# ============================================================================


class CloudbedsHotelCandidate(BaseModel):
    """Hotel needing Cloudbeds enrichment."""
    id: int
    name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    address: Optional[str] = None
    booking_url: str
    slug: Optional[str] = None


async def get_cloudbeds_hotels_needing_enrichment(
    limit: int = 100,
) -> List[CloudbedsHotelCandidate]:
    """Get hotels with Cloudbeds booking URLs that need name or location enrichment."""
    async with get_conn() as conn:
        rows = await queries.get_cloudbeds_hotels_needing_enrichment(conn, limit=limit)
        return [CloudbedsHotelCandidate(**dict(r)) for r in rows]


async def get_cloudbeds_hotels_needing_enrichment_count() -> int:
    """Count Cloudbeds hotels needing enrichment."""
    async with get_conn() as conn:
        result = await queries.get_cloudbeds_hotels_needing_enrichment_count(conn)
        # Handle both scalar and Record returns
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def get_cloudbeds_hotels_total_count() -> int:
    """Count total Cloudbeds hotels."""
    async with get_conn() as conn:
        result = await queries.get_cloudbeds_hotels_total_count(conn)
        # Handle both scalar and Record returns
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def set_last_enrichment_attempt(hotel_id: int) -> None:
    """Record when enrichment was last attempted (for rate limit cooldown)."""
    async with get_conn() as conn:
        await queries.set_last_enrichment_attempt(conn, hotel_id=hotel_id)


async def set_enrichment_status(hotel_id: int, status: int) -> None:
    """Set enrichment status. 1=success, -1=failed/dead."""
    async with get_conn() as conn:
        await queries.set_enrichment_status(conn, hotel_id=hotel_id, status=status)


async def mark_enrichment_dead(hotel_id: int) -> None:
    """Mark a booking URL as permanently dead (404)."""
    async with get_conn() as conn:
        await queries.mark_enrichment_dead(conn, hotel_id=hotel_id)


async def batch_mark_enrichment_dead(hotel_ids: List[int]) -> int:
    """Batch mark booking URLs as permanently dead (404).
    
    These will not be re-queued for enrichment.
    """
    if not hotel_ids:
        return 0
    
    async with get_conn() as conn:
        result = await conn.execute(batch_sql.BATCH_MARK_ENRICHMENT_DEAD, hotel_ids)
        if result and result.startswith("UPDATE"):
            return int(result.split()[1])
        return 0


async def batch_set_last_enrichment_attempt(hotel_ids: List[int]) -> int:
    """Batch set last enrichment attempt for failed hotels."""
    if not hotel_ids:
        return 0
    
    async with get_conn() as conn:
        result = await conn.execute(batch_sql.BATCH_SET_LAST_ENRICHMENT_ATTEMPT, hotel_ids)
        if result and result.startswith("UPDATE"):
            return int(result.split()[1])
        return 0


async def batch_update_cloudbeds_enrichment(
    updates: List[Dict],
) -> int:
    """Batch update hotels with Cloudbeds enrichment results.
    
    For Cloudbeds, scraped data overrides existing (crawl sources often have wrong location).
    Supports lat/lon from the property_info API.
    """
    if not updates:
        return 0
    
    hotel_ids = []
    names = []
    addresses = []
    cities = []
    states = []
    countries = []
    phones = []
    emails = []
    lats = []
    lons = []
    zip_codes = []
    contact_names = []
    
    for u in updates:
        hotel_ids.append(u["hotel_id"])
        names.append(u.get("name"))
        addresses.append(u.get("address"))
        cities.append(u.get("city"))
        # State normalization is done in Service layer before calling repo
        states.append(u.get("state"))
        countries.append(u.get("country"))
        phones.append(u.get("phone"))
        emails.append(u.get("email"))
        lats.append(u.get("lat"))
        lons.append(u.get("lon"))
        zip_codes.append(u.get("zip_code"))
        contact_names.append(u.get("contact_name"))
    
    # Cloudbeds: API data always overwrites (COALESCE handles NULLs)
    sql = batch_sql.BATCH_UPDATE_CLOUDBEDS_ENRICHMENT
    
    async with get_conn() as conn:
        result = await conn.execute(
            sql,
            hotel_ids,
            names,
            addresses,
            cities,
            states,
            countries,
            phones,
            emails,
            lats,
            lons,
            zip_codes,
            contact_names,
        )
        count = int(result.split()[-1]) if result else len(updates)
    
    return count


# ============================================================================
# MEWS ENRICHMENT
# ============================================================================


class MewsHotelCandidate(BaseModel):
    """Hotel needing Mews enrichment."""
    id: int
    name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    address: Optional[str] = None
    booking_url: str
    slug: Optional[str] = None


async def get_mews_hotels_needing_enrichment(
    limit: int = 100,
) -> List[MewsHotelCandidate]:
    """Get hotels with Mews booking URLs that need name enrichment."""
    async with get_conn() as conn:
        rows = await queries.get_mews_hotels_needing_enrichment(conn, limit=limit)
        return [MewsHotelCandidate(**dict(r)) for r in rows]


async def get_mews_hotels_needing_enrichment_count() -> int:
    """Count Mews hotels needing enrichment."""
    async with get_conn() as conn:
        result = await queries.get_mews_hotels_needing_enrichment_count(conn)
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def get_mews_hotels_total_count() -> int:
    """Count total Mews hotels."""
    async with get_conn() as conn:
        result = await queries.get_mews_hotels_total_count(conn)
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def batch_update_mews_enrichment(
    updates: List[Dict],
    force_overwrite: bool = False,
) -> int:
    """Batch update hotels with Mews enrichment results.
    
    Supports: name, address, city, state, country, email, phone, lat, lon
    Uses phone_website column and PostGIS location geography.
    
    Default behavior: API data always wins when non-empty (overwrites DB values).
    This is correct because these hotels were scraped from the booking engine,
    so the API is the source of truth.
    
    Args:
        updates: List of dicts with hotel_id and enrichment data
        force_overwrite: If True, even overwrite with NULL. Default False = only overwrite with non-empty.
    """
    if not updates:
        return 0
    
    hotel_ids = []
    names = []
    addresses = []
    cities = []
    states = []
    countries = []
    emails = []
    phones = []
    lats = []
    lons = []
    
    for u in updates:
        hotel_ids.append(u["hotel_id"])
        names.append(u.get("name"))
        addresses.append(u.get("address"))
        cities.append(u.get("city"))
        states.append(u.get("state"))
        countries.append(u.get("country"))
        emails.append(u.get("email"))
        phones.append(u.get("phone"))
        lats.append(u.get("lat"))
        lons.append(u.get("lon"))
    
    async with get_conn() as conn:
        result = await conn.execute(
            batch_sql.BATCH_UPDATE_MEWS_ENRICHMENT,
            hotel_ids, names, addresses, cities, states, countries, emails, phones, lats, lons
        )
        count = int(result.split()[-1]) if result else len(updates)
    
    # Also update enrichment status on hotel_booking_engines
    async with get_conn() as conn:
        await conn.execute(
            batch_sql.BATCH_SET_MEWS_ENRICHMENT_STATUS, hotel_ids, 1
        )
    
    return count


# ============================================================================
# LOCATION NORMALIZATION FUNCTIONS
# ============================================================================

async def get_normalization_status() -> dict:
    """Get counts of data needing location normalization."""
    async with get_conn() as conn:
        result = await queries.get_normalization_status(conn)
        return dict(result) if result else {}


async def get_country_counts() -> list:
    """Get counts of each country code that needs normalization."""
    async with get_conn() as conn:
        results = await queries.get_country_counts(conn)
        return [(r['country'], r['cnt']) for r in results]


async def get_states_with_zips() -> list:
    """Get distinct states that have zip codes attached."""
    async with get_conn() as conn:
        results = await queries.get_states_with_zips(conn)
        return [r['state'] for r in results]


async def normalize_country(old_country: str, new_country: str) -> int:
    """Normalize a country code to full name. Returns count of updated records."""
    async with get_conn() as conn:
        result = await queries.normalize_country(conn, old_country=old_country, new_country=new_country)
        return int(result.split()[-1]) if result else 0


async def normalize_us_state(old_state: str, new_state: str) -> int:
    """Normalize a US state code to full name. Returns count of updated records."""
    async with get_conn() as conn:
        result = await queries.normalize_us_state(conn, old_state=old_state, new_state=new_state)
        return int(result.split()[-1]) if result else 0


async def fix_australian_state(old_state: str, new_state: str) -> int:
    """Fix Australian state incorrectly in USA. Returns count of updated records."""
    async with get_conn() as conn:
        result = await queries.fix_australian_state(conn, old_state=old_state, new_state=new_state)
        return int(result.split()[-1]) if result else 0


async def fix_state_with_zip(old_state: str, new_state: str) -> int:
    """Fix state that has zip code attached. Returns count of updated records."""
    async with get_conn() as conn:
        result = await queries.fix_state_with_zip(conn, old_state=old_state, new_state=new_state)
        return int(result.split()[-1]) if result else 0


# ============================================================================
# LOCATION ENRICHMENT FUNCTIONS (reverse geocoding)
# ============================================================================


class LocationEnrichmentStatus(BaseModel):
    """Status of location enrichment."""
    pending_count: int = 0


async def get_hotels_pending_location_enrichment(limit: int = 100) -> List[Dict[str, Any]]:
    """Get hotels with coordinates but missing city for reverse geocoding."""
    async with get_conn() as conn:
        rows = await queries.get_hotels_pending_location_enrichment(conn, limit=limit)
        return [dict(r) for r in rows]


async def get_pending_location_enrichment_count() -> int:
    """Count hotels needing location enrichment (have coords, missing city)."""
    async with get_conn() as conn:
        result = await queries.get_pending_location_enrichment_count(conn)
        return result["count"] if result else 0


async def update_hotel_location_fields(
    hotel_id: int,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
) -> None:
    """Update hotel with reverse geocoded location data."""
    async with get_conn() as conn:
        await queries.update_hotel_location(
            conn,
            hotel_id=hotel_id,
            address=address,
            city=city,
            state=state,
            country=country,
        )


# ============================================================================
# STATE EXTRACTION FUNCTIONS
# ============================================================================


class HotelForStateExtraction(BaseModel):
    """Hotel record for state extraction from address."""
    id: int
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None


async def get_us_hotels_missing_state(limit: int = 1000) -> List[HotelForStateExtraction]:
    """Get US hotels that have address but no state."""
    async with get_conn() as conn:
        rows = await queries.get_us_hotels_missing_state(conn, limit=limit)
        return [HotelForStateExtraction.model_validate(dict(r)) for r in rows]


async def batch_update_extracted_states(updates: List[Dict[str, Any]]) -> int:
    """Batch update hotels with extracted state data.
    
    Args:
        updates: List of dicts with 'id' and 'state' keys
        
    Returns:
        Number of hotels updated
    """
    if not updates:
        return 0
    
    hotel_ids = [u["id"] for u in updates]
    states = [u["state"] for u in updates]
    
    async with get_conn() as conn:
        result = await queries.batch_update_extracted_states(
            conn, hotel_ids=hotel_ids, states=states
        )
        return int(result.split()[-1]) if result else 0


# ============================================================================
# SITEMINDER ENRICHMENT FUNCTIONS
# ============================================================================


class SiteMinderHotelCandidate(BaseModel):
    """Hotel needing SiteMinder enrichment."""
    id: int
    name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    address: Optional[str] = None
    booking_url: str


async def get_siteminder_hotels_needing_enrichment(
    limit: int = 100,
) -> List[SiteMinderHotelCandidate]:
    """Get SiteMinder hotels that need name enrichment."""
    async with get_conn() as conn:
        rows = await queries.get_siteminder_hotels_needing_enrichment(conn, limit=limit)
        return [SiteMinderHotelCandidate(**dict(r)) for r in rows]


async def get_siteminder_hotels_needing_enrichment_count() -> int:
    """Count SiteMinder hotels needing name enrichment."""
    async with get_conn() as conn:
        result = await queries.get_siteminder_hotels_needing_enrichment_count(conn)
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def get_siteminder_hotels_missing_location(
    limit: int = 100,
    country: Optional[str] = None,
) -> List[SiteMinderHotelCandidate]:
    """Get SiteMinder hotels with booking URLs but missing state."""
    async with get_conn() as conn:
        rows = await queries.get_siteminder_hotels_missing_location(
            conn, limit=limit, country=country
        )
        return [SiteMinderHotelCandidate(**dict(r)) for r in rows]


async def get_siteminder_hotels_missing_location_count(
    country: Optional[str] = None,
) -> int:
    """Count SiteMinder hotels needing location enrichment."""
    async with get_conn() as conn:
        result = await queries.get_siteminder_hotels_missing_location_count(
            conn, country=country
        )
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def get_siteminder_hotels_total_count() -> int:
    """Count total SiteMinder hotels."""
    async with get_conn() as conn:
        result = await queries.get_siteminder_hotels_total_count(conn)
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


async def batch_update_siteminder_enrichment(
    updates: List[Dict],
) -> int:
    """Batch update hotels with SiteMinder enrichment results.
    
    API data always wins when non-empty (overwrites DB values).
    Falls back to existing DB value only when API returns NULL/empty.
    """
    if not updates:
        return 0
    
    hotel_ids = []
    names = []
    addresses = []
    cities = []
    states = []
    countries = []
    emails = []
    phones = []
    websites = []
    lats = []
    lons = []
    
    for u in updates:
        hotel_ids.append(u.get("hotel_id"))
        names.append(u.get("name"))
        addresses.append(u.get("address"))
        cities.append(u.get("city"))
        states.append(u.get("state"))
        countries.append(u.get("country"))
        emails.append(u.get("email"))
        phones.append(u.get("phone"))
        websites.append(u.get("website"))
        lats.append(u.get("lat"))
        lons.append(u.get("lon"))
    
    async with get_conn() as conn:
        result = await conn.execute(
            batch_sql.BATCH_UPDATE_SITEMINDER_ENRICHMENT,
            hotel_ids, names, addresses, cities, states, countries, emails, phones, websites, lats, lons
        )
        count = int(result.split()[-1]) if result else len(updates)
    
    # Also update enrichment status on hotel_booking_engines
    async with get_conn() as conn:
        await conn.execute(
            batch_sql.BATCH_SET_SITEMINDER_ENRICHMENT_STATUS, hotel_ids, 1
        )
    
    return count


async def batch_set_siteminder_enrichment_failed(hotel_ids: List[int]) -> int:
    """Mark SiteMinder hotels as enrichment failed (status=-1)."""
    if not hotel_ids:
        return 0
    
    async with get_conn() as conn:
        result = await conn.execute(
            batch_sql.BATCH_SET_SITEMINDER_ENRICHMENT_STATUS, hotel_ids, -1
        )
        return int(result.split()[-1]) if result else 0


# ============================================================================
# MEWS LOCATION ENRICHMENT FUNCTIONS
# ============================================================================


async def get_mews_hotels_missing_location(
    limit: int = 100,
    country: Optional[str] = None,
) -> List[MewsHotelCandidate]:
    """Get Mews hotels with booking URLs but missing state."""
    async with get_conn() as conn:
        rows = await queries.get_mews_hotels_missing_location(
            conn, limit=limit, country=country
        )
        return [MewsHotelCandidate(**dict(r)) for r in rows]


async def get_mews_hotels_missing_location_count(
    country: Optional[str] = None,
) -> int:
    """Count Mews hotels needing location enrichment."""
    async with get_conn() as conn:
        result = await queries.get_mews_hotels_missing_location_count(
            conn, country=country
        )
        if hasattr(result, 'get'):
            return result.get('count', 0) or 0
        return result or 0


# ============================================================================
# NORMALIZATION REPO FUNCTIONS
# All accept an optional `conn` parameter. When the workflow passes a single
# connection, all reads/writes go through the same backend — no stale reads
# from different pooler connections. When conn is None, gets its own.
# ============================================================================


def _parse_update_count(result) -> int:
    """Parse row count from aiosql execute result like 'UPDATE 42'."""
    return int(result.split()[-1]) if result else 0


# -- Country normalization --

async def count_hotels_by_country_values(old_values: List[str], conn=None) -> Dict[str, int]:
    """Count hotels matching each country value (for dry-run reporting)."""
    async def _run(c):
        rows = await queries.count_hotels_by_country_values(c, old_values=old_values)
        return {r['old_value']: r['count'] for r in rows}
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def batch_update_country_values(old_values: List[str], new_values: List[str], conn=None) -> int:
    """Batch rename country values. Returns count of updated rows.

    Uses inline SQL — aiosql sorts params alphabetically which swaps
    old_values/new_values in unnest.
    """
    _SQL = """
        UPDATE sadie_gtm.hotels h
        SET country = m.new_value, updated_at = NOW()
        FROM unnest($1::text[], $2::text[]) AS m(old_value, new_value)
        WHERE h.country = m.old_value
    """
    async def _run(c):
        return await c.execute(_SQL, old_values, new_values)
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


async def batch_null_country_values(old_values: List[str], conn=None) -> int:
    """Batch NULL out garbage country values. Returns count of updated rows."""
    async def _run(c):
        return await queries.batch_null_country_values(c, old_values=old_values)
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


# -- State normalization --

async def get_state_counts_for_country(country: str, conn=None) -> list:
    """Get unique state values and counts for a country."""
    async def _run(c):
        return await queries.get_state_counts_for_country(c, country=country)
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def batch_null_global_junk_states(junk_values: List[str], conn=None) -> int:
    """NULL out globally invalid state values across all countries."""
    async def _run(c):
        return await queries.batch_null_global_junk_states(c, junk_values=junk_values)
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


async def batch_update_state_values(country: str, old_states: List[str], new_states: List[str], conn=None) -> int:
    """Batch rename state abbreviations to full names for a country.

    Uses inline SQL — aiosql sorts params alphabetically which breaks
    unnest($1, $2) when param names don't sort in positional order.
    """
    _SQL = """
        UPDATE sadie_gtm.hotels h
        SET state = m.new_state, updated_at = NOW()
        FROM unnest($1::text[], $2::text[]) AS m(old_state, new_state)
        WHERE h.country = $3 AND h.state = m.old_state
    """
    async def _run(c):
        return await c.execute(_SQL, old_states, new_states, country)
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


async def batch_null_state_values(country: str, old_states: List[str], conn=None) -> int:
    """Batch NULL out junk state values for a country."""
    async def _run(c):
        return await queries.batch_null_state_values(
            c, country=country, old_states=old_states,
        )
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


# -- Location inference --

async def get_hotels_for_location_inference(country: str, include_null: bool, conn=None) -> list:
    """Get hotels that may need country/state inference."""
    async def _run(c):
        return await queries.get_hotels_for_location_inference(
            c, country=country, include_null=include_null,
        )
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def batch_fix_hotel_locations(ids: List[int], countries: List[str], states: List[str], conn=None) -> int:
    """Batch update country and state for multiple hotels by ID.

    Uses inline SQL — aiosql sorts params alphabetically which breaks
    unnest positional params.
    """
    _SQL = """
        UPDATE sadie_gtm.hotels h
        SET country = m.country,
            state = CASE
                WHEN m.state IS NULL THEN h.state
                WHEN m.state = '' THEN NULL
                ELSE m.state
            END,
            updated_at = NOW()
        FROM unnest($1::bigint[], $2::text[], $3::text[]) AS m(id, country, state)
        WHERE h.id = m.id
    """
    async def _run(c):
        return await c.execute(_SQL, ids, countries, states)
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


# -- Address enrichment --

async def get_hotels_for_address_enrichment(country: str, conn=None) -> list:
    """Get hotels with address but missing state or city."""
    async def _run(c):
        return await queries.get_hotels_for_address_enrichment(c, country=country)
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def batch_enrich_hotel_state_city(ids: List[int], states: List[str], cities: List[str], conn=None) -> int:
    """Batch set state and city from address parsing.

    Uses inline SQL — aiosql sorts params alphabetically which breaks
    unnest positional params (cities, ids, states vs ids, states, cities).
    """
    _SQL = """
        UPDATE sadie_gtm.hotels h
        SET state = COALESCE(h.state, m.state),
            city = COALESCE(h.city, m.city),
            updated_at = NOW()
        FROM unnest($1::bigint[], $2::text[], $3::text[]) AS m(id, state, city)
        WHERE h.id = m.id
          AND (h.state IS NULL OR h.city IS NULL)
    """
    async def _run(c):
        return await c.execute(_SQL, ids, states, cities)
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


# -- Inference helpers --

async def get_all_hotels_for_location_inference(conn=None) -> list:
    """Get ALL hotels with signals for location inference (single query)."""
    async def _run(c):
        return await queries.get_all_hotels_for_location_inference(c)
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def get_all_countries(conn=None) -> list:
    """Get all distinct non-null country values."""
    async def _run(c):
        return await c.fetch(
            "SELECT DISTINCT country FROM sadie_gtm.hotels WHERE country IS NOT NULL ORDER BY country"
        )
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def cleanup_garbage_cities(dry_run: bool = False, conn=None) -> int:
    """NULL out city values that are clearly LLM hallucinations.

    Catches two patterns:
    1. Long sentences (>30 chars) with articles/prepositions
    2. Short phrases starting with articles that contain prepositions
       (e.g. "the heart of", "a myriad of", "our peaceful pond")
    """
    _GARBAGE_CITY_SQL = """
        (
            length(city) > 30
            AND city ~ '\\y(of|the|is|has|are|was|were|an|in|on|at|for|with|by|to|our|while|that|and)\\y'
        )
        OR (
            city ~* '^(the|a|an|our|in|on|at) '
            AND city ~ '\\y(of|is|has|are|was|were|at|for|with|by|to|our|while|that|and|on|in)\\y'
        )
    """
    async def _run(c):
        if dry_run:
            rows = await c.fetch(f"SELECT count(*) as cnt FROM sadie_gtm.hotels WHERE {_GARBAGE_CITY_SQL}")
            return rows[0]["cnt"]
        result = await c.execute(f"""
            UPDATE sadie_gtm.hotels
            SET city = NULL, updated_at = NOW()
            WHERE {_GARBAGE_CITY_SQL}
        """)
        return _parse_update_count(result)
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


# -- City -> State inference --

async def get_city_state_reference_pairs(conn=None) -> list:
    """Get distinct (city, country, state) triples for building lookup."""
    async def _run(c):
        return await queries.get_city_state_reference_pairs(c)
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def get_hotels_missing_state_with_city(conn=None) -> list:
    """Get hotels that have a city but no state."""
    async def _run(c):
        return await queries.get_hotels_missing_state_with_city(c)
    if conn:
        return await _run(conn)
    async with get_conn() as c:
        return await _run(c)


async def batch_set_state_from_city(ids: List[int], states: List[str], conn=None) -> int:
    """Batch set state inferred from city."""
    async def _run(c):
        return await queries.batch_set_state_from_city(
            c, ids=ids, states=states,
        )
    if conn:
        return _parse_update_count(await _run(conn))
    async with get_conn() as c:
        return _parse_update_count(await _run(c))


# ============================================================================
# RMS AVAILABILITY FUNCTIONS
# ============================================================================


async def get_rms_hotels_pending_availability(limit: int, force: bool = False) -> List[Dict[str, Any]]:
    """Get Australia RMS hotels needing availability check.

    Args:
        limit: Max hotels to return
        force: If True, return all hotels (for re-check). If False, only pending.
    """
    async with get_conn() as conn:
        if force:
            rows = await queries.get_rms_hotels_all_for_recheck(conn, limit=limit)
        else:
            rows = await queries.get_rms_hotels_pending_availability(conn, limit=limit)
        return [dict(row) for row in rows]


async def batch_update_rms_availability(hotel_ids: List[int], statuses: List[bool]) -> int:
    """Batch update availability status for RMS hotels.

    Uses unnest for efficient single-query batch update.
    Returns count of updated rows.
    """
    if not hotel_ids:
        return 0

    _SQL = """
        UPDATE sadie_gtm.hotel_booking_engines AS hbe
        SET has_availability = m.has_availability,
            availability_checked_at = NOW()
        FROM (
            SELECT unnest($1::int[]) AS hotel_id,
                   unnest($2::boolean[]) AS has_availability
        ) AS m
        WHERE hbe.hotel_id = m.hotel_id
    """
    for attempt in range(3):
        try:
            async with get_conn() as conn:
                await conn.execute(_SQL, hotel_ids, statuses)
            return len(hotel_ids)
        except Exception as e:
            if attempt < 2:
                import asyncio
                await asyncio.sleep(1.0 * (attempt + 1))
            else:
                raise
    return 0


async def reset_rms_availability() -> int:
    """Reset all Australia RMS availability results to NULL."""
    async with get_conn() as conn:
        result = await queries.reset_rms_availability(conn)
        return _parse_update_count(result)


async def get_rms_availability_stats() -> Dict[str, int]:
    """Get availability check statistics for Australia RMS hotels."""
    async with get_conn() as conn:
        result = await queries.get_rms_availability_stats(conn)
        return dict(result) if result else {
            "total": 0, "pending": 0, "has_availability": 0, "no_availability": 0,
        }


async def get_rms_verification_samples(sample_size: int = 10) -> List[Dict[str, Any]]:
    """Get random samples of available and no-availability hotels for verification.

    Each dict includes a 'has_availability' key reflecting the DB state.
    """
    half = max(sample_size // 2, 1)
    async with get_conn() as conn:
        avail_rows = await queries.get_rms_available_sample(conn, limit=half)
        no_avail_rows = await queries.get_rms_no_availability_sample(conn, limit=half)
    samples = []
    for r in avail_rows:
        d = dict(r)
        d["has_availability"] = True
        samples.append(d)
    for r in no_avail_rows:
        d = dict(r)
        d["has_availability"] = False
        samples.append(d)
    return samples


async def upsert_big4_parks(
    names: List[str], slugs: List[str], phones: List[str],
    emails: List[str], websites: List[str], addresses: List[str],
    cities: List[str], states: List[str], postcodes: List[str],
    lats: List[float], lons: List[float],
) -> None:
    """Upsert BIG4 parks with cross-source dedup (all logic in SQL)."""
    async with get_conn() as conn:
        await conn.execute(
            batch_sql.BATCH_BIG4_UPSERT,
            names, slugs, phones, emails, websites,
            addresses, cities, states, postcodes, lats, lons,
        )


async def get_big4_count() -> int:
    """Count BIG4 parks in the database."""
    async with get_conn() as conn:
        result = await queries.get_big4_count(conn)
        return result["count"] if result else 0
