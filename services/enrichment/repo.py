"""Repository for enrichment service database operations."""

from typing import Optional, List, Dict, Any
from decimal import Decimal
from pydantic import BaseModel
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
    """Update hotel with enriched website and advance pipeline stage."""
    async with get_conn() as conn:
        await queries.update_hotel_website(conn, hotel_id=hotel_id, website=website)
        await queries.advance_to_has_website(conn, hotel_id=hotel_id)


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
) -> List[HotelGeocodingCandidate]:
    """Get hotels with names but missing location data for Serper Places geocoding."""
    async with get_conn() as conn:
        source_pattern = f"%{source}%" if source else None
        results = await queries.get_hotels_needing_geocoding(
            conn, limit=limit, source=source_pattern
        )
        return [HotelGeocodingCandidate.model_validate(dict(row)) for row in results]


async def get_hotels_needing_geocoding_count(source: Optional[str] = None) -> int:
    """Count hotels needing geocoding."""
    async with get_conn() as conn:
        source_pattern = f"%{source}%" if source else None
        result = await queries.get_hotels_needing_geocoding_count(
            conn, source=source_pattern
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
    
    # Bulk UPDATE using unnest - single query for all updates
    sql = """
    UPDATE sadie_gtm.hotels h
    SET 
        address = COALESCE(v.address, h.address),
        city = COALESCE(v.city, h.city),
        state = COALESCE(v.state, h.state),
        country = COALESCE(v.country, h.country),
        location = CASE 
            WHEN v.latitude IS NOT NULL AND v.longitude IS NOT NULL 
            THEN ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography
            ELSE h.location
        END,
        phone_google = COALESCE(v.phone, h.phone_google),
        email = COALESCE(v.email, h.email),
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT * FROM unnest(
            $1::integer[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[],
            $6::float[],
            $7::float[],
            $8::text[],
            $9::text[]
        ) AS t(hotel_id, address, city, state, country, latitude, longitude, phone, email)
    ) v
    WHERE h.id = v.hotel_id
    """
    
    async with get_conn() as conn:
        result = await conn.execute(
            sql,
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
        sql = """
        UPDATE sadie_gtm.hotel_booking_engines
        SET enrichment_status = -1,
            last_enrichment_attempt = NOW()
        WHERE hotel_id = ANY($1::integer[])
        """
        result = await conn.execute(sql, hotel_ids)
        if result and result.startswith("UPDATE"):
            return int(result.split()[1])
        return 0


async def batch_set_last_enrichment_attempt(hotel_ids: List[int]) -> int:
    """Batch set last enrichment attempt for failed hotels.
    
    Note: Inline SQL required because aiosql doesn't support array parameters
    with ANY($1::integer[]) syntax.
    """
    if not hotel_ids:
        return 0
    
    async with get_conn() as conn:
        sql = """
        UPDATE sadie_gtm.hotel_booking_engines
        SET last_enrichment_attempt = NOW()
        WHERE hotel_id = ANY($1::integer[])
        """
        result = await conn.execute(sql, hotel_ids)
        # Parse "UPDATE N" to get count
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
        states.append(u.get("state"))
        countries.append(u.get("country"))
        phones.append(u.get("phone"))
        emails.append(u.get("email"))
        lats.append(u.get("lat"))
        lons.append(u.get("lon"))
        zip_codes.append(u.get("zip_code"))
        contact_names.append(u.get("contact_name"))
    
    # Scraped/API data overrides existing (Cloudbeds page is authoritative)
    sql = """
    UPDATE sadie_gtm.hotels h
    SET 
        name = CASE WHEN v.name IS NOT NULL AND v.name != '' 
                    THEN v.name ELSE h.name END,
        address = CASE WHEN v.address IS NOT NULL AND v.address != '' 
                       THEN v.address ELSE h.address END,
        city = CASE WHEN v.city IS NOT NULL AND v.city != '' 
                    THEN v.city ELSE h.city END,
        state = CASE WHEN v.state IS NOT NULL AND v.state != '' 
                     THEN v.state ELSE h.state END,
        country = CASE WHEN v.country IS NOT NULL AND v.country != '' 
                       THEN v.country ELSE h.country END,
        phone_website = CASE WHEN v.phone IS NOT NULL AND v.phone != '' 
                             THEN v.phone ELSE h.phone_website END,
        email = CASE WHEN v.email IS NOT NULL AND v.email != '' 
                     THEN v.email ELSE h.email END,
        location = CASE 
            WHEN v.lat IS NOT NULL AND v.lon IS NOT NULL 
            THEN ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)::geography
            ELSE h.location 
        END,
        zip_code = CASE WHEN v.zip_code IS NOT NULL AND v.zip_code != '' 
                        THEN v.zip_code ELSE h.zip_code END,
        contact_name = CASE WHEN v.contact_name IS NOT NULL AND v.contact_name != '' 
                            THEN v.contact_name ELSE h.contact_name END,
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT * FROM unnest(
            $1::integer[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::text[],
            $9::float[],
            $10::float[],
            $11::text[],
            $12::text[]
        ) AS t(hotel_id, name, address, city, state, country, phone, email, lat, lon, zip_code, contact_name)
    ) v
    WHERE h.id = v.hotel_id
    """
    
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
) -> int:
    """Batch update hotels with Mews enrichment results.
    
    Supports: name, address, city, country, email, phone, lat, lon
    Uses phone_website column and PostGIS location geography.
    """
    if not updates:
        return 0
    
    hotel_ids = []
    names = []
    addresses = []
    cities = []
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
        countries.append(u.get("country"))
        emails.append(u.get("email"))
        phones.append(u.get("phone"))
        lats.append(u.get("lat"))
        lons.append(u.get("lon"))
    
    sql = """
    UPDATE sadie_gtm.hotels h
    SET 
        name = CASE WHEN v.name IS NOT NULL AND v.name != '' AND h.name LIKE 'Unknown (%'
                    THEN v.name ELSE h.name END,
        address = CASE WHEN v.address IS NOT NULL AND v.address != '' AND h.address IS NULL
                    THEN v.address ELSE h.address END,
        city = CASE WHEN v.city IS NOT NULL AND v.city != '' AND h.city IS NULL
                    THEN v.city ELSE h.city END,
        country = CASE WHEN v.country IS NOT NULL AND v.country != '' AND h.country IS NULL
                    THEN v.country ELSE h.country END,
        email = CASE WHEN v.email IS NOT NULL AND v.email != '' AND h.email IS NULL
                    THEN v.email ELSE h.email END,
        phone_website = CASE WHEN v.phone IS NOT NULL AND v.phone != '' AND h.phone_website IS NULL
                    THEN v.phone ELSE h.phone_website END,
        location = CASE WHEN v.lat IS NOT NULL AND v.lon IS NOT NULL AND h.location IS NULL
                    THEN ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)::geography 
                    ELSE h.location END,
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT * FROM unnest(
            $1::integer[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::float[],
            $9::float[]
        ) AS t(hotel_id, name, address, city, country, email, phone, lat, lon)
    ) v
    WHERE h.id = v.hotel_id
    """
    
    async with get_conn() as conn:
        result = await conn.execute(
            sql, hotel_ids, names, addresses, cities, countries, emails, phones, lats, lons
        )
        count = int(result.split()[-1]) if result else len(updates)
    
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
