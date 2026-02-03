"""
Ingestor Repository - Database operations for ingested data.
"""

from typing import Optional, List, Tuple, Dict, Any
from db.client import queries, get_conn
from db.queries.batch import BATCH_INSERT_HOTELS, BATCH_INSERT_ROOM_COUNTS, BATCH_INSERT_CRAWLED_HOTELS, BATCH_INSERT_IPMS247_HOTEL


# =============================================================================
# Booking Engine Operations
# =============================================================================

async def get_booking_engine_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    Look up booking engine by name.
    Returns dict with id, name, domains, tier if found.
    """
    async with get_conn() as conn:
        return await queries.get_booking_engine_by_name(conn, name=name)


async def get_or_create_booking_engine(name: str, tier: int = 1) -> int:
    """
    Get existing booking engine by name or create new one.
    Returns the booking engine ID.
    """
    async with get_conn() as conn:
        existing = await queries.get_booking_engine_by_name(conn, name=name)
        if existing:
            return existing["id"]
        
        engine_id = await queries.insert_booking_engine(
            conn, name=name, domains=None, tier=tier
        )
        return engine_id


# =============================================================================
# Crawl Ingestor Operations
# =============================================================================

async def get_hotel_by_booking_url(booking_url: str) -> Optional[Dict[str, Any]]:
    """
    Check if a booking URL already exists in hotel_booking_engines.
    Returns hotel info if found, None otherwise.
    """
    async with get_conn() as conn:
        return await queries.get_hotel_by_booking_url(conn, booking_url=booking_url)


async def insert_crawled_hotel(
    name: str,
    source: str,
    external_id: str,
    external_id_type: str,
    booking_engine_id: int,
    booking_url: str,
    slug: str,
    detection_method: str = "crawl_import",
) -> Optional[int]:
    """
    Insert a crawled hotel and link to booking engine.
    
    Returns hotel_id if inserted, None if duplicate.
    """
    async with get_conn() as conn:
        # Check if booking URL already exists
        existing = await queries.get_hotel_by_booking_url(conn, booking_url=booking_url)
        if existing:
            return None
        
        # Insert hotel
        hotel_id = await queries.insert_hotel_with_external_id(
            conn,
            name=name,
            website=None,
            source=source,
            status=0,  # PENDING
            address=None,
            city=None,
            state=None,
            country="United States",
            phone=None,
            category=None,
            external_id=external_id,
            external_id_type=external_id_type,
        )
        
        if not hotel_id:
            return None
        
        # Link to booking engine
        await queries.insert_hotel_booking_engine(
            conn,
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            booking_url=booking_url,
            engine_property_id=slug,
            detection_method=detection_method,
            status=1,
        )
        
        return hotel_id


async def batch_insert_crawled_hotels(
    records: List[Tuple[str, str, str, str, int, str, str, str]],
) -> int:
    """
    Batch insert crawled hotels with booking engine linking.
    
    Uses executemany for fast bulk inserts. Single query per record inserts
    both hotel and booking_engine link atomically.
    
    Wrapped in a transaction for:
    - Single commit (faster)
    - Atomicity (all or nothing)
    - Clean rollback on failure
    
    Args:
        records: List of tuples (name, source, external_id, external_id_type,
                                 booking_engine_id, booking_url, slug, detection_method)
    
    Returns:
        Number of records processed
    """
    if not records:
        return 0
    
    async with get_conn() as conn:
        async with conn.transaction():
            await conn.executemany(BATCH_INSERT_CRAWLED_HOTELS, records)
            return len(records)


async def batch_insert_ipms247_hotels(
    records: List[Tuple],
) -> int:
    """
    Batch insert IPMS247 hotels with full scraped data.
    
    Args:
        records: List of tuples (name, source, external_id, external_id_type, 
                                 booking_engine_id, booking_url, slug, detection_method,
                                 email, phone, address, city, state, country, lat, lng)
    
    Returns:
        Number of records processed
    """
    if not records:
        return 0
    
    async with get_conn() as conn:
        async with conn.transaction():
            await conn.executemany(BATCH_INSERT_IPMS247_HOTEL, records)
            return len(records)


async def get_existing_booking_urls(booking_urls: List[str]) -> set:
    """
    Check which booking URLs already exist in the database.
    
    Args:
        booking_urls: List of booking URLs to check
    
    Returns:
        Set of booking URLs that already exist
    """
    if not booking_urls:
        return set()
    
    async with get_conn() as conn:
        results = await queries.get_existing_booking_urls(
            conn, booking_urls=booking_urls
        )
        return {r["booking_url"] for r in results}


async def get_hotel_by_external_id(external_id_type: str, external_id: str) -> Optional[int]:
    """
    Look up hotel by external ID.
    Returns hotel_id if found, None otherwise.
    """
    async with get_conn() as conn:
        result = await queries.get_hotel_by_external_id(
            conn, external_id_type=external_id_type, external_id=external_id
        )
        return result["id"] if result else None


async def insert_hotel(
    name: str,
    source: str,
    external_id: Optional[str] = None,
    external_id_type: Optional[str] = None,
    status: int = 0,
    website: Optional[str] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: str = "United States",
    phone: Optional[str] = None,
    category: Optional[str] = None,
) -> Optional[int]:
    """
    Insert a hotel from ingestion source.

    Dedup strategy:
    - If external_id provided, DB constraint prevents duplicates
    - Also dedup on name + city as fallback

    Returns hotel ID (None if duplicate).
    """
    async with get_conn() as conn:
        # Check external_id first if provided
        if external_id and external_id_type:
            existing = await queries.get_hotel_by_external_id(
                conn, external_id_type=external_id_type, external_id=external_id
            )
            if existing:
                return None  # Already exists

        # Check name + city dedup
        existing = await queries.get_hotel_by_name_city(conn, name=name, city=city)

        if existing:
            hotel_id = existing["id"]
            # Update with ingestor data (won't overwrite existing non-null values)
            await queries.update_hotel_from_ingestor(
                conn,
                hotel_id=hotel_id,
                category=category,
                address=address,
                phone=phone,
            )
            # Also set external_id if not already set
            if external_id and external_id_type:
                await queries.update_hotel_external_id(
                    conn,
                    hotel_id=hotel_id,
                    external_id=external_id,
                    external_id_type=external_id_type,
                )
            return None  # Return None for duplicates

        # Insert new hotel with external_id
        if external_id and external_id_type:
            hotel_id = await queries.insert_hotel_with_external_id(
                conn,
                name=name,
                website=website,
                source=source,
                status=status,
                address=address,
                city=city,
                state=state,
                country=country,
                phone=phone,
                category=category,
                external_id=external_id,
                external_id_type=external_id_type,
            )
        else:
            hotel_id = await queries.insert_hotel_with_category(
                conn,
                name=name,
                website=website,
                source=source,
                status=status,
                address=address,
                city=city,
                state=state,
                country=country,
                phone=phone,
                category=category,
            )

        return hotel_id


async def insert_room_count(
    hotel_id: int,
    room_count: int,
    source: str,
    confidence: Optional[float] = None,
    status: int = 1,
) -> Optional[int]:
    """
    Insert or update room count for a hotel.
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


async def batch_insert_hotels(
    records: List[Tuple],
    external_id_type: Optional[str] = None,
) -> int:
    """
    Batch insert hotels using executemany.

    Args:
        records: List of tuples (name, source, status, address, city, state, country, phone, category, external_id, lat, lon)
                 If external_id_type is provided, external_id is the 10th element
                 lat/lon are optional at positions 10 and 11
        external_id_type: Type of external ID (e.g., "texas_hot", "dbpr_license")

    Returns:
        Number of records processed
    """
    if not records:
        return 0

    async with get_conn() as conn:
        if external_id_type:
            # Check which external_ids already exist
            external_ids = [r[9] for r in records if len(r) > 9 and r[9]]
            if external_ids:
                existing = await queries.get_hotels_by_external_ids(
                    conn, external_id_type=external_id_type, external_ids=external_ids
                )
                existing_set = {r["external_id"] for r in existing}
                records = [r for r in records if len(r) <= 9 or r[9] not in existing_set]

            if not records:
                return 0

            # Add external_id_type and ensure lat/lon are included
            # Format: (name, source, status, address, city, state, country, phone, category, external_id, external_id_type, lat, lon)
            full_records = [
                (
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8],
                    r[9] if len(r) > 9 else None,
                    external_id_type,
                    r[10] if len(r) > 10 else None,
                    r[11] if len(r) > 11 else None,
                )
                for r in records
            ]
            await conn.executemany(BATCH_INSERT_HOTELS, full_records)
        else:
            # Legacy format without external_id
            legacy_records = [
                (
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8],
                    None, None,
                    r[10] if len(r) > 10 else None,
                    r[11] if len(r) > 11 else None,
                )
                for r in records
            ]
            await conn.executemany(BATCH_INSERT_HOTELS, legacy_records)

        return len(records)


async def batch_insert_room_counts(
    records: List[Tuple],
    external_id_type: Optional[str] = None,
    confidence: float = 1.0,
) -> int:
    """
    Batch insert room counts.

    Args:
        records: List of tuples (room_count, external_id, source_name)
        external_id_type: Type of external ID for lookup
        confidence: Confidence score (default 10 for license data)

    Returns:
        Number of records processed
    """
    if not records or not external_id_type:
        return 0

    async with get_conn() as conn:
        # Format: (room_count, external_id_type, external_id, source_name, confidence)
        batch_records = [(r[0], external_id_type, r[1], r[2], confidence) for r in records]
        await conn.executemany(BATCH_INSERT_ROOM_COUNTS, batch_records)
        return len(records)
