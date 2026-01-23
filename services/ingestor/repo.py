"""
Ingestor Repository - Database operations for ingested data.
"""

from typing import Optional
from db.client import queries, get_conn
from db.queries.batch import BATCH_INSERT_HOTELS, BATCH_INSERT_ROOM_COUNTS


async def insert_hotel(
    name: str,
    source: str,
    status: int = 0,
    website: Optional[str] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: str = "USA",
    phone: Optional[str] = None,
    category: Optional[str] = None,
) -> Optional[int]:
    """
    Insert a hotel from ingestion source.

    Dedup strategy:
    - If source contains unique ID (e.g., "texas_hot:12345:00001"), dedup on source
    - Otherwise, dedup on name + city

    Returns hotel ID (None if duplicate).
    """
    async with get_conn() as conn:
        existing = None

        # If source has unique ID format (contains ":"), dedup on source
        if ":" in source:
            existing = await queries.get_hotel_by_source(conn, source=source)
        else:
            # Fallback to name + city dedup
            existing = await queries.get_hotel_by_name_city(conn, name=name, city=city)

        if existing:
            # Update with ingestor data (won't overwrite existing non-null values)
            await queries.update_hotel_from_ingestor(
                conn,
                hotel_id=existing["id"],
                category=category,
                address=address,
                phone=phone,
            )
            return None  # Return None for duplicates

        # Insert new hotel
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

    Args:
        hotel_id: Hotel ID
        room_count: Number of rooms
        source: Data source (e.g., "texas_hot", "enrichment")
        confidence: Confidence score (0-1)
        status: -1=processing, 0=failed, 1=success

    Returns:
        Room count record ID
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


async def batch_insert_hotels(records: list[tuple]) -> int:
    """
    Batch insert hotels using executemany.

    Args:
        records: List of tuples (name, source, status, address, city, state, country, phone, category)

    Returns:
        Number of records processed
    """
    async with get_conn() as conn:
        await conn.executemany(BATCH_INSERT_HOTELS, records)
        return len(records)


async def batch_insert_room_counts(records: list[tuple]) -> int:
    """
    Batch insert room counts using executemany.

    Args:
        records: List of tuples (room_count, source, source_name)
                 source is used to lookup hotel_id

    Returns:
        Number of records processed
    """
    async with get_conn() as conn:
        await conn.executemany(BATCH_INSERT_ROOM_COUNTS, records)
        return len(records)
