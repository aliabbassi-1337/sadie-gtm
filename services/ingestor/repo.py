"""
Ingestor Repository - Database operations for ingested data.
"""

from typing import Optional, List, Tuple
from db.client import queries, get_conn
from db.queries.batch import (
    BATCH_INSERT_HOTELS,
    BATCH_INSERT_EXTERNAL_IDS,
    BATCH_INSERT_ROOM_COUNTS_BY_EXTERNAL_ID,
)


async def get_hotel_by_external_id(id_type: str, external_id: str) -> Optional[int]:
    """
    Look up hotel by external ID.
    Returns hotel_id if found, None otherwise.
    """
    async with get_conn() as conn:
        result = await conn.fetchval(
            """
            SELECT hotel_id FROM sadie_gtm.hotel_external_ids
            WHERE id_type = $1 AND external_id = $2
            """,
            id_type,
            external_id,
        )
        return result


async def get_hotels_by_external_ids(
    id_type: str, external_ids: List[str]
) -> dict[str, int]:
    """
    Batch lookup hotels by external IDs.
    Returns dict mapping external_id -> hotel_id for existing records.
    """
    if not external_ids:
        return {}

    async with get_conn() as conn:
        results = await conn.fetch(
            """
            SELECT external_id, hotel_id FROM sadie_gtm.hotel_external_ids
            WHERE id_type = $1 AND external_id = ANY($2)
            """,
            id_type,
            external_ids,
        )
        return {r["external_id"]: r["hotel_id"] for r in results}


async def insert_hotel(
    name: str,
    source: str,
    external_id: Optional[str] = None,
    id_type: Optional[str] = None,
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
    - If external_id provided, check hotel_external_ids first
    - Otherwise, dedup on name + city

    Returns hotel ID (None if duplicate).
    """
    async with get_conn() as conn:
        # Check external_id first if provided
        if external_id and id_type:
            existing_id = await conn.fetchval(
                """
                SELECT hotel_id FROM sadie_gtm.hotel_external_ids
                WHERE id_type = $1 AND external_id = $2
                """,
                id_type,
                external_id,
            )
            if existing_id:
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
            # Add external_id if not already linked
            if external_id and id_type:
                await conn.execute(
                    """
                    INSERT INTO sadie_gtm.hotel_external_ids (id_type, external_id, hotel_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (id_type, external_id) DO NOTHING
                    """,
                    id_type,
                    external_id,
                    hotel_id,
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

        # Add external_id
        if external_id and id_type and hotel_id:
            await conn.execute(
                """
                INSERT INTO sadie_gtm.hotel_external_ids (id_type, external_id, hotel_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (id_type, external_id) DO NOTHING
                """,
                id_type,
                external_id,
                hotel_id,
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


async def batch_insert_hotels_with_external_ids(
    records: List[Tuple],
    id_type: str,
) -> Tuple[int, int]:
    """
    Batch insert hotels with external IDs.

    Args:
        records: List of tuples (name, source, status, address, city, state, country, phone, category, external_id)
        id_type: Type of external ID (e.g., "texas_hot", "dbpr_license")

    Returns:
        Tuple of (hotels_inserted, external_ids_inserted)
    """
    if not records:
        return 0, 0

    async with get_conn() as conn:
        # Extract external_ids for dedup check
        external_ids = [r[9] for r in records if r[9]]
        existing = await conn.fetch(
            """
            SELECT external_id FROM sadie_gtm.hotel_external_ids
            WHERE id_type = $1 AND external_id = ANY($2)
            """,
            id_type,
            external_ids,
        )
        existing_set = {r["external_id"] for r in existing}

        # Filter out already existing records
        new_records = [r for r in records if r[9] not in existing_set]

        if not new_records:
            return 0, 0

        # Insert hotels (without external_id column)
        hotel_records = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]) for r in new_records]
        await conn.executemany(BATCH_INSERT_HOTELS, hotel_records)

        # Get hotel IDs by name + city
        name_city_pairs = [(r[0], r[4]) for r in new_records]  # name, city
        hotel_ids = await conn.fetch(
            """
            SELECT id, name, city FROM sadie_gtm.hotels
            WHERE (name, city) = ANY($1::text[][])
            """,
            [(n, c) for n, c in name_city_pairs],
        )
        name_city_to_id = {(r["name"], r["city"]): r["id"] for r in hotel_ids}

        # Insert external IDs
        external_records = [
            (id_type, r[9], name_city_to_id.get((r[0], r[4])))
            for r in new_records
            if r[9] and name_city_to_id.get((r[0], r[4]))
        ]
        if external_records:
            await conn.executemany(BATCH_INSERT_EXTERNAL_IDS, external_records)

        return len(new_records), len(external_records)


async def batch_insert_room_counts_by_external_id(
    records: List[Tuple],
    id_type: str,
) -> int:
    """
    Batch insert room counts using external_id lookup.

    Args:
        records: List of tuples (room_count, external_id, source_name)
        id_type: Type of external ID (e.g., "texas_hot")

    Returns:
        Number of records processed
    """
    if not records:
        return 0

    async with get_conn() as conn:
        # Convert to format for BATCH_INSERT_ROOM_COUNTS_BY_EXTERNAL_ID
        # Params: (room_count, id_type, external_id, source_name)
        batch_records = [(r[0], id_type, r[1], r[2]) for r in records]
        await conn.executemany(BATCH_INSERT_ROOM_COUNTS_BY_EXTERNAL_ID, batch_records)
        return len(records)


# Legacy functions for backwards compatibility
async def batch_insert_hotels(records: list[tuple]) -> int:
    """
    Legacy batch insert hotels using executemany.
    Use batch_insert_hotels_with_external_ids for new code.
    """
    async with get_conn() as conn:
        await conn.executemany(BATCH_INSERT_HOTELS, records)
        return len(records)


async def batch_insert_room_counts(records: list[tuple]) -> int:
    """
    Legacy batch insert room counts.
    Use batch_insert_room_counts_by_external_id for new code.
    """
    # This won't work anymore since we changed the batch query
    # Keep for backwards compat but should migrate callers
    raise NotImplementedError(
        "batch_insert_room_counts is deprecated. Use batch_insert_room_counts_by_external_id"
    )
