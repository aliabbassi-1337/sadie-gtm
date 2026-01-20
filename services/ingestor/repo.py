"""
Ingestor Repository - Database operations for ingested data.
"""

from typing import Optional
from loguru import logger

from db.client import get_conn


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
) -> Optional[int]:
    """
    Insert a hotel from ingestion source.

    Returns hotel ID if inserted, None if duplicate.
    """
    pool = await get_conn()

    # Check for existing by name + city (dedup)
    existing = await pool.fetchrow(
        """
        SELECT id FROM sadie_gtm.hotels
        WHERE LOWER(name) = LOWER($1)
        AND LOWER(COALESCE(city, '')) = LOWER(COALESCE($2, ''))
        LIMIT 1
        """,
        name, city
    )

    if existing:
        return None  # Duplicate

    # Insert new hotel
    row = await pool.fetchrow(
        """
        INSERT INTO sadie_gtm.hotels (name, website, source, status, address, city, state, country, phone_google)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        """,
        name, website, source, status, address, city, state, country, phone
    )

    return row["id"] if row else None


async def get_hotels_without_websites(
    limit: int = 100,
    source_filter: Optional[str] = None,
    state_filter: Optional[str] = None,
) -> list:
    """
    Get hotels that need website enrichment.

    Args:
        limit: Max hotels to return
        source_filter: Filter by source (e.g., 'dbpr')
        state_filter: Filter by state (e.g., 'FL')

    Returns:
        List of hotel dicts
    """
    pool = await get_conn()

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

    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]


async def update_hotel_website(hotel_id: int, website: str) -> bool:
    """
    Update hotel with enriched website.

    Returns True if updated.
    """
    pool = await get_conn()

    result = await pool.execute(
        "UPDATE sadie_gtm.hotels SET website = $1 WHERE id = $2",
        website, hotel_id
    )

    return result == "UPDATE 1"


async def get_ingestion_stats(source_prefix: str = "dbpr") -> dict:
    """
    Get stats for ingested hotels.

    Args:
        source_prefix: Source prefix to filter by

    Returns:
        Dict with counts
    """
    pool = await get_conn()

    row = await pool.fetchrow(
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
