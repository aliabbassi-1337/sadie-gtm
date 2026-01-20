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
    async with get_conn() as conn:
        # Check for existing by name + city (dedup)
        existing = await conn.fetchrow(
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
        row = await conn.fetchrow(
            """
            INSERT INTO sadie_gtm.hotels (name, website, source, status, address, city, state, country, phone_google)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            """,
            name, website, source, status, address, city, state, country, phone
        )

        return row["id"] if row else None
