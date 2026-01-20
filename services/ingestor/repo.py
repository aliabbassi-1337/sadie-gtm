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
    category: Optional[str] = None,
) -> Optional[int]:
    """
    Insert a hotel from ingestion source.

    If duplicate exists, updates category if provided.
    Returns hotel ID if inserted/updated, None if duplicate with no updates.
    """
    async with get_conn() as conn:
        # Check for existing by name + city (dedup)
        existing = await conn.fetchrow(
            """
            SELECT id, category FROM sadie_gtm.hotels
            WHERE LOWER(name) = LOWER($1)
            AND LOWER(COALESCE(city, '')) = LOWER(COALESCE($2, ''))
            LIMIT 1
            """,
            name, city
        )

        if existing:
            # Update category if provided and not already set
            if category and not existing["category"]:
                await conn.execute(
                    "UPDATE sadie_gtm.hotels SET category = $1 WHERE id = $2",
                    category, existing["id"]
                )
            return existing["id"]

        # Insert new hotel
        row = await conn.fetchrow(
            """
            INSERT INTO sadie_gtm.hotels (name, website, source, status, address, city, state, country, phone_google, category)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
            """,
            name, website, source, status, address, city, state, country, phone, category
        )

        return row["id"] if row else None
