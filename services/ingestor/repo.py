"""
Ingestor Repository - Database operations for ingested data.
"""

from typing import Optional
from db.client import queries, get_conn


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
        existing = await queries.get_hotel_by_name_city(conn, name=name, city=city)

        if existing:
            # Update category if provided and not already set
            if category and not existing["category"]:
                await queries.update_hotel_category(
                    conn, hotel_id=existing["id"], category=category
                )
            return existing["id"]

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
