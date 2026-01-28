"""Repository for RMS booking engine database operations."""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from db.client import queries, get_conn


@dataclass
class RMSHotelRecord:
    """RMS hotel record from database."""
    hotel_id: int
    booking_url: str


async def get_booking_engine_id() -> int:
    """Get RMS Cloud booking engine ID from database."""
    async with get_conn() as conn:
        result = await queries.get_rms_booking_engine_id(conn)
        if result:
            return result["id"]
        raise ValueError("RMS Cloud booking engine not found in database")


async def get_hotels_needing_enrichment(limit: int = 1000) -> List[RMSHotelRecord]:
    """Get RMS hotels that need enrichment."""
    async with get_conn() as conn:
        results = await queries.get_rms_hotels_needing_enrichment(conn, limit=limit)
        return [RMSHotelRecord(hotel_id=r["hotel_id"], booking_url=r["booking_url"]) for r in results]


async def insert_hotel(
    name: Optional[str],
    address: Optional[str],
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    phone: Optional[str],
    email: Optional[str],
    website: Optional[str],
    source: str = "rms_scan",
    status: int = 1,
) -> Optional[int]:
    """Insert a new RMS hotel and return the ID."""
    async with get_conn() as conn:
        result = await queries.insert_rms_hotel(
            conn,
            name=name,
            address=address,
            city=city,
            state=state,
            country=country,
            phone=phone,
            email=email,
            website=website,
            source=source,
            status=status,
        )
        return result


async def insert_hotel_booking_engine(
    hotel_id: int,
    booking_engine_id: int,
    booking_url: str,
    enrichment_status: str = "enriched",
) -> None:
    """Insert or update hotel booking engine relation."""
    async with get_conn() as conn:
        await queries.insert_rms_hotel_booking_engine(
            conn,
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            booking_url=booking_url,
            enrichment_status=enrichment_status,
        )


async def update_hotel(
    hotel_id: int,
    name: Optional[str] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    website: Optional[str] = None,
) -> None:
    """Update hotel with enriched data."""
    async with get_conn() as conn:
        await queries.update_rms_hotel(
            conn,
            hotel_id=hotel_id,
            name=name,
            address=address,
            city=city,
            state=state,
            country=country,
            phone=phone,
            email=email,
            website=website,
        )


async def update_enrichment_status(
    booking_url: str,
    status: str,
) -> None:
    """Update enrichment status for a hotel booking engine."""
    async with get_conn() as conn:
        await queries.update_rms_enrichment_status(
            conn,
            booking_url=booking_url,
            status=status,
        )


async def get_stats() -> Dict[str, int]:
    """Get RMS hotel statistics."""
    async with get_conn() as conn:
        result = await queries.get_rms_stats(conn)
        if result:
            return dict(result)
        return {
            "total": 0,
            "with_name": 0,
            "with_city": 0,
            "with_email": 0,
            "with_phone": 0,
            "enriched": 0,
            "no_data": 0,
            "dead": 0,
        }


async def count_needing_enrichment() -> int:
    """Count RMS hotels needing enrichment."""
    async with get_conn() as conn:
        result = await queries.count_rms_needing_enrichment(conn)
        return result["count"] if result else 0
