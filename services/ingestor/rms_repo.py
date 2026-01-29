"""RMS Repository - Database operations for RMS ingestion."""

from typing import Optional

from db.client import queries, get_conn


class RMSRepo:
    """Database operations for RMS ingestion."""
    
    def __init__(self):
        self._booking_engine_id: Optional[int] = None
    
    async def get_booking_engine_id(self) -> int:
        """Get RMS Cloud booking engine ID from database."""
        if self._booking_engine_id is None:
            async with get_conn() as conn:
                result = await queries.get_rms_booking_engine_id(conn)
                if result:
                    self._booking_engine_id = result["id"]
                else:
                    raise ValueError("RMS Cloud booking engine not found")
        return self._booking_engine_id
    
    async def insert_hotel(
        self,
        name: Optional[str],
        address: Optional[str],
        city: Optional[str],
        state: Optional[str],
        country: Optional[str],
        phone: Optional[str],
        email: Optional[str],
        website: Optional[str],
        external_id: Optional[str],
        source: str = "rms_scan",
        status: int = 1,
    ) -> Optional[int]:
        """Insert a new RMS hotel."""
        async with get_conn() as conn:
            return await queries.insert_rms_hotel(
                conn, name=name, address=address, city=city, state=state,
                country=country, phone=phone, email=email, website=website,
                external_id=external_id, source=source, status=status,
            )
    
    async def insert_hotel_booking_engine(
        self,
        hotel_id: int,
        booking_engine_id: int,
        booking_url: str,
        enrichment_status: int = 1,
    ) -> None:
        """Insert hotel booking engine relation."""
        async with get_conn() as conn:
            await queries.insert_rms_hotel_booking_engine(
                conn, hotel_id=hotel_id, booking_engine_id=booking_engine_id,
                booking_url=booking_url, enrichment_status=enrichment_status,
            )
