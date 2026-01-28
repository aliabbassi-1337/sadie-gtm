"""Repository for RMS booking engine database operations."""

from typing import Optional, List, Dict, Any, Protocol, runtime_checkable
from pydantic import BaseModel

from db.client import queries, get_conn


class RMSHotelRecord(BaseModel):
    """RMS hotel record from database."""
    hotel_id: int
    booking_url: str


@runtime_checkable
class IRMSRepo(Protocol):
    """Protocol for RMS repository operations."""
    
    async def get_booking_engine_id(self) -> int:
        """Get RMS Cloud booking engine ID."""
        ...
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        """Get RMS hotels that need enrichment."""
        ...
    
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
        external_id: Optional[str] = None,
        source: str = "rms_scan",
        status: int = 1,
    ) -> Optional[int]:
        """Insert a new RMS hotel and return the ID."""
        ...
    
    async def insert_hotel_booking_engine(
        self,
        hotel_id: int,
        booking_engine_id: int,
        booking_url: str,
        enrichment_status: str = "enriched",
    ) -> None:
        """Insert or update hotel booking engine relation."""
        ...
    
    async def update_hotel(
        self,
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
        ...
    
    async def update_enrichment_status(self, booking_url: str, status: str) -> None:
        """Update enrichment status for a hotel booking engine."""
        ...
    
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        ...
    
    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment."""
        ...


class RMSRepo(IRMSRepo):
    """Implementation of RMS repository."""
    
    def __init__(self):
        self._booking_engine_id: Optional[int] = None
    
    async def get_booking_engine_id(self) -> int:
        """Get RMS Cloud booking engine ID from database (cached)."""
        if self._booking_engine_id is None:
            async with get_conn() as conn:
                result = await queries.get_rms_booking_engine_id(conn)
                if result:
                    self._booking_engine_id = result["id"]
                else:
                    raise ValueError("RMS Cloud booking engine not found in database")
        return self._booking_engine_id

    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        """Get RMS hotels that need enrichment."""
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            results = await queries.get_rms_hotels_needing_enrichment(
                conn, 
                booking_engine_id=booking_engine_id,
                limit=limit,
            )
            return [RMSHotelRecord(hotel_id=r["hotel_id"], booking_url=r["booking_url"]) for r in results]

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
        external_id: Optional[str] = None,
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
                external_id=external_id,
                source=source,
                status=status,
            )
            return result

    async def insert_hotel_booking_engine(
        self,
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
        self,
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
        self,
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

    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            result = await queries.get_rms_stats(conn, booking_engine_id=booking_engine_id)
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

    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment."""
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            result = await queries.count_rms_needing_enrichment(
                conn, 
                booking_engine_id=booking_engine_id,
            )
            return result["count"] if result else 0
