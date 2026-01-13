"""Repository for leadgen service database operations."""

from typing import Optional
from db.client import queries, get_conn
from db.models.hotel import Hotel


async def get_hotel_by_id(hotel_id: int) -> Optional[Hotel]:
    """Get hotel by ID with location coordinates."""
    async with get_conn() as conn:
        result = await queries.get_hotel_by_id(conn, hotel_id)
        if result:
            return Hotel.model_validate(result)
        return None
