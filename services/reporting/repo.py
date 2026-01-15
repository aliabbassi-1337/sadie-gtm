"""Repository for reporting service database operations."""

from typing import List, Optional
from db.client import queries, get_conn
from db.models.reporting import HotelLead, CityStats, EngineCount, LaunchableHotel


async def get_leads_for_city(city: str, state: str) -> List[HotelLead]:
    """Get hotel leads for a city with booking engine, room count, and proximity."""
    async with get_conn() as conn:
        results = await queries.get_leads_for_city(conn, city=city, state=state)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_leads_for_state(state: str) -> List[HotelLead]:
    """Get hotel leads for an entire state."""
    async with get_conn() as conn:
        results = await queries.get_leads_for_state(conn, state=state)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_city_stats(city: str, state: str) -> CityStats:
    """Get analytics stats for a city."""
    async with get_conn() as conn:
        result = await queries.get_city_stats(conn, city=city, state=state)
        if result:
            return CityStats.model_validate(dict(result))
        return CityStats()


async def get_state_stats(state: str) -> CityStats:
    """Get analytics stats for a state."""
    async with get_conn() as conn:
        result = await queries.get_state_stats(conn, state=state)
        if result:
            return CityStats.model_validate(dict(result))
        return CityStats()


async def get_top_engines_for_city(city: str, state: str) -> List[EngineCount]:
    """Get top booking engines for a city."""
    async with get_conn() as conn:
        results = await queries.get_top_engines_for_city(conn, city=city, state=state)
        return [EngineCount.model_validate(dict(row)) for row in results]


async def get_top_engines_for_state(state: str) -> List[EngineCount]:
    """Get top booking engines for a state."""
    async with get_conn() as conn:
        results = await queries.get_top_engines_for_state(conn, state=state)
        return [EngineCount.model_validate(dict(row)) for row in results]


async def get_cities_in_state(state: str) -> List[str]:
    """Get all cities in a state that have detected hotels."""
    async with get_conn() as conn:
        results = await queries.get_cities_in_state(conn, state=state)
        return [row["city"] for row in results]


# ============================================================================
# LAUNCHER FUNCTIONS
# ============================================================================


async def get_launchable_hotels(limit: int = 100) -> List[LaunchableHotel]:
    """Get hotels ready to be launched (fully enriched with all data)."""
    async with get_conn() as conn:
        results = await queries.get_launchable_hotels(conn, limit=limit)
        return [LaunchableHotel.model_validate(dict(row)) for row in results]


async def get_launchable_count() -> int:
    """Count hotels ready to be launched."""
    async with get_conn() as conn:
        result = await queries.get_launchable_count(conn)
        return result["count"] if result else 0


async def launch_hotels(hotel_ids: List[int]) -> None:
    """Mark hotels as launched (status=1)."""
    if not hotel_ids:
        return
    async with get_conn() as conn:
        await queries.launch_hotels(conn, hotel_ids=hotel_ids)


async def launch_all_ready_hotels() -> None:
    """Mark ALL ready hotels as launched (status=1)."""
    async with get_conn() as conn:
        await queries.launch_all_ready_hotels(conn)


async def get_launched_count() -> int:
    """Count hotels that have been launched (status=1)."""
    async with get_conn() as conn:
        result = await queries.get_launched_count(conn)
        return result["count"] if result else 0
