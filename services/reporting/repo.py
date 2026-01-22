"""Repository for reporting service database operations."""

from typing import List, Optional
from db.client import queries, get_conn
from db.models.reporting import HotelLead, CityStats, EngineCount, LaunchableHotel, DetectionFunnel


async def get_leads_for_city(city: str, state: str) -> List[HotelLead]:
    """Get hotel leads for a city with booking engine, room count, and proximity."""
    async with get_conn() as conn:
        results = await queries.get_leads_for_city(conn, city=city, state=state)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_leads_for_state(state: str, source_pattern: str = None) -> List[HotelLead]:
    """Get hotel leads for an entire state, optionally filtered by source."""
    async with get_conn() as conn:
        if source_pattern:
            results = await queries.get_leads_for_state_by_source(
                conn, state=state, source_pattern=source_pattern
            )
        else:
            results = await queries.get_leads_for_state(conn, state=state)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_city_stats(city: str, state: str) -> CityStats:
    """Get analytics stats for a city."""
    async with get_conn() as conn:
        result = await queries.get_city_stats(conn, city=city, state=state)
        if result:
            return CityStats.model_validate(dict(result))
        return CityStats()


async def get_state_stats(state: str, source_pattern: str = None) -> CityStats:
    """Get analytics stats for a state, optionally filtered by source."""
    async with get_conn() as conn:
        if source_pattern:
            result = await queries.get_state_stats_by_source(
                conn, state=state, source_pattern=source_pattern
            )
        else:
            result = await queries.get_state_stats(conn, state=state)
        if result:
            return CityStats.model_validate(dict(result))
        return CityStats()


async def get_top_engines_for_city(city: str, state: str) -> List[EngineCount]:
    """Get top booking engines for a city."""
    async with get_conn() as conn:
        results = await queries.get_top_engines_for_city(conn, city=city, state=state)
        return [EngineCount.model_validate(dict(row)) for row in results]


async def get_top_engines_for_state(state: str, source_pattern: str = None) -> List[EngineCount]:
    """Get top booking engines for a state, optionally filtered by source."""
    async with get_conn() as conn:
        if source_pattern:
            results = await queries.get_top_engines_for_state_by_source(
                conn, state=state, source_pattern=source_pattern
            )
        else:
            results = await queries.get_top_engines_for_state(conn, state=state)
        return [EngineCount.model_validate(dict(row)) for row in results]


async def get_cities_in_state(state: str) -> List[str]:
    """Get all cities in a state that have detected hotels."""
    async with get_conn() as conn:
        results = await queries.get_cities_in_state(conn, state=state)
        return [row["city"] for row in results]


async def get_detection_funnel(state: str, source_pattern: str = None) -> DetectionFunnel:
    """Get comprehensive detection funnel metrics for a state."""
    async with get_conn() as conn:
        if source_pattern:
            result = await queries.get_detection_funnel_by_source(
                conn, state=state, source_pattern=source_pattern
            )
        else:
            result = await queries.get_detection_funnel(conn, state=state)
        if result:
            return DetectionFunnel.model_validate(dict(result))
        return DetectionFunnel()


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


async def launch_hotels(hotel_ids: List[int]) -> List[int]:
    """Atomically claim and launch specific hotels (multi-worker safe).

    Uses FOR UPDATE SKIP LOCKED so multiple EC2 instances can run concurrently.
    Returns list of hotel IDs that were actually launched.
    """
    if not hotel_ids:
        return []
    async with get_conn() as conn:
        results = await queries.launch_hotels(conn, hotel_ids=hotel_ids)
        return [row["id"] for row in results]


async def launch_ready_hotels(limit: int = 100) -> List[int]:
    """Atomically claim and launch ready hotels (multi-worker safe).

    Uses FOR UPDATE SKIP LOCKED so multiple EC2 instances can run concurrently.
    Returns list of hotel IDs that were launched.
    """
    async with get_conn() as conn:
        results = await queries.launch_ready_hotels(conn, limit=limit)
        return [row["id"] for row in results]


async def get_launched_count() -> int:
    """Count hotels that have been launched (status=1)."""
    async with get_conn() as conn:
        result = await queries.get_launched_count(conn)
        return result["count"] if result else 0
