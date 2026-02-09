"""Repository for reporting service database operations."""

from typing import List, Optional
from db.client import queries, get_conn
from db.models.reporting import HotelLead, CityStats, EngineCount, LaunchableHotel, EnrichmentStats


async def get_leads_for_city(city: str, state: str) -> List[HotelLead]:
    """Get hotel leads for a city with booking engine, room count, and proximity."""
    async with get_conn() as conn:
        results = await queries.get_leads_for_city(conn, city=city, state=state)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_leads_for_country(country: str) -> List[HotelLead]:
    """Get ALL hotel leads for a country."""
    async with get_conn() as conn:
        results = await queries.get_leads_for_country(conn, country=country)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_leads_for_state(state: str, source_pattern: str = None, country: str = "United States") -> List[HotelLead]:
    """Get hotel leads for an entire state, optionally filtered by source."""
    async with get_conn() as conn:
        if source_pattern:
            results = await queries.get_leads_for_state_by_source(
                conn, state=state, source_pattern=source_pattern, country=country
            )
        else:
            results = await queries.get_leads_for_state(conn, state=state, country=country)
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_leads_by_booking_engine(booking_engine: str, source_pattern: str) -> List[HotelLead]:
    """Get hotel leads by booking engine and source pattern.
    
    For crawl data exports - doesn't require launched status.
    """
    async with get_conn() as conn:
        results = await queries.get_leads_by_booking_engine(
            conn, booking_engine=booking_engine, source_pattern=source_pattern
        )
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_leads_by_source(source_pattern: str) -> List[HotelLead]:
    """Get hotel leads by source pattern (e.g., 'ipms247%').
    
    For direct source exports like IPMS247.
    """
    async with get_conn() as conn:
        results = await queries.get_leads_by_source(
            conn, source_pattern=source_pattern
        )
        return [HotelLead.model_validate(dict(row)) for row in results]


async def get_city_stats(city: str, state: str) -> CityStats:
    """Get analytics stats for a city."""
    async with get_conn() as conn:
        result = await queries.get_city_stats(conn, city=city, state=state)
        if result:
            return CityStats.model_validate(dict(result))
        return CityStats()


async def get_state_stats(state: str, source_pattern: str = None, country: str = "United States") -> CityStats:
    """Get analytics stats for a state, optionally filtered by source."""
    async with get_conn() as conn:
        if source_pattern:
            result = await queries.get_state_stats_by_source(
                conn, state=state, source_pattern=source_pattern, country=country
            )
        else:
            result = await queries.get_state_stats(conn, state=state, country=country)
        if result:
            return CityStats.model_validate(dict(result))
        return CityStats()


async def get_top_engines_for_city(city: str, state: str) -> List[EngineCount]:
    """Get top booking engines for a city."""
    async with get_conn() as conn:
        results = await queries.get_top_engines_for_city(conn, city=city, state=state)
        return [EngineCount.model_validate(dict(row)) for row in results]


async def get_top_engines_for_state(state: str, source_pattern: str = None, country: str = "United States") -> List[EngineCount]:
    """Get top booking engines for a state, optionally filtered by source."""
    async with get_conn() as conn:
        if source_pattern:
            results = await queries.get_top_engines_for_state_by_source(
                conn, state=state, source_pattern=source_pattern, country=country
            )
        else:
            results = await queries.get_top_engines_for_state(conn, state=state, country=country)
        return [EngineCount.model_validate(dict(row)) for row in results]


async def get_cities_in_state(state: str, country: str = "United States") -> List[str]:
    """Get all cities in a state that have detected hotels."""
    async with get_conn() as conn:
        results = await queries.get_cities_in_state(conn, state=state, country=country)
        return [row["city"] for row in results]


async def get_detection_funnel(state: str, country: str = "United States") -> dict:
    """Get detection funnel stats for a state."""
    async with get_conn() as conn:
        result = await queries.get_detection_funnel(conn, state=state, country=country)
        return dict(result) if result else {}


async def get_detection_funnel_by_source(state: str, source_pattern: str, country: str = "United States") -> dict:
    """Get detection funnel stats for a state filtered by source."""
    async with get_conn() as conn:
        result = await queries.get_detection_funnel_by_source(
            conn, state=state, source_pattern=source_pattern, country=country
        )
        return dict(result) if result else {}


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


# ============================================================================
# PIPELINE STATUS FUNCTIONS
# ============================================================================


async def get_pipeline_summary() -> list:
    """Get count of hotels at each pipeline stage."""
    async with get_conn() as conn:
        results = await queries.get_pipeline_summary(conn)
        return [(r['status'], r['count']) for r in results]


async def get_pipeline_by_source() -> list:
    """Get pipeline breakdown by source."""
    async with get_conn() as conn:
        results = await queries.get_pipeline_by_source(conn)
        return [dict(r) for r in results]


async def get_pipeline_by_source_name(source: str) -> list:
    """Get pipeline breakdown for a specific source."""
    async with get_conn() as conn:
        results = await queries.get_pipeline_by_source_name(conn, source=source)
        return [(r['status'], r['count']) for r in results]


async def get_distinct_states(country: str = "United States") -> List[str]:
    """Get all distinct states that have launched hotels for a country.

    Normalizes abbreviations to full names and deduplicates, so "CA" and
    "California" merge into just "California". Filters out junk values.
    """
    from services.enrichment.state_utils import is_valid_state, normalize_state
    async with get_conn() as conn:
        results = await queries.get_distinct_states(conn, country=country)
        seen = set()
        out = []
        for r in results:
            raw = r["state"]
            if not is_valid_state(raw, country):
                continue
            normalized = normalize_state(raw, country) or raw
            if normalized not in seen:
                seen.add(normalized)
                out.append(normalized)
        return sorted(out)


async def get_distinct_states_for_country(country: str) -> List[str]:
    """Get all distinct states for a specific country that have launched leads.

    Delegates to get_distinct_states() which uses parameterized SQL
    and Python-side state validation.
    """
    return await get_distinct_states(country=country)


# ============================================================================
# ENRICHMENT STATS FUNCTIONS
# ============================================================================


async def get_enrichment_stats_by_engine(source_pattern: str = None, country: str = "United States") -> List[EnrichmentStats]:
    """Get enrichment stats grouped by booking engine.

    Args:
        source_pattern: Optional source pattern to filter (e.g., '%crawl%')
        country: Country to filter by

    Returns:
        List of EnrichmentStats, one per booking engine
    """
    async with get_conn() as conn:
        if source_pattern:
            results = await queries.get_enrichment_stats_by_engine_source(
                conn, source_pattern=source_pattern, country=country
            )
        else:
            results = await queries.get_enrichment_stats_by_engine(conn, country=country)
        return [EnrichmentStats.model_validate(dict(row)) for row in results]
