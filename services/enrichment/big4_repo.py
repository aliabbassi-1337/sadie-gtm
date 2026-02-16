"""BIG4 Repository - Database operations for BIG4 park imports."""

from typing import List, Dict

from loguru import logger

from db.client import queries, get_conn
from db.queries import enrichment_batch as ebatch
from lib.big4.models import Big4Park


class Big4Repo:
    """Database operations for BIG4 holiday park imports."""

    async def upsert_parks(self, parks: List[Big4Park]) -> None:
        """Upsert BIG4 parks with cross-source dedup.

        Single SQL query that:
        1. Normalizes names (strips brand prefixes + property type suffixes)
        2. Matches against existing AU hotels by normalized name + state
        3. For matches: fills empty fields on the existing hotel
        4. For non-matches: inserts as new with external_id_type=big4
        """
        if not parks:
            return

        names = [p.name for p in parks]
        slugs = [p.slug for p in parks]
        phones = [p.phone for p in parks]
        emails = [p.email for p in parks]
        websites = [p.full_url for p in parks]
        addresses = [p.address for p in parks]
        cities = [p.city for p in parks]
        states = [p.state for p in parks]
        postcodes = [p.postcode for p in parks]
        lats = [p.latitude for p in parks]
        lons = [p.longitude for p in parks]

        async with get_conn() as conn:
            await conn.execute(
                ebatch.BATCH_BIG4_UPSERT,
                names, slugs, phones, emails, websites,
                addresses, cities, states, postcodes, lats, lons,
            )

        logger.info(f"Upserted {len(parks)} BIG4 parks")

    async def get_big4_count(self) -> int:
        """Count existing BIG4 parks in the database."""
        async with get_conn() as conn:
            result = await queries.get_big4_count(conn)
            return result["count"] if result else 0

    async def get_dedup_stats(self) -> Dict[str, int]:
        """Get stats about BIG4 parks and their overlap with other sources."""
        async with get_conn() as conn:
            result = await queries.get_big4_dedup_stats(conn)
            return dict(result) if result else {}
