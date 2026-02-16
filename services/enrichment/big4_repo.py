"""BIG4 Repository - Database operations for BIG4 park imports."""

import re
from typing import List, Dict, Tuple, Optional

from loguru import logger

from db.client import queries, get_conn
from db.queries.batch import BATCH_UPSERT_BIG4_PARK
from db.queries import enrichment_batch as ebatch
from lib.big4.models import Big4Park


# Prefixes/suffixes to strip when matching names across sources
_STRIP_PREFIXES = [
    "big4 ", "big 4 ", "nrma ", "ingenia holidays ",
    "tasman holiday parks ", "tasman holiday parks - ",
    "breeze holiday parks - ", "holiday haven ",
]
_STRIP_SUFFIXES = [
    " holiday park", " caravan park", " tourist park",
    " holiday village", " holiday resort", " camping ground",
    " glamping retreat", " lifestyle park",
]


def _normalize_name(name: str) -> str:
    """Normalize park name for fuzzy matching across sources."""
    if not name:
        return ""
    n = name.strip().lower()
    for prefix in _STRIP_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):]
    for suffix in _STRIP_SUFFIXES:
        if n.endswith(suffix):
            n = n[:-len(suffix)]
    # Collapse whitespace and strip punctuation
    n = re.sub(r'[^\w\s]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


class Big4Repo:
    """Database operations for BIG4 holiday park imports."""

    async def _load_existing_au_hotels(self) -> Dict[str, List[dict]]:
        """Load existing Australian hotels keyed by normalized name + state.

        Returns dict mapping "normalized_name|state" to list of hotel records.
        """
        async with get_conn() as conn:
            rows = await queries.get_existing_au_hotels(conn)

        index: Dict[str, List[dict]] = {}
        for row in rows:
            record = dict(row)
            key = self._match_key(record["name"], record["state"])
            if key:
                index.setdefault(key, []).append(record)
        return index

    @staticmethod
    def _match_key(name: Optional[str], state: Optional[str]) -> Optional[str]:
        """Build a match key from name + state."""
        norm = _normalize_name(name or "")
        if not norm:
            return None
        st = (state or "").strip().upper()
        return f"{norm}|{st}"

    async def upsert_parks(self, parks: List[Big4Park]) -> int:
        """Upsert BIG4 parks into the hotels table with cross-source dedup.

        1. Loads all existing Australian hotels
        2. Matches BIG4 parks by normalized name + state
        3. For matches: enriches existing hotel (fills empty fields)
        4. For non-matches: inserts as new with external_id_type=big4

        Returns number of new parks inserted.
        """
        if not parks:
            return 0

        existing = await self._load_existing_au_hotels()
        logger.info(f"Loaded {sum(len(v) for v in existing.values())} existing AU hotels for dedup")

        to_insert: List[Big4Park] = []
        to_update: List[Tuple[int, Big4Park]] = []  # (existing_hotel_id, park)

        for park in parks:
            key = self._match_key(park.name, park.state)
            matches = existing.get(key, []) if key else []

            if matches:
                # Take the first match (usually only one)
                match = matches[0]
                to_update.append((match["id"], park))
                logger.debug(
                    f"DEDUP: '{park.name}' matched existing #{match['id']} "
                    f"'{match['name']}' ({match.get('external_id_type', '?')})"
                )
            else:
                to_insert.append(park)

        # --- Batch insert new parks (fill-empty-only on conflict) ---
        if to_insert:
            records = []
            for park in to_insert:
                records.append((
                    park.name,
                    "big4_scrape",
                    1,
                    park.address,
                    park.city,
                    park.state,
                    "Australia",
                    park.phone,
                    "holiday_park",
                    park.external_id,
                    "big4",
                    park.latitude,
                    park.longitude,
                ))
            async with get_conn() as conn:
                await conn.executemany(BATCH_UPSERT_BIG4_PARK, records)
            logger.info(f"Inserted {len(records)} new BIG4 parks")

        # --- Batch update matched parks (fill empty fields only) ---
        if to_update:
            updated = await self._batch_enrich_existing(to_update)
            logger.info(f"Enriched {updated} existing hotels with BIG4 data")

        logger.info(
            f"BIG4 upsert: {len(to_insert)} new, "
            f"{len(to_update)} matched existing, "
            f"{len(parks)} total"
        )
        return len(to_insert)

    async def _batch_enrich_existing(
        self,
        updates: List[Tuple[int, Big4Park]],
    ) -> int:
        """Enrich existing hotels with BIG4 data (fill empty fields only)."""
        if not updates:
            return 0

        hotel_ids = [u[0] for u in updates]
        emails = [u[1].email for u in updates]
        phones = [u[1].phone for u in updates]
        websites = [u[1].full_url for u in updates]
        addresses = [u[1].address for u in updates]
        cities = [u[1].city for u in updates]
        latitudes = [u[1].latitude for u in updates]
        longitudes = [u[1].longitude for u in updates]

        async with get_conn() as conn:
            result = await conn.execute(
                ebatch.BATCH_ENRICH_BIG4_EXISTING,
                hotel_ids, emails, phones, websites,
                addresses, cities, latitudes, longitudes,
            )
            return int(result.split()[-1]) if result else 0

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
