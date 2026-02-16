"""BIG4 Repository - Database operations for BIG4 park imports."""

import re
from typing import List, Dict, Set, Tuple, Optional

from loguru import logger

from db.client import get_conn
from lib.big4.models import Big4Park


# Fill-empty-only upsert: on conflict, only fills NULL/empty fields — never overwrites.
# Params: (name, source, status, address, city, state, country, phone, category,
#          external_id, external_id_type, lat, lon)
_UPSERT_BIG4_PARK = """
INSERT INTO sadie_gtm.hotels (
    name, source, status, address, city, state, country,
    phone_google, category, external_id, external_id_type, location
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    CASE WHEN $12::float8 IS NOT NULL AND $13::float8 IS NOT NULL
         THEN ST_SetSRID(ST_MakePoint($13::float8, $12::float8), 4326)::geography
         ELSE NULL END
)
ON CONFLICT (external_id_type, external_id) WHERE external_id IS NOT NULL
DO UPDATE SET
    address = CASE WHEN (sadie_gtm.hotels.address IS NULL OR sadie_gtm.hotels.address = '')
                   THEN COALESCE(EXCLUDED.address, sadie_gtm.hotels.address)
                   ELSE sadie_gtm.hotels.address END,
    city = CASE WHEN (sadie_gtm.hotels.city IS NULL OR sadie_gtm.hotels.city = '')
                THEN COALESCE(EXCLUDED.city, sadie_gtm.hotels.city)
                ELSE sadie_gtm.hotels.city END,
    phone_google = CASE WHEN (sadie_gtm.hotels.phone_google IS NULL OR sadie_gtm.hotels.phone_google = '')
                        THEN COALESCE(EXCLUDED.phone_google, sadie_gtm.hotels.phone_google)
                        ELSE sadie_gtm.hotels.phone_google END,
    category = COALESCE(sadie_gtm.hotels.category, EXCLUDED.category),
    location = COALESCE(sadie_gtm.hotels.location, EXCLUDED.location)
"""


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
            rows = await conn.fetch(
                """
                SELECT id, name, state, city, email, website, address,
                       external_id, external_id_type
                FROM sadie_gtm.hotels
                WHERE country IN ('Australia', 'AU')
                  AND status >= 0
                """
            )

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
                await conn.executemany(_UPSERT_BIG4_PARK, records)
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
                """
                UPDATE sadie_gtm.hotels h
                SET
                    email = CASE WHEN (h.email IS NULL OR h.email = '') AND v.email IS NOT NULL AND v.email != ''
                                 THEN v.email ELSE h.email END,
                    phone_website = CASE WHEN (h.phone_website IS NULL OR h.phone_website = '') AND v.phone IS NOT NULL AND v.phone != ''
                                        THEN v.phone ELSE h.phone_website END,
                    website = CASE WHEN (h.website IS NULL OR h.website = '') AND v.website IS NOT NULL AND v.website != ''
                                   THEN v.website ELSE h.website END,
                    address = CASE WHEN (h.address IS NULL OR h.address = '') AND v.address IS NOT NULL AND v.address != ''
                                   THEN v.address ELSE h.address END,
                    city = CASE WHEN (h.city IS NULL OR h.city = '') AND v.city IS NOT NULL AND v.city != ''
                                THEN v.city ELSE h.city END,
                    location = CASE
                        WHEN h.location IS NULL AND v.latitude IS NOT NULL AND v.longitude IS NOT NULL
                        THEN ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography
                        ELSE h.location
                    END,
                    updated_at = NOW()
                FROM (
                    SELECT * FROM unnest(
                        $1::int[], $2::text[], $3::text[], $4::text[],
                        $5::text[], $6::text[], $7::float[], $8::float[]
                    ) AS t(hotel_id, email, phone, website, address, city, latitude, longitude)
                ) v
                WHERE h.id = v.hotel_id
                """,
                hotel_ids, emails, phones, websites,
                addresses, cities, latitudes, longitudes,
            )
            return int(result.split()[-1]) if result else 0

    async def get_big4_count(self) -> int:
        """Count existing BIG4 parks in the database."""
        async with get_conn() as conn:
            result = await conn.fetchrow(
                "SELECT COUNT(*) as count FROM sadie_gtm.hotels WHERE external_id_type = 'big4'"
            )
            return result["count"] if result else 0

    async def get_dedup_stats(self) -> Dict[str, int]:
        """Get stats about BIG4 parks and their overlap with other sources."""
        async with get_conn() as conn:
            result = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE external_id_type = 'big4') AS big4_only,
                    COUNT(*) FILTER (WHERE external_id_type != 'big4'
                                     AND country IN ('Australia', 'AU')) AS other_au,
                    COUNT(*) FILTER (WHERE email IS NOT NULL AND email != ''
                                     AND country IN ('Australia', 'AU')) AS with_email
                FROM sadie_gtm.hotels
                WHERE status >= 0
                """
            )
            return dict(result) if result else {}
