"""BIG4 Repository - Database operations for BIG4 park imports."""

from typing import List

from loguru import logger

from db.client import get_conn
from db.queries.batch import BATCH_INSERT_HOTELS
from lib.big4.models import Big4Park


class Big4Repo:
    """Database operations for BIG4 holiday park imports."""

    async def upsert_parks(self, parks: List[Big4Park]) -> int:
        """Upsert BIG4 parks into the hotels table.

        Uses BATCH_INSERT_HOTELS with external_id dedup (big4_{slug}).
        Returns number of records processed.
        """
        if not parks:
            return 0

        records = []
        for park in parks:
            records.append((
                park.name,               # $1: name
                "big4_scrape",            # $2: source
                1,                        # $3: status (1 = active)
                park.address,             # $4: address
                park.city,                # $5: city
                park.state,               # $6: state
                "Australia",              # $7: country
                park.phone,               # $8: phone
                "holiday_park",           # $9: category
                park.external_id,         # $10: external_id (big4_{slug})
                "big4",                   # $11: external_id_type
                park.latitude,            # $12: lat
                park.longitude,           # $13: lon
            ))

        async with get_conn() as conn:
            await conn.executemany(BATCH_INSERT_HOTELS, records)

        logger.info(f"Upserted {len(records)} BIG4 parks")
        return len(records)

    async def get_big4_count(self) -> int:
        """Count existing BIG4 parks in the database."""
        async with get_conn() as conn:
            result = await conn.fetchrow(
                "SELECT COUNT(*) as count FROM sadie_gtm.hotels WHERE external_id_type = 'big4'"
            )
            return result["count"] if result else 0

    async def update_park_email(self, external_id: str, email: str) -> None:
        """Update email for a specific BIG4 park."""
        async with get_conn() as conn:
            await conn.execute(
                """
                UPDATE sadie_gtm.hotels
                SET email = $1, updated_at = NOW()
                WHERE external_id = $2 AND external_id_type = 'big4'
                """,
                email, external_id,
            )

    async def update_park_website(self, external_id: str, website: str) -> None:
        """Update website for a specific BIG4 park."""
        async with get_conn() as conn:
            await conn.execute(
                """
                UPDATE sadie_gtm.hotels
                SET website = $1, updated_at = NOW()
                WHERE external_id = $2 AND external_id_type = 'big4'
                """,
                website, external_id,
            )

    async def batch_update_contacts(self, updates: list[dict]) -> int:
        """Batch update email and website for BIG4 parks.

        Args:
            updates: List of dicts with external_id, email, website keys
        """
        if not updates:
            return 0

        external_ids = [u["external_id"] for u in updates]
        emails = [u.get("email") for u in updates]
        websites = [u.get("website") for u in updates]

        async with get_conn() as conn:
            result = await conn.execute(
                """
                UPDATE sadie_gtm.hotels h
                SET
                    email = COALESCE(v.email, h.email),
                    website = COALESCE(v.website, h.website),
                    updated_at = NOW()
                FROM (
                    SELECT * FROM unnest($1::text[], $2::text[], $3::text[])
                    AS t(external_id, email, website)
                ) v
                WHERE h.external_id = v.external_id AND h.external_id_type = 'big4'
                """,
                external_ids, emails, websites,
            )
            return int(result.split()[-1]) if result else 0
