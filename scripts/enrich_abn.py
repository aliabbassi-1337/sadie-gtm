"""Look up all Big4 parks on ABN registry to find entity names + sole trader owners.

Uses lib/owner_discovery/abn_lookup.py to search abr.business.gov.au.
Rate limited to ~1 req/s per ABR guidelines.

Usage:
    uv run python3 scripts/enrich_abn.py --dry-run          # preview
    uv run python3 scripts/enrich_abn.py --apply             # write to DB
    uv run python3 scripts/enrich_abn.py --apply --limit 10  # test with 10
"""

import argparse
import asyncio
import sys

import asyncpg
import httpx
from loguru import logger

from lib.owner_discovery.abn_lookup import abn_search_by_name, abn_to_decision_makers, AbnEntity

DB_CONFIG = dict(
    host="aws-1-ap-southeast-1.pooler.supabase.com",
    port=6543, database="postgres",
    user="postgres.yunairadgmaqesxejqap",
    password="SadieGTM321-",
    statement_cache_size=0,
)

BIG4_WHERE = "(h.external_id_type = 'big4' OR h.source LIKE '%::big4%')"

ENTITY_RE = (
    r"(PTY|LTD|LIMITED|LLC|INC|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|"
    r"COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|"
    r"TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT)"
)


async def main():
    parser = argparse.ArgumentParser(description="ABN lookup for all Big4 parks")
    parser.add_argument("--apply", action="store_true", help="Write results to DB")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--limit", type=int, default=0, help="Limit parks to process")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip parks that already have an ABN-sourced DM")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    conn = await asyncpg.connect(**DB_CONFIG)

    # Load all Big4 parks
    rows = await conn.fetch(
        f"SELECT h.id, h.name, h.state_province FROM sadie_gtm.hotels h"
        f" WHERE {BIG4_WHERE} ORDER BY h.name"
    )
    parks = [(r["id"], r["name"], r.get("state_province") or None) for r in rows]
    print(f"Total Big4 parks: {len(parks)}")

    # Skip parks that already have ABN-sourced DMs
    if args.skip_existing:
        existing = await conn.fetch(
            "SELECT DISTINCT hotel_id FROM sadie_gtm.hotel_decision_makers"
            " WHERE 'abn_lookup' = ANY(sources)"
        )
        existing_ids = {r["hotel_id"] for r in existing}
        before = len(parks)
        parks = [(hid, name, state) for hid, name, state in parks if hid not in existing_ids]
        print(f"Skipping {before - len(parks)} parks with existing ABN data, {len(parks)} remaining")

    if args.limit:
        parks = parks[:args.limit]
        print(f"Limited to {len(parks)} parks")

    if not parks:
        print("Nothing to do!")
        await conn.close()
        return

    # Process parks — rate limited (ABR wants ~1 req/s, we do 0.3s between detail fetches
    # which are inside abn_search_by_name, plus we add 0.5s between parks)
    total_entities = 0
    total_dms = 0
    inserted = 0
    skipped = 0
    no_match = 0

    async with httpx.AsyncClient() as client:
        for i, (hotel_id, name, state) in enumerate(parks):
            logger.info(f"[{i+1}/{len(parks)}] Searching ABN: {name}")

            dms, best_entity = await abn_to_decision_makers(client, name, state=state)

            if best_entity:
                total_entities += 1
                logger.info(f"  Entity: {best_entity.entity_name} (ABN={best_entity.abn}, type={best_entity.entity_type})")
                if best_entity.business_names:
                    logger.info(f"  Trading as: {', '.join(best_entity.business_names[:3])}")
            else:
                no_match += 1
                logger.warning(f"  No ABN match for: {name}")

            if dms:
                total_dms += len(dms)
                for dm in dms:
                    logger.info(f"  -> {dm.full_name} ({dm.title}) [conf={dm.confidence}]")

                    if args.apply:
                        # Insert DM, skip if already exists for this hotel
                        result = await conn.fetchval(
                            "INSERT INTO sadie_gtm.hotel_decision_makers"
                            " (hotel_id, full_name, title, sources, confidence, raw_source_url, created_at, updated_at)"
                            " SELECT $1, $2, $3, $4, $5, $6, NOW(), NOW()"
                            " WHERE NOT EXISTS ("
                            "   SELECT 1 FROM sadie_gtm.hotel_decision_makers"
                            "   WHERE hotel_id = $1 AND lower(full_name) = lower($2)"
                            " ) RETURNING id",
                            hotel_id, dm.full_name, dm.title,
                            dm.sources, dm.confidence, dm.raw_source_url,
                        )
                        if result:
                            inserted += 1
                        else:
                            skipped += 1

            # Rate limit between parks
            await asyncio.sleep(0.5)

    print(f"\n{'='*70}")
    print(f"ABN LOOKUP RESULTS")
    print(f"{'='*70}")
    print(f"Parks searched:     {len(parks)}")
    print(f"Entities found:     {total_entities}")
    print(f"No match:           {no_match}")
    print(f"Decision makers:    {total_dms}")
    if args.apply:
        print(f"Inserted:           {inserted}")
        print(f"Skipped (exists):   {skipped}")
    else:
        print(f"\nDRY RUN — run with --apply to write to DB")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
