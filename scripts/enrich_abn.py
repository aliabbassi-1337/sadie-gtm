"""Look up all Big4 parks on ABN registry to find entity names + sole trader owners.

Uses lib/owner_discovery/abn_lookup.py to search abr.business.gov.au.
Runs all lookups concurrently with semaphore.

Usage:
    PYTHONPATH=. uv run python3 scripts/enrich_abn.py --dry-run
    PYTHONPATH=. uv run python3 scripts/enrich_abn.py --apply
    PYTHONPATH=. uv run python3 scripts/enrich_abn.py --apply --limit 10
"""

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field

import asyncpg
import httpx
from dotenv import load_dotenv
from loguru import logger

from lib.owner_discovery.abn_lookup import abn_to_decision_makers, AbnEntity

load_dotenv()
_ENV = os.environ

DB_CONFIG = dict(
    host=_ENV['SADIE_DB_HOST'],
    port=int(_ENV.get('SADIE_DB_PORT', '6543')),
    database=_ENV.get('SADIE_DB_NAME', 'postgres'),
    user=_ENV['SADIE_DB_USER'],
    password=_ENV['SADIE_DB_PASSWORD'],
    statement_cache_size=0,
)

BIG4_WHERE = "(h.external_id_type = 'big4' OR h.source LIKE '%::big4%')"


@dataclass
class Result:
    hotel_id: int
    name: str
    entity: AbnEntity | None = None
    dms: list = field(default_factory=list)
    error: str | None = None


async def lookup_one(client, sem, hotel_id, name, state):
    r = Result(hotel_id=hotel_id, name=name)
    async with sem:
        try:
            dms, entity = await abn_to_decision_makers(client, name, state=state)
            r.entity = entity
            r.dms = dms
        except Exception as e:
            r.error = str(e)
    return r


async def main():
    parser = argparse.ArgumentParser(description="ABN lookup for all Big4 parks")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--no-skip", action="store_true", help="Don't skip parks with existing ABN data")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    conn = await asyncpg.connect(**DB_CONFIG)

    rows = await conn.fetch(
        f"SELECT h.id, h.name, h.state FROM sadie_gtm.hotels h WHERE {BIG4_WHERE} ORDER BY h.name"
    )
    parks = [(r["id"], r["name"], r.get("state") or None) for r in rows]
    print(f"Total Big4 parks: {len(parks)}")

    if not args.no_skip:
        existing = await conn.fetch(
            "SELECT DISTINCT hotel_id FROM sadie_gtm.hotel_decision_makers WHERE 'abn_lookup' = ANY(sources)"
        )
        existing_ids = {r["hotel_id"] for r in existing}
        before = len(parks)
        parks = [(hid, name, state) for hid, name, state in parks if hid not in existing_ids]
        print(f"Skipping {before - len(parks)} with existing ABN data, {len(parks)} remaining")

    if args.limit:
        parks = parks[:args.limit]

    if not parks:
        print("Nothing to do!")
        await conn.close()
        return

    print(f"Running {len(parks)} ABN lookups (concurrency={args.concurrency})...")

    sem = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[
            lookup_one(client, sem, hid, name, state)
            for hid, name, state in parks
        ])

    # Results
    entities_found = sum(1 for r in results if r.entity)
    total_dms = sum(len(r.dms) for r in results)
    errors = sum(1 for r in results if r.error)
    inserted = 0
    skipped = 0

    for r in results:
        if r.entity:
            logger.info(f"{r.name} -> {r.entity.entity_name} (ABN={r.entity.abn})")
        if r.error:
            logger.warning(f"{r.name} -> ERROR: {r.error}")

        for dm in r.dms:
            if not dm.full_name or not dm.full_name.strip():
                continue
            logger.info(f"  + {dm.full_name} ({dm.title})")
            if args.apply:
                result = await conn.fetchval(
                    "INSERT INTO sadie_gtm.hotel_decision_makers"
                    " (hotel_id, full_name, title, sources, confidence, raw_source_url, created_at, updated_at)"
                    " SELECT $1, $2, $3, $4, $5, $6, NOW(), NOW()"
                    " WHERE NOT EXISTS ("
                    "   SELECT 1 FROM sadie_gtm.hotel_decision_makers"
                    "   WHERE hotel_id = $1 AND lower(full_name) = lower($2)"
                    " ) RETURNING id",
                    r.hotel_id, dm.full_name, dm.title,
                    dm.sources, dm.confidence, dm.raw_source_url,
                )
                if result:
                    inserted += 1
                else:
                    skipped += 1

    print(f"\n{'='*70}")
    print(f"ABN LOOKUP RESULTS")
    print(f"{'='*70}")
    print(f"Parks searched:     {len(parks)}")
    print(f"Entities found:     {entities_found}")
    print(f"No match:           {len(parks) - entities_found - errors}")
    print(f"Errors:             {errors}")
    print(f"Decision makers:    {total_dms}")
    if args.apply:
        print(f"Inserted:           {inserted}")
        print(f"Skipped (exists):   {skipped}")
    else:
        print("\nDRY RUN — run with --apply to write to DB")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
