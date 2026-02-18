"""Insert Big4 crawl results into database.

Reads the JSON output from crawl_big4_parks.py and:
1. Inserts new decision_makers (owner/manager names)
2. Updates hotel phone_website if we found new phones
3. Updates hotel email if missing and we found one

Usage:
    uv run python3 scripts/insert_big4_crawl_results.py --dry-run
    uv run python3 scripts/insert_big4_crawl_results.py --apply
"""

import argparse
import asyncio
import json
import sys

import asyncpg
from loguru import logger

DB_CONFIG = dict(
    host='aws-1-ap-southeast-1.pooler.supabase.com',
    port=6543, database='postgres',
    user='postgres.yunairadgmaqesxejqap',
    password='SadieGTM321-',
    statement_cache_size=0,
)

RESULTS_FILE = '/tmp/big4_crawl_results.json'


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='Actually write to DB')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes only')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    if not args.apply and not args.dry_run:
        print("Must specify --dry-run or --apply")
        sys.exit(1)

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    with open(RESULTS_FILE) as f:
        results = json.load(f)

    logger.info(f"Loaded {len(results)} parks with new data")

    conn = await asyncpg.connect(**DB_CONFIG)

    dm_inserts = 0
    dm_skipped = 0
    phone_updates = 0
    email_updates = 0

    for park in results:
        hotel_id = park['hotel_id']
        hotel_name = park['hotel_name']
        website = park['website']

        # 1. Insert decision makers
        for owner in park.get('owner_names', []):
            name = owner.get('name', '').strip()
            title = owner.get('title', 'Owner').strip()

            if not name or len(name) < 3:
                continue
            # Must have first + last name
            if ' ' not in name:
                logger.debug(f"  Skip first-name-only: {name!r} for {hotel_name}")
                continue
            # Reject business/place name patterns
            import re
            if re.search(r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism|beach|river)', name):
                logger.debug(f"  Skip business/place name: {name!r} for {hotel_name}")
                continue

            # Check for existing
            existing = await conn.fetchval(
                "SELECT id FROM sadie_gtm.hotel_decision_makers "
                "WHERE hotel_id = $1 AND LOWER(full_name) = LOWER($2)",
                hotel_id, name,
            )

            if existing:
                logger.debug(f"  Skip existing DM: {name} for {hotel_name}")
                dm_skipped += 1
                continue

            if args.apply:
                await conn.execute(
                    """INSERT INTO sadie_gtm.hotel_decision_makers
                       (hotel_id, full_name, title, sources, confidence, raw_source_url)
                       VALUES ($1, $2, $3, $4, $5, $6)""",
                    hotel_id, name, title,
                    ['crawl4ai_llm'],
                    0.65,
                    website,
                )
            logger.info(f"  + DM: {name} ({title}) -> {hotel_name}")
            dm_inserts += 1

        # 2. Update phone if hotel has none and we found one
        new_phones = park.get('new_phones', [])
        if new_phones:
            current = await conn.fetchrow(
                "SELECT phone_google, phone_website FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id,
            )
            if current and not current['phone_website'] and not current['phone_google']:
                phone = new_phones[0]  # Use first found
                if args.apply:
                    await conn.execute(
                        "UPDATE sadie_gtm.hotels SET phone_website = $1, updated_at = NOW() WHERE id = $2",
                        phone, hotel_id,
                    )
                logger.info(f"  + Phone: {phone} -> {hotel_name}")
                phone_updates += 1

        # 3. Update email if hotel has none and we found personal emails
        new_emails = park.get('new_emails', [])
        if new_emails:
            current_email = await conn.fetchval(
                "SELECT email FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id,
            )
            if not current_email:
                # Pick the most specific email (not marketing@, not example@)
                best = None
                for e in new_emails:
                    if 'example' in e.lower():
                        continue
                    if 'marketing' in e.lower() or 'corp' in e.lower():
                        if not best:
                            best = e
                        continue
                    best = e
                    break

                if best:
                    if args.apply:
                        await conn.execute(
                            "UPDATE sadie_gtm.hotels SET email = $1, updated_at = NOW() WHERE id = $2",
                            best, hotel_id,
                        )
                    logger.info(f"  + Email: {best} -> {hotel_name}")
                    email_updates += 1

    await conn.close()

    print(f"\n{'='*50}")
    print(f"{'DRY RUN' if args.dry_run else 'APPLIED'}")
    print(f"{'='*50}")
    print(f"Decision makers inserted:  {dm_inserts}")
    print(f"Decision makers skipped:   {dm_skipped} (already exist)")
    print(f"Phone numbers updated:     {phone_updates}")
    print(f"Emails updated:            {email_updates}")

    if args.dry_run:
        print(f"\nRun with --apply to write to DB")


if __name__ == '__main__':
    asyncio.run(main())
