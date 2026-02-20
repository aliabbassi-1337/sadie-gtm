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
import os
import sys

import asyncpg
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
_ENV = os.environ

DB_CONFIG = dict(
    host=_ENV.get('SADIE_DB_HOST', 'aws-1-ap-southeast-1.pooler.supabase.com'),
    port=int(_ENV.get('SADIE_DB_PORT', '6543')),
    database=_ENV.get('SADIE_DB_NAME', 'postgres'),
    user=_ENV.get('SADIE_DB_USER', 'postgres.yunairadgmaqesxejqap'),
    password=_ENV.get('SADIE_DB_PASSWORD', ''),
    statement_cache_size=0,
)

RESULTS_FILE = '/tmp/big4_crawl_results.json'


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='Actually write to DB')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes only')
    parser.add_argument('--input', default=RESULTS_FILE, help='Path to crawl results JSON')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    if not args.apply and not args.dry_run:
        print("Must specify --dry-run or --apply")
        sys.exit(1)

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    with open(args.input) as f:
        results = json.load(f)

    logger.info(f"Loaded {len(results)} parks with new data")

    conn = await asyncpg.connect(**DB_CONFIG)

    import re
    import json as json_mod

    BUSINESS_RE = re.compile(r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism|beach|river)')

    # Build batch arrays for DMs
    dm_ids, dm_names, dm_titles, dm_emails = [], [], [], []
    dm_verified, dm_phones, dm_sources_json, dm_conf, dm_urls = [], [], [], [], []
    seen = set()

    # Build batch arrays for phone/email updates
    phone_hotel_ids, phone_vals = [], []
    email_hotel_ids, email_vals = [], []

    for park in results:
        hotel_id = park['hotel_id']
        website = park['website']

        for owner in park.get('owner_names', []):
            name = owner.get('name', '').strip()
            title = owner.get('title', 'Owner').strip()
            if not name or len(name) < 3 or ' ' not in name:
                continue
            if BUSINESS_RE.search(name):
                continue
            key = (hotel_id, name.lower(), title.lower())
            if key in seen:
                continue
            seen.add(key)
            dm_ids.append(hotel_id)
            dm_names.append(name)
            dm_titles.append(title)
            dm_emails.append(None)
            dm_verified.append(False)
            dm_phones.append(None)
            dm_sources_json.append(json_mod.dumps(["crawl4ai_llm"]))
            dm_conf.append(0.65)
            dm_urls.append(website)

        new_phones = park.get('new_phones', [])
        if new_phones:
            phone_hotel_ids.append(hotel_id)
            phone_vals.append(new_phones[0])

        new_emails = park.get('new_emails', [])
        if new_emails:
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
                email_hotel_ids.append(hotel_id)
                email_vals.append(best)

    # Log preview
    for i in range(len(dm_ids)):
        logger.info(f"  + DM: {dm_names[i]} ({dm_titles[i]}) -> hotel_id={dm_ids[i]}")
    for hid, phone in zip(phone_hotel_ids, phone_vals):
        logger.info(f"  + Phone: {phone} -> hotel_id={hid}")
    for hid, email in zip(email_hotel_ids, email_vals):
        logger.info(f"  + Email: {email} -> hotel_id={hid}")

    if args.apply:
        # Batch insert DMs
        if dm_ids:
            await conn.execute(
                "INSERT INTO sadie_gtm.hotel_decision_makers"
                "  (hotel_id, full_name, title, email, email_verified, phone,"
                "   sources, confidence, raw_source_url)"
                " SELECT"
                "  v.hotel_id, v.full_name, v.title, v.email, v.email_verified, v.phone,"
                "  ARRAY(SELECT jsonb_array_elements_text(v.sources_json)),"
                "  v.confidence, v.raw_source_url"
                " FROM unnest("
                "  $1::int[], $2::text[], $3::text[], $4::text[],"
                "  $5::bool[], $6::text[], $7::jsonb[], $8::float4[], $9::text[]"
                " ) AS v(hotel_id, full_name, title, email, email_verified, phone,"
                "        sources_json, confidence, raw_source_url)"
                " ON CONFLICT (hotel_id, full_name, title) DO UPDATE"
                " SET email = COALESCE(NULLIF(EXCLUDED.email, ''), sadie_gtm.hotel_decision_makers.email),"
                "     phone = COALESCE(NULLIF(EXCLUDED.phone, ''), sadie_gtm.hotel_decision_makers.phone),"
                "     sources = (SELECT array_agg(DISTINCT s) FROM unnest(array_cat(sadie_gtm.hotel_decision_makers.sources, EXCLUDED.sources)) s),"
                "     confidence = GREATEST(EXCLUDED.confidence, sadie_gtm.hotel_decision_makers.confidence),"
                "     updated_at = NOW()",
                dm_ids, dm_names, dm_titles, dm_emails,
                dm_verified, dm_phones, dm_sources_json, dm_conf, dm_urls,
            )

        # Batch update phones (only where hotel has no phone)
        if phone_hotel_ids:
            await conn.execute(
                "UPDATE sadie_gtm.hotels h SET phone_website = v.phone, updated_at = NOW()"
                " FROM unnest($1::int[], $2::text[]) AS v(id, phone)"
                " WHERE h.id = v.id"
                "   AND (h.phone_website IS NULL OR h.phone_website = '')"
                "   AND (h.phone_google IS NULL OR h.phone_google = '')",
                phone_hotel_ids, phone_vals,
            )

        # Batch update emails (only where hotel has no email)
        if email_hotel_ids:
            await conn.execute(
                "UPDATE sadie_gtm.hotels h SET email = v.email, updated_at = NOW()"
                " FROM unnest($1::int[], $2::text[]) AS v(id, email)"
                " WHERE h.id = v.id AND (h.email IS NULL OR h.email = '')",
                email_hotel_ids, email_vals,
            )

    await conn.close()

    print(f"\n{'='*50}")
    print(f"{'DRY RUN' if args.dry_run else 'APPLIED'}")
    print(f"{'='*50}")
    print(f"Decision makers:  {len(dm_ids)}")
    print(f"Phone updates:    {len(phone_hotel_ids)}")
    print(f"Email updates:    {len(email_hotel_ids)}")

    if args.dry_run:
        print(f"\nRun with --apply to write to DB")


if __name__ == '__main__':
    asyncio.run(main())
