"""Audit Big4 decision makers data.

Usage:
    uv run python3 scripts/_audit_big4_dm.py               # summary only
    uv run python3 scripts/_audit_big4_dm.py --full         # full per-hotel detail
    uv run python3 scripts/_audit_big4_dm.py --bugs         # bug checks only
    uv run python3 scripts/_audit_big4_dm.py --blanks       # parks with zero DMs
"""
import argparse
import asyncio
import os

import asyncpg
from dotenv import load_dotenv

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

ENTITY_RE = (
    r'(PTY|LTD|LIMITED|LLC|INC\b|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|'
    r'COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|'
    r'TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT|PROPRIETARY|COMPANY|'
    r'COMMISSION|FOUNDATION|TRADING|NOMINEES|SUPERANNUATION|ENTERPRISES)'
)


async def print_summary(conn):
    """Print high-level stats."""
    total = await conn.fetchval(
        "SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        " WHERE " + BIG4_WHERE
    )
    with_dm = await conn.fetchval(
        "SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        " JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        " WHERE " + BIG4_WHERE
    )
    with_real = await conn.fetchval(
        "SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        " JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        " WHERE " + BIG4_WHERE + " AND dm.full_name !~* $1",
        ENTITY_RE,
    )
    total_people = await conn.fetchval(
        "SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE + " AND dm.full_name !~* $1",
        ENTITY_RE,
    )
    total_dms = await conn.fetchval(
        "SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE
    )
    blanks = await conn.fetchval(
        "SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        " WHERE " + BIG4_WHERE
        + " AND h.id NOT IN (SELECT dm.hotel_id FROM sadie_gtm.hotel_decision_makers dm)"
    )
    entity_only = await conn.fetchval(
        "SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        " JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        " WHERE " + BIG4_WHERE
        + " AND h.id NOT IN ("
        "   SELECT dm2.hotel_id FROM sadie_gtm.hotel_decision_makers dm2"
        "   WHERE dm2.full_name !~* $1"
        " )",
        ENTITY_RE,
    )
    # Source breakdown
    sources = await conn.fetch(
        "SELECT unnest(dm.sources) AS src, COUNT(*) AS cnt"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE
        + " GROUP BY src ORDER BY cnt DESC"
    )

    print(f"{'='*60}")
    print(f"BIG4 NETWORK ENRICHMENT SUMMARY")
    print(f"{'='*60}")
    print(f"Total Big4 parks:           {total}")
    print(f"Parks with any DM:          {with_dm} ({100*with_dm//total}%)")
    print(f"Parks with real people:      {with_real} ({100*with_real//total}%)")
    print(f"Parks entity-only (no ppl):  {entity_only}")
    print(f"Parks with ZERO DMs:         {blanks}")
    print(f"Total DM rows:               {total_dms}")
    print(f"Total real people DMs:       {total_people}")
    print(f"\nDM Source Breakdown:")
    for r in sources:
        print(f"  {r['src']:<30} {r['cnt']:>5}")


async def print_blanks(conn):
    """Print parks with zero DMs."""
    blanks = await conn.fetch(
        "SELECT h.id, h.name, h.website FROM sadie_gtm.hotels h"
        " WHERE " + BIG4_WHERE
        + " AND h.id NOT IN (SELECT dm.hotel_id FROM sadie_gtm.hotel_decision_makers dm)"
        " ORDER BY h.name"
    )
    print(f"\nParks with ZERO DMs: {len(blanks)}")
    for r in blanks:
        print(f"  {r['id']:>6}  {r['name'][:55]:<55}  {r['website'] or '(no website)'}")


async def print_full(conn):
    """Print full per-hotel detail."""
    rows = await conn.fetch(
        "SELECT"
        "  h.id AS hotel_id, h.name AS hotel_name, h.website,"
        "  COUNT(dm.*) AS contact_count,"
        "  string_agg(dm.full_name, ', ' ORDER BY dm.confidence DESC) AS contacts,"
        "  string_agg(dm.title, ', ' ORDER BY dm.confidence DESC) AS titles,"
        "  string_agg(dm.email, ', ' ORDER BY dm.confidence DESC) AS emails,"
        "  string_agg(CASE WHEN dm.email_verified THEN 'Y' ELSE 'N' END, ', ' ORDER BY dm.confidence DESC) AS verified,"
        "  string_agg(dm.phone, ', ' ORDER BY dm.confidence DESC) AS phones,"
        "  MAX(dm.confidence) AS best_confidence,"
        "  string_agg(array_to_string(dm.sources, '+'), ', ' ORDER BY dm.confidence DESC) AS sources"
        " FROM sadie_gtm.hotels h"
        " JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        " WHERE " + BIG4_WHERE
        + " GROUP BY h.id, h.name, h.website"
        " ORDER BY contact_count DESC"
    )
    print(f"Hotels with decision makers: {len(rows)}\n")
    for r in rows:
        print(f"{'='*70}")
        print(f"Hotel: {r['hotel_name']} (id={r['hotel_id']})")
        print(f"Website: {r['website']}")
        print(f"Contacts ({r['contact_count']}): {r['contacts']}")
        print(f"Titles: {r['titles']}")
        print(f"Emails: {r['emails']}")
        print(f"Verified: {r['verified']}")
        print(f"Phones: {r['phones']}")
        print(f"Confidence: {r['best_confidence']}")
        print(f"Sources: {r['sources']}")


async def print_bugs(conn):
    """Print bug checks."""
    print(f"\n{'='*70}")
    print("BUG AUDIT")
    print(f"{'='*70}")

    first_only = await conn.fetch(
        "SELECT dm.id, dm.full_name, dm.title, h.name AS hotel_name, dm.hotel_id"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE + " AND dm.full_name NOT LIKE '%% %%'"
    )
    print(f"\n1. FIRST NAME ONLY (no surname): {len(first_only)}")
    for r in first_only:
        print(f"   dm.id={r['id']} | '{r['full_name']}' ({r['title']}) -> {r['hotel_name']}")

    dupes = await conn.fetch(
        "SELECT dm.hotel_id, dm.full_name, COUNT(*) as cnt, h.name AS hotel_name"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE
        + " GROUP BY dm.hotel_id, dm.full_name, h.name"
        " HAVING COUNT(*) > 1"
    )
    print(f"\n2. DUPLICATE NAMES (same hotel): {len(dupes)}")
    for r in dupes:
        print(f"   '{r['full_name']}' x{r['cnt']} -> {r['hotel_name']} (hotel_id={r['hotel_id']})")

    cross_dupes = await conn.fetch(
        "SELECT dm.full_name, COUNT(DISTINCT dm.hotel_id) as hotel_count,"
        "  string_agg(DISTINCT h.name, ' | ') as hotel_names"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE
        + " GROUP BY dm.full_name"
        " HAVING COUNT(DISTINCT dm.hotel_id) > 1"
    )
    print(f"\n3. SAME NAME ACROSS MULTIPLE HOTEL IDs: {len(cross_dupes)}")
    for r in cross_dupes:
        print(f"   '{r['full_name']}' in {r['hotel_count']} hotels: {r['hotel_names']}")

    suspicious = await conn.fetch(
        "SELECT dm.id, dm.full_name, dm.title, h.name AS hotel_name"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE
        + " AND (LENGTH(dm.full_name) < 4 OR dm.full_name ~ '^[A-Z][a-z]{1,2}$')"
    )
    print(f"\n4. SUSPICIOUS SHORT NAMES: {len(suspicious)}")
    for r in suspicious:
        print(f"   dm.id={r['id']} | '{r['full_name']}' ({r['title']}) -> {r['hotel_name']}")

    corp = await conn.fetch(
        "SELECT dm.id, dm.full_name, dm.title, h.name AS hotel_name"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE
        + " AND dm.title ILIKE ANY(ARRAY['%CEO%', '%CFO%', '%Chief%', '%Head of%', '%New Zealand%', '%Marketing%'])"
    )
    print(f"\n5. CORPORATE/HQ ROLES (may not be park-specific): {len(corp)}")
    for r in corp:
        print(f"   dm.id={r['id']} | '{r['full_name']}' ({r['title']}) -> {r['hotel_name']}")

    dup_hotels = await conn.fetch(
        "SELECT LOWER(TRIM(h.name)) as norm_name, COUNT(*) as cnt,"
        "  string_agg(h.id::text, ', ') as ids,"
        "  COUNT(DISTINCT dm.id) as total_dms"
        " FROM sadie_gtm.hotels h"
        " LEFT JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        " WHERE " + BIG4_WHERE
        + " GROUP BY LOWER(TRIM(h.name))"
        " HAVING COUNT(*) > 1 AND COUNT(DISTINCT dm.id) > 0"
        " ORDER BY cnt DESC"
    )
    print(f"\n6. DUPLICATE HOTEL ENTRIES with DMs: {len(dup_hotels)}")
    for r in dup_hotels:
        print(f"   '{r['norm_name']}' x{r['cnt']} (ids: {r['ids']}) -> {r['total_dms']} DMs")


async def main():
    parser = argparse.ArgumentParser(description="Audit Big4 decision makers data")
    parser.add_argument('--full', action='store_true', help='Show full per-hotel detail')
    parser.add_argument('--bugs', action='store_true', help='Show bug checks only')
    parser.add_argument('--blanks', action='store_true', help='Show parks with zero DMs')
    args = parser.parse_args()

    conn = await asyncpg.connect(**DB_CONFIG)

    if args.full:
        await print_full(conn)
    elif args.bugs:
        await print_bugs(conn)
    elif args.blanks:
        await print_blanks(conn)
    else:
        await print_summary(conn)

    await conn.close()

asyncio.run(main())
