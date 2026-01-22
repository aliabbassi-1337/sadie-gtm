#!/usr/bin/env python3
"""Clean up corrupted data (articles, job posts, etc.) from the database.

These are URLs that got scraped as hotels but are actually:
- Blog articles
- News stories
- Job postings
- Case studies
- Software comparison pages

Usage:
    uv run python scripts/cleanup_bad_data.py --dry-run
    uv run python scripts/cleanup_bad_data.py --execute
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.client import get_conn
from loguru import logger

# Domains that should never be hotel websites
BAD_DOMAINS = [
    "mews.com",
    "hoteltechreport.com",
    "hotel-online.com",
    "hospitalityleaderonline.com",
    "hospitalitynet.org",
    "hotelmanagement.net",
    "hotelnewsnow.com",
    "skift.com",
    "phocuswire.com",
    "startup.jobs",
    "lever.co",
    "greenhouse.io",
    "g2.com",
    "capterra.com",
    "redawning.com",
]

# URL patterns that indicate bad data
BAD_PATTERNS = [
    "/blog/",
    "/news/",
    "/article/",
    "/customers/",
    "/case-study/",
    "/resources/",
    "/events/",
    "/compare/",
    "/jobs/",
    "/careers/",
    "/matt-talks/",
    "/webinar/",
]


async def find_bad_data() -> list:
    """Find hotels with bad URLs."""
    async with get_conn() as conn:
        # Build WHERE clause for bad domains
        domain_conditions = " OR ".join(
            f"h.website LIKE '%{domain}%'" for domain in BAD_DOMAINS
        )

        # Build WHERE clause for bad patterns
        pattern_conditions = " OR ".join(
            f"h.website LIKE '%{pattern}%'" for pattern in BAD_PATTERNS
        )

        query = f"""
            SELECT h.id, h.name, h.website, h.status
            FROM sadie_gtm.hotels h
            WHERE ({domain_conditions})
               OR ({pattern_conditions})
            ORDER BY h.id
        """

        results = await conn.fetch(query)
        return [dict(r) for r in results]


async def cleanup_bad_data(dry_run: bool = True):
    """Mark bad data as no_booking_engine."""
    bad_hotels = await find_bad_data()

    logger.info(f"Found {len(bad_hotels)} hotels with bad URLs")

    if not bad_hotels:
        return

    # Group by domain/pattern for summary
    by_reason = {}
    for hotel in bad_hotels:
        url = hotel["website"] or ""
        reason = "unknown"
        for domain in BAD_DOMAINS:
            if domain in url:
                reason = domain
                break
        for pattern in BAD_PATTERNS:
            if pattern in url:
                reason = pattern
                break
        by_reason.setdefault(reason, []).append(hotel)

    print("\nBreakdown by reason:")
    for reason, hotels in sorted(by_reason.items(), key=lambda x: -len(x[1])):
        print(f"  {reason}: {len(hotels)}")

    print(f"\nSample bad hotels:")
    for hotel in bad_hotels[:10]:
        print(f"  {hotel['id']}: {hotel['name'][:50]}")
        print(f"       {hotel['website'][:80]}")

    if dry_run:
        print(f"\n[DRY RUN] Would mark {len(bad_hotels)} hotels as no_booking_engine")
        return

    # Actually delete
    hotel_ids = [h["id"] for h in bad_hotels]

    async with get_conn() as conn:
        # Delete HBE records
        result = await conn.execute(
            "DELETE FROM sadie_gtm.hotel_booking_engines WHERE hotel_id = ANY($1)",
            hotel_ids
        )
        deleted_hbe = int(result.split()[-1])
        logger.info(f"Deleted {deleted_hbe} HBE records")

        # Mark hotels as no_booking_engine
        result = await conn.execute(
            "UPDATE sadie_gtm.hotels SET status = -1 WHERE id = ANY($1)",
            hotel_ids
        )
        updated = int(result.split()[-1])
        logger.info(f"Marked {updated} hotels as no_booking_engine")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Clean up bad hotel data")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without executing")
    parser.add_argument("--execute", action="store_true", help="Actually execute cleanup")

    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Must specify --dry-run or --execute")
        sys.exit(1)

    asyncio.run(cleanup_bad_data(dry_run=not args.execute))


if __name__ == "__main__":
    main()
