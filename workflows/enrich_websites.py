#!/usr/bin/env python3
"""
Website Enrichment Workflow - Find websites for hotels missing them.

Queries hotels without websites from the database and uses Serper
to search for their official websites.

Usage:
    # Enrich hotels without websites (limit 100)
    uv run python -m workflows.enrich_websites --limit 100

    # Enrich specific source (e.g., DBPR hotels)
    uv run python -m workflows.enrich_websites --source dbpr --limit 500

    # Dry run (search but don't update DB)
    uv run python -m workflows.enrich_websites --limit 50 --dry-run

    # Filter to specific state
    uv run python -m workflows.enrich_websites --state FL --limit 1000
"""

import argparse
import asyncio
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.leadgen.website_enricher import WebsiteEnricher
from db.client import init_db, get_conn


async def get_hotels_without_websites(
    limit: int = 100,
    source_filter: str = None,
    state_filter: str = None,
) -> list:
    """Query hotels that need website enrichment."""
    pool = await get_conn()

    query = """
        SELECT id, name, city, state, address
        FROM sadie_gtm.hotels
        WHERE website IS NULL
        AND city IS NOT NULL
        AND name IS NOT NULL
    """
    params = []

    if source_filter:
        query += " AND source LIKE $1"
        params.append(f"%{source_filter}%")

    if state_filter:
        idx = len(params) + 1
        query += f" AND state = ${idx}"
        params.append(state_filter)

    query += f" ORDER BY created_at DESC LIMIT ${len(params) + 1}"
    params.append(limit)

    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]


async def update_hotel_website(hotel_id: int, website: str):
    """Update hotel with found website."""
    pool = await get_conn()
    await pool.execute(
        "UPDATE sadie_gtm.hotels SET website = $1 WHERE id = $2",
        website, hotel_id
    )


async def main():
    parser = argparse.ArgumentParser(
        description="Enrich hotels with websites via Serper search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=100,
        help="Max hotels to enrich (default: 100)",
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Filter to hotels from specific source (e.g., 'dbpr')",
    )
    parser.add_argument(
        "--state",
        type=str,
        help="Filter to specific state (e.g., 'FL')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search but don't update database",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay between API calls in seconds (default: 0.1)",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Check API key
    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        logger.error("SERPER_API_KEY environment variable not set")
        sys.exit(1)

    # Initialize database
    await init_db()

    # Get hotels without websites
    logger.info(f"Querying hotels without websites (limit: {args.limit})...")
    if args.source:
        logger.info(f"  Filtering to source: {args.source}")
    if args.state:
        logger.info(f"  Filtering to state: {args.state}")

    hotels = await get_hotels_without_websites(
        limit=args.limit,
        source_filter=args.source,
        state_filter=args.state,
    )

    if not hotels:
        logger.info("No hotels found needing website enrichment")
        return

    logger.info(f"Found {len(hotels)} hotels to enrich")

    # Enrich with websites
    enricher = WebsiteEnricher(api_key=api_key, delay_between_requests=args.delay)

    found = 0
    not_found = 0
    errors = 0

    for i, hotel in enumerate(hotels):
        if (i + 1) % 50 == 0:
            logger.info(f"  Progress: {i + 1}/{len(hotels)} ({found} found)")

        result = await enricher.find_website(
            name=hotel["name"],
            city=hotel["city"],
            state=hotel.get("state", "FL"),
        )

        if result.website:
            found += 1
            logger.debug(f"  Found: {hotel['name']} -> {result.website}")

            if not args.dry_run:
                await update_hotel_website(hotel["id"], result.website)
        elif result.error == "no_match":
            not_found += 1
        else:
            errors += 1
            logger.debug(f"  Error for {hotel['name']}: {result.error}")

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Website Enrichment Complete")
    logger.info("=" * 60)
    logger.info(f"Hotels processed: {len(hotels)}")
    logger.info(f"Websites found: {found}")
    logger.info(f"Not found: {not_found}")
    logger.info(f"Errors: {errors}")
    logger.info(f"API calls: {len(hotels)}")
    logger.info(f"Estimated cost: ${len(hotels) * 0.001:.2f}")

    if args.dry_run:
        logger.info("")
        logger.info("(Dry run - no database changes made)")


if __name__ == "__main__":
    asyncio.run(main())
