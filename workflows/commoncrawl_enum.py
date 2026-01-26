#!/usr/bin/env python3
"""
Common Crawl Enumeration Workflow - Discover Cloudbeds hotels from archived web data.

Common Crawl indexes billions of web pages monthly. This workflow queries their
CDX API to find all indexed Cloudbeds reservation URLs, extracts hotel details,
and saves them to the database.

This finds MORE hotels than the sitemap because:
1. Historical data - hotels removed from sitemap still in archives
2. Direct crawled pages - not dependent on sitemap discovery
3. Multiple monthly snapshots across years

Usage:
    # Quick test with 3 recent indices
    uv run python -m workflows.commoncrawl_enum --max-indices 3

    # Query 2024 indices only
    uv run python -m workflows.commoncrawl_enum --year 2024

    # Full scan (all ~350 indices, takes ~30 min for slugs, longer with details)
    uv run python -m workflows.commoncrawl_enum --output data/cloudbeds_commoncrawl.json

    # Just get slugs without fetching hotel details
    uv run python -m workflows.commoncrawl_enum --slugs-only

    # Save to database
    uv run python -m workflows.commoncrawl_enum --max-indices 10 --save-db
"""

import argparse
import asyncio
import json
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.leadgen.service import Service


async def main():
    parser = argparse.ArgumentParser(
        description="Enumerate Cloudbeds hotels from Common Crawl archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Common Crawl has indexed billions of web pages since 2008.
This workflow finds Cloudbeds booking URLs from their archives.

Examples:
    # Quick test (3 indices, ~10 sec)
    uv run python -m workflows.commoncrawl_enum --max-indices 3

    # Get all slugs from 2024 (no scraping Cloudbeds)
    uv run python -m workflows.commoncrawl_enum --year 2024 --slugs-only

    # Full scan with hotel details and save to DB
    uv run python -m workflows.commoncrawl_enum --save-db
"""
    )

    parser.add_argument(
        "--max-indices",
        type=int,
        help="Limit number of Common Crawl indices to query",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Only query indices from specific year (e.g., 2024)",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=5,
        help="Concurrent requests (default: 5)",
    )
    parser.add_argument(
        "--slugs-only",
        action="store_true",
        help="Only get slugs, don't fetch hotel details",
    )
    parser.add_argument(
        "--scrape-cloudbeds",
        action="store_true",
        help="Fetch details by scraping Cloudbeds (slower, rate limited). "
             "Default is to use Common Crawl archives (faster, no limits).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--save-db",
        action="store_true",
        help="Save results to database as hotel leads",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    logger.info("Common Crawl Cloudbeds Enumeration")
    logger.info("=" * 60)
    if args.max_indices:
        logger.info(f"Max indices: {args.max_indices}")
    if args.year:
        logger.info(f"Year filter: {args.year}")
    logger.info(f"Fetch details: {not args.slugs_only}")
    if not args.slugs_only:
        if args.scrape_cloudbeds:
            logger.info("Source: Cloudbeds (scraping - slower, rate limited)")
        else:
            logger.info("Source: Common Crawl archives (fast, no limits)")

    # Use service for enumeration
    service = Service()
    hotels = await service.enumerate_commoncrawl(
        max_indices=args.max_indices,
        year=args.year,
        concurrency=args.concurrency,
        fetch_details=not args.slugs_only,
        use_archives=not args.scrape_cloudbeds,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total hotels found: {len(hotels)}")

    if not args.slugs_only:
        # Count with names
        with_names = sum(1 for h in hotels if h.get("name") and h["name"] != "Unknown")
        logger.info(f"Hotels with names: {with_names}")

    # Output to JSON
    if args.output:
        with open(args.output, "w") as f:
            json.dump(hotels, f, indent=2)
        logger.info(f"Saved {len(hotels)} hotels to {args.output}")

    # Save to database using service
    if args.save_db:
        from db.client import init_db
        
        await init_db()
        
        # Prepare leads
        leads = []
        for h in hotels:
            name = h.get("name", "Unknown")
            if name == "Unknown":
                continue
            leads.append({
                "name": name,
                "website": h.get("booking_url"),
                "external_id": h.get("external_id") or f"cloudbeds_{h['slug']}",
                "external_id_type": "commoncrawl",
            })
        
        logger.info(f"Saving {len(leads)} leads to database...")
        stats = await service.save_booking_engine_leads(
            leads=leads,
            source="commoncrawl",
            booking_engine="Cloudbeds",
        )
        
        logger.info(f"Database results:")
        logger.info(f"  Inserted: {stats['inserted']}")
        logger.info(f"  Engines linked: {stats['engines_linked']}")
        logger.info(f"  Skipped (exists): {stats['skipped_exists']}")
        logger.info(f"  Errors: {stats['errors']}")

    # Show sample results
    if hotels and not args.output:
        logger.info("")
        logger.info("Sample hotels (first 10):")
        for h in hotels[:10]:
            name = h.get("name", "—")
            slug = h.get("slug", "?")
            logger.info(f"  [{slug}] {name}")


if __name__ == "__main__":
    asyncio.run(main())
