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

    # Filter to specific state
    uv run python -m workflows.enrich_websites --state FL --limit 1000

    # Location-only mode: find locations for hotels that have websites but no coordinates
    uv run python -m workflows.enrich_websites --location-only --source texas_hot --limit 500
"""

import argparse
import asyncio
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.enrichment.service import Service as EnrichmentService
from db.client import init_db


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
        "--delay",
        type=float,
        default=0.1,
        help="Delay between API calls in seconds (default: 0.1)",
    )
    parser.add_argument(
        "--location-only",
        action="store_true",
        help="Only enrich locations for hotels that have websites but no coordinates",
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

    # Run enrichment
    service = EnrichmentService()

    if args.location_only:
        # Location-only mode: find locations for hotels with websites but no coordinates
        logger.info(f"Starting location-only enrichment (limit: {args.limit})...")
        if args.source:
            logger.info(f"  Filtering to source: {args.source}")
        if args.state:
            logger.info(f"  Filtering to state: {args.state}")

        stats = await service.enrich_locations_only(
            limit=args.limit,
            source_filter=args.source,
            state_filter=args.state,
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Location Enrichment Complete")
        logger.info("=" * 60)
        logger.info(f"Hotels processed: {stats['total']}")
        logger.info(f"Locations found: {stats['found']}")
        logger.info(f"Not found: {stats['not_found']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"API calls: {stats['api_calls']}")
        logger.info(f"Estimated cost: ${stats['api_calls'] * 0.001:.2f}")
    else:
        # Standard website enrichment mode
        logger.info(f"Starting website enrichment (limit: {args.limit})...")
        if args.source:
            logger.info(f"  Filtering to source: {args.source}")
        if args.state:
            logger.info(f"  Filtering to state: {args.state}")

        stats = await service.enrich_websites(
            limit=args.limit,
            source_filter=args.source,
            state_filter=args.state,
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Website Enrichment Complete")
        logger.info("=" * 60)
        logger.info(f"Hotels processed: {stats['total']}")
        logger.info(f"Websites found: {stats['found']}")
        logger.info(f"Not found: {stats['not_found']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"API calls: {stats['api_calls']}")
        logger.info(f"Estimated cost: ${stats['api_calls'] * 0.001:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
