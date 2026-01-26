#!/usr/bin/env python3
"""
Export crawl data by booking engine.

Generates Excel reports for hotels ingested from crawl data (Common Crawl, etc.)
grouped by booking engine.

Usage:
    # Export all Cloudbeds crawl data
    uv run python -m workflows.export_crawl --engine cloudbeds

    # Export all booking engines
    uv run python -m workflows.export_crawl --all

    # Export with custom source pattern
    uv run python -m workflows.export_crawl --engine mews --source "%crawl%"
"""

import argparse
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BOOKING_ENGINES = ["cloudbeds", "mews", "rms", "siteminder"]


async def main():
    parser = argparse.ArgumentParser(
        description="Export crawl data by booking engine"
    )

    # Engine selection
    engine_group = parser.add_mutually_exclusive_group(required=True)
    engine_group.add_argument(
        "--engine", "-e",
        type=str,
        choices=BOOKING_ENGINES,
        help="Booking engine to export"
    )
    engine_group.add_argument(
        "--all",
        action="store_true",
        help="Export all booking engines"
    )

    # Options
    parser.add_argument(
        "--source", "-s",
        type=str,
        default="%commoncrawl%",
        help="Source pattern to filter (default: %%commoncrawl%%)"
    )

    args = parser.parse_args()

    # Initialize database
    from db.client import init_db
    await init_db()

    # Initialize service
    from services.reporting.service import Service
    service = Service()

    # Determine engines to export
    engines = BOOKING_ENGINES if args.all else [args.engine]

    total_leads = 0
    exports = []

    for engine in engines:
        logger.info(f"\nExporting {engine}...")
        try:
            s3_uri, count = await service.export_by_booking_engine(
                booking_engine=engine,
                source_pattern=args.source,
            )
            if count > 0:
                exports.append((engine, s3_uri, count))
                total_leads += count
                logger.info(f"  Exported {count} leads to {s3_uri}")
            else:
                logger.info(f"  No leads found for {engine}")
        except Exception as e:
            logger.error(f"  Failed to export {engine}: {e}")

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("EXPORT SUMMARY")
    logger.info("=" * 50)
    for engine, s3_uri, count in exports:
        logger.info(f"  {engine}: {count} leads -> {s3_uri}")
    logger.info(f"Total leads exported: {total_leads}")


if __name__ == "__main__":
    asyncio.run(main())
