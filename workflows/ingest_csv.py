#!/usr/bin/env python3
"""
Generic CSV Ingestion Workflow - Import data from any configured CSV source.

This workflow uses source configurations defined in the `sources/` directory.
Each source is a Python file with a CONFIG variable defining the schema.

Usage:
    # List available sources
    uv run python -m workflows.ingest_csv --list

    # Ingest from a configured source
    uv run python -m workflows.ingest_csv --source georgia_hotels

    # Dry run (parse but don't save)
    uv run python -m workflows.ingest_csv --source georgia_hotels --dry-run

    # Filter by county
    uv run python -m workflows.ingest_csv --source georgia_hotels --county "Fulton"

To add a new source:
    1. Create sources/your_source.py with a CONFIG variable
    2. Run: uv run python -m workflows.ingest_csv --source your_source
"""

import argparse
import asyncio
import sys
import os

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sources import get_source_config, list_sources
from services.ingestor import GenericCSVIngestor
from db.client import init_db, close_db


async def main():
    parser = argparse.ArgumentParser(
        description="Ingest data from a configured CSV source",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--source", "-s",
        type=str,
        help="Source name (from sources/ directory)",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available sources",
    )
    parser.add_argument(
        "--county", "-c",
        action="append",
        help="Filter to specific county (can be repeated)",
    )
    parser.add_argument(
        "--state",
        action="append",
        help="Filter to specific state (can be repeated)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse but don't save to database",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics only",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # List sources
    if args.list:
        sources = list_sources()
        if sources:
            logger.info("Available sources:")
            for s in sources:
                config = get_source_config(s)
                if config:
                    logger.info(f"  {s}: {config.name} ({config.source_type})")
        else:
            logger.info("No sources configured. Add a config to sources/")
        return

    # Require source
    if not args.source:
        parser.error("--source is required (use --list to see available sources)")

    # Load config
    config = get_source_config(args.source)
    if not config:
        logger.error(f"Source not found: {args.source}")
        logger.info("Use --list to see available sources")
        return

    logger.info(f"Loading source: {config.name}")
    logger.info(f"  Type: {config.source_type}")
    if config.source_type == "s3":
        logger.info(f"  Bucket: {config.s3_bucket}")
        logger.info(f"  Prefix: {config.s3_prefix}")

    # Build filters
    filters = {}
    if args.county:
        filters["counties"] = args.county
    if args.state:
        filters["states"] = args.state

    # Create ingestor
    ingestor = GenericCSVIngestor(config)

    # Dry-run or stats mode: parse without saving
    if args.dry_run or args.stats:
        from unittest.mock import AsyncMock, patch

        logger.info(f"Starting ingestion from {config.name} (dry run)...")
        with patch.object(ingestor, "_batch_save", new_callable=AsyncMock) as mock:
            mock.return_value = 0
            records, stats = await ingestor.ingest(
                filters=filters if filters else None,
                upload_logs=False,
            )
    else:
        await init_db()
        logger.info(f"Starting ingestion from {config.name}...")
        records, stats = await ingestor.ingest(filters=filters if filters else None)
        await close_db()

    # Output summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Ingestion Complete")
    logger.info("=" * 60)
    logger.info(f"Source: {config.name}")
    logger.info(f"Files processed: {stats.files_processed}")
    logger.info(f"Records parsed: {stats.records_parsed:,}")
    logger.info(f"Records saved: {stats.records_saved:,}")
    logger.info(f"Duplicates skipped: {stats.duplicates_skipped:,}")
    logger.info(f"Errors: {stats.errors}")

    if records and not args.dry_run and not args.stats:
        logger.info("")
        logger.info("Sample records (first 5):")
        for rec in records[:5]:
            logger.info(f"  {rec.name}")
            if rec.address:
                logger.info(f"    {rec.address}, {rec.city}, {rec.state}")
            if rec.phone:
                logger.info(f"    Phone: {rec.phone}")
            if rec.room_count:
                logger.info(f"    Rooms: {rec.room_count}")

    if args.stats:
        # Show more detailed stats
        by_county = {}
        by_city = {}
        with_phone = 0
        with_rooms = 0
        total_rooms = 0

        for rec in records:
            county = rec.county or "Unknown"
            by_county[county] = by_county.get(county, 0) + 1

            city = rec.city or "Unknown"
            by_city[city] = by_city.get(city, 0) + 1

            if rec.phone:
                with_phone += 1
            if rec.room_count:
                with_rooms += 1
                total_rooms += rec.room_count

        logger.info("")
        logger.info(f"With phone: {with_phone:,}")
        logger.info(f"With room count: {with_rooms:,}")
        logger.info(f"Total rooms: {total_rooms:,}")

        logger.info("")
        logger.info("Top 10 Counties:")
        for county, count in sorted(by_county.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  {county}: {count:,}")

        logger.info("")
        logger.info("Top 10 Cities:")
        for city, count in sorted(by_city.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  {city}: {count:,}")


if __name__ == "__main__":
    asyncio.run(main())
