#!/usr/bin/env python3
"""
Texas Hotel Tax Ingestion Workflow - Import Texas hotel data.

Parses hotel occupancy tax data from the Texas Comptroller and imports
into the hotels database. Includes room counts from tax filings.

Data source: Texas Comptroller Open Records

Usage:
    # Ingest from all available quarters (merge-unique)
    uv run python workflows/ingest_texas.py

    # Ingest from specific quarter directory only
    uv run python workflows/ingest_texas.py --quarter "HOT 25 Q3"

    # Dry run (parse but don't save)
    uv run python workflows/ingest_texas.py --dry-run

    # Show statistics only
    uv run python workflows/ingest_texas.py --stats
"""

import argparse
import asyncio
import sys
import os
from typing import List

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.ingestor import Service, TexasHotel
from db.client import init_db, close_db


def print_stats(hotels: List[TexasHotel], stats: dict):
    """Print statistics about loaded hotels."""
    by_city = {}
    with_phone = 0
    with_room_count = 0
    total_rooms = 0

    for hotel in hotels:
        city = hotel.city or "Unknown"
        by_city[city] = by_city.get(city, 0) + 1

        if hotel.phone:
            with_phone += 1

        if hotel.room_count:
            with_room_count += 1
            total_rooms += hotel.room_count

    logger.info("")
    logger.info("=" * 60)
    logger.info("Texas Hotel Tax Data Statistics")
    logger.info("=" * 60)
    logger.info(f"Records parsed: {stats.get('records_parsed', 0):,}")
    logger.info(f"Unique hotels: {len(hotels):,}")
    if hotels:
        logger.info(f"With phone: {with_phone:,} ({100*with_phone/len(hotels):.1f}%)")
        logger.info(f"With room count: {with_room_count:,} ({100*with_room_count/len(hotels):.1f}%)")
    logger.info(f"Total rooms: {total_rooms:,}")
    if with_room_count:
        logger.info(f"Avg rooms per hotel: {total_rooms/with_room_count:.1f}")
    logger.info("")
    logger.info("Top 20 Cities:")
    for city, count in sorted(by_city.items(), key=lambda x: -x[1])[:20]:
        logger.info(f"  {city}: {count:,}")


async def main():
    parser = argparse.ArgumentParser(
        description="Ingest Texas hotel tax data into the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--quarter", "-q",
        type=str,
        default=None,
        help="Quarter directory name (default: load all quarters and merge)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse but don't save to database",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics only (no database changes)",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Initialize service
    service = Service()

    # Determine save mode
    save_to_db = not args.dry_run and not args.stats

    if save_to_db:
        await init_db()

    source = args.quarter if args.quarter else "all quarters"
    logger.info(f"Starting Texas hotel ingestion from {source}...")

    # Run ingestion via service
    hotels, stats = await service.ingest_texas(
        quarter=args.quarter,
        save_to_db=save_to_db,
    )

    if save_to_db:
        await close_db()

    # Output summary
    if args.stats:
        print_stats(hotels, stats)
    else:
        logger.info("")
        logger.info("=" * 60)
        logger.info("Ingestion Complete")
        logger.info("=" * 60)
        logger.info(f"Files processed: {stats.get('files_processed', 0)}")
        logger.info(f"Records parsed: {stats.get('records_parsed', 0):,}")
        logger.info(f"Records saved: {stats.get('records_saved', 0):,}")
        logger.info(f"Duplicates skipped: {stats.get('duplicates_skipped', 0):,}")
        logger.info(f"Errors: {stats.get('errors', 0)}")

        if hotels and not args.dry_run:
            logger.info("")
            logger.info("Sample records (first 5):")
            for hotel in hotels[:5]:
                logger.info(f"  {hotel.name}")
                logger.info(f"    {hotel.address}, {hotel.city}, {hotel.state}")
                if hotel.phone:
                    logger.info(f"    Phone: {hotel.phone}")
                if hotel.room_count:
                    logger.info(f"    Rooms: {hotel.room_count}")


if __name__ == "__main__":
    asyncio.run(main())
