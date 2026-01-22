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
from typing import Optional

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.ingestor.texas import TexasIngestor, TexasHotel
from services.ingestor import repo
from db.client import init_db, close_db


async def show_stats(quarter: Optional[str]):
    """Show statistics about the Texas data without saving."""
    ingester = TexasIngestor()

    if quarter:
        hotels, stats = ingester.load_quarterly_data(quarter)
        unique_hotels = ingester.deduplicate_hotels(hotels)
    else:
        unique_hotels, stats = ingester.load_all_quarters()

    # Aggregate stats
    by_city = {}
    with_phone = 0
    with_room_count = 0
    total_rooms = 0

    for hotel in unique_hotels:
        # By city
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
    logger.info(f"Total records: {len(hotels):,}")
    logger.info(f"Unique hotels (name+city): {len(unique_hotels):,}")
    logger.info(f"With phone: {with_phone:,} ({100*with_phone/len(unique_hotels):.1f}%)")
    logger.info(f"With room count: {with_room_count:,} ({100*with_room_count/len(unique_hotels):.1f}%)")
    logger.info(f"Total rooms: {total_rooms:,}")
    logger.info(f"Avg rooms per hotel: {total_rooms/with_room_count:.1f}" if with_room_count else "N/A")
    logger.info("")
    logger.info("Top 20 Cities:")
    for city, count in sorted(by_city.items(), key=lambda x: -x[1])[:20]:
        logger.info(f"  {city}: {count:,}")


async def ingest_hotels(quarter: Optional[str], dry_run: bool = False):
    """Ingest Texas hotel data into database."""
    ingester = TexasIngestor()

    # Load and parse - either single quarter or all quarters
    if quarter:
        hotels, stats = ingester.load_quarterly_data(quarter)
        unique_hotels = ingester.deduplicate_hotels(hotels)
    else:
        unique_hotels, stats = ingester.load_all_quarters()

    logger.info(f"Unique hotels after deduplication: {len(unique_hotels):,}")

    if dry_run:
        logger.info("Dry run - not saving to database")
        return unique_hotels, stats

    # Initialize database
    await init_db()

    # Insert hotels
    saved = 0
    duplicates = 0
    errors = 0

    for i, hotel in enumerate(unique_hotels):
        try:
            hotel_id = await repo.insert_hotel(
                name=hotel.name,
                source="texas_hot",
                status=0,  # Pending
                address=hotel.address,
                city=hotel.city,
                state=hotel.state,
                country="USA",
                phone=hotel.phone,
                category="hotel",
            )

            if hotel_id:
                saved += 1

                # Insert room count if available
                if hotel.room_count:
                    try:
                        from db.client import get_conn, queries
                        async with get_conn() as conn:
                            await queries.insert_room_count(
                                conn,
                                hotel_id=hotel_id,
                                room_count=hotel.room_count,
                                source="texas_hot",
                                status=1,  # Verified from tax records
                            )
                    except Exception as e:
                        logger.debug(f"Failed to insert room count: {e}")
            else:
                duplicates += 1

        except Exception as e:
            logger.debug(f"Failed to insert hotel {hotel.name}: {e}")
            errors += 1

        # Progress logging
        if (i + 1) % 1000 == 0:
            logger.info(f"  Progress: {i+1}/{len(unique_hotels)} ({saved} saved, {duplicates} duplicates)")

    await close_db()

    stats.records_saved = saved
    stats.duplicates_skipped = duplicates
    stats.errors = errors

    return unique_hotels, stats


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

    if args.stats:
        await show_stats(args.quarter)
        return

    # Run ingestion
    source = args.quarter if args.quarter else "all quarters"
    logger.info(f"Starting Texas hotel ingestion from {source}...")

    hotels, stats = await ingest_hotels(args.quarter, args.dry_run)

    # Output summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Ingestion Complete")
    logger.info("=" * 60)
    logger.info(f"Files processed: {stats.files_processed}")
    logger.info(f"Records parsed: {stats.records_parsed:,}")
    logger.info(f"Records saved: {stats.records_saved:,}")
    logger.info(f"Duplicates skipped: {stats.duplicates_skipped:,}")
    logger.info(f"Errors: {stats.errors}")

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
