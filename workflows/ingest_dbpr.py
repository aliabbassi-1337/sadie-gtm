#!/usr/bin/env python3
"""
DBPR License Ingestion Workflow - Import Florida lodging licenses.

Downloads lodging license data from the Florida Department of Business
and Professional Regulation (DBPR) and imports into the hotels database.

Data source: https://www2.myfloridalicense.com/hotels-restaurants/lodging-public-records/

Usage:
    # Download all Florida lodging licenses (~193,000)
    uv run python -m workflows.ingest_dbpr --all

    # Download only new licenses (current fiscal year, ~6,000)
    uv run python -m workflows.ingest_dbpr --new-only

    # Filter to specific counties
    uv run python -m workflows.ingest_dbpr --county "Palm Beach" --county "Miami-Dade"

    # Filter to hotels and motels only
    uv run python -m workflows.ingest_dbpr --type Hotel --type Motel

    # Dry run (download and parse but don't save)
    uv run python -m workflows.ingest_dbpr --all --dry-run

    # Show summary statistics only
    uv run python -m workflows.ingest_dbpr --stats
"""

import argparse
import asyncio
import sys
import os

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.ingestor import Service, DBPRIngestor, LICENSE_TYPES, RANK_CODES
from db.client import init_db


def list_types():
    """List all DBPR license types."""
    return list(set(LICENSE_TYPES.values()))


def list_ranks():
    """List all DBPR rank codes."""
    return list(set(RANK_CODES.values()))


async def show_stats():
    """Download and show statistics without saving."""
    ingestor = DBPRIngestor()

    logger.info("Downloading DBPR lodging data for statistics...")
    licenses, stats = await ingestor.ingest(save_to_db=False)

    # Aggregate stats
    by_type = {}
    by_county = {}
    by_status = {}

    for lic in licenses:
        # By type
        key = lic.license_type
        by_type[key] = by_type.get(key, 0) + 1

        # By county
        key = lic.county or "Unknown"
        by_county[key] = by_county.get(key, 0) + 1

        # By status
        key = lic.status
        by_status[key] = by_status.get(key, 0) + 1

    logger.info("")
    logger.info("=" * 60)
    logger.info("DBPR Lodging License Statistics")
    logger.info("=" * 60)
    logger.info(f"Total licenses: {len(licenses):,}")
    logger.info("")

    logger.info("By License Type:")
    for t, count in sorted(by_type.items(), key=lambda x: -x[1]):
        logger.info(f"  {t}: {count:,}")

    logger.info("")
    logger.info("By Status:")
    for s, count in sorted(by_status.items(), key=lambda x: -x[1]):
        logger.info(f"  {s}: {count:,}")

    logger.info("")
    logger.info("Top 20 Counties:")
    for county, count in sorted(by_county.items(), key=lambda x: -x[1])[:20]:
        logger.info(f"  {county}: {count:,}")


async def main():
    parser = argparse.ArgumentParser(
        description="Ingest Florida DBPR lodging licenses into the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all licenses
    uv run python -m workflows.ingest_dbpr --all

    # Download new licenses only
    uv run python -m workflows.ingest_dbpr --new-only

    # Filter to Palm Beach County hotels
    uv run python -m workflows.ingest_dbpr --county "Palm Beach" --type Hotel

License Types: """ + ", ".join(list_types())
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Download all active lodging licenses (~193,000)",
    )
    parser.add_argument(
        "--new-only",
        action="store_true",
        help="Download only new licenses (current fiscal year)",
    )
    parser.add_argument(
        "-c", "--county",
        action="append",
        help="Filter to specific county (can be specified multiple times)",
    )
    parser.add_argument(
        "-t", "--type",
        action="append",
        help="Filter to specific license type (can be specified multiple times)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and parse but don't save to database",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics about DBPR data (no database changes)",
    )
    parser.add_argument(
        "--list-types",
        action="store_true",
        help="List all license types and exit",
    )
    parser.add_argument(
        "--list-counties",
        action="store_true",
        help="List top counties by license count and exit",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Handle list commands
    if args.list_types:
        logger.info("DBPR License Types:")
        for t in sorted(list_types()):
            logger.info(f"  - {t}")
        return

    if args.stats or args.list_counties:
        await show_stats()
        return

    # Require --all or --new-only
    if not args.all and not args.new_only:
        parser.error("Must specify --all or --new-only")

    # Initialize database
    if not args.dry_run:
        await init_db()

    # Run ingestion
    service = Service()

    logger.info("Starting DBPR license ingestion...")
    if args.county:
        logger.info(f"Filtering to counties: {args.county}")
    if args.type:
        logger.info(f"Filtering to types: {args.type}")

    licenses, stats = await service.ingest_dbpr(
        counties=args.county,
        license_types=args.type,
        new_only=args.new_only,
        save_to_db=not args.dry_run,
    )

    # Output summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Ingestion Complete")
    logger.info("=" * 60)
    logger.info(f"Files downloaded: {stats['files_downloaded']}")
    logger.info(f"Records parsed: {stats['records_parsed']:,}")
    logger.info(f"Records saved: {stats['records_saved']:,}")
    logger.info(f"Duplicates skipped: {stats['duplicates_skipped']:,}")
    logger.info(f"Errors: {stats['errors']}")

    if licenses and not args.dry_run:
        logger.info("")
        logger.info("Sample records (first 5):")
        for lic in licenses[:5]:
            logger.info(f"  {lic.business_name or lic.licensee_name} [{lic.license_type}]")
            logger.info(f"    {lic.address}, {lic.city}, {lic.state}")
            if lic.phone:
                logger.info(f"    Phone: {lic.phone}")


if __name__ == "__main__":
    asyncio.run(main())
