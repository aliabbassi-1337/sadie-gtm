"""
Workflow: Backfill Missing Location Data
=========================================
Fills in missing city/state for USA leads (crawl sources) using:

1. Reverse geocoding - for hotels WITH coordinates but missing city/state
2. State normalization - ensures all state names are full names (not abbreviations)

USAGE:
    # Check status
    uv run python workflows/backfill_locations.py status

    # Reverse geocode hotels with coordinates (1 req/sec rate limit)
    uv run python workflows/backfill_locations.py reverse --limit 100

    # Normalize state names (CA -> California)
    uv run python workflows/backfill_locations.py normalize
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def show_status():
    """Show current location data status."""
    await init_db()
    try:
        service = Service()
        status = await service.get_location_backfill_status()
        
        logger.info("=" * 60)
        logger.info("LOCATION DATA STATUS")
        logger.info("=" * 60)
        logger.info(f"Total USA hotels: {status.total_usa_hotels:,}")
        logger.info(f"USA leads (with booking engine): {status.total_usa_leads:,}")
        logger.info(f"USA leads missing state: {status.leads_missing_state:,}")
        logger.info(f"  - Can fix (have coords): {status.can_reverse_geocode:,}")
        logger.info(f"  - Need forward geocoding: {status.need_forward_geocode:,}")
        logger.info(f"States needing normalization: {status.states_need_normalization:,}")
        logger.info("=" * 60)
    finally:
        await close_db()


async def reverse_geocode(limit: int):
    """Reverse geocode hotels with coordinates."""
    await init_db()
    try:
        service = Service()
        result = await service.backfill_locations_reverse_geocode(limit=limit)
        logger.info(f"Result: {result}")
    finally:
        await close_db()


async def normalize():
    """Normalize state abbreviations."""
    await init_db()
    try:
        service = Service()
        result = await service.normalize_us_states()
        logger.info(f"Result: {result}")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Backfill missing location data")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    subparsers.add_parser("status", help="Show location data status")
    
    reverse_parser = subparsers.add_parser("reverse", help="Reverse geocode hotels with coordinates")
    reverse_parser.add_argument("--limit", "-l", type=int, default=100, help="Max hotels to process")
    
    subparsers.add_parser("normalize", help="Normalize state abbreviations to full names")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.command == "status":
        asyncio.run(show_status())
    elif args.command == "reverse":
        asyncio.run(reverse_geocode(limit=args.limit))
    elif args.command == "normalize":
        asyncio.run(normalize())


if __name__ == "__main__":
    main()
