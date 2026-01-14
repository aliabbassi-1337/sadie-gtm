"""Enrichment workflow - Enrich hotels with room counts and customer proximity.

USAGE:

1. Room count enrichment:
   uv run python workflows/enrichment.py room-counts --limit 100

2. Customer proximity calculation:
   uv run python workflows/enrichment.py proximity --limit 100 --max-distance 100

3. Check status:
   uv run python workflows/enrichment.py status

NOTES:
- Room count enrichment uses Groq API (requires ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY in .env)
- Customer proximity uses PostGIS for efficient spatial queries
- Both operations are idempotent (can be re-run safely)
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run_room_counts(limit: int) -> None:
    """Run room count enrichment."""
    await init_db()
    try:
        service = Service()

        # Get pending count first
        pending = await service.get_pending_enrichment_count()
        logger.info(f"Hotels pending room count enrichment: {pending}")

        if pending == 0:
            logger.info("No hotels pending enrichment")
            return

        logger.info(f"Starting room count enrichment (limit={limit})...")
        count = await service.enrich_room_counts(limit=limit)

        logger.info("=" * 60)
        logger.info("ROOM COUNT ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels enriched: {count}")
        logger.info("=" * 60)

    finally:
        await close_db()


async def run_proximity(limit: int, max_distance_km: float) -> None:
    """Run customer proximity calculation."""
    await init_db()
    try:
        service = Service()

        # Get pending count first
        pending = await service.get_pending_proximity_count()
        logger.info(f"Hotels pending proximity calculation: {pending}")

        if pending == 0:
            logger.info("No hotels pending proximity calculation")
            return

        logger.info(f"Starting customer proximity calculation (limit={limit}, max_distance={max_distance_km}km)...")
        count = await service.calculate_customer_proximity(
            limit=limit,
            max_distance_km=max_distance_km,
        )

        logger.info("=" * 60)
        logger.info("PROXIMITY CALCULATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels with nearby customers: {count}")
        logger.info("=" * 60)

    finally:
        await close_db()


async def show_status() -> None:
    """Show enrichment status."""
    await init_db()
    try:
        service = Service()
        pending_enrichment = await service.get_pending_enrichment_count()
        pending_proximity = await service.get_pending_proximity_count()

        logger.info("=" * 60)
        logger.info("ENRICHMENT STATUS")
        logger.info("=" * 60)
        logger.info(f"Hotels pending room count enrichment: {pending_enrichment}")
        logger.info(f"Hotels pending proximity calculation: {pending_proximity}")
        logger.info("=" * 60)

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Run enrichment workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich room counts for 100 hotels
  uv run python workflows/enrichment.py room-counts --limit 100

  # Calculate proximity for 100 hotels (max 50km)
  uv run python workflows/enrichment.py proximity --limit 100 --max-distance 50

  # Check status
  uv run python workflows/enrichment.py status
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Room counts command
    room_parser = subparsers.add_parser(
        "room-counts",
        help="Enrich hotels with room counts using regex + Groq LLM"
    )
    room_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="Max hotels to process (default: 100)"
    )

    # Proximity command
    prox_parser = subparsers.add_parser(
        "proximity",
        help="Calculate nearest existing customer for hotels"
    )
    prox_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="Max hotels to process (default: 100)"
    )
    prox_parser.add_argument(
        "--max-distance", "-d",
        type=float,
        default=100.0,
        help="Max distance in km to search for customers (default: 100)"
    )

    # Status command
    subparsers.add_parser("status", help="Show enrichment status")

    args = parser.parse_args()

    if args.command == "room-counts":
        logger.info(f"Running room count enrichment (limit={args.limit})")
        asyncio.run(run_room_counts(limit=args.limit))
    elif args.command == "proximity":
        logger.info(f"Running proximity calculation (limit={args.limit}, max_distance={args.max_distance}km)")
        asyncio.run(run_proximity(limit=args.limit, max_distance_km=args.max_distance))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
