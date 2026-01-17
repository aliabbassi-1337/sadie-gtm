"""Enrichment workflow - Enrich hotels with room counts and customer proximity.

USAGE:

1. Room count enrichment (fast, paid tier - 15 concurrent requests):
   uv run python workflows/enrichment.py room-counts --limit 100

2. Room count enrichment (slow, free tier - sequential):
   uv run python workflows/enrichment.py room-counts --limit 100 --free-tier

3. Customer proximity calculation:
   uv run python workflows/enrichment.py proximity --limit 100 --max-distance 100

4. Check status:
   uv run python workflows/enrichment.py status

NOTES:
- Room count enrichment uses Groq API (requires ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY in .env)
- Default mode uses paid tier rate limits (1000 RPM, 15 concurrent requests)
- Use --free-tier for slow sequential mode (30 RPM)
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
from infra import slack


async def run_room_counts(limit: int, free_tier: bool = False, concurrency: int = 15, tier: int = None, notify: bool = True) -> None:
    """Run room count enrichment."""
    await init_db()
    try:
        service = Service()

        # Get pending count first
        pending = await service.get_pending_enrichment_count(tier=tier)
        tier_label = f"tier {tier}" if tier else "all tiers"
        logger.info(f"Hotels pending room count enrichment ({tier_label}): {pending}")

        if pending == 0:
            logger.info("No hotels pending enrichment")
            return

        mode = "free tier (sequential)" if free_tier else f"paid tier ({concurrency} concurrent)"
        logger.info(f"Starting room count enrichment (limit={limit}, {tier_label}, {mode})...")
        count = await service.enrich_room_counts(
            limit=limit,
            free_tier=free_tier,
            concurrency=concurrency,
            tier=tier,
        )

        logger.info("=" * 60)
        logger.info("ROOM COUNT ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels enriched: {count}")
        logger.info("=" * 60)

        if notify and count > 0:
            slack.send_message(
                f"*Room Count Enrichment Complete*\n"
                f"• Hotels enriched: {count}\n"
                f"• Tier: {tier_label}\n"
                f"• Mode: {mode}"
            )

    except Exception as e:
        logger.error(f"Room count enrichment failed: {e}")
        if notify:
            slack.send_error("Room Count Enrichment", str(e))
        raise
    finally:
        await close_db()


async def run_proximity(limit: int, max_distance_km: float, notify: bool = True) -> None:
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

        if notify and count > 0:
            slack.send_message(
                f"*Proximity Calculation Complete*\n"
                f"• Hotels processed: {count}\n"
                f"• Max distance: {max_distance_km}km"
            )

    except Exception as e:
        logger.error(f"Proximity calculation failed: {e}")
        if notify:
            slack.send_error("Proximity Calculation", str(e))
        raise
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
    room_parser.add_argument(
        "--free-tier",
        action="store_true",
        help="Use slow sequential mode for free tier (30 RPM). Default is fast paid tier (1000 RPM)."
    )
    room_parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=15,
        help="Max concurrent requests in paid tier mode (default: 15)"
    )
    room_parser.add_argument(
        "--tier", "-t",
        type=int,
        choices=[1, 2, 3],
        default=None,
        help="Only enrich hotels with this booking engine tier (1=high priority, 2=medium, 3=low). Default: all tiers."
    )
    room_parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
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
    prox_parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
    )

    # Status command
    subparsers.add_parser("status", help="Show enrichment status")

    args = parser.parse_args()

    if args.command == "room-counts":
        mode = "free tier" if args.free_tier else f"paid tier ({args.concurrency} concurrent)"
        tier_label = f"tier {args.tier}" if args.tier else "all tiers"
        logger.info(f"Running room count enrichment (limit={args.limit}, {tier_label}, {mode})")
        asyncio.run(run_room_counts(
            limit=args.limit,
            free_tier=args.free_tier,
            concurrency=args.concurrency,
            tier=args.tier,
            notify=not args.no_notify,
        ))
    elif args.command == "proximity":
        logger.info(f"Running proximity calculation (limit={args.limit}, max_distance={args.max_distance}km)")
        asyncio.run(run_proximity(limit=args.limit, max_distance_km=args.max_distance, notify=not args.no_notify))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
