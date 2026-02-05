"""Location enrichment workflow - Fill in missing city/state/address from coordinates.

USAGE:

1. Check status:
   uv run python workflows/location_enrichment.py status

2. Enrich locations using Serper (fast, concurrent):
   uv run python workflows/location_enrichment.py enrich --limit 100 --concurrency 50

3. Enrich locations using Nominatim (free, slow - 1 req/sec):
   uv run python workflows/location_enrichment.py enrich --limit 100 --use-nominatim

NOTES:
- Default uses Serper Places API (fast, supports concurrency)
- Optional: Use Nominatim (free, 1 request per second rate limit) with --use-nominatim
- Only processes hotels that have coordinates but missing state
- Normalizes state abbreviations to full names (CA -> California)
- Idempotent (can be re-run safely)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service
from infra import slack


async def run_location_enrichment(
    limit: int,
    concurrency: int = 10,
    use_nominatim: bool = False,
    notify: bool = True,
) -> None:
    """Run location enrichment for hotels missing city data."""
    await init_db()
    try:
        service = Service()
        
        # Get pending count first
        pending = await service.get_pending_location_enrichment_count()
        logger.info(f"Hotels pending location enrichment: {pending}")

        if pending == 0:
            logger.info("No hotels pending location enrichment")
            return

        # Run enrichment
        api_name = "Nominatim" if use_nominatim else "Serper"
        logger.info(f"Using {api_name} API (concurrency={concurrency})")
        
        stats = await service.enrich_locations_reverse_geocode(
            limit=limit,
            concurrency=concurrency,
            use_nominatim=use_nominatim,
        )

        logger.info("=" * 60)
        logger.info("LOCATION ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels enriched: {stats['enriched']}")
        logger.info(f"Hotels failed: {stats['failed']}")
        logger.info("=" * 60)

        if notify and stats["enriched"] > 0:
            slack.send_message(
                f"*Location Enrichment Complete*\n"
                f"• Hotels enriched: {stats['enriched']}\n"
                f"• Hotels failed: {stats['failed']}"
            )

    except Exception as e:
        logger.error(f"Location enrichment failed: {e}")
        if notify:
            slack.send_error("Location Enrichment", str(e))
        raise
    finally:
        await close_db()


async def show_status() -> None:
    """Show location enrichment status."""
    await init_db()
    try:
        service = Service()
        pending = await service.get_pending_location_enrichment_count()

        logger.info("=" * 60)
        logger.info("LOCATION ENRICHMENT STATUS")
        logger.info("=" * 60)
        logger.info(f"Hotels pending location enrichment: {pending}")
        logger.info("=" * 60)

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Run location enrichment workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich locations using Serper (fast, default)
  uv run python workflows/location_enrichment.py enrich --limit 100 --concurrency 10

  # Enrich locations using Nominatim (free but slow)
  uv run python workflows/location_enrichment.py enrich --limit 100 --use-nominatim

  # Check status
  uv run python workflows/location_enrichment.py status
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Enrich command
    enrich_parser = subparsers.add_parser(
        "enrich",
        help="Enrich hotels with missing city/state using reverse geocoding"
    )
    enrich_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="Max hotels to process (default: 100)"
    )
    enrich_parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=50,
        help="Number of concurrent requests (default: 50, only for Serper)"
    )
    enrich_parser.add_argument(
        "--use-nominatim",
        action="store_true",
        help="Use free Nominatim API (slow, 1 req/sec) instead of Serper"
    )
    enrich_parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
    )

    # Status command
    subparsers.add_parser("status", help="Show location enrichment status")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    if args.command == "enrich":
        logger.info(f"Running location enrichment (limit={args.limit}, concurrency={args.concurrency})")
        asyncio.run(run_location_enrichment(
            limit=args.limit,
            concurrency=args.concurrency,
            use_nominatim=args.use_nominatim,
            notify=not args.no_notify,
        ))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
