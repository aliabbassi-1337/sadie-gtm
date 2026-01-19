"""Location enrichment workflow - Fill in missing city/state/address from coordinates.

USAGE:

1. Enrich locations (respects Nominatim rate limit of 1 req/sec):
   uv run python workflows/location_enrichment.py enrich --limit 100

2. Check status:
   uv run python workflows/location_enrichment.py status

NOTES:
- Uses OpenStreetMap Nominatim API (free, 1 request per second rate limit)
- Only processes hotels that have coordinates but missing city
- Idempotent (can be re-run safely)
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db, queries, get_conn
from services.leadgen.geocoding import reverse_geocode
from infra import slack


async def run_location_enrichment(limit: int, notify: bool = True) -> None:
    """Run location enrichment for hotels missing city data."""
    await init_db()
    try:
        # Get pending count first
        async with get_conn() as conn:
            result = await queries.get_pending_location_enrichment_count(conn)
            pending = result["count"] if result else 0

        logger.info(f"Hotels pending location enrichment: {pending}")

        if pending == 0:
            logger.info("No hotels pending location enrichment")
            return

        # Get hotels to process
        async with get_conn() as conn:
            hotels = await queries.get_hotels_pending_location_enrichment(conn, limit=limit)

        logger.info(f"Processing {len(hotels)} hotels for location enrichment...")

        enriched_count = 0
        failed_count = 0

        for hotel in hotels:
            hotel_id = hotel["id"]
            hotel_name = hotel["name"]
            lat = hotel["latitude"]
            lng = hotel["longitude"]

            logger.info(f"  {hotel_name} ({lat}, {lng})...")

            # Call Nominatim reverse geocoding
            result = await reverse_geocode(lat, lng)

            if result and result.city:
                # Update hotel with location data
                async with get_conn() as conn:
                    await queries.update_hotel_location(
                        conn,
                        hotel_id=hotel_id,
                        address=result.address,
                        city=result.city,
                        state=result.state,
                        country=result.country,
                    )
                logger.info(f"    -> {result.city}, {result.state}")
                enriched_count += 1
            else:
                logger.warning(f"    -> No city found")
                failed_count += 1

            # Rate limit: Nominatim requires 1 request per second
            await asyncio.sleep(1.1)

        logger.info("=" * 60)
        logger.info("LOCATION ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels enriched: {enriched_count}")
        logger.info(f"Hotels failed: {failed_count}")
        logger.info("=" * 60)

        if notify and enriched_count > 0:
            slack.send_message(
                f"*Location Enrichment Complete*\n"
                f"• Hotels enriched: {enriched_count}\n"
                f"• Hotels failed: {failed_count}"
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
        async with get_conn() as conn:
            result = await queries.get_pending_location_enrichment_count(conn)
            pending = result["count"] if result else 0

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
  # Enrich locations for up to 100 hotels
  uv run python workflows/location_enrichment.py enrich --limit 100

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
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
    )

    # Status command
    subparsers.add_parser("status", help="Show location enrichment status")

    args = parser.parse_args()

    if args.command == "enrich":
        logger.info(f"Running location enrichment (limit={args.limit})")
        asyncio.run(run_location_enrichment(
            limit=args.limit,
            notify=not args.no_notify,
        ))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
