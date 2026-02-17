"""Launcher workflow - Launch and take down hotels.

USAGE:

1. Check status:
   uv run python workflows/launcher.py status

2. Preview launchable hotels:
   uv run python workflows/launcher.py preview --limit 50

3. Launch a batch of ready hotels:
   uv run python workflows/launcher.py launch --limit 100

4. Debug why hotels aren't launching:
   uv run python workflows/launcher.py debug --hotel-id 12345

5. Preview hotels to take down (launched but no engine):
   uv run python workflows/launcher.py takedown-preview

6. Take down hotels without an active booking engine:
   uv run python workflows/launcher.py takedown

LAUNCH CRITERIA (defined in services/reporting/launch_conditions.py):
- status = 0 (pending)
- valid name (not null, not empty, not junk/test names)
- country (required; state/city optional)
- booking engine detected (hbe.status = 1)
- enrichment completed (hbe.enrichment_status = 1)

TAKEDOWN CRITERIA:
- status = 1 (launched)
- no active booking engine (no hbe record with status = 1)

Uses FOR UPDATE SKIP LOCKED for multi-worker safety.
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.reporting.service import Service
from services.reporting.launch_conditions import get_rejection_reason, is_launchable
from infra import slack


async def show_status() -> None:
    """Show launcher status."""
    await init_db()
    try:
        service = Service()
        launchable = await service.get_launchable_count()
        launched = await service.get_launched_count()
        takedown = await service.get_takedown_count()

        logger.info("=" * 60)
        logger.info("LAUNCHER STATUS")
        logger.info("=" * 60)
        logger.info(f"Hotels ready to launch: {launchable}")
        logger.info(f"Hotels already launched: {launched}")
        logger.info(f"Hotels to take down (no engine): {takedown}")
        logger.info("=" * 60)

    finally:
        await close_db()


async def preview_launchable(limit: int) -> None:
    """Preview hotels that are ready to launch."""
    await init_db()
    try:
        service = Service()
        hotels = await service.get_launchable_hotels(limit=limit)

        logger.info("=" * 60)
        logger.info(f"LAUNCHABLE HOTELS (showing up to {limit})")
        logger.info("=" * 60)

        if not hotels:
            logger.info("No hotels ready to launch")
            return

        for hotel in hotels:
            logger.info(
                f"  [{hotel.id}] {hotel.hotel_name} - {hotel.city}, {hotel.state} "
                f"| Engine: {hotel.booking_engine_name} "
                f"| Rooms: {hotel.room_count} "
                f"| Nearest: {hotel.nearest_customer_name} ({hotel.nearest_customer_distance_km}km)"
            )

        total = await service.get_launchable_count()
        logger.info("=" * 60)
        logger.info(f"Showing {len(hotels)} of {total} total launchable hotels")
        logger.info("=" * 60)

    finally:
        await close_db()


async def debug_hotel(hotel_id: int) -> None:
    """Debug why a specific hotel isn't launching."""
    await init_db()
    try:
        async with get_conn() as conn:
            # Get hotel details
            row = await conn.fetchrow("""
                SELECT 
                    h.id, h.name, h.status, h.state, h.country, h.city,
                    hbe.status as be_status,
                    be.name as booking_engine_name
                FROM sadie_gtm.hotels h
                LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
                WHERE h.id = $1
            """, hotel_id)
            
            if not row:
                logger.error(f"Hotel {hotel_id} not found")
                return
            
            logger.info("=" * 60)
            logger.info(f"DEBUG: Hotel {hotel_id}")
            logger.info("=" * 60)
            logger.info(f"Name: {row['name']}")
            logger.info(f"Status: {row['status']}")
            logger.info(f"Location: {row['city']}, {row['state']}, {row['country']}")
            logger.info(f"Booking Engine: {row['booking_engine_name']} (status={row['be_status']})")
            logger.info("")
            
            has_be = row['be_status'] == 1
            reason = get_rejection_reason(
                status=row['status'],
                name=row['name'],
                state=row['state'],
                country=row['country'],
                has_booking_engine=has_be,
            )
            
            if reason:
                logger.warning(f"NOT LAUNCHABLE: {reason}")
            else:
                logger.success("LAUNCHABLE: Hotel meets all criteria")
                
    finally:
        await close_db()


async def launch_batch(limit: int, notify: bool = True) -> None:
    """Launch a batch of hotels (multi-worker safe)."""
    await init_db()
    try:
        service = Service()

        logger.info(f"Attempting to launch up to {limit} hotels...")

        # Atomically claim and launch hotels (safe for multiple EC2 instances)
        count = await service.launch_ready(limit=limit)

        pending = await service.get_launchable_count()
        total_launched = await service.get_launched_count()

        logger.info("=" * 60)
        logger.info("LAUNCH COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels launched this batch: {count}")
        logger.info(f"Hotels still pending: {pending}")
        logger.info(f"Total launched (all time): {total_launched}")
        logger.info("=" * 60)

        if notify and count > 0:
            slack.send_message(
                f"*Hotel Launch Complete*\n"
                f"• Hotels launched: {count}\n"
                f"• Still pending: {pending}\n"
                f"• Total launched (all time): {total_launched}"
            )

    except Exception as e:
        logger.error(f"Launch failed: {e}")
        if notify:
            slack.send_error("Hotel Launch", str(e))
        raise
    finally:
        await close_db()


async def preview_takedown(limit: int) -> None:
    """Preview launched hotels that would be taken down (no active booking engine)."""
    await init_db()
    try:
        service = Service()
        hotels = await service.get_takedown_candidates(limit=limit)

        logger.info("=" * 60)
        logger.info(f"TAKEDOWN CANDIDATES (showing up to {limit})")
        logger.info("=" * 60)

        if not hotels:
            logger.info("No hotels to take down")
            return

        by_country = {}
        for h in hotels:
            country = h["country"] or "Unknown"
            by_country.setdefault(country, []).append(h)

        for country, items in sorted(by_country.items()):
            logger.info(f"\n{country} ({len(items)}):")
            for h in items[:10]:
                logger.info(f"  [{h['id']}] {h['hotel_name']} - {h['city']}, {h['state']} | source: {h['source']}")
            if len(items) > 10:
                logger.info(f"  ... and {len(items) - 10} more")

        total = await service.get_takedown_count()
        logger.info("=" * 60)
        logger.info(f"Showing {len(hotels)} of {total} total takedown candidates")
        logger.info("=" * 60)

    finally:
        await close_db()


async def run_takedown(limit: int, notify: bool = True) -> None:
    """Take down launched hotels that have no active booking engine."""
    await init_db()
    try:
        service = Service()

        takedown_count_before = await service.get_takedown_count()
        logger.info(f"Found {takedown_count_before} hotels to take down...")

        count = await service.takedown_hotels_without_engine(limit=limit)

        launched = await service.get_launched_count()
        remaining = await service.get_takedown_count()

        logger.info("=" * 60)
        logger.info("TAKEDOWN COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels taken down: {count}")
        logger.info(f"Still need takedown: {remaining}")
        logger.info(f"Total still launched: {launched}")
        logger.info("=" * 60)

        if notify and count > 0:
            slack.send_message(
                f"*Hotel Takedown Complete*\n"
                f"• Hotels taken down (no engine): {count}\n"
                f"• Still need takedown: {remaining}\n"
                f"• Total still launched: {launched}"
            )

    except Exception as e:
        logger.error(f"Takedown failed: {e}")
        if notify:
            slack.send_error("Hotel Takedown", str(e))
        raise
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Launch hotels that have completed enrichment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check how many hotels are ready to launch
  uv run python workflows/launcher.py status

  # Preview launchable hotels
  uv run python workflows/launcher.py preview --limit 50

  # Launch a batch of hotels (default 100)
  uv run python workflows/launcher.py launch

  # Launch more hotels at once
  uv run python workflows/launcher.py launch --limit 500

Notes:
  - Uses FOR UPDATE SKIP LOCKED for multi-worker safety
  - Safe to run on multiple EC2 instances concurrently
  - Each instance will claim different hotels (no duplicates)
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Status command
    subparsers.add_parser("status", help="Show launcher status")

    # Debug command
    debug_parser = subparsers.add_parser(
        "debug",
        help="Debug why a specific hotel isn't launching"
    )
    debug_parser.add_argument(
        "--hotel-id", "-i",
        type=int,
        required=True,
        help="Hotel ID to debug"
    )

    # Preview command
    preview_parser = subparsers.add_parser(
        "preview",
        help="Preview hotels that are ready to launch"
    )
    preview_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=50,
        help="Max hotels to show (default: 50)"
    )

    # Launch command (batch)
    launch_parser = subparsers.add_parser(
        "launch",
        help="Launch a batch of ready hotels (multi-worker safe)"
    )
    launch_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="Max hotels to launch per batch (default: 100)"
    )
    launch_parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
    )

    # Keep launch-all as alias for backwards compatibility
    subparsers.add_parser(
        "launch-all",
        help="Launch all ready hotels (alias for 'launch --limit 10000')"
    )

    # Takedown preview command
    takedown_preview_parser = subparsers.add_parser(
        "takedown-preview",
        help="Preview launched hotels with no active booking engine"
    )
    takedown_preview_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="Max hotels to show (default: 100)"
    )

    # Takedown command
    takedown_parser = subparsers.add_parser(
        "takedown",
        help="Take down launched hotels with no active booking engine"
    )
    takedown_parser.add_argument(
        "--limit", "-l",
        type=int,
        default=10000,
        help="Max hotels to take down (default: 10000)"
    )
    takedown_parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
    )

    args = parser.parse_args()

    if args.command == "status":
        asyncio.run(show_status())
    elif args.command == "debug":
        asyncio.run(debug_hotel(hotel_id=args.hotel_id))
    elif args.command == "preview":
        asyncio.run(preview_launchable(limit=args.limit))
    elif args.command == "launch":
        asyncio.run(launch_batch(limit=args.limit, notify=not args.no_notify))
    elif args.command == "launch-all":
        asyncio.run(launch_batch(limit=10000, notify=True))
    elif args.command == "takedown-preview":
        asyncio.run(preview_takedown(limit=args.limit))
    elif args.command == "takedown":
        asyncio.run(run_takedown(limit=args.limit, notify=not args.no_notify))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
