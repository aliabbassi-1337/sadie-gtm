"""Launcher workflow - Launch hotels that have completed enrichment.

USAGE:

1. Check status (how many hotels are ready to launch):
   uv run python workflows/launcher.py status

2. Preview launchable hotels:
   uv run python workflows/launcher.py preview --limit 50

3. Launch a batch of ready hotels:
   uv run python workflows/launcher.py launch --limit 100

LAUNCH CRITERIA (all required):
- status = 0 (pending)
- valid name (not null, not empty, not junk/test names)
- state + country (location required, city optional)
- booking engine detected (hbe.status = 1)

NOT REQUIRED (optional but displayed if available):
- email
- phone
- room_count
- customer proximity

Launching sets the hotel status to 1 (live).
Uses FOR UPDATE SKIP LOCKED for multi-worker safety.
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.reporting.service import Service
from infra import slack


async def show_status() -> None:
    """Show launcher status."""
    await init_db()
    try:
        service = Service()
        launchable = await service.get_launchable_count()
        launched = await service.get_launched_count()

        logger.info("=" * 60)
        logger.info("LAUNCHER STATUS")
        logger.info("=" * 60)
        logger.info(f"Hotels ready to launch: {launchable}")
        logger.info(f"Hotels already launched: {launched}")
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

    args = parser.parse_args()

    if args.command == "status":
        asyncio.run(show_status())
    elif args.command == "preview":
        asyncio.run(preview_launchable(limit=args.limit))
    elif args.command == "launch":
        asyncio.run(launch_batch(limit=args.limit, notify=not args.no_notify))
    elif args.command == "launch-all":
        asyncio.run(launch_batch(limit=10000, notify=True))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
