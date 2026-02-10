"""Enrichment workflow - CLI wrapper for enrichment service.

USAGE:
    uv run python workflows/enrichment.py room-counts --limit 100
    uv run python workflows/enrichment.py room-counts --limit 100 --state California
    uv run python workflows/enrichment.py room-counts --limit 100 --free-tier
    uv run python workflows/enrichment.py proximity --limit 100 --max-distance 100
    uv run python workflows/enrichment.py status
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


async def run_room_counts(
    limit: int, free_tier: bool = False, concurrency: int = 15,
    notify: bool = True, state: str = None, country: str = None,
) -> None:
    await init_db()
    try:
        service = Service()
        count = await service.enrich_room_counts(
            limit=limit, free_tier=free_tier, concurrency=concurrency,
            state=state, country=country,
        )
        if notify and count > 0:
            mode = "free tier" if free_tier else f"paid tier ({concurrency} concurrent)"
            slack.send_message(
                f"*Room Count Enrichment Complete*\n"
                f"• Hotels enriched: {count}\n• Mode: {mode}"
                + (f"\n• State: {state}" if state else "")
                + (f"\n• Country: {country}" if country else "")
            )
    except Exception as e:
        logger.error(f"Room count enrichment failed: {e}")
        if notify:
            slack.send_error("Room Count Enrichment", str(e))
        raise
    finally:
        await close_db()


async def run_proximity(limit: int, max_distance_km: float, notify: bool = True) -> None:
    await init_db()
    try:
        service = Service()
        count = await service.calculate_customer_proximity(limit=limit, max_distance_km=max_distance_km)
        if notify and count > 0:
            slack.send_message(
                f"*Proximity Calculation Complete*\n"
                f"• Hotels processed: {count}\n• Max distance: {max_distance_km}km"
            )
    except Exception as e:
        logger.error(f"Proximity calculation failed: {e}")
        if notify:
            slack.send_error("Proximity Calculation", str(e))
        raise
    finally:
        await close_db()


async def show_status() -> None:
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
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Run enrichment workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    room_parser = subparsers.add_parser("room-counts")
    room_parser.add_argument("--limit", "-l", type=int, default=100)
    room_parser.add_argument("--free-tier", action="store_true")
    room_parser.add_argument("--concurrency", "-c", type=int, default=50)
    room_parser.add_argument("--no-notify", action="store_true")
    room_parser.add_argument("--state", "-s", type=str, default=None)
    room_parser.add_argument("--country", type=str, default=None)

    prox_parser = subparsers.add_parser("proximity")
    prox_parser.add_argument("--limit", "-l", type=int, default=100)
    prox_parser.add_argument("--max-distance", "-d", type=float, default=100.0)
    prox_parser.add_argument("--no-notify", action="store_true")

    subparsers.add_parser("status")

    args = parser.parse_args()

    if args.command == "room-counts":
        asyncio.run(run_room_counts(
            limit=args.limit, free_tier=args.free_tier, concurrency=args.concurrency,
            notify=not args.no_notify, state=args.state, country=args.country,
        ))
    elif args.command == "proximity":
        asyncio.run(run_proximity(args.limit, args.max_distance, not args.no_notify))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
