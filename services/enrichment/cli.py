#!/usr/bin/env python3
"""
Enrichment Service CLI
======================
Run enrichment jobs from the command line.

Usage:
    uv run python -m services.enrichment.cli room-counts --limit 10
    uv run python -m services.enrichment.cli proximity --limit 100
    uv run python -m services.enrichment.cli status
"""

import asyncio
import argparse

from services.enrichment.service import Service


async def run_room_counts(limit: int) -> None:
    """Run room count enrichment."""
    service = Service()
    print(f"\nStarting room count enrichment (limit={limit})...\n")
    count = await service.enrich_room_counts(limit=limit)
    print(f"\nEnriched {count} hotels with room counts")


async def run_proximity(limit: int, max_distance_km: float) -> None:
    """Run customer proximity calculation."""
    service = Service()
    print(f"\nStarting customer proximity calculation (limit={limit}, max_distance={max_distance_km}km)...\n")
    count = await service.calculate_customer_proximity(
        limit=limit,
        max_distance_km=max_distance_km,
    )
    print(f"\nProcessed {count} hotels for proximity")


async def show_status() -> None:
    """Show enrichment status."""
    service = Service()
    pending_enrichment = await service.get_pending_enrichment_count()
    pending_proximity = await service.get_pending_proximity_count()

    print("\n=== Enrichment Status ===")
    print(f"Hotels pending room count enrichment: {pending_enrichment}")
    print(f"Hotels pending proximity calculation: {pending_proximity}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Enrichment Service CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Room counts command
    room_parser = subparsers.add_parser("room-counts", help="Enrich hotels with room counts")
    room_parser.add_argument("--limit", type=int, default=100, help="Max hotels to process (default: 100)")

    # Proximity command
    prox_parser = subparsers.add_parser("proximity", help="Calculate customer proximity")
    prox_parser.add_argument("--limit", type=int, default=100, help="Max hotels to process (default: 100)")
    prox_parser.add_argument("--max-distance", type=float, default=100.0, help="Max distance in km (default: 100)")

    # Status command
    subparsers.add_parser("status", help="Show enrichment status")

    args = parser.parse_args()

    if args.command == "room-counts":
        asyncio.run(run_room_counts(limit=args.limit))
    elif args.command == "proximity":
        asyncio.run(run_proximity(limit=args.limit, max_distance_km=args.max_distance))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
