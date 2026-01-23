"""
Workflow: Export Reports
========================
Generates Excel reports for hotel leads and uploads to S3.

Usage:
    # Export all FL hotels with booking engines
    uv run python workflows/export.py --state FL

    # Export only DBPR leads (filter by source)
    uv run python workflows/export.py --state FL --source dbpr

    # Export single city
    uv run python workflows/export.py --city "Miami Beach" --state FL
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


async def export_city_workflow(
    city: str,
    state: str,
    country: str = "USA",
    notify: bool = True,
) -> str:
    """Export a single city report."""
    await init_db()

    try:
        service = Service()
        from services.reporting import repo

        leads = await repo.get_leads_for_city(city, state)
        lead_count = len(leads)

        s3_uri = await service.export_city(city, state, country)
        logger.info(f"Exported to S3: {s3_uri}")

        if notify:
            service.send_slack_notification(
                location=f"{city}, {state}",
                lead_count=lead_count,
                s3_uri=s3_uri,
            )

        return s3_uri

    finally:
        await close_db()


async def export_state_workflow(
    state: str,
    country: str = "USA",
    notify: bool = True,
    source: str = None,
) -> str:
    """Export all cities in a state plus state aggregate.

    Args:
        state: State code (e.g., 'FL')
        country: Country code
        notify: Send Slack notification
        source: Filter by source pattern (e.g., 'dbpr%' for DBPR only)
    """
    await init_db()

    try:
        service = Service()
        from services.reporting import repo

        leads = await repo.get_leads_for_state(state, source_pattern=source)
        lead_count = len(leads)

        s3_uri = await service.export_state(state, country, source_pattern=source)
        logger.info(f"Exported to S3: {s3_uri}")

        if notify:
            service.send_slack_notification(
                location=state,
                lead_count=lead_count,
                s3_uri=s3_uri,
            )

        return s3_uri

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Export hotel leads to Excel")

    # City export
    parser.add_argument("--city", type=str, help="City name (e.g., 'Miami Beach')")

    # State export
    parser.add_argument("--state", type=str, help="State name (e.g., 'FL')")

    # Country
    parser.add_argument("--country", type=str, default="USA", help="Country (default: USA)")

    # Slack notification (on by default)
    parser.add_argument("--no-notify", action="store_true", help="Disable Slack notification")

    # Source filter
    parser.add_argument("--source", type=str, help="Filter by source (e.g., 'dbpr' for DBPR leads only)")

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    if args.city and args.state:
        # Export single city
        result = asyncio.run(export_city_workflow(
            args.city,
            args.state,
            args.country,
            not args.no_notify,
        ))
        print(f"\nExported: {result}")

    elif args.state and not args.city:
        # Export all cities in state
        source = args.source
        if source and '%' not in source:
            source = f"%{source}%"
        result = asyncio.run(export_state_workflow(
            args.state,
            args.country,
            not args.no_notify,
            source,
        ))
        print(f"\nExported: {result}")

    else:
        parser.error("Provide --city and --state, or just --state for full state export")


if __name__ == "__main__":
    main()
