"""
Workflow: Export Reports
========================
Generates Excel reports for hotel leads and uploads to S3.

Usage:
    # Export a single city
    uv run python workflows/export.py --city "Miami Beach" --state FL

    # Export all cities in a state plus state aggregate
    uv run python workflows/export.py --state FL

    # Export a city without uploading to S3 (local file only)
    uv run python workflows/export.py --city Miami --state FL --local

    # Export and send Slack notification
    uv run python workflows/export.py --city Miami --state FL --notify
"""

import sys
import asyncio
import argparse

from loguru import logger

from db.client import init_db, close_db
from services.reporting.service import Service


async def export_city_workflow(
    city: str,
    state: str,
    country: str = "USA",
    local_only: bool = False,
    notify: bool = False,
) -> str:
    """Export a single city report."""
    await init_db()

    try:
        service = Service()

        if local_only:
            # Generate Excel locally without S3 upload
            from services.reporting import repo
            from db.models.reporting import ReportStats

            leads = await repo.get_leads_for_city(city, state)
            stats = await repo.get_city_stats(city, state)
            top_engines = await repo.get_top_engines_for_city(city, state)

            report_stats = ReportStats(
                location_name=city,
                stats=stats,
                top_engines=top_engines,
            )

            workbook = service._create_workbook(leads, report_stats)

            # Save to current directory
            filename = f"{city.replace(' ', '_')}.xlsx"
            workbook.save(filename)
            logger.info(f"Exported to local file: {filename}")
            return filename
        else:
            # Get lead count for notification
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
    local_only: bool = False,
    notify: bool = False,
) -> list:
    """Export all cities in a state plus state aggregate."""
    await init_db()

    try:
        service = Service()

        if local_only:
            # Generate Excel locally for state aggregate only
            from services.reporting import repo
            from db.models.reporting import ReportStats

            leads = await repo.get_leads_for_state(state)
            stats = await repo.get_state_stats(state)
            top_engines = await repo.get_top_engines_for_state(state)

            report_stats = ReportStats(
                location_name=state,
                stats=stats,
                top_engines=top_engines,
            )

            workbook = service._create_workbook(leads, report_stats)

            filename = f"{state.replace(' ', '_')}.xlsx"
            workbook.save(filename)
            logger.info(f"Exported to local file: {filename}")
            return [filename]
        else:
            # Get lead count for notification
            from services.reporting import repo
            leads = await repo.get_leads_for_state(state)
            lead_count = len(leads)

            uris = await service.export_state_with_cities(state, country)
            logger.info(f"Exported {len(uris)} reports to S3")

            if notify:
                service.send_slack_notification(
                    location=f"{state} (all cities)",
                    lead_count=lead_count,
                    s3_uri=f"{len(uris)} files uploaded",
                )

            return uris

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

    # Local only (no S3 upload)
    parser.add_argument("--local", action="store_true", help="Save locally instead of uploading to S3")

    # Slack notification
    parser.add_argument("--notify", action="store_true", help="Send Slack notification after export")

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
            args.local,
            args.notify,
        ))
        print(f"\nExported: {result}")

    elif args.state and not args.city:
        # Export all cities in state
        results = asyncio.run(export_state_workflow(
            args.state,
            args.country,
            args.local,
            args.notify,
        ))
        print(f"\nExported {len(results)} reports:")
        for r in results:
            print(f"  - {r}")

    else:
        parser.error("Provide --city and --state, or just --state for full state export")


if __name__ == "__main__":
    main()
