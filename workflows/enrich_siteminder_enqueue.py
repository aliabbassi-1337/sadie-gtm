"""Enqueue SiteMinder hotels for distributed enrichment via SQS.

Usage:
    uv run python -m workflows.enrich_siteminder_enqueue --limit 5000
    uv run python -m workflows.enrich_siteminder_enqueue --missing-location --country "United States"
    uv run python -m workflows.enrich_siteminder_enqueue --dry-run --limit 100
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run(limit: int, dry_run: bool, missing_location: bool, country: str = None):
    await init_db()
    try:
        service = Service()
        await service.enqueue_siteminder_for_enrichment(
            limit=limit, missing_location=missing_location, country=country, dry_run=dry_run,
        )
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Enqueue SiteMinder hotels for enrichment")
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--missing-location", action="store_true")
    parser.add_argument("--country", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(run(args.limit, args.dry_run, args.missing_location, args.country))


if __name__ == "__main__":
    main()
