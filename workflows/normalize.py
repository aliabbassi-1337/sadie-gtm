"""
Workflow: Normalize Location Data
=================================
Unified normalization job that runs countries first, then states.

Order matters: countries must be normalized before states, because
state normalization is scoped by country name.

USAGE:
    # Dry run (show what would be fixed)
    uv run python -m workflows.normalize --dry-run

    # Apply fixes
    uv run python -m workflows.normalize

    # Countries only
    uv run python -m workflows.normalize --countries-only

    # States only (assumes countries are already normalized)
    uv run python -m workflows.normalize --states-only
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run(dry_run: bool = False, countries_only: bool = False, states_only: bool = False):
    """Run normalization: countries first, then states."""
    await init_db()

    try:
        service = Service()

        country_result = {"total_fixed": 0}
        state_result = {"total_fixed": 0}

        # Step 1: Normalize countries
        if not states_only:
            logger.info("=" * 50)
            logger.info("STEP 1: COUNTRY NORMALIZATION")
            logger.info("=" * 50)
            country_result = await service.normalize_countries_bulk(dry_run=dry_run)

        # Step 2: Normalize states (must run after countries)
        if not countries_only:
            logger.info("")
            logger.info("=" * 50)
            logger.info("STEP 2: STATE NORMALIZATION")
            logger.info("=" * 50)
            state_result = await service.normalize_states_bulk(dry_run=dry_run)

        # Summary
        logger.info("")
        logger.info("=" * 50)
        logger.info("NORMALIZATION SUMMARY")
        logger.info("=" * 50)
        if dry_run:
            logger.info(f"Countries: would fix {country_result.get('total_fixed', 0)} hotels")
            logger.info(f"States: would fix {state_result.get('would_fix', state_result.get('total_fixed', 0))} hotels")
        else:
            logger.info(f"Countries: fixed {country_result.get('total_fixed', 0)} hotels")
            logger.info(f"States: fixed {state_result.get('total_fixed', 0)} hotels")

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Normalize location data (countries + states)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without applying")
    parser.add_argument("--countries-only", action="store_true", help="Only normalize countries")
    parser.add_argument("--states-only", action="store_true", help="Only normalize states")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    asyncio.run(run(
        dry_run=args.dry_run,
        countries_only=args.countries_only,
        states_only=args.states_only,
    ))


if __name__ == "__main__":
    main()
