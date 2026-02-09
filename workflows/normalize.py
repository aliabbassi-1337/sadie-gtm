"""
Workflow: Normalize Location Data
=================================
Unified normalization job that runs:
  1. Country code/variation normalization (AU -> Australia, USA -> United States)
  2. State abbreviation normalization (CA -> California, NSW -> New South Wales)
  3. Location inference (fix misclassified hotels using TLD, phone, address signals)

Order matters: countries must be normalized before states, and both before inference.

USAGE:
    # Dry run (show what would be fixed)
    uv run python -m workflows.normalize --dry-run

    # Apply fixes
    uv run python -m workflows.normalize

    # Countries only
    uv run python -m workflows.normalize --countries-only

    # States only (assumes countries are already normalized)
    uv run python -m workflows.normalize --states-only

    # Inference only (fix misclassified countries using signals)
    uv run python -m workflows.normalize --infer-only
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run(
    dry_run: bool = False,
    countries_only: bool = False,
    states_only: bool = False,
    infer_only: bool = False,
):
    """Run normalization: countries first, then states, then inference."""
    await init_db()

    try:
        service = Service()

        country_result = {"total_fixed": 0}
        state_result = {"total_fixed": 0}
        infer_result = {"country_fixes": 0, "state_fixes": 0}

        run_countries = not states_only and not infer_only
        run_states = not countries_only and not infer_only
        run_infer = not countries_only and not states_only

        # Step 1: Normalize countries
        if run_countries:
            logger.info("=" * 50)
            logger.info("STEP 1: COUNTRY NORMALIZATION")
            logger.info("=" * 50)
            country_result = await service.normalize_countries_bulk(dry_run=dry_run)

        # Step 2: Normalize states (must run after countries)
        if run_states:
            logger.info("")
            logger.info("=" * 50)
            logger.info("STEP 2: STATE NORMALIZATION")
            logger.info("=" * 50)
            state_result = await service.normalize_states_bulk(dry_run=dry_run)

        # Step 3: Infer and fix misclassified locations
        if run_infer:
            logger.info("")
            logger.info("=" * 50)
            logger.info("STEP 3: LOCATION INFERENCE")
            logger.info("=" * 50)
            infer_result = await service.infer_locations_bulk(dry_run=dry_run)

        # Summary
        logger.info("")
        logger.info("=" * 50)
        logger.info("NORMALIZATION SUMMARY")
        logger.info("=" * 50)
        if dry_run:
            logger.info(f"Countries: would fix {country_result.get('total_fixed', 0)} hotels")
            logger.info(f"States: would fix {state_result.get('would_fix', state_result.get('total_fixed', 0))} hotels")
            logger.info(f"Inference: would fix {infer_result.get('country_fixes', 0)} countries, {infer_result.get('state_fixes', 0)} states")
        else:
            logger.info(f"Countries: fixed {country_result.get('total_fixed', 0)} hotels")
            logger.info(f"States: fixed {state_result.get('total_fixed', 0)} hotels")
            logger.info(f"Inference: fixed {infer_result.get('country_fixes', 0)} countries, {infer_result.get('state_fixes', 0)} states")

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Normalize location data (countries + states + inference)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without applying")
    parser.add_argument("--countries-only", action="store_true", help="Only normalize countries")
    parser.add_argument("--states-only", action="store_true", help="Only normalize states")
    parser.add_argument("--infer-only", action="store_true", help="Only run location inference")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    asyncio.run(run(
        dry_run=args.dry_run,
        countries_only=args.countries_only,
        states_only=args.states_only,
        infer_only=args.infer_only,
    ))


if __name__ == "__main__":
    main()
