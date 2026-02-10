"""
Workflow: Normalize Location Data
=================================
Unified normalization job that runs:
  1. Country code/variation normalization (AU -> Australia, USA -> United States)
  2. Location inference (fix misclassified hotels using TLD, phone, address signals)
  3. State abbreviation normalization (CA -> California, NSW -> New South Wales)
  4. Address enrichment (extract state/city from address text)
  5. City→state inference (infer state from city using self-referencing data)

Order matters: all country-changing steps (1, 2) run before state normalization (3).

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

    # City->state inference only
    uv run python -m workflows.normalize --city-state-only
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.enrichment.service import Service


async def run(
    dry_run: bool = False,
    countries_only: bool = False,
    states_only: bool = False,
    infer_only: bool = False,
    enrich_only: bool = False,
    city_state_only: bool = False,
):
    """Run normalization: countries -> inference -> states -> address enrichment -> city-state."""
    await init_db()

    try:
        service = Service()

        country_result = {"total_fixed": 0}
        state_result = {"total_fixed": 0}
        infer_result = {"country_fixes": 0, "state_fixes": 0}
        enrich_result = {"state_fixes": 0, "city_fixes": 0}
        city_state_result = {"total_missing": 0, "matched": 0, "updated": 0}

        only_flags = [countries_only, states_only, infer_only, enrich_only, city_state_only]
        any_only = any(only_flags)

        run_countries = not any_only or countries_only
        run_states = not any_only or states_only
        run_infer = not any_only or infer_only
        run_enrich = not any_only or enrich_only
        run_city_state = not any_only or city_state_only

        # Single connection for the entire pipeline — ensures reads see prior writes
        # (Supabase pooler may route different connections to different backends)
        async with get_conn() as conn:

            # Step 1: Normalize countries
            if run_countries:
                logger.info("=" * 50)
                logger.info("STEP 1: COUNTRY NORMALIZATION")
                logger.info("=" * 50)
                country_result = await service.normalize_countries_bulk(dry_run=dry_run, conn=conn)

            # Step 2: Infer and fix misclassified locations (also changes countries)
            if run_infer:
                logger.info("")
                logger.info("=" * 50)
                logger.info("STEP 2: LOCATION INFERENCE")
                logger.info("=" * 50)
                infer_result = await service.infer_locations_bulk(dry_run=dry_run, conn=conn)

            # Step 3: Normalize states (runs after ALL country-changing steps)
            if run_states:
                logger.info("")
                logger.info("=" * 50)
                logger.info("STEP 3: STATE NORMALIZATION")
                logger.info("=" * 50)
                state_result = await service.normalize_states_bulk(dry_run=dry_run, conn=conn)

            # Step 4: Enrich state/city from address text
            if run_enrich:
                logger.info("")
                logger.info("=" * 50)
                logger.info("STEP 4: ADDRESS ENRICHMENT")
                logger.info("=" * 50)
                enrich_result = await service.enrich_state_city_from_address_bulk(dry_run=dry_run, conn=conn)

            # Step 5: Infer state from city (self-referencing)
            if run_city_state:
                logger.info("")
                logger.info("=" * 50)
                logger.info("STEP 5: CITY→STATE INFERENCE")
                logger.info("=" * 50)
                city_state_result = await service.infer_state_from_city_bulk(dry_run=dry_run, conn=conn)

        # Summary
        logger.info("")
        logger.info("=" * 50)
        logger.info("NORMALIZATION SUMMARY")
        logger.info("=" * 50)
        if dry_run:
            logger.info(f"Countries: would fix {country_result.get('total_fixed', 0)} hotels")
            logger.info(f"States: would fix {state_result.get('would_fix', state_result.get('total_fixed', 0))} hotels")
            logger.info(f"Inference: would fix {infer_result.get('country_fixes', 0)} countries, {infer_result.get('state_fixes', 0)} states")
            logger.info(f"Address enrichment: would enrich {enrich_result.get('state_fixes', 0)} states, {enrich_result.get('city_fixes', 0)} cities")
            logger.info(f"City→state: would infer {city_state_result.get('matched', 0)} states from city")
        else:
            logger.info(f"Countries: fixed {country_result.get('total_fixed', 0)} hotels")
            logger.info(f"States: fixed {state_result.get('total_fixed', 0)} hotels")
            logger.info(f"Inference: fixed {infer_result.get('country_fixes', 0)} countries, {infer_result.get('state_fixes', 0)} states")
            logger.info(f"Address enrichment: enriched {enrich_result.get('state_fixes', 0)} states, {enrich_result.get('city_fixes', 0)} cities")
            logger.info(f"City→state: inferred {city_state_result.get('matched', 0)} states from city")

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Normalize location data (countries + states + inference)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without applying")
    parser.add_argument("--countries-only", action="store_true", help="Only normalize countries")
    parser.add_argument("--states-only", action="store_true", help="Only normalize states")
    parser.add_argument("--infer-only", action="store_true", help="Only run location inference")
    parser.add_argument("--enrich-only", action="store_true", help="Only run address enrichment (state/city)")
    parser.add_argument("--city-state-only", action="store_true", help="Only run city→state inference")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    asyncio.run(run(
        dry_run=args.dry_run,
        countries_only=args.countries_only,
        states_only=args.states_only,
        infer_only=args.infer_only,
        enrich_only=args.enrich_only,
        city_state_only=args.city_state_only,
    ))


if __name__ == "__main__":
    main()
