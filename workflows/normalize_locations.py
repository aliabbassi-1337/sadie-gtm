#!/usr/bin/env python3
"""
Location Normalization Workflow - Normalize country, state, and city data.

Converts short codes to full names:
- Countries: USA -> United States, AU -> Australia, etc.
- States: FL -> Florida, CA -> California, NSW -> New South Wales, etc.
- Fixes malformed data: "WY 83012" -> "Wyoming", state="VIC" with country="USA" -> country="Australia"

Usage:
    # Check what needs normalization
    uv run python -m workflows.normalize_locations --status

    # Dry run - show what would change
    uv run python -m workflows.normalize_locations --dry-run

    # Run normalization
    uv run python -m workflows.normalize_locations
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service as EnrichmentService


async def run_status():
    """Show normalization status."""
    await init_db()
    try:
        service = EnrichmentService()
        stats = await service.get_normalization_status()
        
        logger.info("=== Location Normalization Status ===\n")
        logger.info(f"Countries needing normalization: {stats.get('countries_to_normalize', 0)}")
        logger.info(f"Australian hotels incorrectly in USA: {stats.get('australian_in_usa', 0)}")
        logger.info(f"US state codes to normalize: {stats.get('us_state_codes', 0)}")
        logger.info(f"States with zip codes attached: {stats.get('states_with_zips', 0)}")
        
    finally:
        await close_db()


async def run_normalize(dry_run: bool = False):
    """Run location normalization."""
    await init_db()
    
    try:
        service = EnrichmentService()
        
        if dry_run:
            stats = await service.get_normalization_status()
            logger.info("[DRY RUN] Would normalize:")
            logger.info(f"  Countries: {stats.get('countries_to_normalize', 0)}")
            logger.info(f"  Australian hotels in USA: {stats.get('australian_in_usa', 0)}")
            logger.info(f"  US state codes: {stats.get('us_state_codes', 0)}")
            logger.info(f"  States with zips: {stats.get('states_with_zips', 0)}")
            return
        
        stats = await service.normalize_locations(dry_run=dry_run)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("NORMALIZATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"  Australian hotels fixed: {stats['australian_fixed']}")
        logger.info(f"  States with zips fixed:  {stats['zips_fixed']}")
        logger.info(f"  Countries normalized:    {stats['countries_fixed']}")
        logger.info(f"  US states normalized:    {stats['states_fixed']}")
        logger.info(f"  Total:                   {stats['total']}")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Normalize location data (countries, states, cities)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--status", action="store_true", help="Show normalization status only")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without making changes")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(run_status())
    else:
        asyncio.run(run_normalize(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
