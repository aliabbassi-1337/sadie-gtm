"""
Workflow: Normalize State Names
===============================
Normalizes state abbreviations to full names.

Processes each supported country with its own state map:
- United States: CA -> California, TX -> Texas, etc.
- Australia: NSW -> New South Wales, VIC -> Victoria, etc.

Uses centralized state mappings from services/enrichment/state_utils.py

USAGE:
    # Check what would be fixed (dry run)
    uv run python workflows/normalize_states.py --dry-run

    # Apply fixes
    uv run python workflows/normalize_states.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run(dry_run: bool = False):
    """Run state normalization via enrichment service."""
    await init_db()
    
    try:
        service = Service()
        result = await service.normalize_states_bulk(dry_run=dry_run)
        
        logger.info("=" * 50)
        logger.info(f"Total: {len(result.get('fixes', []))} variations to normalize")
        if result.get('would_fix'):
            logger.info(f"Would fix: {result['would_fix']} hotels")
        elif result.get('total_fixed'):
            logger.info(f"Fixed: {result['total_fixed']} hotels")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Normalize state names")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed without applying")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
