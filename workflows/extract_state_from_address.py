"""Extract state from address field for US hotels missing state data.

Usage:
    # Dry run to see what would be updated
    uv run python -m workflows.extract_state_from_address --dry-run
    
    # Actually update
    uv run python -m workflows.extract_state_from_address
    
    # Limit to specific number
    uv run python -m workflows.extract_state_from_address --limit 1000
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run(limit: int = 1000, dry_run: bool = False):
    """Find US hotels without state and extract from address."""
    await init_db()
    
    try:
        service = Service()
        result = await service.extract_states_from_address(limit=limit, dry_run=dry_run)
        
        logger.info(f"Total: {result['total']}, Matched: {result['matched']}, Updated: {result.get('updated', 0)}")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Extract state from address for US hotels")
    parser.add_argument("--limit", type=int, default=1000, help="Max hotels to process")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without updating")
    
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
