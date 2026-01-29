"""Cloudbeds enrichment workflow - thin wrapper around the enrichment service.

Cloudbeds pages are JS-rendered (React/Chakra) so we use Playwright.
Visits each booking page once to extract name, address, city, state, country, phone, email.

Usage:
    # Check status
    uv run python -m workflows.enrich_cloudbeds --status
    
    # Dry run (show what would be enriched)
    uv run python -m workflows.enrich_cloudbeds --dry-run --limit 10
    
    # Run enrichment
    uv run python -m workflows.enrich_cloudbeds --limit 100
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run_status():
    """Show enrichment status."""
    await init_db()
    
    try:
        service = Service()
        status = await service.get_cloudbeds_enrichment_status()
        
        print("\n" + "=" * 60)
        print("CLOUDBEDS ENRICHMENT STATUS")
        print("=" * 60)
        print(f"  Total Cloudbeds hotels:     {status['total']:,}")
        print(f"  Needing enrichment:         {status['needing_enrichment']:,}")
        print(f"  Already enriched:           {status['already_enriched']:,}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


async def run_dry_run(limit: int):
    """Show what would be enriched without making changes."""
    await init_db()
    
    try:
        service = Service()
        candidates = await service.get_cloudbeds_hotels_needing_enrichment(limit=limit)
        
        print(f"\n=== DRY RUN: Would enrich {len(candidates)} hotels ===\n")
        
        for h in candidates[:20]:
            needs = []
            if not h.name or h.name.startswith('Unknown'):
                needs.append('name')
            if not h.city:
                needs.append('city')
            if not h.state:
                needs.append('state')
            
            print(f"  ID={h.id}: {h.name or 'NO NAME'}")
            print(f"    URL: {h.booking_url[:60]}...")
            print(f"    Needs: {', '.join(needs)}")
            print()
        
        if len(candidates) > 20:
            print(f"  ... and {len(candidates) - 20} more\n")
            
    finally:
        await close_db()


async def run_enrichment(limit: int, concurrency: int = 6, delay: float = 1.0):
    """Run the enrichment workflow."""
    await init_db()
    
    try:
        service = Service()
        result = await service.enrich_cloudbeds_hotels(limit=limit, concurrency=concurrency, delay=delay)
        
        print("\n" + "=" * 60)
        print("ENRICHMENT COMPLETE")
        print("=" * 60)
        print(f"  Processed:  {result.processed}")
        print(f"  Enriched:   {result.enriched}")
        print(f"  Failed:     {result.failed}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Cloudbeds hotel enrichment")
    parser.add_argument("--status", action="store_true", help="Show enrichment status")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enriched")
    parser.add_argument("--limit", type=int, default=100, help="Max hotels to process")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent browser contexts")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between batches (rate limiting)")
    
    args = parser.parse_args()
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit))
    else:
        asyncio.run(run_enrichment(args.limit, args.concurrency, args.delay))


if __name__ == "__main__":
    main()
