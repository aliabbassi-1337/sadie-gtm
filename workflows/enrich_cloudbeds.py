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
from services.enrichment import repo
from services.enrichment.cloudbeds_service import CloudbedsEnrichmentService


async def run_status():
    """Show enrichment status."""
    await init_db()
    
    try:
        count = await repo.get_cloudbeds_hotels_needing_enrichment_count()
        total = await repo.get_cloudbeds_hotels_total_count()
        
        print("\n" + "=" * 60)
        print("CLOUDBEDS ENRICHMENT STATUS")
        print("=" * 60)
        print(f"  Total Cloudbeds hotels:     {total:,}")
        print(f"  Needing enrichment:         {count:,}")
        print(f"  Already enriched:           {total - count:,}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


async def run_dry_run(limit: int):
    """Show what would be enriched without making changes."""
    await init_db()
    
    try:
        candidates = await repo.get_cloudbeds_hotels_needing_enrichment(limit=limit)
        
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


async def run_enrichment(limit: int, concurrency: int = 3):
    """Run the enrichment workflow."""
    await init_db()
    
    try:
        service = CloudbedsEnrichmentService()
        result = await service.enrich_hotels(limit=limit, concurrency=concurrency)
        
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
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit))
    else:
        asyncio.run(run_enrichment(args.limit, args.concurrency))


if __name__ == "__main__":
    main()
