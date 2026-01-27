#!/usr/bin/env python3
"""
Geocoding Workflow - Enrich hotels with location data using Serper Places API.

For crawl data hotels that have names but no city/state/coordinates,
search Google Places by hotel name to find their location and contact info.

Usage:
    # Check how many need geocoding
    uv run python -m workflows.geocode_by_name --status

    # Geocode all crawl hotels
    uv run python -m workflows.geocode_by_name --limit 1000

    # Geocode specific source
    uv run python -m workflows.geocode_by_name --source cloudbeds_crawl --limit 500

    # Dry run (show what would be processed)
    uv run python -m workflows.geocode_by_name --limit 100 --dry-run
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service as EnrichmentService
from infra import slack


async def run_status(source: str = None):
    """Show geocoding status."""
    await init_db()
    try:
        service = EnrichmentService()
        pending = await service.get_hotels_needing_geocoding_count(source=source)
        
        if source:
            logger.info(f"Hotels needing geocoding (source={source}): {pending}")
        else:
            logger.info(f"Hotels needing geocoding (all sources): {pending}")
            
    finally:
        await close_db()


async def run_dry_run(limit: int, source: str = None):
    """Show what would be processed without making API calls."""
    await init_db()
    try:
        service = EnrichmentService()
        hotels = await service.get_hotels_needing_geocoding(limit=limit, source=source)
        
        logger.info(f"Would geocode {len(hotels)} hotels:")
        
        # Group by engine
        by_engine = {}
        for h in hotels:
            eng = h.engine_name or h.source or "unknown"
            by_engine[eng] = by_engine.get(eng, 0) + 1
        
        for eng, count in sorted(by_engine.items(), key=lambda x: -x[1]):
            logger.info(f"  {eng}: {count}")
        
        # Show sample
        logger.info("\nSample hotels:")
        for h in hotels[:10]:
            logger.info(f"  [{h.id}] {h.name[:50]}...")
            
    finally:
        await close_db()


async def run_geocode(
    limit: int,
    source: str = None,
    concurrency: int = 10,
    notify: bool = True,
):
    """Run geocoding enrichment."""
    await init_db()
    
    try:
        service = EnrichmentService()
        
        # Show what we're about to process
        pending = await service.get_hotels_needing_geocoding_count(source=source)
        logger.info(f"Hotels needing geocoding: {pending}")
        logger.info(f"Processing limit: {limit}")
        if source:
            logger.info(f"Filtering by source: {source}")
        
        # Run geocoding
        stats = await service.geocode_hotels_by_name(
            limit=limit,
            source=source,
            concurrency=concurrency,
        )
        
        # Log results
        logger.info("")
        logger.info("=" * 60)
        logger.info("GEOCODING RESULTS")
        logger.info("=" * 60)
        logger.info(f"  Total processed: {stats['total']}")
        logger.info(f"  Enriched:        {stats['enriched']}")
        logger.info(f"  Not found:       {stats['not_found']}")
        logger.info(f"  Errors:          {stats['errors']}")
        logger.info(f"  API calls:       {stats['api_calls']}")
        logger.info(f"  Est. cost:       ${stats['api_calls'] * 0.001:.2f}")
        
        # Slack notification
        if notify and stats["enriched"] > 0:
            slack.send_message(
                f"*Geocoding Complete*\n"
                f"• Enriched: {stats['enriched']}\n"
                f"• Not found: {stats['not_found']}\n"
                f"• API calls: {stats['api_calls']}\n"
                f"• Est. cost: ${stats['api_calls'] * 0.001:.2f}"
            )
        
        return stats
        
    except Exception as e:
        logger.error(f"Geocoding failed: {e}")
        if notify:
            slack.send_error("Geocoding", str(e))
        raise
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Geocode hotels using Serper Places API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Check status
    uv run python -m workflows.geocode_by_name --status

    # Geocode 1000 hotels
    uv run python -m workflows.geocode_by_name --limit 1000

    # Geocode only Cloudbeds crawl data
    uv run python -m workflows.geocode_by_name --source cloudbeds_crawl --limit 500

    # Dry run
    uv run python -m workflows.geocode_by_name --limit 100 --dry-run

Environment:
    SERPER_API_KEY - Required. Get from https://serper.dev
    
Cost:
    ~$0.001 per API call. 1000 hotels = ~$1.00
        """
    )
    
    parser.add_argument("-l", "--limit", type=int, default=100,
                        help="Max hotels to process (default: 100)")
    parser.add_argument("-s", "--source", type=str,
                        help="Filter by source (e.g., 'cloudbeds_crawl', 'crawl')")
    parser.add_argument("--status", action="store_true",
                        help="Show geocoding status only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed without API calls")
    parser.add_argument("--concurrency", type=int, default=10,
                        help="Max concurrent API requests (default: 10)")
    parser.add_argument("--no-notify", action="store_true",
                        help="Disable Slack notification")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(run_status(args.source))
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit, args.source))
    else:
        asyncio.run(run_geocode(
            limit=args.limit,
            source=args.source,
            concurrency=args.concurrency,
            notify=not args.no_notify,
        ))


if __name__ == "__main__":
    main()
