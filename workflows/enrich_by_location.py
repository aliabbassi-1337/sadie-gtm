#!/usr/bin/env python3
"""
Coordinate-based Enrichment Workflow - Find hotel details using coordinates.

For parcel data (SF, Maryland) that has coordinates but no real hotel names,
use Serper Places API to find the actual hotel at those coordinates and
update with real name, website, phone, etc.

Usage:
    # Enrich hotels with coordinates (default limit 100)
    uv run python -m workflows.enrich_by_location --limit 100

    # Check status
    uv run python -m workflows.enrich_by_location --status

    # Dry run (don't save to database)
    uv run python -m workflows.enrich_by_location --limit 10 --dry-run
"""

import argparse
import asyncio
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.client import init_db, close_db, queries, get_conn
from services.enrichment.website_enricher import WebsiteEnricher
from infra import slack


async def run_coordinate_enrichment(
    limit: int,
    dry_run: bool = False,
    notify: bool = True,
    concurrency: int = 10,
) -> dict:
    """
    Enrich hotels with coordinates using Serper Places API.
    
    Returns stats dict with counts.
    """
    await init_db()
    
    stats = {
        "total": 0,
        "enriched": 0,
        "not_found": 0,
        "errors": 0,
        "api_calls": 0,
    }
    
    try:
        # Get pending count
        async with get_conn() as conn:
            result = await queries.get_pending_coordinate_enrichment_count(conn)
            pending = result["count"] if result else 0
        
        logger.info(f"Hotels pending coordinate enrichment: {pending}")
        
        if pending == 0:
            logger.info("No hotels pending coordinate enrichment")
            return stats
        
        # Get hotels to process
        async with get_conn() as conn:
            hotels = await queries.get_hotels_pending_coordinate_enrichment(
                conn, limit=limit
            )
        
        stats["total"] = len(hotels)
        logger.info(f"Processing {len(hotels)} hotels...")
        
        # Check API key
        api_key = os.environ.get("SERPER_API_KEY")
        if not api_key:
            logger.error("SERPER_API_KEY environment variable not set")
            return stats
        
        # Process hotels
        async with WebsiteEnricher(
            api_key=api_key,
            max_concurrent=concurrency,
            validate_urls=False,  # Skip URL validation for speed
        ) as enricher:
            
            semaphore = asyncio.Semaphore(concurrency)
            
            async def process_hotel(hotel: dict) -> dict:
                async with semaphore:
                    hotel_id = hotel["id"]
                    lat = hotel["latitude"]
                    lon = hotel["longitude"]
                    category = hotel.get("category", "hotel")
                    original_name = hotel["name"]
                    
                    # Search for hotel at coordinates
                    result = await enricher.find_by_coordinates(lat, lon, category)
                    stats["api_calls"] += 1
                    
                    if result and result.get("name"):
                        new_name = result["name"]
                        website = result.get("website")
                        phone = result.get("phone")
                        rating = result.get("rating")
                        address = result.get("address")
                        
                        logger.info(
                            f"  {original_name[:40]:<40} -> {new_name}"
                            f"{' [website]' if website else ''}"
                        )
                        
                        if not dry_run:
                            async with get_conn() as conn:
                                await queries.update_hotel_from_places(
                                    conn,
                                    hotel_id=hotel_id,
                                    name=new_name,
                                    website=website,
                                    phone=phone,
                                    rating=rating,
                                    address=address,
                                )
                        
                        return {"status": "enriched", "website": website is not None}
                    else:
                        logger.debug(f"  {original_name[:40]:<40} -> NOT FOUND")
                        return {"status": "not_found"}
            
            # Process all hotels concurrently
            tasks = [process_hotel(h) for h in hotels]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for r in results:
                if isinstance(r, Exception):
                    stats["errors"] += 1
                    logger.error(f"Error: {r}")
                elif r["status"] == "enriched":
                    stats["enriched"] += 1
                else:
                    stats["not_found"] += 1
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("COORDINATE ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels processed: {stats['total']}")
        logger.info(f"Enriched: {stats['enriched']}")
        logger.info(f"Not found: {stats['not_found']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"API calls: {stats['api_calls']}")
        logger.info(f"Estimated cost: ${stats['api_calls'] * 0.001:.2f}")
        if dry_run:
            logger.info("(DRY RUN - no changes saved)")
        logger.info("=" * 60)
        
        if notify and stats["enriched"] > 0 and not dry_run:
            slack.send_message(
                f"*Coordinate Enrichment Complete*\n"
                f"• Hotels enriched: {stats['enriched']}\n"
                f"• Not found: {stats['not_found']}\n"
                f"• API calls: {stats['api_calls']}"
            )
        
        return stats
        
    except Exception as e:
        logger.error(f"Coordinate enrichment failed: {e}")
        if notify:
            slack.send_error("Coordinate Enrichment", str(e))
        raise
    finally:
        await close_db()


async def show_status() -> None:
    """Show coordinate enrichment status."""
    await init_db()
    try:
        async with get_conn() as conn:
            result = await queries.get_pending_coordinate_enrichment_count(conn)
            pending = result["count"] if result else 0
        
        logger.info("=" * 60)
        logger.info("COORDINATE ENRICHMENT STATUS")
        logger.info("=" * 60)
        logger.info(f"Hotels pending enrichment: {pending}")
        logger.info("  (Hotels with coordinates but no website)")
        logger.info("  (Sources: sf_assessor, md_sdat_cama)")
        logger.info("=" * 60)
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Enrich hotels using coordinates (Serper Places API)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich up to 100 hotels
  uv run python -m workflows.enrich_by_location --limit 100

  # Dry run (preview without saving)
  uv run python -m workflows.enrich_by_location --limit 10 --dry-run

  # Check pending count
  uv run python -m workflows.enrich_by_location --status
        """
    )
    
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=100,
        help="Max hotels to process (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without saving to database",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pending count and exit",
    )
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent API calls (default: 10)",
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(show_status())
    else:
        logger.info(f"Running coordinate enrichment (limit={args.limit})")
        asyncio.run(run_coordinate_enrichment(
            limit=args.limit,
            dry_run=args.dry_run,
            notify=not args.no_notify,
            concurrency=args.concurrency,
        ))


if __name__ == "__main__":
    main()
