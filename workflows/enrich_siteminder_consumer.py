#!/usr/bin/env python3
"""SiteMinder enrichment worker - Fast API-based enrichment.

Uses the SiteMinder GraphQL API (direct-book.com) to extract hotel data.
Much faster than Playwright (~100ms vs 10s per hotel).

Usage:
    uv run python -m workflows.enrich_siteminder_consumer
    uv run python -m workflows.enrich_siteminder_consumer --limit 1000
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
from typing import List, Tuple
from loguru import logger

from db.client import init_db, close_db, get_conn, queries
from lib.siteminder.api_client import SiteMinderClient, extract_channel_code


async def get_pending_siteminder_hotels(limit: int = 1000, retry_failed: bool = False) -> List[Tuple[int, str, str]]:
    """Get SiteMinder hotels needing enrichment.
    
    Args:
        limit: Max hotels to return
        retry_failed: If True, include previously failed hotels (status=-1)
    
    Returns list of (hotel_id, booking_url, current_name).
    """
    async with get_conn() as conn:
        status_filter = "IN (0, -1)" if retry_failed else "= 0"
        results = await conn.fetch(f'''
            SELECT h.id, hbe.booking_url, h.name
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
            WHERE be.name = 'SiteMinder'
              AND hbe.enrichment_status {status_filter}
              AND hbe.booking_url IS NOT NULL
              AND hbe.booking_url LIKE '%direct-book.com%'
            ORDER BY h.id
            LIMIT $1
        ''', limit)
        
        return [(r['id'], r['booking_url'], r['name']) for r in results]


async def batch_update_enrichment(
    updates: List[Tuple[int, str, int]],  # (hotel_id, name, status)
) -> int:
    """Batch update hotel names and enrichment status.
    
    Returns number of updated records.
    """
    if not updates:
        return 0
    
    async with get_conn() as conn:
        # Update hotel names
        name_updates = [(h_id, name) for h_id, name, _ in updates if name]
        if name_updates:
            await conn.executemany('''
                UPDATE sadie_gtm.hotels
                SET name = $2, updated_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND (name IS NULL OR name = '' OR name LIKE 'Unknown%')
            ''', name_updates)
        
        # Update enrichment status
        status_updates = [(h_id, status) for h_id, _, status in updates]
        await conn.executemany('''
            UPDATE sadie_gtm.hotel_booking_engines hbe
            SET enrichment_status = $2, last_enrichment_attempt = CURRENT_TIMESTAMP
            FROM sadie_gtm.booking_engines be
            WHERE hbe.hotel_id = $1
              AND hbe.booking_engine_id = be.id
              AND be.name = 'SiteMinder'
        ''', status_updates)
        
        return len(updates)


async def run(
    limit: int = 1000,
    concurrency: int = 20,
    batch_size: int = 50,
    retry_failed: bool = False,
    use_brightdata: bool = False,
):
    """Run SiteMinder enrichment."""
    await init_db()
    
    try:
        # Get pending hotels
        hotels = await get_pending_siteminder_hotels(limit, retry_failed=retry_failed)
        
        if not hotels:
            logger.info("No SiteMinder hotels pending enrichment")
            return
        
        logger.info(f"Found {len(hotels)} SiteMinder hotels to enrich")
        if use_brightdata:
            logger.info("Using Brightdata proxy for requests")
        
        # Stats
        enriched = 0
        failed = 0
        updates = []
        
        async with SiteMinderClient(use_brightdata=use_brightdata) as client:
            semaphore = asyncio.Semaphore(concurrency)
            
            async def process_hotel(hotel_id: int, booking_url: str, current_name: str):
                nonlocal enriched, failed
                
                async with semaphore:
                    data = await client.get_hotel_data_from_url(booking_url)
                    
                    if data and data.name:
                        # Success - got name
                        needs_name = not current_name or current_name.startswith('Unknown')
                        name_to_update = data.name if needs_name else None
                        updates.append((hotel_id, name_to_update, 1))
                        enriched += 1
                    else:
                        # Failed - no data
                        updates.append((hotel_id, None, -1))
                        failed += 1
            
            # Process in batches
            for i in range(0, len(hotels), batch_size):
                batch = hotels[i:i + batch_size]
                
                # Process batch concurrently
                await asyncio.gather(*[
                    process_hotel(h_id, url, name)
                    for h_id, url, name in batch
                ])
                
                # Flush updates
                if updates:
                    await batch_update_enrichment(updates)
                    updates = []
                
                # Progress
                processed = min(i + batch_size, len(hotels))
                logger.info(f"Progress: {processed}/{len(hotels)} (enriched: {enriched}, failed: {failed})")
        
        # Final flush
        if updates:
            await batch_update_enrichment(updates)
        
        logger.info("=" * 50)
        logger.info("SITEMINDER ENRICHMENT COMPLETE")
        logger.info("=" * 50)
        logger.info(f"Total processed: {len(hotels)}")
        logger.info(f"Enriched: {enriched}")
        logger.info(f"Failed: {failed}")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Enrich SiteMinder hotels via API")
    parser.add_argument("--limit", "-l", type=int, default=1000, help="Max hotels to process")
    parser.add_argument("--concurrency", "-c", type=int, default=20, help="Concurrent API calls")
    parser.add_argument("--batch-size", "-b", type=int, default=50, help="Batch size for DB updates")
    parser.add_argument("--retry-failed", "-r", action="store_true", help="Retry previously failed hotels")
    parser.add_argument("--brightdata", action="store_true", help="Use Brightdata proxy for requests")
    
    args = parser.parse_args()
    
    asyncio.run(run(
        limit=args.limit,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        retry_failed=args.retry_failed,
        use_brightdata=args.brightdata,
    ))


if __name__ == "__main__":
    main()
