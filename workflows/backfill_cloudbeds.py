"""Backfill ALL Cloudbeds hotels with API enrichment.

Re-enriches ALL Cloudbeds hotels regardless of current state.
Uses the property_info API with Brightdata proxy.

Usage:
    # Check count
    uv run python -m workflows.backfill_cloudbeds --status
    
    # Dry run
    uv run python -m workflows.backfill_cloudbeds --dry-run --limit 20
    
    # Run backfill (default concurrency=20)
    uv run python -m workflows.backfill_cloudbeds --limit 1000
    
    # Run all with higher concurrency
    uv run python -m workflows.backfill_cloudbeds --concurrency 30
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db, get_conn
from lib.cloudbeds.api_client import CloudbedsApiClient, extract_property_code
from services.enrichment import repo


async def get_all_cloudbeds_hotels(limit: int = 0):
    """Get ALL Cloudbeds hotels for backfill."""
    async with get_conn() as conn:
        sql = '''
            SELECT h.id, h.name, h.city, h.state, h.country,
                   hbe.booking_url, hbe.engine_property_id as slug
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%cloudbeds%'
              AND hbe.booking_url IS NOT NULL
              AND hbe.booking_url != ''
              AND h.status >= 0
            ORDER BY h.id
        '''
        if limit > 0:
            sql += f' LIMIT {limit}'
        return await conn.fetch(sql)


async def get_total_count():
    """Get total count of Cloudbeds hotels."""
    async with get_conn() as conn:
        return await conn.fetchval('''
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%cloudbeds%'
              AND hbe.booking_url IS NOT NULL
              AND h.status >= 0
        ''')


async def run_status():
    """Show backfill status."""
    await init_db()
    try:
        total = await get_total_count()
        
        async with get_conn() as conn:
            missing_state = await conn.fetchval('''
                SELECT COUNT(*)
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
                WHERE be.name ILIKE '%cloudbeds%'
                  AND hbe.booking_url IS NOT NULL
                  AND h.status >= 0
                  AND (h.state IS NULL OR h.state = '')
            ''')
        
        print("\n" + "=" * 60)
        print("CLOUDBEDS BACKFILL STATUS")
        print("=" * 60)
        print(f"  Total Cloudbeds hotels:  {total:,}")
        print(f"  Missing state:           {missing_state:,}")
        print(f"  Have state:              {total - missing_state:,}")
        print("=" * 60 + "\n")
    finally:
        await close_db()


async def run_dry_run(limit: int):
    """Show what would be enriched."""
    await init_db()
    try:
        hotels = await get_all_cloudbeds_hotels(limit)
        
        print(f"\n=== DRY RUN: Would enrich {len(hotels)} hotels ===\n")
        
        for h in hotels[:20]:
            missing = []
            if not h['state']:
                missing.append('state')
            if not h['city']:
                missing.append('city')
            
            status = f"missing: {', '.join(missing)}" if missing else "complete"
            print(f"  ID={h['id']:>6}: {h['name'][:35]:35} | {status}")
        
        if len(hotels) > 20:
            print(f"\n  ... and {len(hotels) - 20} more\n")
    finally:
        await close_db()


async def run_backfill(limit: int, concurrency: int, use_brightdata: bool):
    """Run the backfill."""
    await init_db()
    
    try:
        hotels = await get_all_cloudbeds_hotels(limit)
        
        if not hotels:
            logger.info("No Cloudbeds hotels to backfill")
            return
        
        logger.info(f"Backfilling {len(hotels)} Cloudbeds hotels (concurrency={concurrency})")
        
        client = CloudbedsApiClient(use_brightdata=use_brightdata)
        logger.info(f"Using Brightdata proxy: {bool(client._proxy_url)}")
        
        semaphore = asyncio.Semaphore(concurrency)
        results_buffer = []
        failed_count = 0
        processed = 0
        
        async def process_hotel(hotel):
            nonlocal processed, failed_count
            
            async with semaphore:
                hotel_id = hotel['id']
                url = hotel['booking_url']
                code = extract_property_code(url)
                
                if not code:
                    # Bad URL - skip silently
                    failed_count += 1
                    processed += 1
                    return
                
                try:
                    # Try API first, then title fallback
                    data = await client.extract_with_fallback(code)
                    
                    if data and data.has_data():
                        results_buffer.append({
                            "hotel_id": hotel_id,
                            "name": data.name,
                            "address": data.address,
                            "city": data.city,
                            "state": data.state,
                            "country": data.country,
                            "phone": data.phone,
                            "email": data.email,
                            "lat": data.latitude,
                            "lon": data.longitude,
                        })
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.debug(f"Error enriching {hotel_id}: {e}")
                    failed_count += 1
                
                processed += 1
                if processed % 100 == 0:
                    logger.info(f"Progress: {processed}/{len(hotels)} ({len(results_buffer)} enriched, {failed_count} failed)")
        
        # Process in batches to do periodic DB updates
        batch_size = 200
        for i in range(0, len(hotels), batch_size):
            batch = hotels[i:i + batch_size]
            await asyncio.gather(*[process_hotel(h) for h in batch])
            
            # Flush to DB
            if results_buffer:
                updated = await repo.batch_update_cloudbeds_enrichment(results_buffer)
                logger.info(f"Batch update: {updated} hotels written to DB")
                results_buffer = []
        
        # Final flush
        if results_buffer:
            updated = await repo.batch_update_cloudbeds_enrichment(results_buffer)
            logger.info(f"Final batch: {updated} hotels written to DB")
        
        print("\n" + "=" * 60)
        print("BACKFILL COMPLETE")
        print("=" * 60)
        print(f"  Total processed:  {processed}")
        print(f"  Enriched:         {processed - failed_count}")
        print(f"  Failed:           {failed_count}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Backfill ALL Cloudbeds hotels")
    parser.add_argument("--status", action="store_true", help="Show backfill status")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enriched")
    parser.add_argument("--limit", type=int, default=0, help="Max hotels to process (0=all)")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent API requests")
    parser.add_argument("--no-proxy", action="store_true", help="Disable Brightdata proxy")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit or 100))
    else:
        asyncio.run(run_backfill(
            limit=args.limit,
            concurrency=args.concurrency,
            use_brightdata=not args.no_proxy,
        ))


if __name__ == "__main__":
    main()
