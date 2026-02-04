#!/usr/bin/env python3
"""
Backfill RMS hotels - re-enrich ALL RMS hotels regardless of current state.

Usage:
    # Check count
    uv run python -m workflows.backfill_rms --status
    
    # Dry run
    uv run python -m workflows.backfill_rms --dry-run --limit 20
    
    # Run backfill for US only
    uv run python -m workflows.backfill_rms --country "United States" --limit 1000
    
    # Run all
    uv run python -m workflows.backfill_rms --limit 0
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.enrichment.rms_repo import RMSRepo


async def get_all_rms_hotels(limit: int = 0, country: str = None):
    """Get ALL RMS hotels for backfill."""
    async with get_conn() as conn:
        sql = '''
            SELECT h.id as hotel_id, h.name, h.city, h.state, h.country,
                   hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%rms%'
              AND hbe.booking_url IS NOT NULL
              AND hbe.booking_url != ''
              AND h.status >= 0
        '''
        if country:
            sql += f" AND h.country = '{country}'"
        sql += ' ORDER BY h.id'
        if limit > 0:
            sql += f' LIMIT {limit}'
        return await conn.fetch(sql)


async def get_counts():
    """Get counts of RMS hotels."""
    async with get_conn() as conn:
        total = await conn.fetchval('''
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%rms%'
              AND hbe.booking_url IS NOT NULL
              AND h.status >= 0
        ''')
        
        missing_city = await conn.fetchval('''
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%rms%'
              AND hbe.booking_url IS NOT NULL
              AND h.status >= 0
              AND (h.city IS NULL OR h.city = '')
        ''')
        
        us_total = await conn.fetchval('''
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%rms%'
              AND hbe.booking_url IS NOT NULL
              AND h.status >= 0
              AND h.country = 'United States'
        ''')
        
        us_missing_city = await conn.fetchval('''
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%rms%'
              AND hbe.booking_url IS NOT NULL
              AND h.status >= 0
              AND h.country = 'United States'
              AND (h.city IS NULL OR h.city = '')
        ''')
        
        return {
            "total": total,
            "missing_city": missing_city,
            "us_total": us_total,
            "us_missing_city": us_missing_city,
        }


async def run_status():
    """Show backfill status."""
    await init_db()
    try:
        counts = await get_counts()
        
        print("\n" + "=" * 60)
        print("RMS BACKFILL STATUS")
        print("=" * 60)
        print(f"  Total RMS hotels:       {counts['total']:,}")
        print(f"  Missing city:           {counts['missing_city']:,}")
        print(f"  Have city:              {counts['total'] - counts['missing_city']:,}")
        print()
        print(f"  US Total:               {counts['us_total']:,}")
        print(f"  US Missing city:        {counts['us_missing_city']:,}")
        print("=" * 60 + "\n")
    finally:
        await close_db()


async def run_dry_run(limit: int, country: str = None):
    """Show what would be enriched."""
    await init_db()
    try:
        hotels = await get_all_rms_hotels(limit, country)
        
        label = f" (country={country})" if country else ""
        print(f"\n=== DRY RUN: Would enrich {len(hotels)} RMS hotels{label} ===\n")
        
        for h in hotels[:20]:
            missing = []
            if not h['city']:
                missing.append('city')
            if not h['state']:
                missing.append('state')
            
            status = f"missing: {', '.join(missing)}" if missing else "complete"
            name = (h['name'] or 'NO NAME')[:35]
            print(f"  ID={h['hotel_id']:>6}: {name:35} | {status}")
        
        if len(hotels) > 20:
            print(f"\n  ... and {len(hotels) - 20} more\n")
    finally:
        await close_db()


async def run_backfill(limit: int, concurrency: int, country: str = None):
    """Run the backfill."""
    from lib.rms.api_client import AdaptiveRMSApiClient
    import re
    
    await init_db()
    
    try:
        hotels = await get_all_rms_hotels(limit, country)
        
        if not hotels:
            logger.info("No RMS hotels to backfill")
            return
        
        label = f" (country={country})" if country else ""
        logger.info(f"Backfilling {len(hotels)} RMS hotels{label} (concurrency={concurrency})")
        
        repo = RMSRepo()
        semaphore = asyncio.Semaphore(concurrency)
        results_buffer = []
        failed_urls = []
        processed = 0
        
        async with AdaptiveRMSApiClient() as api_client:
            async def process_hotel(hotel):
                nonlocal processed
                
                async with semaphore:
                    hotel_id = hotel['hotel_id']
                    url = hotel['booking_url']
                    if not url.startswith("http"):
                        url = f"https://{url}"
                    
                    # Parse slug and server from URL
                    slug = None
                    server = "bookings.rmscloud.com"
                    
                    match = re.search(r'/Search/Index/([^/]+)/\d+/?', url)
                    if match:
                        slug = match.group(1)
                        if "bookings12" in url:
                            server = "bookings12.rmscloud.com"
                        elif "bookings10" in url:
                            server = "bookings10.rmscloud.com"
                        elif "bookings8" in url:
                            server = "bookings8.rmscloud.com"
                    else:
                        ibe_match = re.search(r'(ibe\d+\.rmscloud\.com)/(\d+)', url)
                        if ibe_match:
                            server = ibe_match.group(1)
                            slug = ibe_match.group(2)
                    
                    if not slug:
                        failed_urls.append(url)
                        processed += 1
                        return
                    
                    try:
                        data = await api_client.extract(slug, server)
                        if not data or not data.has_data():
                            data = await api_client.extract_from_html(slug, server)
                        
                        if data and data.has_data():
                            results_buffer.append({
                                "hotel_id": hotel_id,
                                "booking_url": url,
                                "name": data.name,
                                "address": data.address,
                                "city": data.city,
                                "state": data.state,
                                "country": data.country,
                                "phone": data.phone,
                                "email": data.email,
                                "website": data.website,
                                "latitude": data.latitude,
                                "longitude": data.longitude,
                            })
                        else:
                            failed_urls.append(url)
                    except Exception as e:
                        logger.debug(f"Error enriching {hotel_id}: {e}")
                        failed_urls.append(url)
                    
                    processed += 1
                    if processed % 100 == 0:
                        logger.info(f"Progress: {processed}/{len(hotels)} ({len(results_buffer)} enriched, {len(failed_urls)} failed)")
            
            # Process in batches
            batch_size = 200
            for i in range(0, len(hotels), batch_size):
                batch = hotels[i:i + batch_size]
                await asyncio.gather(*[process_hotel(h) for h in batch])
                
                # Flush to DB (force_overwrite=True to overwrite existing data)
                if results_buffer:
                    updated = await repo.batch_update_enrichment(results_buffer, [], force_overwrite=True)
                    logger.info(f"Batch update: {updated} hotels written to DB")
                    results_buffer = []
            
            # Final flush
            if results_buffer:
                updated = await repo.batch_update_enrichment(results_buffer, [], force_overwrite=True)
                logger.info(f"Final batch: {updated} hotels written to DB")
        
        enriched = processed - len(failed_urls)
        print("\n" + "=" * 60)
        print("BACKFILL COMPLETE")
        print("=" * 60)
        print(f"  Total processed:  {processed}")
        print(f"  Enriched:         {enriched}")
        print(f"  Failed:           {len(failed_urls)}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Backfill RMS hotels")
    parser.add_argument("--status", action="store_true", help="Show backfill status")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enriched")
    parser.add_argument("--limit", type=int, default=0, help="Max hotels to process (0=all)")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent API requests")
    parser.add_argument("--country", type=str, help="Filter by country (e.g., 'United States')")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit or 100, args.country))
    else:
        asyncio.run(run_backfill(
            limit=args.limit,
            concurrency=args.concurrency,
            country=args.country,
        ))


if __name__ == "__main__":
    main()
