#!/usr/bin/env python3
"""
Ingest IPMS247 hotels from discovered slugs.

Downloads slug list from S3 or local file and scrapes hotel data using the IPMS247 scraper.

Usage:
    python -m workflows.ingest_ipms247_slugs --s3-key crawl-data/ipms247_archive_discovery_20260202.txt
    python -m workflows.ingest_ipms247_slugs --input data/ipms247_slugs.txt --limit 100
    python -m workflows.ingest_ipms247_slugs --s3-key crawl-data/ipms247_archive_discovery_20260202.txt --dry-run
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import os
from typing import Optional

import aioboto3
from loguru import logger

from db.client import init_db, close_db, get_conn
from lib.ipms247.scraper import IPMS247Scraper
from services.ingestor import repo


IPMS247_ENGINE_ID = 22  # JEHS / iPMS / Yanolja Cloud Solution


async def download_slugs_from_s3(s3_key: str) -> list[str]:
    """Download slug list from S3."""
    session = aioboto3.Session()
    async with session.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "eu-north-1"),
    ) as s3:
        response = await s3.get_object(Bucket="sadie-gtm", Key=s3_key)
        body = await response["Body"].read()
        lines = body.decode("utf-8").strip().split("\n")
        return [line.strip() for line in lines if line.strip()]


def load_slugs_from_file(path: str) -> list[str]:
    """Load slugs from local file."""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


async def get_existing_slugs() -> set[str]:
    """Get existing IPMS247 slugs from database."""
    async with get_conn() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT engine_property_id
            FROM sadie_gtm.hotel_booking_engines
            WHERE booking_engine_id = $1
              AND engine_property_id IS NOT NULL
        """, IPMS247_ENGINE_ID)
        return {r["engine_property_id"].lower() for r in rows}


async def ingest_slugs(
    slugs: list[str],
    concurrency: int = 5,
    dry_run: bool = False,
    use_playwright: bool = True,
) -> tuple[int, int, int]:
    """
    Ingest IPMS247 hotels from slugs.
    
    Args:
        slugs: List of hotel slugs to scrape
        concurrency: Number of concurrent scrapers (lower for Playwright)
        dry_run: If True, don't save to database
        use_playwright: If True, use Playwright for full data; else HTTP-only
    
    Returns (total, scraped, saved)
    """
    scraper = IPMS247Scraper()
    semaphore = asyncio.Semaphore(concurrency)
    
    scraped = 0
    saved = 0
    errors = 0
    
    async def process_slug(slug: str) -> Optional[dict]:
        nonlocal scraped, errors
        async with semaphore:
            try:
                # Use scrape() for full data (Playwright) or extract() for quick HTTP-only
                if use_playwright:
                    data = await scraper.scrape(slug)
                else:
                    data = await scraper.extract(slug)
                
                if data and data.has_data():
                    scraped += 1
                    return {
                        "slug": slug,
                        "data": data,
                    }
                else:
                    errors += 1
                    return None
            except Exception as e:
                logger.debug(f"Error scraping {slug}: {e}")
                errors += 1
                return None
    
    # Process all slugs
    logger.info(f"Scraping {len(slugs)} IPMS247 slugs (concurrency={concurrency})...")
    tasks = [process_slug(slug) for slug in slugs]
    results = await asyncio.gather(*tasks)
    
    # Filter successful results
    valid_results = [r for r in results if r is not None]
    logger.info(f"Scraped {len(valid_results)}/{len(slugs)} hotels successfully")
    
    if dry_run:
        logger.info("Dry run - skipping database insert")
        return len(slugs), len(valid_results), 0
    
    # Save to database with full scraped data
    records = []
    for r in valid_results:
        data = r["data"]
        slug = r["slug"]
        
        # Build booking URL
        booking_url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        
        # Prepare record for batch insert with all data
        # Order: (name, source, external_id, external_id_type, booking_engine_id, booking_url, slug, detection_method, email, phone, address, city, state, country, lat, lng)
        records.append((
            data.name or f"IPMS247 Hotel {slug}",  # $1 name
            "ipms247_archive",  # $2 source
            slug,  # $3 external_id
            "ipms247_slug",  # $4 external_id_type
            IPMS247_ENGINE_ID,  # $5 booking_engine_id
            booking_url,  # $6 booking_url
            slug,  # $7 engine_property_id (slug)
            "archive_discovery",  # $8 detection_method
            data.email,  # $9 email
            data.phone,  # $10 phone
            data.address,  # $11 address
            data.city,  # $12 city
            data.state,  # $13 state
            data.country,  # $14 country
            data.latitude,  # $15 lat
            data.longitude,  # $16 lng
        ))
    
    if records:
        try:
            saved = await repo.batch_insert_ipms247_hotels(records)
            logger.info(f"Saved {saved} hotels to database")
        except Exception as e:
            logger.error(f"Failed to save hotels: {e}")
    
    return len(slugs), len(valid_results), saved


async def main():
    parser = argparse.ArgumentParser(description="Ingest IPMS247 hotels from discovered slugs")
    parser.add_argument("--s3-key", type=str, help="S3 key for slug list (e.g., crawl-data/ipms247_archive_discovery_20260202.txt)")
    parser.add_argument("--input", type=str, help="Local file with slugs (one per line)")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent scrapes (default: 5 for Playwright)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of slugs (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--http-only", action="store_true", help="Use HTTP-only scraping (faster but less data)")
    args = parser.parse_args()
    
    if not args.s3_key and not args.input:
        parser.error("Must specify either --s3-key or --input")
    
    # Load slugs
    if args.s3_key:
        logger.info(f"Downloading slugs from s3://sadie-gtm/{args.s3_key}")
        slugs = await download_slugs_from_s3(args.s3_key)
    else:
        logger.info(f"Loading slugs from {args.input}")
        slugs = load_slugs_from_file(args.input)
    
    logger.info(f"Loaded {len(slugs)} slugs")
    
    # Initialize database
    await init_db()
    
    try:
        # Filter out existing slugs
        existing = await get_existing_slugs()
        new_slugs = [s for s in slugs if s.lower() not in existing]
        logger.info(f"After filtering existing: {len(new_slugs)} new slugs ({len(existing)} already in DB)")
        
        if args.limit > 0:
            new_slugs = new_slugs[:args.limit]
            logger.info(f"Limited to {len(new_slugs)} slugs")
        
        if not new_slugs:
            logger.info("No new slugs to process")
            return
        
        # Ingest
        total, scraped, saved = await ingest_slugs(
            new_slugs,
            concurrency=args.concurrency,
            dry_run=args.dry_run,
            use_playwright=not args.http_only,
        )
        
        print(f"\n{'=' * 50}")
        print("IPMS247 INGESTION SUMMARY")
        print(f"{'=' * 50}")
        print(f"Slugs provided: {total}")
        print(f"Successfully scraped: {scraped}")
        print(f"Saved to database: {saved}")
        if args.dry_run:
            print("\n(Dry run - no data saved)")
    finally:
        # Cleanup
        await close_db()
        # Close Playwright browser pool if used
        if not args.http_only:
            try:
                from lib.ipms247.scraper import PlaywrightPool
                pool = PlaywrightPool._instance
                if pool:
                    await pool.close()
            except:
                pass


if __name__ == "__main__":
    asyncio.run(main())
