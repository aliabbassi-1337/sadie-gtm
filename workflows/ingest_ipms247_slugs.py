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
    concurrency: int = 20,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """
    Ingest IPMS247 hotels from slugs.
    
    Returns (total, scraped, saved)
    """
    scraper = IPMS247Scraper(use_proxy=False)  # Direct requests work
    semaphore = asyncio.Semaphore(concurrency)
    
    scraped = 0
    saved = 0
    errors = 0
    
    async def process_slug(slug: str) -> Optional[dict]:
        nonlocal scraped, errors
        async with semaphore:
            try:
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
    
    # Save to database
    records = []
    for r in valid_results:
        data = r["data"]
        slug = r["slug"]
        
        # Build booking URL
        booking_url = f"https://live.ipms247.com/booking/book-rooms-{slug}"
        
        # Prepare record for batch insert
        records.append((
            data.name or f"IPMS247 Hotel {slug}",  # name
            "ipms247_archive",  # source
            slug,  # external_id
            booking_url,  # booking_url
            IPMS247_ENGINE_ID,  # booking_engine_id
            slug,  # engine_property_id
            "ipms247_archive",  # external_id_type
            "archive_discovery",  # detection_method
        ))
    
    if records:
        try:
            saved = await repo.batch_insert_crawled_hotels(records)
            logger.info(f"Saved {saved} hotels to database")
        except Exception as e:
            logger.error(f"Failed to save hotels: {e}")
    
    return len(slugs), len(valid_results), saved


async def main():
    parser = argparse.ArgumentParser(description="Ingest IPMS247 hotels from discovered slugs")
    parser.add_argument("--s3-key", type=str, help="S3 key for slug list (e.g., crawl-data/ipms247_archive_discovery_20260202.txt)")
    parser.add_argument("--input", type=str, help="Local file with slugs (one per line)")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent scrapes (default: 20)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of slugs (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
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
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
