#!/usr/bin/env python3
"""
Ingest RMS hotels from a list of discovered slugs.

Reads slugs from JSON file (output of discover_archive_slugs.py) and uses
the existing RMSIngestor to scrape and save hotel data.

Usage:
    python -m workflows.ingest_rms_slugs --input data/archive_slugs.json
    python -m workflows.ingest_rms_slugs --input data/archive_slugs.json --dry-run
    python -m workflows.ingest_rms_slugs --input data/archive_slugs.json --limit 50
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import json
import signal

from loguru import logger

from db.client import init_db, close_db
from services.ingestor.ingestors.rms import RMSIngestor


def load_rms_slugs(input_path: str, limit: int = 0) -> list[str]:
    """Load RMS slugs from JSON file with case-insensitive deduplication."""
    with open(input_path) as f:
        data = json.load(f)
    
    engines = data.get("engines", {})
    
    # Combine rms and rms_ibe slugs
    all_slugs = []
    for engine_key in ["rms", "rms_ibe"]:
        if engine_key in engines:
            for item in engines[engine_key]:
                all_slugs.append(item["slug"])
    
    # Deduplicate case-insensitively
    seen = set()
    unique_slugs = []
    for slug in all_slugs:
        slug_lower = slug.lower()
        if slug_lower not in seen:
            seen.add(slug_lower)
            unique_slugs.append(slug)
    
    logger.info(f"Loaded {len(all_slugs)} slugs, {len(unique_slugs)} unique after deduplication")
    
    if limit > 0:
        unique_slugs = unique_slugs[:limit]
        logger.info(f"Limited to {len(unique_slugs)} slugs")
    
    return unique_slugs


def main():
    parser = argparse.ArgumentParser(description="Ingest RMS hotels from discovered slugs")
    parser.add_argument("--input", type=str, required=True, help="Input JSON file from discover_archive_slugs.py")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent browser pages (default: 6)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of slugs to process (0 = all)")
    parser.add_argument("--source", type=str, default="rms_archive_discovery", help="Source name for hotels")
    args = parser.parse_args()
    
    asyncio.run(run(args))


async def run(args):
    logger.info(f"RMS Slug Ingestion")
    logger.info(f"Input: {args.input}")
    logger.info(f"Concurrency: {args.concurrency}")
    logger.info(f"Dry run: {args.dry_run}")
    
    # Load slugs
    slugs = load_rms_slugs(args.input, args.limit)
    if not slugs:
        logger.warning("No slugs to process")
        return
    
    await init_db()
    ingestor = RMSIngestor()
    
    # Handle shutdown
    signal.signal(signal.SIGTERM, lambda s, f: ingestor.request_shutdown())
    signal.signal(signal.SIGINT, lambda s, f: ingestor.request_shutdown())
    
    try:
        result = await ingestor.ingest_slugs(
            slugs=slugs,
            concurrency=args.concurrency,
            dry_run=args.dry_run,
            source_name=args.source,
        )
        
        print(f"\n{'=' * 50}")
        print("INGESTION SUMMARY")
        print(f"{'=' * 50}")
        print(f"Slugs provided: {len(slugs)}")
        print(f"Scanned: {result.total_scanned}")
        print(f"Found: {result.hotels_found}")
        print(f"Saved: {result.hotels_saved}")
        if args.dry_run:
            print("\n(Dry run - no data saved)")
    finally:
        await close_db()


if __name__ == "__main__":
    main()
