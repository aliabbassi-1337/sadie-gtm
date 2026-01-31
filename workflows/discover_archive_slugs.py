#!/usr/bin/env python3
"""Discover booking engine slugs from web archives.

Thin workflow - delegates to IngestorService.

Queries Wayback Machine and multiple historical Common Crawl indexes,
then deduplicates against existing slugs in the database.

Usage:
    python -m workflows.discover_archive_slugs
    python -m workflows.discover_archive_slugs --engine rms
    python -m workflows.discover_archive_slugs --engine rms --s3-upload
    python -m workflows.discover_archive_slugs --cc-indexes 20 --s3-upload
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import logging

from dotenv import load_dotenv

from lib.archive.discovery import BOOKING_ENGINE_PATTERNS
from services.ingestor.service import Service

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


async def main():
    parser = argparse.ArgumentParser(
        description="Discover booking engine slugs from archives"
    )
    parser.add_argument(
        "--engine",
        choices=[p.name for p in BOOKING_ENGINE_PATTERNS] + ["all"],
        default="all",
        help="Booking engine to query (default: all)",
    )
    parser.add_argument("--output", type=str, help="Output file path (JSON)")
    parser.add_argument("--s3-upload", action="store_true", help="Upload to S3")
    parser.add_argument(
        "--timeout", type=float, default=120.0, help="Request timeout (default: 120s)"
    )
    parser.add_argument(
        "--limit", type=int, default=50000, help="Max results per query (default: 50000)"
    )
    parser.add_argument(
        "--cc-indexes",
        type=int,
        default=12,
        help="Number of Common Crawl historical indexes to query (default: 12)",
    )
    parser.add_argument(
        "--skip-db-dedupe",
        action="store_true",
        help="Skip deduplication against database (not recommended)",
    )
    args = parser.parse_args()

    service = Service()
    results = await service.discover_archive_slugs(
        engine=args.engine,
        s3_upload=args.s3_upload,
        output_path=args.output,
        timeout=args.timeout,
        max_results=args.limit,
        cc_index_count=args.cc_indexes,
        dedupe_from_db=not args.skip_db_dedupe,
    )

    print(f"\n{'=' * 50}")
    print("DISCOVERY SUMMARY (NEW SLUGS ONLY)")
    print(f"{'=' * 50}")
    total = 0
    for r in results:
        print(f"{r.engine}: {r.total_slugs} new slugs")
        if r.s3_key:
            print(f"  -> s3://sadie-gtm/{r.s3_key}")
        total += r.total_slugs

    print(f"\nTOTAL NEW SLUGS: {total}")


if __name__ == "__main__":
    asyncio.run(main())
