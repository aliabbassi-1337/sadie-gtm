#!/usr/bin/env python3
"""Discover booking engine slugs from web archives.

Thin workflow - delegates to IngestorService.

Usage:
    python -m workflows.discover_archive_slugs
    python -m workflows.discover_archive_slugs --engine rms
    python -m workflows.discover_archive_slugs --engine rms --s3-upload
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import logging

from lib.archive.discovery import BOOKING_ENGINE_PATTERNS
from services.ingestor.service import Service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


async def main():
    parser = argparse.ArgumentParser(description="Discover booking engine slugs from archives")
    parser.add_argument(
        "--engine",
        choices=[p.name for p in BOOKING_ENGINE_PATTERNS] + ["all"],
        default="all",
        help="Booking engine to query (default: all)",
    )
    parser.add_argument("--output", type=str, help="Output file path (JSON)")
    parser.add_argument("--s3-upload", action="store_true", help="Upload to S3")
    parser.add_argument("--timeout", type=float, default=60.0, help="Request timeout")
    parser.add_argument("--limit", type=int, default=10000, help="Max results per query")
    args = parser.parse_args()

    service = Service()
    results = await service.discover_archive_slugs(
        engine=args.engine,
        s3_upload=args.s3_upload,
        output_path=args.output,
        timeout=args.timeout,
        max_results=args.limit,
    )

    print(f"\n{'=' * 50}")
    print("DISCOVERY SUMMARY")
    print(f"{'=' * 50}")
    for r in results:
        print(f"{r.engine}: {r.total_slugs} slugs")
        if r.s3_key:
            print(f"  -> s3://sadie-gtm/{r.s3_key}")


if __name__ == "__main__":
    asyncio.run(main())
