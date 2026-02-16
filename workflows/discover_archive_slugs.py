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

from lib.archive.discovery import BOOKING_ENGINE_PATTERNS, _get_brightdata_proxy_url
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
        default=120,
        help="Number of Common Crawl historical indexes to query (default: 120 = all)",
    )
    parser.add_argument(
        "--skip-db-dedupe",
        action="store_true",
        help="Skip deduplication against database (not recommended)",
    )
    parser.add_argument(
        "--no-alienvault",
        action="store_true",
        help="Disable AlienVault OTX queries",
    )
    parser.add_argument(
        "--no-urlscan",
        action="store_true",
        help="Disable URLScan.io queries",
    )
    parser.add_argument(
        "--no-virustotal",
        action="store_true",
        help="Disable VirusTotal queries",
    )
    parser.add_argument(
        "--no-arquivo",
        action="store_true",
        help="Disable Arquivo.pt queries",
    )
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="Disable Brightdata proxy (auto-detected from env vars)",
    )
    args = parser.parse_args()

    # Auto-detect Brightdata proxy unless disabled
    proxy_url = None
    if not args.no_proxy:
        proxy_url = _get_brightdata_proxy_url()
        if proxy_url:
            logging.getLogger(__name__).info("Using Brightdata datacenter proxy")
        else:
            logging.getLogger(__name__).info("No Brightdata proxy configured, using direct connections")

    service = Service()
    results = await service.discover_archive_slugs(
        engine=args.engine,
        s3_upload=args.s3_upload,
        output_path=args.output,
        timeout=args.timeout,
        max_results=args.limit,
        cc_index_count=args.cc_indexes,
        dedupe_from_db=not args.skip_db_dedupe,
        enable_alienvault=not args.no_alienvault,
        enable_urlscan=not args.no_urlscan,
        enable_virustotal=not args.no_virustotal,
        enable_arquivo=not args.no_arquivo,
        proxy_url=proxy_url,
    )

    print(f"\n{'=' * 50}")
    print("DISCOVERY SUMMARY (NEW SLUGS ONLY)")
    print(f"{'=' * 50}")
    total = 0
    for r in results:
        parts = [f"wayback: {r.wayback_count}", f"cc: {r.commoncrawl_count}"]
        if r.alienvault_count:
            parts.append(f"alienvault: {r.alienvault_count}")
        if r.urlscan_count:
            parts.append(f"urlscan: {r.urlscan_count}")
        if r.virustotal_count:
            parts.append(f"virustotal: {r.virustotal_count}")
        if r.arquivo_count:
            parts.append(f"arquivo: {r.arquivo_count}")
        breakdown = ", ".join(parts)
        print(f"{r.engine}: {r.total_slugs} new slugs ({breakdown})")
        if r.s3_key:
            print(f"  -> s3://sadie-gtm/{r.s3_key}")
        total += r.total_slugs

    print(f"\nTOTAL NEW SLUGS: {total}")


if __name__ == "__main__":
    asyncio.run(main())
