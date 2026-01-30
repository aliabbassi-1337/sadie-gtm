#!/usr/bin/env python3
"""Discover booking engine slugs from web archives.

Queries Wayback Machine and Common Crawl for booking URLs to extract slugs.

Usage:
    python -m workflows.discover_archive_slugs
    python -m workflows.discover_archive_slugs --engine rms
    python -m workflows.discover_archive_slugs --engine rms --output slugs.json
    python -m workflows.discover_archive_slugs --engine rms --s3-bucket sadie-gtm
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import boto3

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.archive.discovery import (
    ArchiveSlugDiscovery,
    BOOKING_ENGINE_PATTERNS,
    discover_slugs_for_engine,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# S3 configuration
S3_BUCKET = "sadie-gtm"
S3_PREFIX = "crawl-data/"
S3_REGION = "eu-north-1"


def save_to_s3(engine: str, slugs: list, source: str) -> str:
    """Save slugs to S3 as txt file.
    
    Format matches existing files: one URL path per line.
    Returns the S3 key.
    """
    if not slugs:
        return ""
    
    # Build URL paths (without https://)
    lines = []
    for slug_data in slugs:
        slug = slug_data["slug"]
        # Use standardized URL format
        if engine in ("rms", "rms_ibe"):
            url_path = f"bookings.rmscloud.com/search/index/{slug}"
        elif engine == "cloudbeds":
            url_path = f"hotels.cloudbeds.com/reservation/{slug}"
        elif engine == "mews":
            url_path = f"app.mews.com/distributor/{slug}"
        elif engine == "siteminder":
            url_path = f"book-directonline.com/properties/{slug}"
        else:
            url_path = slug_data.get("source_url", slug).replace("https://", "").replace("http://", "")
        lines.append(url_path)
    
    # Deduplicate lines (case-insensitive)
    seen = set()
    unique_lines = []
    for line in lines:
        line_lower = line.lower()
        if line_lower not in seen:
            seen.add(line_lower)
            unique_lines.append(line)
    
    content = "\n".join(sorted(unique_lines))
    
    # Upload to S3
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    s3_key = f"{S3_PREFIX}{engine}_{source}_{timestamp}.txt"
    
    s3_client = boto3.client("s3", region_name=S3_REGION)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=content.encode("utf-8"),
        ContentType="text/plain",
    )
    
    logger.info(f"Uploaded {len(unique_lines)} slugs to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


async def main():
    parser = argparse.ArgumentParser(
        description="Discover booking engine slugs from web archives"
    )
    parser.add_argument(
        "--engine",
        choices=[p.name for p in BOOKING_ENGINE_PATTERNS] + ["all"],
        default="all",
        help="Booking engine to query (default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (JSON format)",
    )
    parser.add_argument(
        "--s3-upload",
        action="store_true",
        help="Upload results to S3 (s3://sadie-gtm/crawl-data/)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Request timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Max results per query (default: 10000)",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Archive Slug Discovery")
    logger.info("=" * 60)

    discovery = ArchiveSlugDiscovery(
        timeout=args.timeout,
        max_results_per_query=args.limit,
    )

    if args.engine == "all":
        results = await discovery.discover_all()
    else:
        slugs = await discover_slugs_for_engine(args.engine)
        results = {args.engine: slugs}

    # Print summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("DISCOVERY RESULTS")
    logger.info("=" * 60)

    total_slugs = 0
    output_data = {}

    for engine, slugs in results.items():
        logger.info(f"\n{engine.upper()}:")
        logger.info(f"  Total unique slugs: {len(slugs)}")

        if slugs:
            # Count by source
            wayback_count = sum(1 for s in slugs if s.archive_source == "wayback")
            cc_count = sum(1 for s in slugs if s.archive_source == "commoncrawl")
            logger.info(f"  From Wayback: {wayback_count}")
            logger.info(f"  From Common Crawl: {cc_count}")

            # Show sample slugs
            logger.info(f"  Sample slugs:")
            for slug in slugs[:5]:
                logger.info(f"    - {slug.slug}")
            if len(slugs) > 5:
                logger.info(f"    ... and {len(slugs) - 5} more")

        total_slugs += len(slugs)

        # Prepare output data
        output_data[engine] = [
            {
                "slug": s.slug,
                "source_url": s.source_url,
                "archive_source": s.archive_source,
                "timestamp": s.timestamp,
            }
            for s in slugs
        ]

    logger.info("")
    logger.info(f"TOTAL UNIQUE SLUGS: {total_slugs}")

    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(
                {
                    "discovered_at": datetime.utcnow().isoformat(),
                    "engines": output_data,
                    "total_slugs": total_slugs,
                },
                f,
                indent=2,
            )
        logger.info(f"\nResults saved to: {output_path}")

    # Upload to S3 if requested
    if args.s3_upload:
        logger.info("\nUploading to S3...")
        for engine, slugs_list in output_data.items():
            if slugs_list:
                save_to_s3(engine, slugs_list, source="archive_discovery")

    return results


if __name__ == "__main__":
    asyncio.run(main())
