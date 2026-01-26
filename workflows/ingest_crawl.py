#!/usr/bin/env python3
"""
Ingest crawled booking engine URLs into the database.

Reads text files containing slugs/URLs (one per line) from S3 or local files
and ingests them into the hotels table.

Uses the CrawlIngestor from services/ingestor/ which:
- Inserts hotels with placeholder names ("Unknown (slug)")
- Links to booking_engines table
- SQS enrichment workers later scrape real names from live pages

Usage:
    # Ingest all deduped files from S3 (recommended)
    uv run python -m workflows.ingest_crawl --s3 sadie-gtm --prefix crawl-data/

    # Ingest from local directory
    uv run python -m workflows.ingest_crawl --dir data/crawl --all

    # Single engine from local file
    uv run python -m workflows.ingest_crawl --file data/crawl/cloudbeds.txt --engine cloudbeds

    # Auto-enqueue for SQS enrichment after ingestion
    uv run python -m workflows.ingest_crawl --s3 sadie-gtm --prefix crawl-data/ --enqueue
"""

import argparse
import asyncio
from pathlib import Path

from loguru import logger


def main():
    parser = argparse.ArgumentParser(
        description="Ingest crawled booking engine URLs into database"
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--file", "-f",
        type=str,
        help="Path to text file with slugs/URLs (one per line)"
    )
    input_group.add_argument(
        "--dir", "-d",
        type=str,
        help="Directory containing crawl files (use with --all)"
    )
    input_group.add_argument(
        "--s3",
        type=str,
        metavar="BUCKET",
        help="S3 bucket containing crawl files"
    )
    
    # Engine selection
    parser.add_argument(
        "--engine", "-e",
        type=str,
        choices=["cloudbeds", "mews", "rms", "siteminder"],
        help="Booking engine name (required with --file, auto-detected otherwise)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all recognized files in directory"
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="crawl-data/",
        help="S3 key prefix (default: crawl-data/)"
    )
    
    # Options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without writing to DB"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of slugs to process (for testing)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for database inserts (default: 500)"
    )
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="Auto-enqueue newly inserted hotels for SQS name enrichment"
    )
    parser.add_argument(
        "--enqueue-limit",
        type=int,
        default=10000,
        help="Max hotels to enqueue for enrichment (default: 10000)"
    )
    
    args = parser.parse_args()
    
    # Validate args
    if args.file and not args.engine:
        parser.error("--engine is required when using --file")
    
    if args.dir and not args.all:
        parser.error("--all is required when using --dir")
    
    asyncio.run(run_ingest(args))


async def run_ingest(args):
    """Run the ingestion."""
    from db.client import init_db
    from services.ingestor.ingestors.crawl import CrawlIngestor
    
    # Collect ingestors to run
    ingestors = []
    
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return
        ingestors.append(CrawlIngestor(engine=args.engine, file_path=str(file_path)))
    
    elif args.dir:
        dir_path = Path(args.dir)
        if not dir_path.exists():
            logger.error(f"Directory not found: {dir_path}")
            return
        ingestors = CrawlIngestor.from_directory(str(dir_path))
    
    elif args.s3:
        logger.info(f"Loading crawl files from s3://{args.s3}/{args.prefix}")
        ingestors = await CrawlIngestor.from_s3(
            bucket=args.s3,
            prefix=args.prefix,
            cache_dir="data/crawl_cache",
        )
    
    if not ingestors:
        logger.error("No files to process")
        return
    
    logger.info(f"Found {len(ingestors)} engine(s) to process:")
    for ing in ingestors:
        slug_count = len(ing.slugs) if ing.slugs else "file"
        logger.info(f"  - {ing.engine}: {slug_count} slugs")
    
    # Dry run - just show stats
    if args.dry_run:
        logger.info("Dry run complete - no changes made")
        return
    
    # Initialize database
    await init_db()
    
    # Process each ingestor
    total_stats = {
        "files": 0,
        "records_parsed": 0,
        "records_saved": 0,
        "duplicates_skipped": 0,
        "errors": 0,
    }
    
    for ingestor in ingestors:
        logger.info(f"\nProcessing {ingestor.engine}...")
        
        try:
            # Apply limit if specified
            if args.limit and ingestor.slugs:
                ingestor.slugs = ingestor.slugs[:args.limit]
            
            records, stats = await ingestor.ingest(
                batch_size=args.batch_size,
                upload_logs=False,
            )
            
            total_stats["files"] += 1
            total_stats["records_parsed"] += stats.records_parsed
            total_stats["records_saved"] += stats.records_saved
            total_stats["duplicates_skipped"] += stats.duplicates_skipped
            total_stats["errors"] += stats.errors
            
            logger.info(f"  Parsed: {stats.records_parsed}")
            logger.info(f"  Saved: {stats.records_saved}")
            logger.info(f"  Duplicates skipped: {stats.duplicates_skipped}")
            logger.info(f"  Errors: {stats.errors}")
            
        except Exception as e:
            logger.error(f"Failed to process {ingestor.engine}: {e}")
            import traceback
            traceback.print_exc()
            total_stats["errors"] += 1
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Engines processed: {total_stats['files']}")
    logger.info(f"Total slugs parsed: {total_stats['records_parsed']}")
    logger.info(f"Hotels saved: {total_stats['records_saved']}")
    logger.info(f"Duplicates skipped: {total_stats['duplicates_skipped']}")
    logger.info(f"Errors: {total_stats['errors']}")
    
    # Auto-enqueue for enrichment if requested
    if args.enqueue and total_stats['records_saved'] > 0:
        logger.info("\n" + "-" * 50)
        logger.info("ENQUEUEING FOR NAME ENRICHMENT")
        logger.info("-" * 50)
        
        await enqueue_for_enrichment(limit=args.enqueue_limit)
    elif total_stats['records_saved'] > 0:
        logger.info("\n" + "-" * 50)
        logger.info("Hotels inserted with placeholder names.")
        logger.info("To enqueue for name enrichment, run:")
        logger.info("  uv run python -m workflows.enrich_names_enqueue --limit 10000")


async def enqueue_for_enrichment(limit: int = 10000):
    """Enqueue newly inserted hotels for SQS name enrichment."""
    import os
    from db.client import get_conn, queries
    from infra.sqs import send_messages_batch, get_queue_attributes
    
    queue_url = os.getenv("SQS_NAME_ENRICHMENT_QUEUE_URL", "")
    if not queue_url:
        logger.warning("SQS_NAME_ENRICHMENT_QUEUE_URL not set - skipping enqueue")
        return
    
    # Get hotels needing names
    async with get_conn() as conn:
        results = await queries.get_hotels_needing_names(conn, limit=limit)
        hotels = [dict(row) for row in results]
    
    if not hotels:
        logger.info("No hotels found needing name enrichment")
        return
    
    logger.info(f"Found {len(hotels)} hotels needing names")
    
    # Group by engine for logging
    by_engine = {}
    for h in hotels:
        eng = h["engine_name"]
        by_engine[eng] = by_engine.get(eng, 0) + 1
    for eng, count in sorted(by_engine.items()):
        logger.info(f"  {eng}: {count}")
    
    # Create messages
    messages = []
    for h in hotels:
        messages.append({
            "hotel_id": h["id"],
            "booking_url": h["booking_url"],
            "slug": h["slug"],
            "engine": h["engine_name"],
        })
    
    # Send to SQS
    sent = send_messages_batch(queue_url, messages)
    logger.info(f"Sent {sent} messages to SQS")
    
    # Show queue stats
    attrs = get_queue_attributes(queue_url)
    logger.info(f"Queue: {attrs.get('ApproximateNumberOfMessages', 0)} messages pending")


if __name__ == "__main__":
    main()
