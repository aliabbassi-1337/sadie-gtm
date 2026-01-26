#!/usr/bin/env python3
"""
Ingest crawled booking engine URLs into the database.

Reads text files containing slugs/URLs (one per line) from Common Crawl
or similar sources and ingests them into the hotels table.

Uses the proper CrawlIngestor from services/ingestor/ which:
- Inserts hotels with placeholder names ("Unknown (slug)")
- Links to booking_engines table
- SQS enrichment workers later scrape real names from live pages

Usage:
    # Ingest Cloudbeds slugs
    uv run python -m workflows.ingest_crawl \
        --file "data/crawl/cloudbeds.txt" \
        --engine cloudbeds

    # Ingest all booking engine files from a directory
    uv run python -m workflows.ingest_crawl \
        --dir "data/crawl" \
        --all
        
    # Dry run to see what would be imported
    uv run python -m workflows.ingest_crawl \
        --file "data/crawl/mews.txt" \
        --engine mews \
        --dry-run
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
    
    # Engine selection
    parser.add_argument(
        "--engine", "-e",
        type=str,
        choices=["cloudbeds", "mews", "rms", "siteminder"],
        help="Booking engine name (required with --file, auto-detected with --dir)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all recognized files in directory"
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
    
    if not ingestors:
        logger.error("No files to process")
        return
    
    logger.info(f"Found {len(ingestors)} file(s) to process")
    
    # Dry run - just show stats
    if args.dry_run:
        for ingestor in ingestors:
            if ingestor.file_path:
                path = Path(ingestor.file_path)
                lines = path.read_text().strip().split("\n")
                slugs = [l.strip() for l in lines if l.strip()]
                unique_slugs = len(set(slugs))
                logger.info(f"  {path.name}: {len(slugs)} lines, {unique_slugs} unique ({ingestor.engine})")
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
            if args.limit and ingestor.file_path:
                path = Path(ingestor.file_path)
                lines = path.read_text().strip().split("\n")[:args.limit]
                ingestor.slugs = [l.strip().lower() for l in lines if l.strip()]
                ingestor.file_path = None  # Use slugs instead of file
            
            records, stats = await ingestor.ingest(
                batch_size=args.batch_size,
                upload_logs=False,  # Don't upload logs for now
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
    logger.info("TOTAL SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Files processed: {total_stats['files']}")
    logger.info(f"Total slugs parsed: {total_stats['records_parsed']}")
    logger.info(f"Hotels saved: {total_stats['records_saved']}")
    logger.info(f"Duplicates skipped: {total_stats['duplicates_skipped']}")
    logger.info(f"Errors: {total_stats['errors']}")
    
    # Reminder about SQS enrichment
    if total_stats['records_saved'] > 0:
        logger.info("\n" + "-" * 50)
        logger.info("Hotels inserted with placeholder names.")
        logger.info("Run the SQS enqueuer to start name enrichment:")
        logger.info("  uv run python -m workflows.enrich_names_enqueue")


if __name__ == "__main__":
    main()
