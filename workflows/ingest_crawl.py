#!/usr/bin/env python3
"""
Ingest crawled booking engine URLs into the database.

Reads text files containing slugs/URLs (one per line) from Common Crawl
or similar sources and ingests them into the hotels table.

Usage:
    # Ingest Cloudbeds slugs
    uv run python -m workflows.ingest_crawl \
        --file "/path/to/cloudbeds_commoncrawl_full.txt" \
        --engine cloudbeds

    # Ingest all booking engine files from a directory
    uv run python -m workflows.ingest_crawl \
        --dir "/path/to/crawled booking engine urls" \
        --all
        
    # Dry run to see what would be imported
    uv run python -m workflows.ingest_crawl \
        --file "/path/to/mews_commoncrawl_full.txt" \
        --engine mews \
        --dry-run
"""

import argparse
import asyncio
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# File patterns for auto-detection
ENGINE_FILE_PATTERNS = {
    "cloudbeds": "cloudbeds",
    "mews": "mews",
    "rms": "rms",
    "siteminder": "siteminder",
}


from typing import Optional

def detect_engine_from_filename(filename: str) -> Optional[str]:
    """Detect booking engine from filename."""
    filename_lower = filename.lower()
    for engine, pattern in ENGINE_FILE_PATTERNS.items():
        if pattern in filename_lower:
            return engine
    return None


async def main():
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
        "--source",
        type=str,
        default="commoncrawl",
        help="Source tag for tracking (default: commoncrawl)"
    )
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
        "--no-scrape",
        action="store_true",
        help="Skip scraping hotel names (import slugs only - NOT recommended)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Number of concurrent requests (default: 50)"
    )
    parser.add_argument(
        "--no-common-crawl",
        action="store_true",
        help="Use Wayback instead of Common Crawl (slower but works for all engines)"
    )
    parser.add_argument(
        "--no-fuzzy",
        action="store_true",
        help="Disable fuzzy name matching (exact match only)"
    )
    parser.add_argument(
        "--fuzzy-threshold",
        type=float,
        default=0.7,
        help="Fuzzy match similarity threshold 0.0-1.0 (default: 0.7)"
    )
    parser.add_argument(
        "--checkpoint",
        type=str,
        help="Path to checkpoint file for resume capability"
    )
    
    args = parser.parse_args()
    
    # Validate args
    if args.file and not args.engine:
        parser.error("--engine is required when using --file")
    
    if args.dir and not args.all:
        parser.error("--all is required when using --dir")
    
    # Collect files to process
    files_to_process = []
    
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return
        files_to_process.append((file_path, args.engine))
    
    elif args.dir:
        dir_path = Path(args.dir)
        if not dir_path.exists():
            logger.error(f"Directory not found: {dir_path}")
            return
        
        for file_path in dir_path.glob("*.txt"):
            engine = detect_engine_from_filename(file_path.name)
            if engine:
                files_to_process.append((file_path, engine))
            else:
                logger.warning(f"Skipping unrecognized file: {file_path.name}")
    
    if not files_to_process:
        logger.error("No files to process")
        return
    
    logger.info(f"Found {len(files_to_process)} file(s) to process")
    
    # Dry run - just show stats
    if args.dry_run:
        for file_path, engine in files_to_process:
            lines = file_path.read_text().strip().split("\n")
            slugs = [l.strip() for l in lines if l.strip()]
            unique_slugs = len(set(slugs))
            logger.info(f"  {file_path.name}: {len(slugs)} lines, {unique_slugs} unique ({engine})")
        logger.info("Dry run complete - no changes made")
        return
    
    # Initialize database
    from db.client import init_db
    await init_db()
    
    # Initialize service
    from services.leadgen.service import Service
    service = Service()
    
    # Process each file
    total_stats = {
        "files": 0,
        "total": 0,
        "inserted": 0,
        "updated": 0,
        "fuzzy_matched": 0,
        "websites_found": 0,
        "engines_linked": 0,
        "skipped_no_name": 0,
        "skipped_duplicate": 0,
        "errors": 0,
    }
    
    for file_path, engine in files_to_process:
        logger.info(f"\nProcessing {file_path.name} ({engine})...")
        
        # Apply limit if specified
        if args.limit:
            # Read file, limit lines, write to temp file
            lines = file_path.read_text().strip().split("\n")[:args.limit]
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                f.write("\n".join(lines))
                temp_path = f.name
            file_to_process = temp_path
        else:
            file_to_process = str(file_path)
        
        try:
            stats = await service.ingest_crawled_urls(
                file_path=file_to_process,
                booking_engine=engine,
                source_tag=args.source,
                scrape_names=not args.no_scrape,
                concurrency=args.concurrency,
                use_common_crawl=not args.no_common_crawl,
                fuzzy_match=not args.no_fuzzy,
                fuzzy_threshold=args.fuzzy_threshold,
                checkpoint_file=args.checkpoint,
            )
            
            total_stats["files"] += 1
            for key in ["total", "inserted", "updated", "fuzzy_matched", "websites_found", "engines_linked", "skipped_no_name", "skipped_duplicate", "errors"]:
                total_stats[key] += stats.get(key, 0)
            
            logger.info(f"  New hotels: {stats['inserted']}")
            logger.info(f"  Updated (source appended): {stats['updated']}")
            logger.info(f"  Fuzzy matched: {stats.get('fuzzy_matched', 0)}")
            logger.info(f"  Websites found: {stats.get('websites_found', 0)}")
            logger.info(f"  Engines linked: {stats['engines_linked']}")
            logger.info(f"  Skipped (no name): {stats['skipped_no_name']}")
            logger.info(f"  Skipped (duplicate): {stats['skipped_duplicate']}")
            logger.info(f"  Errors: {stats['errors']}")
            
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")
            total_stats["errors"] += 1
        
        finally:
            if args.limit:
                import os
                os.unlink(temp_path)
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TOTAL SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Files processed: {total_stats['files']}")
    logger.info(f"Total slugs: {total_stats['total']}")
    logger.info(f"New hotels inserted: {total_stats['inserted']}")
    logger.info(f"Existing hotels updated: {total_stats['updated']}")
    logger.info(f"Fuzzy matched: {total_stats['fuzzy_matched']}")
    logger.info(f"Websites extracted: {total_stats['websites_found']}")
    logger.info(f"Booking engines linked: {total_stats['engines_linked']}")
    logger.info(f"Skipped (no name): {total_stats['skipped_no_name']}")
    logger.info(f"Skipped (duplicate): {total_stats['skipped_duplicate']}")
    logger.info(f"Errors: {total_stats['errors']}")


if __name__ == "__main__":
    asyncio.run(main())
