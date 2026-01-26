#!/usr/bin/env python3
"""
Ingest crawled booking engine URLs into the database.

Reads text files containing slugs/URLs (one per line) from Common Crawl
or similar sources and ingests them into the hotels table.

Usage:
    # Ingest all files from S3 (recommended)
    uv run python -m workflows.ingest_crawl --s3
    
    # Ingest from S3 with custom path
    uv run python -m workflows.ingest_crawl --s3 --s3-prefix "crawl-data/"

    # Ingest Cloudbeds slugs from local file
    uv run python -m workflows.ingest_crawl \
        --file "/path/to/cloudbeds_commoncrawl_full.txt" \
        --engine cloudbeds

    # Ingest all booking engine files from a local directory
    uv run python -m workflows.ingest_crawl \
        --dir "/path/to/crawled booking engine urls" \
        --all
        
    # Dry run to see what would be imported
    uv run python -m workflows.ingest_crawl --s3 --dry-run
"""

import argparse
import asyncio
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

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

# Default S3 bucket and prefix for crawl data
DEFAULT_S3_BUCKET = "sadie-gtm"
DEFAULT_S3_PREFIX = "crawl-data/"


def detect_engine_from_filename(filename: str) -> Optional[str]:
    """Detect booking engine from filename."""
    filename_lower = filename.lower()
    for engine, pattern in ENGINE_FILE_PATTERNS.items():
        if pattern in filename_lower:
            return engine
    return None


async def fetch_s3_files(bucket: str, prefix: str) -> List[Tuple[str, str, bytes]]:
    """
    Fetch crawl files from S3.
    
    Returns list of (filename, engine, content) tuples.
    """
    try:
        import aioboto3
    except ImportError:
        raise ImportError("S3 support requires aioboto3. Install with: pip install aioboto3")
    
    session = aioboto3.Session()
    files = []
    
    async with session.client("s3") as s3:
        # List files in prefix
        paginator = s3.get_paginator("list_objects_v2")
        
        async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]
                
                # Only process .txt files
                if not filename.endswith(".txt"):
                    continue
                
                # Detect engine from filename
                engine = detect_engine_from_filename(filename)
                if not engine:
                    logger.warning(f"Skipping unrecognized file: {filename}")
                    continue
                
                # Fetch file content
                logger.info(f"Fetching s3://{bucket}/{key}...")
                response = await s3.get_object(Bucket=bucket, Key=key)
                content = await response["Body"].read()
                
                files.append((filename, engine, content))
    
    return files


async def main():
    parser = argparse.ArgumentParser(
        description="Ingest crawled booking engine URLs into database"
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--s3",
        action="store_true",
        help="Fetch files from S3 bucket (recommended)"
    )
    input_group.add_argument(
        "--file", "-f",
        type=str,
        help="Path to local text file with slugs/URLs (one per line)"
    )
    input_group.add_argument(
        "--dir", "-d",
        type=str,
        help="Local directory containing crawl files (use with --all)"
    )
    
    # S3 options
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=DEFAULT_S3_BUCKET,
        help=f"S3 bucket name (default: {DEFAULT_S3_BUCKET})"
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default=DEFAULT_S3_PREFIX,
        help=f"S3 key prefix (default: {DEFAULT_S3_PREFIX})"
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
        help="Process all recognized files in directory (required with --dir)"
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
        help="Limit number of slugs to process per file (for testing)"
    )
    
    args = parser.parse_args()
    
    # Validate args
    if args.file and not args.engine:
        parser.error("--engine is required when using --file")
    
    if args.dir and not args.all:
        parser.error("--all is required when using --dir")
    
    # Collect files to process: list of (filename, engine, content_or_path)
    # For S3: content is bytes; for local: content is file path
    files_to_process = []
    is_s3 = args.s3
    
    if args.s3:
        logger.info(f"Fetching files from s3://{args.s3_bucket}/{args.s3_prefix}...")
        try:
            s3_files = await fetch_s3_files(args.s3_bucket, args.s3_prefix)
            for filename, engine, content in s3_files:
                files_to_process.append((filename, engine, content))
        except Exception as e:
            logger.error(f"Failed to fetch S3 files: {e}")
            return
    
    elif args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return
        files_to_process.append((file_path.name, args.engine, str(file_path)))
    
    elif args.dir:
        dir_path = Path(args.dir)
        if not dir_path.exists():
            logger.error(f"Directory not found: {dir_path}")
            return
        
        for file_path in dir_path.glob("*.txt"):
            engine = detect_engine_from_filename(file_path.name)
            if engine:
                files_to_process.append((file_path.name, engine, str(file_path)))
            else:
                logger.warning(f"Skipping unrecognized file: {file_path.name}")
    
    if not files_to_process:
        logger.error("No files to process")
        return
    
    logger.info(f"Found {len(files_to_process)} file(s) to process")
    
    # Dry run - just show stats
    if args.dry_run:
        for filename, engine, content in files_to_process:
            if is_s3:
                text = content.decode("utf-8")
            else:
                text = Path(content).read_text()
            lines = text.strip().split("\n")
            slugs = [l.strip() for l in lines if l.strip()]
            unique_slugs = len(set(slugs))
            logger.info(f"  {filename}: {len(slugs)} lines, {unique_slugs} unique ({engine})")
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
        "updated_existing": 0,
        "engines_linked": 0,
        "skipped_duplicate": 0,
        "errors": 0,
    }
    
    for filename, engine, content in files_to_process:
        logger.info(f"\nProcessing {filename} ({engine})...")
        
        # Get file content as text
        if is_s3:
            text = content.decode("utf-8")
        else:
            text = Path(content).read_text()
        
        lines = text.strip().split("\n")
        
        # Apply limit if specified
        if args.limit:
            lines = lines[:args.limit]
        
        # Write to temp file for service (service expects file path)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("\n".join(lines))
            temp_path = f.name
        
        try:
            stats = await service.ingest_crawled_urls(
                file_path=temp_path,
                booking_engine=engine,
                source_tag=args.source,
            )
            
            total_stats["files"] += 1
            for key in ["total", "inserted", "updated_existing", "engines_linked", "skipped_duplicate", "errors"]:
                total_stats[key] += stats.get(key, 0)
            
            logger.info(f"  Inserted: {stats['inserted']}")
            logger.info(f"  Updated existing: {stats.get('updated_existing', 0)}")
            logger.info(f"  Linked: {stats['engines_linked']}")
            logger.info(f"  Duplicates: {stats['skipped_duplicate']}")
            logger.info(f"  Errors: {stats['errors']}")
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}")
            total_stats["errors"] += 1
        
        finally:
            import os
            os.unlink(temp_path)
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TOTAL SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Files processed: {total_stats['files']}")
    logger.info(f"Total slugs: {total_stats['total']}")
    logger.info(f"Inserted (new): {total_stats['inserted']}")
    logger.info(f"Updated existing: {total_stats['updated_existing']}")
    logger.info(f"Engines linked: {total_stats['engines_linked']}")
    logger.info(f"Duplicates skipped: {total_stats['skipped_duplicate']}")
    logger.info(f"Errors: {total_stats['errors']}")


if __name__ == "__main__":
    asyncio.run(main())
