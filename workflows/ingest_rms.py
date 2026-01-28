#!/usr/bin/env python3
"""
RMS Booking Engine Ingestor

Scans RMS booking engine IDs to discover valid hotels and saves them to the database.

Usage:
    python workflows/ingest_rms.py --start 0 --end 10000
    python workflows/ingest_rms.py --start 0 --end 100 --dry-run
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import signal

from loguru import logger

from db.client import init_db, close_db
from services.ingestor.ingestors.rms import RMSIngestor


def main():
    parser = argparse.ArgumentParser(description="Ingest RMS hotels by scanning IDs")
    parser.add_argument("--start", type=int, default=0, help="Start ID")
    parser.add_argument("--end", type=int, default=10000, help="End ID")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent pages")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()
    
    asyncio.run(run(args))


async def run(args):
    logger.info(f"RMS Ingestion: IDs {args.start} - {args.end}, dry_run={args.dry_run}")
    
    await init_db()
    ingestor = RMSIngestor()
    
    # Handle shutdown
    signal.signal(signal.SIGTERM, lambda s, f: ingestor.request_shutdown())
    signal.signal(signal.SIGINT, lambda s, f: ingestor.request_shutdown())
    
    try:
        result = await ingestor.ingest(
            start_id=args.start,
            end_id=args.end,
            concurrency=args.concurrency,
            dry_run=args.dry_run,
        )
        
        print(f"\n{'=' * 50}")
        print("INGESTION SUMMARY")
        print(f"{'=' * 50}")
        print(f"Range: {args.start} - {args.end}")
        print(f"Scanned: {result.total_scanned}")
        print(f"Found: {result.hotels_found}")
        print(f"Saved: {result.hotels_saved}")
        if args.dry_run:
            print("\n(Dry run - no data saved)")
    finally:
        await close_db()


if __name__ == "__main__":
    main()
