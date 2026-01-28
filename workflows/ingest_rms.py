#!/usr/bin/env python3
"""
RMS Booking Engine Ingestor

Scans RMS booking engine IDs to discover valid hotels and saves them to the database.

Usage:
    python workflows/ingest_rms.py --start 0 --end 10000
    python workflows/ingest_rms.py --start 0 --end 100 --dry-run
    
    # Distributed (different ranges per server)
    python workflows/ingest_rms.py --start 0 --end 5000
    python workflows/ingest_rms.py --start 5000 --end 10000
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import signal

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.rms_service import RMSService


async def main():
    parser = argparse.ArgumentParser(description="Ingest RMS hotels by scanning booking engine IDs")
    parser.add_argument("--start", type=int, default=0, help="Start ID")
    parser.add_argument("--end", type=int, default=10000, help="End ID")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent pages")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()
    
    logger.info(f"Starting RMS ingestion: IDs {args.start} - {args.end}")
    logger.info(f"Concurrency: {args.concurrency}, Dry run: {args.dry_run}")
    
    await init_db()
    
    try:
        service = RMSService()
        
        # Set up signal handlers
        def handle_shutdown(signum, frame):
            service.request_shutdown()
        
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)
        
        result = await service.ingest_from_id_range(
            start_id=args.start,
            end_id=args.end,
            concurrency=args.concurrency,
            dry_run=args.dry_run,
        )
        
        print("\n" + "=" * 50)
        print("INGESTION SUMMARY")
        print("=" * 50)
        print(f"Range scanned: {args.start} - {args.end}")
        print(f"Total IDs scanned: {result.total_scanned}")
        print(f"Hotels found: {result.hotels_found}")
        print(f"Hotels saved: {result.hotels_saved}")
        
        if args.dry_run:
            print("\n(Dry run - no data was saved)")
        
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
