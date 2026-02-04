#!/usr/bin/env python3
"""
RMS Enrichment Consumer

Consumes RMS hotels from SQS and enriches them by scraping booking pages.

Usage:
    # Normal mode (only fill empty fields)
    python workflows/enrich_rms_consumer.py --concurrency 50
    
    # Force overwrite mode (overwrites existing data)
    python workflows/enrich_rms_consumer.py --concurrency 50 --force-overwrite
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import signal

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


def main():
    parser = argparse.ArgumentParser(description="RMS Enrichment Consumer")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent API requests")
    parser.add_argument("--max-messages", type=int, default=0, help="Max messages (0=infinite)")
    parser.add_argument("--force-overwrite", action="store_true", help="Overwrite existing data (default: only fill empty fields)")
    args = parser.parse_args()
    
    asyncio.run(run(args))


async def run(args):
    await init_db()
    service = Service()
    
    # Handle shutdown
    signal.signal(signal.SIGTERM, lambda s, f: service.request_shutdown())
    signal.signal(signal.SIGINT, lambda s, f: service.request_shutdown())
    
    mode = "FORCE OVERWRITE" if args.force_overwrite else "fill empty only"
    logger.info(f"Starting RMS consumer (concurrency={args.concurrency}, mode={mode})")
    
    try:
        result = await service.consume_rms_enrichment_queue(
            concurrency=args.concurrency,
            max_messages=args.max_messages,
            force_overwrite=args.force_overwrite,
        )
        
        logger.success(
            f"Consumer stopped. "
            f"Messages: {result.messages_processed}, "
            f"Hotels: {result.hotels_processed}, "
            f"Enriched: {result.hotels_enriched}, "
            f"Failed: {result.hotels_failed}"
        )
    finally:
        await close_db()


if __name__ == "__main__":
    main()
