#!/usr/bin/env python3
"""
RMS Enrichment Consumer

Consumes RMS hotels from SQS and enriches them by scraping booking pages.

Usage:
    python workflows/enrich_rms_consumer.py --concurrency 6
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import signal

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.rms_service import RMSEnrichmentService


def main():
    parser = argparse.ArgumentParser(description="RMS Enrichment Consumer")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent scrapers")
    parser.add_argument("--max-messages", type=int, default=0, help="Max messages (0=infinite)")
    args = parser.parse_args()
    
    asyncio.run(run(args))


async def run(args):
    await init_db()
    service = RMSEnrichmentService()
    
    # Handle shutdown
    signal.signal(signal.SIGTERM, lambda s, f: service.request_shutdown())
    signal.signal(signal.SIGINT, lambda s, f: service.request_shutdown())
    
    logger.info(f"Starting RMS consumer (concurrency={args.concurrency})")
    
    try:
        result = await service.consume_enrichment_queue(
            concurrency=args.concurrency,
            max_messages=args.max_messages,
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
