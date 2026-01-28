#!/usr/bin/env python3
"""
RMS Enrichment Enqueuer

Enqueues RMS hotels that need enrichment to SQS.

Usage:
    python workflows/enrich_rms_enqueue.py
    python workflows/enrich_rms_enqueue.py --limit 1000
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service


def main():
    parser = argparse.ArgumentParser(description="Enqueue RMS hotels for enrichment")
    parser.add_argument("--limit", type=int, default=5000, help="Max hotels to enqueue")
    parser.add_argument("--batch-size", type=int, default=10, help="Hotels per SQS message")
    args = parser.parse_args()
    
    asyncio.run(run(args))


async def run(args):
    await init_db()
    service = Service()
    
    try:
        result = await service.enqueue_rms_for_enrichment(
            limit=args.limit,
            batch_size=args.batch_size,
        )
        
        if result.skipped:
            logger.warning(f"Skipped: {result.reason}")
        else:
            logger.success(f"Enqueued {result.enqueued} of {result.total_found} hotels")
    finally:
        await close_db()


if __name__ == "__main__":
    main()
