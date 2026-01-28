#!/usr/bin/env python3
"""
RMS Enrichment Enqueuer

Enqueues RMS hotels that need enrichment (missing name, address, etc.) to SQS.

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
from infra.sqs import send_message, get_queue_attributes, get_queue_url

QUEUE_NAME = "sadie-gtm-rms-enrichment"


async def get_hotels_needing_enrichment(pool, limit: int = 5000) -> list:
    """Get RMS hotels that need enrichment."""
    query = """
        SELECT h.id, hbe.booking_url
        FROM sadie_gtm.hotels h
        JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
        JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
        WHERE be.name = 'RMS Cloud'
          AND h.status = 1
          AND (
              h.name IS NULL 
              OR h.name = '' 
              OR h.name LIKE '%rmscloud%'
              OR h.city IS NULL 
              OR h.city = ''
              OR h.state IS NULL
              OR h.state = ''
          )
          AND (
              hbe.enrichment_status IS NULL 
              OR hbe.enrichment_status NOT IN ('dead', 'enriched')
              OR (hbe.enrichment_status = 'no_data' AND hbe.last_enrichment_attempt < NOW() - INTERVAL '7 days')
          )
        ORDER BY hbe.last_enrichment_attempt ASC NULLS FIRST
        LIMIT $1
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, limit)
    return [{"hotel_id": r["id"], "booking_url": r["booking_url"]} for r in rows]


async def main():
    parser = argparse.ArgumentParser(description="Enqueue RMS hotels for enrichment")
    parser.add_argument("--limit", type=int, default=5000, help="Max hotels to enqueue")
    parser.add_argument("--batch-size", type=int, default=10, help="Hotels per SQS message")
    args = parser.parse_args()

    pool = await init_db()
    
    try:
        # Check current queue depth
        queue_url = get_queue_url(QUEUE_NAME)
        attrs = get_queue_attributes(queue_url)
        current_messages = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        
        logger.info(f"Current queue: {current_messages} pending, {in_flight} in flight")
        
        if current_messages > 1000:
            logger.warning(f"Queue already has {current_messages} messages, skipping enqueue")
            return
        
        # Get hotels needing enrichment
        hotels = await get_hotels_needing_enrichment(pool, args.limit)
        logger.info(f"Found {len(hotels)} RMS hotels needing enrichment")
        
        if not hotels:
            logger.info("No hotels to enqueue")
            return
        
        # Send to SQS in batches
        enqueued = 0
        for i in range(0, len(hotels), args.batch_size):
            batch = hotels[i:i + args.batch_size]
            message = {
                "hotels": batch
            }
            send_message(queue_url, message)
            enqueued += len(batch)
            
            if enqueued % 100 == 0:
                logger.info(f"Enqueued {enqueued}/{len(hotels)} hotels")
        
        logger.success(f"Enqueued {enqueued} hotels to {QUEUE_NAME}")
        
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
