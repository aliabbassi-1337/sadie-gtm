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
from services.enrichment.rms_service import RMSService
from infra.sqs import send_message, get_queue_attributes, get_queue_url

QUEUE_NAME = "sadie-gtm-rms-enrichment"


async def main():
    parser = argparse.ArgumentParser(description="Enqueue RMS hotels for enrichment")
    parser.add_argument("--limit", type=int, default=5000, help="Max hotels to enqueue")
    parser.add_argument("--batch-size", type=int, default=10, help="Hotels per SQS message")
    args = parser.parse_args()

    await init_db()
    
    try:
        service = RMSService()
        
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
        hotels = await service.get_hotels_needing_enrichment(args.limit)
        logger.info(f"Found {len(hotels)} RMS hotels needing enrichment")
        
        if not hotels:
            logger.info("No hotels to enqueue")
            return
        
        # Send to SQS in batches
        enqueued = 0
        for i in range(0, len(hotels), args.batch_size):
            batch = hotels[i:i + args.batch_size]
            message = {
                "hotels": [
                    {"hotel_id": h.hotel_id, "booking_url": h.booking_url}
                    for h in batch
                ]
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
