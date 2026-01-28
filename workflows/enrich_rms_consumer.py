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
from typing import Dict, Any, List

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.rms_service import RMSService
from services.enrichment.rms_repo import RMSHotelRecord
from infra.sqs import receive_messages, delete_message, get_queue_url, get_queue_attributes

QUEUE_NAME = "sadie-gtm-rms-enrichment"
VISIBILITY_TIMEOUT = 3600  # 1 hour

# Global shutdown flag
shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def process_message(
    service: RMSService,
    message: Dict[str, Any],
    queue_url: str,
    concurrency: int,
) -> tuple[int, int, int]:
    """Process a single SQS message.
    
    Returns: (processed, enriched, failed)
    """
    receipt_handle = message["receipt_handle"]
    hotels_data = message["body"].get("hotels", [])
    
    if not hotels_data:
        delete_message(queue_url, receipt_handle)
        return (0, 0, 0)
    
    # Convert to RMSHotelRecord
    hotels = [
        RMSHotelRecord(hotel_id=h["hotel_id"], booking_url=h["booking_url"])
        for h in hotels_data
    ]
    
    # Enrich hotels
    result = await service.enrich_hotels(hotels, concurrency=concurrency)
    
    # Delete message on success
    delete_message(queue_url, receipt_handle)
    
    return (result.processed, result.enriched, result.failed)


async def run_consumer(concurrency: int):
    """Run the consumer loop."""
    global shutdown_requested
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    await init_db()
    queue_url = get_queue_url(QUEUE_NAME)
    
    service = RMSService()
    
    total_processed = 0
    total_enriched = 0
    total_failed = 0
    
    logger.info(f"Starting RMS enrichment consumer (concurrency={concurrency})")
    logger.info(f"Queue: {QUEUE_NAME}")
    
    try:
        while not shutdown_requested:
            try:
                # Check queue stats
                attrs = get_queue_attributes(queue_url)
                pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                
                if pending == 0 and in_flight == 0:
                    logger.info("Queue empty, waiting...")
                    await asyncio.sleep(30)
                    continue
                
                # Receive messages
                messages = receive_messages(
                    queue_url,
                    max_messages=min(concurrency, 10),
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    wait_time=20,
                )
                
                if not messages:
                    continue
                
                logger.info(f"Processing {len(messages)} messages ({pending} pending, {in_flight} in flight)")
                
                # Process messages
                for msg in messages:
                    if shutdown_requested:
                        break
                    
                    try:
                        p, e, f = await process_message(service, msg, queue_url, concurrency)
                        total_processed += p
                        total_enriched += e
                        total_failed += f
                    except Exception as e:
                        logger.error(f"Message processing error: {e}")
                
                logger.info(
                    f"Progress: {total_processed} processed, "
                    f"{total_enriched} enriched, {total_failed} failed"
                )
                
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)
    
    finally:
        await close_db()
    
    logger.success(
        f"Consumer stopped. Total: {total_processed} processed, "
        f"{total_enriched} enriched, {total_failed} failed"
    )


def main():
    parser = argparse.ArgumentParser(description="RMS Enrichment Consumer")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent scrapers")
    args = parser.parse_args()
    
    asyncio.run(run_consumer(args.concurrency))


if __name__ == "__main__":
    main()
