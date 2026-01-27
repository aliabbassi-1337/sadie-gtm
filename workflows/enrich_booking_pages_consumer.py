"""Booking page enrichment worker - Polls SQS and enriches hotels.

Run continuously on EC2 instances to process enrichment jobs.
Extracts hotel name, address, city, state, country from booking pages.
Uses archive fallback (Common Crawl/Wayback) for 404 URLs.

Usage:
    uv run python -m workflows.enrich_booking_pages_consumer
    uv run python -m workflows.enrich_booking_pages_consumer --archive-fallback
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import os
import signal
from typing import Dict, Any
from loguru import logger
import httpx

from db.client import init_db, close_db
from services.enrichment.service import Service as EnrichmentService
from services.enrichment import repo as enrichment_repo
from infra.sqs import receive_messages, delete_message, get_queue_attributes

QUEUE_URL = os.getenv("SQS_BOOKING_ENRICHMENT_QUEUE_URL", "")

shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def process_message(
    service: EnrichmentService,
    client: httpx.AsyncClient,
    message: Dict[str, Any],
    queue_url: str,
    delay: float,
    use_archive_fallback: bool = False,
) -> tuple:
    """Process a single SQS message.
    
    Returns (success, name_updated, address_updated).
    """
    receipt_handle = message["receipt_handle"]
    body = message["body"]
    
    hotel_id = body.get("hotel_id")
    booking_url = body.get("booking_url")
    
    if not hotel_id or not booking_url:
        delete_message(queue_url, receipt_handle)
        return (False, False, False)
    
    result = await service.enrich_hotel_from_booking_page(
        client=client,
        hotel_id=hotel_id,
        booking_url=booking_url,
        delay=delay,
        use_archive_fallback=use_archive_fallback,
    )
    
    if result.skipped:
        # Already enriched
        delete_message(queue_url, receipt_handle)
        return (True, False, False)
    
    if result.success:
        if result.name_updated or result.address_updated:
            # Actually enriched something
            parts = []
            if result.name_updated:
                parts.append("name")
            if result.address_updated:
                parts.append("address")
            logger.info(f"  Updated hotel {hotel_id}: {', '.join(parts)}")
            delete_message(queue_url, receipt_handle)
            return (True, result.name_updated, result.address_updated)
        else:
            # Page loaded but no data extracted - mark attempt timestamp
            await enrichment_repo.set_last_enrichment_attempt(hotel_id)
            delete_message(queue_url, receipt_handle)
            logger.debug(f"  Hotel {hotel_id}: no data extracted, will retry in 7 days")
            return (True, False, False)
    else:
        # Error - mark attempt timestamp (will retry after 7 days)
        await enrichment_repo.set_last_enrichment_attempt(hotel_id)
        delete_message(queue_url, receipt_handle)
        logger.warning(f"  Hotel {hotel_id}: enrichment failed, will retry in 7 days")
        return (False, False, False)


async def run_worker(delay: float = 0.5, poll_interval: int = 5, use_archive_fallback: bool = False):
    """Main worker loop - poll SQS and process messages."""
    global shutdown_requested
    
    if not QUEUE_URL:
        logger.error("SQS_BOOKING_ENRICHMENT_QUEUE_URL not set")
        return
    
    await init_db()
    
    service = EnrichmentService()
    
    # Stats
    processed = 0
    names_updated = 0
    addresses_updated = 0
    errors = 0
    
    logger.info(f"Starting enrichment worker (delay={delay}s, archive_fallback={use_archive_fallback})")
    logger.info(f"Queue: {QUEUE_URL}")
    
    async with httpx.AsyncClient() as client:
        try:
            while not shutdown_requested:
                attrs = get_queue_attributes(QUEUE_URL)
                pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                
                if pending == 0 and in_flight == 0:
                    logger.info(f"Queue empty. Processed: {processed}, Names: {names_updated}, Addresses: {addresses_updated}, Errors: {errors}")
                    logger.info(f"Waiting {poll_interval}s...")
                    await asyncio.sleep(poll_interval)
                    continue
                
                messages = receive_messages(
                    QUEUE_URL,
                    max_messages=10,
                    wait_time_seconds=20,
                    visibility_timeout=3600,  # 1 hour - prevents reprocessing
                )
                
                if not messages:
                    continue
                
                logger.info(f"Processing {len(messages)} messages (pending: {pending}, in_flight: {in_flight})")
                
                for msg in messages:
                    if shutdown_requested:
                        break
                    
                    success, name_up, addr_up = await process_message(
                        service, client, msg, QUEUE_URL, delay, use_archive_fallback
                    )
                    processed += 1
                    if name_up:
                        names_updated += 1
                    if addr_up:
                        addresses_updated += 1
                    if not success:
                        errors += 1
                
                if processed % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {names_updated} names, {addresses_updated} addresses, {errors} errors")
                    
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            await close_db()
            logger.info(f"Final stats: {processed} processed, {names_updated} names, {addresses_updated} addresses, {errors} errors")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Enrichment worker - scrape hotel data from booking pages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run python -m workflows.enrich_booking_pages_consumer
    uv run python -m workflows.enrich_booking_pages_consumer --archive-fallback

Environment:
    SQS_BOOKING_ENRICHMENT_QUEUE_URL - Required. The SQS queue URL.
        """
    )
    
    parser.add_argument("--delay", "-d", type=float, default=0.5)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument(
        "--archive-fallback", 
        action="store_true",
        help="Try Common Crawl/Wayback if live page fails (for 404 recovery)"
    )
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    asyncio.run(run_worker(args.delay, args.poll_interval, args.archive_fallback))


if __name__ == "__main__":
    main()
