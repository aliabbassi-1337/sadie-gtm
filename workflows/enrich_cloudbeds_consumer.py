"""Cloudbeds enrichment consumer - thin wrapper around the enrichment service.

Polls SQS and enriches hotels using Playwright.
Run on multiple EC2 instances for parallel processing.

Usage:
    uv run python -m workflows.enrich_cloudbeds_consumer
    uv run python -m workflows.enrich_cloudbeds_consumer --concurrency 10
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import signal

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service
from infra.sqs import receive_messages, delete_message, get_queue_attributes

from playwright.async_api import async_playwright, BrowserContext
from playwright_stealth import Stealth
from lib.cloudbeds import CloudbedsScraper


QUEUE_URL = os.getenv("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 300  # 5 minutes per hotel

shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def run_consumer(concurrency: int = 5):
    """Run the SQS consumer with Playwright."""
    global shutdown_requested
    
    if not QUEUE_URL:
        logger.error("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL not set in .env")
        return
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    await init_db()
    service = Service()
    
    try:
        logger.info(f"Starting Cloudbeds enrichment consumer (concurrency={concurrency})")
        
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create browser contexts pool
            contexts: list[BrowserContext] = []
            scrapers: list[CloudbedsScraper] = []
            
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                scrapers.append(CloudbedsScraper(page))
            
            logger.info(f"Created {concurrency} browser contexts")
            
            total_processed = 0
            total_enriched = 0
            total_failed = 0
            batch_results = []
            batch_failed_ids = []
            
            while not shutdown_requested:
                # Receive messages
                messages = receive_messages(
                    QUEUE_URL,
                    max_messages=min(concurrency, 10),
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    wait_time_seconds=10,
                )
                
                if not messages:
                    logger.debug("No messages, waiting...")
                    continue
                
                # Process messages
                valid_messages = []
                for msg in messages:
                    body = msg["body"]
                    hotel_id = body.get("hotel_id")
                    booking_url = body.get("booking_url")
                    
                    if not hotel_id or not booking_url:
                        delete_message(QUEUE_URL, msg["receipt_handle"])
                        continue
                    
                    valid_messages.append((msg, hotel_id, booking_url))
                
                # Process in batches
                tasks = []
                message_map = {}
                
                for i, (msg, hotel_id, booking_url) in enumerate(valid_messages[:concurrency]):
                    message_map[hotel_id] = msg
                    tasks.append(_process_hotel(scrapers[i], hotel_id, booking_url))
                
                if not tasks:
                    continue
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Task error: {result}")
                        continue
                    
                    hotel_id, success, data, error = result
                    msg = message_map.get(hotel_id)
                    if not msg:
                        continue
                    
                    if success and data:
                        batch_results.append({
                            "hotel_id": hotel_id,
                            "name": data.name,
                            "address": data.address,
                            "city": data.city,
                            "state": data.state,
                            "country": data.country,
                            "phone": data.phone,
                            "email": data.email,
                        })
                        total_enriched += 1
                        
                        parts = []
                        if data.name:
                            parts.append(f"name={data.name[:20]}")
                        if data.city:
                            parts.append(f"city={data.city}")
                        logger.info(f"  Hotel {hotel_id}: {', '.join(parts)}")
                    elif error == "404_not_found":
                        batch_failed_ids.append(hotel_id)
                        total_failed += 1
                        logger.warning(f"  Hotel {hotel_id}: 404 - will retry")
                    elif error:
                        logger.warning(f"  Hotel {hotel_id}: {error}")
                    
                    delete_message(QUEUE_URL, msg["receipt_handle"])
                    total_processed += 1
                
                # Batch update every 50 results
                if len(batch_results) >= 50:
                    updated = await service.batch_update_cloudbeds_enrichment(batch_results)
                    logger.info(f"Batch update: {updated} hotels")
                    batch_results = []
                
                if len(batch_failed_ids) >= 50:
                    marked = await service.batch_mark_cloudbeds_failed(batch_failed_ids)
                    logger.info(f"Marked {marked} hotels for retry in 7 days")
                    batch_failed_ids = []
                
                # Log progress
                attrs = get_queue_attributes(QUEUE_URL)
                remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, {total_failed} failed, ~{remaining} remaining")
            
            # Final batch
            if batch_results:
                updated = await service.batch_update_cloudbeds_enrichment(batch_results)
                logger.info(f"Final batch update: {updated} hotels")
            
            if batch_failed_ids:
                marked = await service.batch_mark_cloudbeds_failed(batch_failed_ids)
                logger.info(f"Final batch: {marked} hotels marked for retry in 7 days")
            
            # Cleanup
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
        
    finally:
        await close_db()


async def _process_hotel(scraper: CloudbedsScraper, hotel_id: int, booking_url: str):
    """Process a single hotel."""
    try:
        data = await scraper.extract(booking_url)
        
        if not data:
            return (hotel_id, False, None, "no_data")
        
        # Check for garbage data
        if data.name and data.name.lower() in ['cloudbeds.com', 'cloudbeds', 'book now', 'reservation']:
            return (hotel_id, False, None, "404_not_found")
        if data.city and 'soluções online' in data.city.lower():
            return (hotel_id, False, None, "404_not_found")
        
        return (hotel_id, True, data, None)
        
    except Exception as e:
        return (hotel_id, False, None, str(e)[:100])


def main():
    parser = argparse.ArgumentParser(description="Cloudbeds enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
