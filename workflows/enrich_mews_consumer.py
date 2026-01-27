"""Mews enrichment worker - Polls SQS and enriches hotels using Playwright.

Extracts hotel name from page title (format: "Hotel Name - New booking").
~60% success rate - some pages don't include hotel name in title.

Usage:
    uv run python -m workflows.enrich_mews_consumer
    uv run python -m workflows.enrich_mews_consumer --concurrency 6
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import signal
import re
from dataclasses import dataclass
from loguru import logger
from playwright.async_api import async_playwright, Page
from playwright_stealth import Stealth

from db.client import init_db, close_db
from services.enrichment import repo
from infra.sqs import receive_messages, delete_message, get_queue_attributes

QUEUE_URL = os.getenv("SQS_MEWS_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 120  # 2 minutes per hotel (simpler extraction)

shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


@dataclass
class EnrichmentResult:
    """Result of enriching a hotel."""
    hotel_id: int
    success: bool
    name: str = None
    error: str = None


async def extract_name_from_page(page: Page) -> str:
    """Extract hotel name from Mews page title.
    
    Title format: "Hotel Name - New booking"
    Returns hotel name or None if not found.
    """
    try:
        title = await page.title()
        
        # Parse "Hotel Name - New booking"
        match = re.match(r'^(.+?) - New booking$', title)
        if match:
            name = match.group(1).strip()
            # Filter out generic names
            if name and name not in ['New booking', 'Booking', 'Reservation']:
                return name
        
        return None
    except Exception:
        return None


async def process_hotel(page: Page, hotel_id: int, booking_url: str) -> EnrichmentResult:
    """Process a single hotel."""
    try:
        await page.goto(booking_url, timeout=20000, wait_until="domcontentloaded")
        await asyncio.sleep(2)  # Wait for React to render title
        
        name = await extract_name_from_page(page)
        
        if not name:
            return EnrichmentResult(hotel_id=hotel_id, success=False, error="no_name_in_title")
        
        return EnrichmentResult(hotel_id=hotel_id, success=True, name=name)
        
    except Exception as e:
        return EnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])


async def run_consumer(concurrency: int = 5):
    """Run the SQS consumer with Playwright."""
    if not QUEUE_URL:
        logger.error("SQS_MEWS_ENRICHMENT_QUEUE_URL not set in .env")
        return
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    await init_db()
    
    try:
        logger.info(f"Starting Mews enrichment consumer (concurrency={concurrency})")
        
        async with Stealth().use_async(async_playwright()) as p:
            # playwright-stealth bypasses headless detection
            browser = await p.chromium.launch(headless=True)
            
            # Create browser contexts pool
            contexts = []
            pages = []
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                pages.append(page)
            
            logger.info(f"Created {concurrency} browser contexts")
            
            total_processed = 0
            total_enriched = 0
            batch_results = []
            
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
                
                # Process messages - one per page
                valid_messages = []
                for msg in messages:
                    body = msg["body"]
                    hotel_id = body.get("hotel_id")
                    booking_url = body.get("booking_url")
                    
                    if not hotel_id or not booking_url:
                        delete_message(QUEUE_URL, msg["receipt_handle"])
                        continue
                    
                    valid_messages.append((msg, hotel_id, booking_url))
                
                # Process batch
                tasks = []
                message_map = {}
                
                for i, (msg, hotel_id, booking_url) in enumerate(valid_messages[:concurrency]):
                    message_map[hotel_id] = msg
                    tasks.append(process_hotel(pages[i], hotel_id, booking_url))
                
                if not tasks:
                    continue
                
                # Wait for all to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Task error: {result}")
                        continue
                    
                    msg = message_map.get(result.hotel_id)
                    if not msg:
                        continue
                    
                    if result.success and result.name:
                        batch_results.append({
                            "hotel_id": result.hotel_id,
                            "name": result.name,
                        })
                        total_enriched += 1
                        logger.info(f"  Hotel {result.hotel_id}: name={result.name[:30]}")
                    elif result.error:
                        logger.debug(f"  Hotel {result.hotel_id}: {result.error}")
                    
                    # Delete from queue
                    delete_message(QUEUE_URL, msg["receipt_handle"])
                    total_processed += 1
                
                # Batch update every 50 results
                if len(batch_results) >= 50:
                    updated = await repo.batch_update_mews_enrichment(batch_results)
                    logger.info(f"Batch update: {updated} hotels")
                    batch_results = []
                
                # Log progress
                attrs = get_queue_attributes(QUEUE_URL)
                remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, ~{remaining} remaining")
            
            # Final batch
            if batch_results:
                updated = await repo.batch_update_mews_enrichment(batch_results)
                logger.info(f"Final batch update: {updated} hotels")
            
            # Cleanup
            for page in pages:
                await page.close()
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Mews enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
