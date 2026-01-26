"""Name enrichment worker - Polls SQS and scrapes hotel names from booking pages.

Run continuously on EC2 instances to process name enrichment jobs.

Usage:
    uv run python -m workflows.enrich_names_consumer
    uv run python -m workflows.enrich_names_consumer --delay 0.5
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import os
import re
import signal
from typing import Dict, Any, Optional
from loguru import logger
import httpx

from db.client import init_db, close_db, get_conn, queries
from infra.sqs import receive_messages, delete_message, get_queue_attributes

# Queue URL from environment
QUEUE_URL = os.getenv("SQS_NAME_ENRICHMENT_QUEUE_URL", "")

# Global flag for graceful shutdown
shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def extract_name_from_page(
    client: httpx.AsyncClient,
    booking_url: str,
    engine: str,
) -> Optional[str]:
    """Scrape hotel name from booking page."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        
        resp = await client.get(booking_url, headers=headers, follow_redirects=True, timeout=30.0)
        
        if resp.status_code != 200:
            logger.warning(f"Failed to fetch {booking_url}: {resp.status_code}")
            return None
        
        html = resp.text
        
        # Try multiple extraction patterns
        for pattern in [
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
            r'<title>([^<]+)</title>',
            r'<h1[^>]*>([^<]+)</h1>',
        ]:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                raw = match.group(1).strip()
                # Remove common suffixes
                parts = re.split(r'\s*[-|–]\s*', raw)
                name = parts[0].strip()
                
                # Skip generic names
                if name.lower() not in ['book now', 'reservation', 'booking', 'home', 'unknown']:
                    return name
        
        return None
        
    except Exception as e:
        logger.warning(f"Error scraping {booking_url}: {e}")
        return None


async def update_hotel_name(hotel_id: int, name: str) -> bool:
    """Update hotel name in database."""
    try:
        async with get_conn() as conn:
            await queries.update_hotel_name(conn, hotel_id=hotel_id, name=name)
        return True
    except Exception as e:
        logger.error(f"Failed to update hotel {hotel_id}: {e}")
        return False


async def process_message(
    client: httpx.AsyncClient,
    message: Dict[str, Any],
    queue_url: str,
    delay: float,
) -> tuple:
    """Process a single SQS message.
    
    Returns (success, found_name).
    """
    receipt_handle = message["receipt_handle"]
    body = message["body"]
    
    hotel_id = body.get("hotel_id")
    booking_url = body.get("booking_url")
    engine = body.get("engine", "unknown")
    
    if not hotel_id or not booking_url:
        # Invalid message, delete it
        delete_message(queue_url, receipt_handle)
        return (False, False)
    
    # Rate limiting delay
    await asyncio.sleep(delay)
    
    # Scrape the name
    name = await extract_name_from_page(client, booking_url, engine)
    
    if name:
        # Update database
        success = await update_hotel_name(hotel_id, name)
        if success:
            logger.info(f"  Updated hotel {hotel_id}: {name}")
            delete_message(queue_url, receipt_handle)
            return (True, True)
        else:
            # DB error - don't delete, will retry
            return (False, True)
    else:
        # Couldn't extract name - delete anyway (won't help to retry)
        delete_message(queue_url, receipt_handle)
        return (True, False)


async def run_worker(delay: float = 0.5, poll_interval: int = 5):
    """Main worker loop - poll SQS and process messages."""
    global shutdown_requested
    
    if not QUEUE_URL:
        logger.error("SQS_NAME_ENRICHMENT_QUEUE_URL not set")
        return
    
    await init_db()
    
    # Stats
    processed = 0
    names_found = 0
    errors = 0
    
    logger.info(f"Starting name enrichment worker (delay={delay}s)")
    logger.info(f"Queue: {QUEUE_URL}")
    
    async with httpx.AsyncClient() as client:
        try:
            while not shutdown_requested:
                # Get queue stats
                attrs = get_queue_attributes(QUEUE_URL)
                pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                
                if pending == 0 and in_flight == 0:
                    logger.info(f"Queue empty. Processed: {processed}, Names found: {names_found}, Errors: {errors}")
                    logger.info(f"Waiting {poll_interval}s...")
                    await asyncio.sleep(poll_interval)
                    continue
                
                # Receive messages
                messages = receive_messages(
                    QUEUE_URL,
                    max_messages=10,
                    wait_time=20,
                    visibility_timeout=60,
                )
                
                if not messages:
                    continue
                
                logger.info(f"Processing {len(messages)} messages (pending: {pending}, in_flight: {in_flight})")
                
                # Process each message
                for msg in messages:
                    if shutdown_requested:
                        break
                    
                    success, found = await process_message(client, msg, QUEUE_URL, delay)
                    processed += 1
                    if found:
                        names_found += 1
                    if not success:
                        errors += 1
                
                # Log progress
                if processed % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {names_found} names found, {errors} errors")
                    
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            await close_db()
            logger.info(f"Final stats: {processed} processed, {names_found} names found, {errors} errors")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Name enrichment worker - scrape hotel names from booking pages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run worker with default settings (0.5s delay)
    uv run python -m workflows.enrich_names_consumer

    # Run with faster rate (use with caution)
    uv run python -m workflows.enrich_names_consumer --delay 0.2

Environment:
    SQS_NAME_ENRICHMENT_QUEUE_URL - Required. The SQS queue URL.
        """
    )
    
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds to wait when queue is empty (default: 5)"
    )
    
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    asyncio.run(run_worker(args.delay, args.poll_interval))


if __name__ == "__main__":
    main()
