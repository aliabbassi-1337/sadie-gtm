"""Enqueue Mews hotels for distributed enrichment via SQS.

Usage:
    uv run python -m workflows.enrich_mews_enqueue --limit 5000
    uv run python -m workflows.enrich_mews_enqueue --dry-run --limit 100
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
from loguru import logger

from db.client import init_db, close_db
from services.enrichment import repo
from infra.sqs import send_messages_batch, get_queue_attributes

QUEUE_URL = os.getenv("SQS_MEWS_ENRICHMENT_QUEUE_URL", "")

# Don't enqueue if queue already has this many messages (prevents duplicates)
MAX_QUEUE_SIZE = 3000


async def run(limit: int = 1000, dry_run: bool = False):
    """Enqueue Mews hotels for enrichment."""
    if not QUEUE_URL and not dry_run:
        logger.error("SQS_MEWS_ENRICHMENT_QUEUE_URL not set in .env")
        return 0
    
    await init_db()
    try:
        # Get current queue status
        waiting = 0
        in_flight = 0
        if QUEUE_URL and not dry_run:
            attrs = get_queue_attributes(QUEUE_URL)
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
            logger.info(f"Queue status: {waiting} waiting, {in_flight} in-flight")
            
            # Skip if queue already has enough messages
            if waiting >= MAX_QUEUE_SIZE:
                logger.info(f"Queue has {waiting} messages (>= {MAX_QUEUE_SIZE}), skipping enqueue")
                return 0
        
        # Only enqueue enough to bring queue up to max size
        enqueue_limit = min(limit, MAX_QUEUE_SIZE - waiting)
        if enqueue_limit <= 0:
            logger.info("Queue at capacity, skipping")
            return 0
        
        # Get hotels needing enrichment
        candidates = await repo.get_mews_hotels_needing_enrichment(limit=enqueue_limit)
        
        if not candidates:
            logger.info("No Mews hotels need enrichment")
            return 0
        
        logger.info(f"Found {len(candidates)} Mews hotels to enqueue")
        
        if dry_run:
            logger.info("Dry run - not sending to SQS")
            for h in candidates[:10]:
                logger.info(f"  Would enqueue: {h.id} - {h.booking_url[:50]}...")
            if len(candidates) > 10:
                logger.info(f"  ... and {len(candidates) - 10} more")
            return len(candidates)
        
        # Create messages
        messages = [
            {
                "hotel_id": h.id,
                "booking_url": h.booking_url,
            }
            for h in candidates
        ]
        
        # Send in batches of 10 (SQS limit)
        sent = 0
        for i in range(0, len(messages), 10):
            batch = messages[i:i+10]
            success = send_messages_batch(QUEUE_URL, batch)
            if success:
                sent += len(batch)
            else:
                logger.error(f"Failed to send batch {i//10 + 1}")
        
        logger.info(f"Enqueued {sent}/{len(candidates)} hotels")
        return sent
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Enqueue Mews hotels for enrichment")
    parser.add_argument("--limit", type=int, default=5000, help="Max hotels to enqueue")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enqueued")
    
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
