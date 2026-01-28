"""Enqueue Cloudbeds hotels for distributed enrichment via SQS.

Usage:
    uv run python -m workflows.enrich_cloudbeds_enqueue --limit 10000
    uv run python -m workflows.enrich_cloudbeds_enqueue --dry-run --limit 100
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

QUEUE_URL = os.getenv("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", "")


async def run(limit: int = 1000, dry_run: bool = False):
    """Enqueue Cloudbeds hotels for enrichment."""
    if not QUEUE_URL and not dry_run:
        logger.error("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL not set in .env")
        return 0
    
    await init_db()
    try:
        # Get current queue status
        if QUEUE_URL and not dry_run:
            attrs = get_queue_attributes(QUEUE_URL)
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
            logger.info(f"Queue status: {waiting} waiting, {in_flight} in-flight")
        
        # Get hotels needing enrichment
        # Query excludes hotels with url_status='404' (broken URLs)
        candidates = await repo.get_cloudbeds_hotels_needing_enrichment(limit=limit)
        
        if not candidates:
            logger.info("No Cloudbeds hotels need enrichment")
            return 0
        
        logger.info(f"Found {len(candidates)} Cloudbeds hotels to enqueue")
        
        # Count what needs enrichment
        needs_name = sum(1 for h in candidates if not h.name or h.name.startswith('Unknown'))
        needs_location = sum(1 for h in candidates if not h.city)
        
        logger.info(f"  Needs name: {needs_name}")
        logger.info(f"  Needs location: {needs_location}")
        
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
    parser = argparse.ArgumentParser(description="Enqueue Cloudbeds hotels for enrichment")
    parser.add_argument("--limit", type=int, default=10000, help="Max hotels to enqueue")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enqueued")
    
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
