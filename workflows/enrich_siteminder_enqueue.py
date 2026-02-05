"""Enqueue SiteMinder hotels for distributed enrichment via SQS.

Usage:
    # Enqueue hotels needing name enrichment
    uv run python -m workflows.enrich_siteminder_enqueue --limit 5000
    
    # Enqueue hotels missing location (state) for US
    uv run python -m workflows.enrich_siteminder_enqueue --missing-location --country "United States" --limit 1000
    
    # Dry run
    uv run python -m workflows.enrich_siteminder_enqueue --dry-run --limit 100
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

QUEUE_URL = os.getenv("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL", "")


async def run(
    limit: int = 1000,
    dry_run: bool = False,
    missing_location: bool = False,
    country: str = None,
):
    """Enqueue SiteMinder hotels for enrichment."""
    if not QUEUE_URL and not dry_run:
        logger.error("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL not set in .env")
        return 0
    
    await init_db()
    try:
        # Get current queue status
        if QUEUE_URL and not dry_run:
            attrs = get_queue_attributes(QUEUE_URL)
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
            logger.info(f"Queue status: {waiting} waiting, {in_flight} in-flight")
            
            # Skip if queue already has significant backlog
            if waiting > 100:
                logger.info(f"Skipping enqueue - queue already has {waiting} messages")
                return 0
        
        # Get hotels based on mode
        if missing_location:
            candidates = await repo.get_siteminder_hotels_missing_location(
                limit=limit, country=country
            )
            mode_desc = f"missing location (country={country or 'all'})"
        else:
            candidates = await repo.get_siteminder_hotels_needing_enrichment(limit=limit)
            mode_desc = "needing name enrichment"
        
        if not candidates:
            logger.info(f"No SiteMinder hotels {mode_desc}")
            return 0
        
        logger.info(f"Found {len(candidates)} SiteMinder hotels {mode_desc}")
        
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
        
        # Send in parallel batches
        sent = send_messages_batch(QUEUE_URL, messages)
        
        logger.info(f"Enqueued {sent}/{len(candidates)} hotels")
        return sent
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Enqueue SiteMinder hotels for enrichment")
    parser.add_argument("--limit", type=int, default=50000, help="Max hotels to enqueue")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enqueued")
    parser.add_argument(
        "--missing-location",
        action="store_true",
        help="Enqueue hotels missing state (for location enrichment)"
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Filter by country (e.g., 'United States')"
    )
    
    args = parser.parse_args()
    asyncio.run(run(
        limit=args.limit,
        dry_run=args.dry_run,
        missing_location=args.missing_location,
        country=args.country,
    ))


if __name__ == "__main__":
    main()
