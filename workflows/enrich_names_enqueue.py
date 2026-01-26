"""Enqueue hotels for enrichment via SQS.

Finds hotels needing name and/or address enrichment, queues them for workers
to scrape from booking pages.

Usage:
    uv run python -m workflows.enrich_names_enqueue --limit 1000
    uv run python -m workflows.enrich_names_enqueue --limit 5000 --engine cloudbeds
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import os
from typing import Optional
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service as EnrichmentService
from infra.sqs import send_messages_batch, get_queue_attributes

QUEUE_URL = os.getenv("SQS_NAME_ENRICHMENT_QUEUE_URL", "")


async def run(
    limit: int = 1000,
    engine: Optional[str] = None,
    dry_run: bool = False,
):
    """Enqueue hotels for enrichment."""
    if not QUEUE_URL:
        logger.error("SQS_NAME_ENRICHMENT_QUEUE_URL not set")
        return 0
    
    await init_db()
    try:
        service = EnrichmentService()
        
        # Get hotels needing any enrichment
        hotels = await service.get_hotels_needing_booking_page_enrichment(
            limit=limit,
            engine=engine,
        )
        
        if not hotels:
            logger.info("No hotels found needing enrichment")
            return 0
        
        # Count what needs enrichment
        needs_name = sum(1 for h in hotels if h.get("needs_name"))
        needs_address = sum(1 for h in hotels if h.get("needs_address"))
        needs_both = sum(1 for h in hotels if h.get("needs_name") and h.get("needs_address"))
        
        logger.info(f"Found {len(hotels)} hotels needing enrichment")
        logger.info(f"  Needs name: {needs_name}")
        logger.info(f"  Needs address: {needs_address}")
        logger.info(f"  Needs both: {needs_both}")
        
        # Group by engine for logging
        by_engine = {}
        for h in hotels:
            eng = h.get("engine_name", "unknown")
            by_engine[eng] = by_engine.get(eng, 0) + 1
        for eng, count in sorted(by_engine.items()):
            logger.info(f"  {eng}: {count}")
        
        if dry_run:
            logger.info("Dry run - not sending to SQS")
            return len(hotels)
        
        # Create messages
        messages = [
            {
                "hotel_id": h["id"],
                "booking_url": h["booking_url"],
                "slug": h.get("slug"),
                "engine": h.get("engine_name"),
            }
            for h in hotels
        ]
        
        # Send to SQS
        sent = send_messages_batch(QUEUE_URL, messages)
        logger.info(f"Sent {sent} messages to SQS")
        
        attrs = get_queue_attributes(QUEUE_URL)
        logger.info(f"Queue stats: {attrs.get('ApproximateNumberOfMessages', 0)} messages pending")
        
        return sent
        
    finally:
        await close_db()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Enqueue hotels for enrichment via SQS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run python -m workflows.enrich_names_enqueue --limit 1000
    uv run python -m workflows.enrich_names_enqueue --engine cloudbeds --limit 5000
    uv run python -m workflows.enrich_names_enqueue --limit 1000 --dry-run

The consumer automatically detects what each hotel needs:
- Missing name (null/empty/Unknown) -> extracts from booking page
- Missing address (null city/state) -> extracts from booking page
- Already has data -> preserves existing values

Environment:
    SQS_NAME_ENRICHMENT_QUEUE_URL - Required. The SQS queue URL.
        """
    )
    
    parser.add_argument("--limit", "-l", type=int, default=1000)
    parser.add_argument("--engine", "-e", type=str, help="Filter by booking engine")
    parser.add_argument("--dry-run", action="store_true")
    
    args = parser.parse_args()
    asyncio.run(run(args.limit, args.engine, args.dry_run))


if __name__ == "__main__":
    main()
