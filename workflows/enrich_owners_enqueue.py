#!/usr/bin/env python3
"""Owner enrichment enqueuer - sends hotels to SQS for owner/GM discovery.

Usage:
    # Enqueue hotels needing owner enrichment
    uv run python workflows/enrich_owners_enqueue.py --limit 500

    # Force re-enqueue all hotels
    uv run python workflows/enrich_owners_enqueue.py --limit 5000 --force

    # Enqueue specific layer only
    uv run python workflows/enrich_owners_enqueue.py --limit 500 --layer website

    # Check queue status
    uv run python workflows/enrich_owners_enqueue.py --status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import os

from loguru import logger

from db.client import init_db, close_db
from services.enrichment import owner_repo as repo
from infra.sqs import send_messages_batch, get_queue_attributes
from lib.owner_discovery.models import (
    LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
    LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
)

QUEUE_URL = os.getenv("SQS_OWNER_ENRICHMENT_QUEUE_URL", "")
MAX_QUEUE_DEPTH = 2000

LAYER_MAP = {
    "rdap": LAYER_RDAP,
    "whois-history": LAYER_WHOIS_HISTORY,
    "dns": LAYER_DNS,
    "website": LAYER_WEBSITE,
    "reviews": LAYER_REVIEWS,
    "email-verify": LAYER_EMAIL_VERIFY,
    "all": 0xFF,
}


async def enqueue(limit: int = 500, layer: str = "all", force: bool = False):
    """Enqueue hotels needing owner enrichment to SQS."""
    if not QUEUE_URL:
        logger.error("SQS_OWNER_ENRICHMENT_QUEUE_URL not set")
        return

    # Check queue depth
    attrs = get_queue_attributes(QUEUE_URL)
    pending = int(attrs.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    if pending + in_flight > MAX_QUEUE_DEPTH and not force:
        logger.warning(f"Queue already has {pending} pending + {in_flight} in-flight. Use --force to override.")
        return

    await init_db()
    try:
        layer_mask = LAYER_MAP.get(layer, 0xFF)
        layer_filter = layer_mask if layer != "all" else None

        hotels = await repo.get_hotels_pending_owner_enrichment(
            limit=limit, layer=layer_filter,
        )
        if not hotels:
            logger.info("No hotels pending owner enrichment")
            return

        # Build SQS messages (one hotel per message for simple retry)
        messages = []
        for h in hotels:
            messages.append({
                "hotel_id": h["hotel_id"],
                "name": h["name"],
                "website": h.get("website", ""),
                "city": h.get("city"),
                "state": h.get("state"),
                "country": h.get("country"),
                "layer": layer,
                "layers_mask": layer_mask,
            })

        sent = send_messages_batch(QUEUE_URL, messages)
        logger.info(f"Enqueued {sent}/{len(hotels)} hotels for owner enrichment (layer={layer})")

    finally:
        await close_db()


async def show_status():
    """Show queue stats."""
    if not QUEUE_URL:
        logger.error("SQS_OWNER_ENRICHMENT_QUEUE_URL not set")
        return

    attrs = get_queue_attributes(QUEUE_URL)
    pending = int(attrs.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))

    await init_db()
    try:
        stats = await repo.get_enrichment_stats()
        print("\n=== Owner Enrichment Queue ===")
        print(f"  SQS pending:    {pending}")
        print(f"  SQS in-flight:  {in_flight}")
        print(f"\n=== DB Status ===")
        print(f"  Hotels w/ website:  {stats.get('total_with_website', 0):,}")
        print(f"  Complete:           {stats.get('complete', 0):,}")
        print(f"  No results:         {stats.get('no_results', 0):,}")
        print(f"  With contacts:      {stats.get('hotels_with_contacts', 0):,}")
        print(f"  Total contacts:     {stats.get('total_contacts', 0):,}")
        print(f"  Verified emails:    {stats.get('verified_emails', 0):,}")
        print()
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Owner enrichment enqueuer")
    parser.add_argument("--limit", type=int, default=500, help="Max hotels to enqueue")
    parser.add_argument(
        "--layer", choices=list(LAYER_MAP.keys()), default="all",
        help="Enqueue for specific layer only",
    )
    parser.add_argument("--force", action="store_true", help="Force enqueue even if queue is full")
    parser.add_argument("--status", action="store_true", help="Show queue and enrichment status")
    args = parser.parse_args()

    if args.status:
        asyncio.run(show_status())
    else:
        asyncio.run(enqueue(limit=args.limit, layer=args.layer, force=args.force))


if __name__ == "__main__":
    main()
