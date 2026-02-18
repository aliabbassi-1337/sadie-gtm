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
from services.enrichment.service import Service
from infra.sqs import send_messages_batch, get_queue_attributes

QUEUE_URL = os.getenv("SQS_OWNER_ENRICHMENT_QUEUE_URL", "")
MAX_QUEUE_DEPTH = 2000

LAYER_CHOICES = ["ct-certs", "rdap", "whois-history", "dns", "website", "reviews", "email-verify", "gov-data", "abn-asic", "all"]
LAYER_MAP = {
    "ct-certs": 128, "rdap": 1, "whois-history": 2, "dns": 4,
    "website": 8, "reviews": 16, "email-verify": 32,
    "gov-data": 64, "abn-asic": 256, "all": 383,
}


BATCH_SIZE = 10  # Hotels per SQS message


async def enqueue(limit: int = 500, layer: str = "all", force: bool = False, source: str = None):
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

        if source:
            from db.client import get_conn
            async with get_conn() as conn:
                rows = await conn.fetch("""
                    SELECT id, name, website, city, state, country
                    FROM sadie_gtm.hotels
                    WHERE (external_id_type = $1 OR source LIKE '%::' || $1)
                      AND website IS NOT NULL AND website != ''
                    ORDER BY id
                    LIMIT $2
                """, source, limit)
            hotels = [{"hotel_id": r["id"], "name": r["name"], "website": r["website"],
                       "city": r["city"], "state": r["state"], "country": r["country"]} for r in rows]
            logger.info(f"Filtered to {len(hotels)} hotels with source={source!r}")
        else:
            svc = Service()
            layer_filter = layer_mask if layer != "all" else None
            hotels = await svc.get_hotels_pending_owner_enrichment(
                limit=limit, layer=layer_filter,
            )

        if not hotels:
            logger.info("No hotels pending owner enrichment")
            return

        # Build SQS messages — batch BATCH_SIZE hotels per message
        messages = []
        for i in range(0, len(hotels), BATCH_SIZE):
            chunk = hotels[i:i + BATCH_SIZE]
            messages.append({
                "hotels": [
                    {
                        "hotel_id": h["hotel_id"],
                        "name": h["name"],
                        "website": h.get("website", ""),
                        "city": h.get("city"),
                        "state": h.get("state"),
                        "country": h.get("country"),
                    }
                    for h in chunk
                ],
                "layer": layer,
                "layers_mask": layer_mask,
            })

        sent = send_messages_batch(QUEUE_URL, messages)
        logger.info(
            f"Enqueued {sent} messages ({len(hotels)} hotels, "
            f"{BATCH_SIZE}/msg) for owner enrichment (layer={layer})"
        )

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
        svc = Service()
        stats = await svc.get_owner_enrichment_stats()
        logger.info(
            "\n=== Owner Enrichment Queue ===\n"
            f"  SQS pending:    {pending}\n"
            f"  SQS in-flight:  {in_flight}\n"
            f"\n=== DB Status ===\n"
            f"  Hotels w/ website:  {stats.get('total_with_website', 0):,}\n"
            f"  Complete:           {stats.get('complete', 0):,}\n"
            f"  No results:         {stats.get('no_results', 0):,}\n"
            f"  With contacts:      {stats.get('hotels_with_contacts', 0):,}\n"
            f"  Total contacts:     {stats.get('total_contacts', 0):,}\n"
            f"  Verified emails:    {stats.get('verified_emails', 0):,}"
        )
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Owner enrichment enqueuer")
    parser.add_argument("--limit", type=int, default=500, help="Max hotels to enqueue")
    parser.add_argument(
        "--layer", choices=LAYER_CHOICES, default="all",
        help="Enqueue for specific layer only",
    )
    parser.add_argument("--force", action="store_true", help="Force enqueue even if queue is full")
    parser.add_argument("--status", action="store_true", help="Show queue and enrichment status")
    parser.add_argument(
        "--source", type=str, default=None,
        help="Filter hotels by source (e.g. 'big4', 'rms')",
    )
    args = parser.parse_args()

    if args.status:
        asyncio.run(show_status())
    else:
        asyncio.run(enqueue(limit=args.limit, layer=args.layer, force=args.force, source=args.source))


if __name__ == "__main__":
    main()
