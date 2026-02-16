#!/usr/bin/env python3
"""Owner enrichment consumer - polls SQS and runs owner/GM discovery waterfall.

Two modes:
1. SQS mode (default): Polls SQS queue for hotel enrichment tasks
2. Direct mode (--direct): Processes hotels directly from DB (for testing)

Usage:
    # SQS consumer mode (production)
    uv run python workflows/enrich_owners_consumer.py
    uv run python workflows/enrich_owners_consumer.py --concurrency 5

    # Direct mode (testing, no SQS needed)
    uv run python workflows/enrich_owners_consumer.py --direct --limit 5
    uv run python workflows/enrich_owners_consumer.py --direct --limit 5 --layer rdap

    # Check status
    uv run python workflows/enrich_owners_consumer.py --status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import os
import signal

from loguru import logger

from db.client import init_db, close_db
from services.enrichment import owner_repo as repo
from services.enrichment.owner_enricher import enrich_batch
from infra.sqs import receive_messages, delete_messages_batch, get_queue_attributes
from lib.owner_discovery.models import (
    LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
    LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
)

QUEUE_URL = os.getenv("SQS_OWNER_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 1800  # 30 minutes per batch

LAYER_MAP = {
    "rdap": LAYER_RDAP,
    "whois-history": LAYER_WHOIS_HISTORY,
    "dns": LAYER_DNS,
    "website": LAYER_WEBSITE,
    "reviews": LAYER_REVIEWS,
    "email-verify": LAYER_EMAIL_VERIFY,
    "all": 0xFF,
}

shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def run_sqs_consumer(concurrency: int = 5, layers: int = 0xFF):
    """Run the SQS consumer loop."""
    if not QUEUE_URL:
        logger.error("SQS_OWNER_ENRICHMENT_QUEUE_URL not set. Use --direct for local testing.")
        return

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    await init_db()
    try:
        logger.info(f"Starting owner enrichment SQS consumer (concurrency={concurrency})")
        total_processed = 0
        total_found = 0
        empty_polls = 0

        while not shutdown_requested:
            messages = receive_messages(
                QUEUE_URL, max_messages=min(concurrency, 10),
                visibility_timeout=VISIBILITY_TIMEOUT, wait_time_seconds=20,
            )

            if not messages:
                empty_polls += 1
                if empty_polls >= 3:
                    logger.info("Queue empty (3 consecutive empty polls), stopping.")
                    break
                continue
            empty_polls = 0

            # Parse hotel data from messages
            hotels = []
            msg_map = {}  # hotel_id -> receipt_handle
            invalid_handles = []

            for msg in messages:
                body = msg["body"]
                hotel_id = body.get("hotel_id")
                website = body.get("website")
                if hotel_id and website:
                    hotels.append(body)
                    msg_map[hotel_id] = msg["receipt_handle"]
                    # Override layers if message specifies
                    if body.get("layers_mask"):
                        layers = body["layers_mask"]
                else:
                    invalid_handles.append(msg["receipt_handle"])

            if invalid_handles:
                delete_messages_batch(QUEUE_URL, invalid_handles)
            if not hotels:
                continue

            # Run enrichment
            results = await enrich_batch(
                hotels=hotels, concurrency=concurrency, layers=layers,
            )

            # Delete successful messages
            handles_to_delete = []
            for result in results:
                total_processed += 1
                if result.decision_makers:
                    total_found += 1
                if result.hotel_id in msg_map:
                    handles_to_delete.append(msg_map[result.hotel_id])

            if handles_to_delete:
                delete_messages_batch(QUEUE_URL, handles_to_delete)

            # Log progress
            attrs = get_queue_attributes(QUEUE_URL)
            remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
            logger.info(
                f"Progress: {total_processed} processed, "
                f"{total_found} with contacts, ~{remaining} remaining"
            )

        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_found} with contacts")

    finally:
        await close_db()


async def run_direct(limit: int = 5, concurrency: int = 3, layer: str = "all"):
    """Direct mode - process hotels from DB without SQS (for local testing)."""
    await init_db()
    try:
        layer_mask = LAYER_MAP.get(layer, 0xFF)
        layer_filter = layer_mask if layer != "all" else None

        logger.info(f"Direct mode: limit={limit}, concurrency={concurrency}, layer={layer}")

        hotels = await repo.get_hotels_pending_owner_enrichment(
            limit=limit, layer=layer_filter,
        )
        if not hotels:
            logger.info("No hotels pending owner enrichment")
            return

        logger.info(f"Processing {len(hotels)} hotels...")
        for h in hotels:
            logger.info(f"  [{h['hotel_id']}] {h['name']} - {h.get('website', 'N/A')[:60]}")

        results = await enrich_batch(
            hotels=hotels, concurrency=concurrency, layers=layer_mask,
        )

        # Print results
        found = sum(1 for r in results if r.found_any)
        total_contacts = sum(len(r.decision_makers) for r in results)
        verified = sum(
            sum(1 for dm in r.decision_makers if dm.email_verified)
            for r in results
        )

        print(f"\n{'='*50}")
        print(f"RESULTS")
        print(f"{'='*50}")
        for r in results:
            print(f"\n  [{r.hotel_id}] {r.domain}:")
            if r.decision_makers:
                for dm in r.decision_makers:
                    v = " [VERIFIED]" if dm.email_verified else ""
                    print(f"    -> {dm.full_name or '?'} | {dm.title or '?'} | {dm.email or '?'}{v} | src={dm.source}")
            else:
                print(f"    (no contacts)")

        print(f"\n  Hotels: {len(results)} | With contacts: {found} | Contacts: {total_contacts} | Verified: {verified}")

    finally:
        await close_db()


async def show_status():
    """Show enrichment pipeline status."""
    await init_db()
    try:
        stats = await repo.get_enrichment_stats()

        print("\n=== Owner Enrichment Status ===")
        print(f"  Hotels w/ website:  {stats.get('total_with_website', 0):,}")
        print(f"  Complete:           {stats.get('complete', 0):,}")
        print(f"  No results:         {stats.get('no_results', 0):,}")
        print(f"  ---")
        print(f"  With contacts:      {stats.get('hotels_with_contacts', 0):,}")
        print(f"  Total contacts:     {stats.get('total_contacts', 0):,}")
        print(f"  Verified emails:    {stats.get('verified_emails', 0):,}")

        if QUEUE_URL:
            attrs = get_queue_attributes(QUEUE_URL)
            pending = int(attrs.get("ApproximateNumberOfMessages", 0))
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            print(f"  ---")
            print(f"  SQS pending:        {pending}")
            print(f"  SQS in-flight:      {in_flight}")
        print()
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Owner enrichment consumer")
    parser.add_argument("--direct", action="store_true", help="Direct DB mode (no SQS, for testing)")
    parser.add_argument("--limit", type=int, default=5, help="Max hotels (direct mode)")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent enrichments")
    parser.add_argument(
        "--layer", choices=list(LAYER_MAP.keys()), default="all",
        help="Run specific layer only",
    )
    parser.add_argument("--status", action="store_true", help="Show enrichment status")
    args = parser.parse_args()

    if args.status:
        asyncio.run(show_status())
    elif args.direct:
        asyncio.run(run_direct(
            limit=args.limit, concurrency=args.concurrency, layer=args.layer,
        ))
    else:
        layer_mask = LAYER_MAP.get(args.layer, 0xFF)
        asyncio.run(run_sqs_consumer(
            concurrency=args.concurrency, layers=layer_mask,
        ))


if __name__ == "__main__":
    main()
