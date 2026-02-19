#!/usr/bin/env python3
"""Owner enrichment SQS consumer - polls queue and runs owner/GM discovery.

Usage:
    uv run python workflows/enrich_owners_consumer.py
    uv run python workflows/enrich_owners_consumer.py --concurrency 5
    uv run python workflows/enrich_owners_consumer.py --layer rdap
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
from services.enrichment.owner_enricher import enrich_batch
from infra.sqs import receive_messages, delete_messages_batch, get_queue_attributes

QUEUE_URL = os.getenv("SQS_OWNER_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 1800  # 30 minutes per batch

LAYER_CHOICES = ["ct-certs", "rdap", "whois-history", "dns", "website", "reviews", "email-verify", "gov-data", "abn-asic", "all"]

shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def run_sqs_consumer(concurrency: int = 5, layer: str = "all"):
    """Run the SQS consumer loop."""
    if not QUEUE_URL:
        logger.error("SQS_OWNER_ENRICHMENT_QUEUE_URL not set.")
        return

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    await init_db()
    try:
        logger.info(f"Starting owner enrichment SQS consumer (concurrency={concurrency})")
        total_processed = 0
        total_found = 0
        empty_polls = 0

        # Build layer mask from layer name
        from services.enrichment.owner_models import (
            LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
            LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
            LAYER_GOV_DATA, LAYER_CT_CERTS, LAYER_ABN_ASIC,
            LAYERS_DEFAULT,
        )
        layer_name_map = {
            "ct-certs": LAYER_CT_CERTS, "rdap": LAYER_RDAP,
            "whois-history": LAYER_WHOIS_HISTORY, "dns": LAYER_DNS,
            "website": LAYER_WEBSITE, "reviews": LAYER_REVIEWS,
            "email-verify": LAYER_EMAIL_VERIFY, "gov-data": LAYER_GOV_DATA,
            "abn-asic": LAYER_ABN_ASIC,
        }

        while not shutdown_requested:
            messages = receive_messages(
                QUEUE_URL, max_messages=10,
                visibility_timeout=VISIBILITY_TIMEOUT, wait_time_seconds=20,
            )

            if not messages:
                empty_polls += 1
                if empty_polls >= 3:
                    logger.info("Queue empty (3 consecutive empty polls), stopping.")
                    break
                continue
            empty_polls = 0

            # Parse batched messages — each message contains {"hotels": [...], "layers_mask": ...}
            # Also support legacy format {"hotel_id": ..., "website": ...}
            msg_tasks = []  # list of (receipt_handle, hotels_list, layers_mask)
            invalid_handles = []

            for msg in messages:
                body = msg["body"]
                receipt = msg["receipt_handle"]

                if "hotels" in body:
                    # New batched format
                    hotels_in_msg = body["hotels"]
                    layers_mask = body.get("layers_mask", LAYERS_DEFAULT)
                    valid = [h for h in hotels_in_msg if h.get("hotel_id") and h.get("website")]
                    if valid:
                        msg_tasks.append((receipt, valid, layers_mask))
                    else:
                        invalid_handles.append(receipt)
                elif body.get("hotel_id") and body.get("website"):
                    # Legacy single-hotel format
                    layers_mask = body.get("layers_mask", LAYERS_DEFAULT)
                    msg_tasks.append((receipt, [body], layers_mask))
                else:
                    invalid_handles.append(receipt)

            if invalid_handles:
                delete_messages_batch(QUEUE_URL, invalid_handles)
            if not msg_tasks:
                continue

            total_hotels_this_poll = sum(len(hotels) for _, hotels, _ in msg_tasks)
            logger.info(
                f"Processing {len(msg_tasks)} messages ({total_hotels_this_poll} hotels)"
            )

            # Process all messages concurrently — each runs enrich_batch on its hotels
            # enrich_batch handles incremental persist (flushes every ~20 hotels)

            async def process_message(receipt, hotels, layers_mask):
                """Process one SQS message's hotel batch."""
                # Override layers_mask if a specific layer was requested via CLI
                if layer != "all":
                    layers_mask = layer_name_map.get(layer, LAYERS_DEFAULT)

                results = await enrich_batch(
                    hotels=hotels,
                    concurrency=concurrency,
                    layers=layers_mask,
                    persist=True,
                )
                return receipt, results

            gather_results = await asyncio.gather(
                *[process_message(r, h, lm) for r, h, lm in msg_tasks],
                return_exceptions=True,
            )

            # Count results and delete messages
            handles_to_delete = []
            for gr in gather_results:
                if isinstance(gr, Exception):
                    logger.error(f"Message processing exception: {gr}")
                    continue

                receipt, results = gr

                for r in results:
                    total_processed += 1
                    if r.decision_makers:
                        total_found += 1

                handles_to_delete.append(receipt)
                logger.debug(
                    f"Message done: {len(results)} hotels, "
                    f"{sum(1 for r in results if r.decision_makers)} with contacts"
                )

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


def main():
    parser = argparse.ArgumentParser(description="Owner enrichment SQS consumer")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent enrichments")
    parser.add_argument(
        "--layer", choices=LAYER_CHOICES, default="all",
        help="Run specific layer only",
    )
    args = parser.parse_args()

    asyncio.run(run_sqs_consumer(
        concurrency=args.concurrency, layer=args.layer,
    ))


if __name__ == "__main__":
    main()
