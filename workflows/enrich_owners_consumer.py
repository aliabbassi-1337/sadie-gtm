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
from services.enrichment.service import Service
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
    svc = Service()
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
                else:
                    invalid_handles.append(msg["receipt_handle"])

            if invalid_handles:
                delete_messages_batch(QUEUE_URL, invalid_handles)
            if not hotels:
                continue

            # Run enrichment + persist via service layer
            result = await svc.run_owner_enrichment(
                hotels=hotels,
                concurrency=concurrency,
                layer=layer if layer != "all" else None,
            )

            # Delete successful messages
            handles_to_delete = []
            for r in result.get("results", []):
                total_processed += 1
                if r.decision_makers:
                    total_found += 1
                if r.hotel_id in msg_map:
                    handles_to_delete.append(msg_map[r.hotel_id])

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
