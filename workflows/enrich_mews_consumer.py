"""Mews enrichment worker - Polls SQS and enriches hotels using Mews API.

Uses lib/mews/api_client.py with hybrid approach:
1. Gets session token via Playwright (once, cached for 30 min)
2. Uses fast parallel httpx API calls for all hotels

Usage:
    uv run python -m workflows.enrich_mews_consumer
    uv run python -m workflows.enrich_mews_consumer --concurrency 10
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import signal
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service
from infra.sqs import receive_messages, delete_messages_batch, get_queue_attributes
from lib.mews.api_client import MewsApiClient

QUEUE_URL = os.getenv("SQS_MEWS_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 600

shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def run_consumer(concurrency: int = 10):
    """Run the SQS consumer with parallel processing."""
    if not QUEUE_URL:
        logger.error("SQS_MEWS_ENRICHMENT_QUEUE_URL not set in .env")
        return

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    await init_db()
    client = MewsApiClient(timeout=30.0, use_brightdata=True)

    try:
        svc = Service()
        logger.info(f"Starting Mews enrichment consumer (concurrency={concurrency})")
        await client.initialize()

        total_processed = 0
        total_enriched = 0
        total_timeouts = 0
        batch_results = []

        while not shutdown_requested:
            messages = receive_messages(
                QUEUE_URL, max_messages=10,
                visibility_timeout=VISIBILITY_TIMEOUT, wait_time_seconds=20,
            )
            if not messages:
                continue

            valid_messages = []
            msg_map = {}  # hotel_id -> receipt_handle
            invalid_handles = []
            for msg in messages:
                body = msg["body"]
                if body.get("hotel_id") and body.get("booking_url"):
                    valid_messages.append((body["hotel_id"], body["booking_url"]))
                    msg_map[body["hotel_id"]] = msg["receipt_handle"]
                else:
                    invalid_handles.append(msg["receipt_handle"])

            if invalid_handles:
                delete_messages_batch(QUEUE_URL, invalid_handles)
            if not valid_messages:
                continue

            results = await asyncio.gather(*[
                svc.process_mews_hotel(h_id, url, client=client)
                for h_id, url in valid_messages
            ])

            # Only delete messages that succeeded or permanently failed.
            # Timeout messages stay in the queue for SQS to redeliver.
            handles_to_delete = []
            for result in results:
                total_processed += 1
                is_timeout = (not result.success and result.error == "timeout")
                if is_timeout:
                    total_timeouts += 1
                    logger.warning(f"  Hotel {result.hotel_id}: TIMEOUT (will retry via SQS)")
                elif result.success and result.name:
                    batch_results.append(result.to_update_dict())
                    total_enriched += 1
                    handles_to_delete.append(msg_map[result.hotel_id])
                    logger.info(f"  Hotel {result.hotel_id}: {result.name[:30]} | {result.city}, {result.country}")
                else:
                    handles_to_delete.append(msg_map[result.hotel_id])

            if handles_to_delete:
                delete_messages_batch(QUEUE_URL, handles_to_delete)

            if len(batch_results) >= 50:
                updated = await svc.batch_update_mews_enrichment(batch_results)
                logger.info(f"Batch update: {updated} hotels")
                batch_results = []

            attrs = get_queue_attributes(QUEUE_URL)
            remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
            logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, {total_timeouts} timeouts, ~{remaining} remaining")

        if batch_results:
            await svc.batch_update_mews_enrichment(batch_results)

        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched, {total_timeouts} timeouts")
    finally:
        svc.send_normalize_trigger()
        await client.close()
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Mews enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
