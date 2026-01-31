"""Cloudbeds enrichment consumer - thin wrapper around the enrichment service.

Polls SQS and enriches hotels using Playwright.
Run on multiple EC2 instances for parallel processing.

Usage:
    uv run python -m workflows.enrich_cloudbeds_consumer
    uv run python -m workflows.enrich_cloudbeds_consumer --concurrency 10
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


QUEUE_URL = os.getenv("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", "")

shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def run_consumer(concurrency: int = 5):
    """Run the SQS consumer."""
    global shutdown_requested

    if not QUEUE_URL:
        logger.error("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL not set in .env")
        return

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    await init_db()

    try:
        service = Service()
        result = await service.consume_cloudbeds_queue(
            queue_url=QUEUE_URL,
            concurrency=concurrency,
            should_stop=lambda: shutdown_requested,
        )

        print("\n" + "=" * 60)
        print("CONSUMER STOPPED")
        print("=" * 60)
        print(f"  Messages processed: {result.messages_processed}")
        print(f"  Hotels processed:   {result.hotels_processed}")
        print(f"  Hotels enriched:    {result.hotels_enriched}")
        print(f"  Hotels failed:      {result.hotels_failed}")
        print("=" * 60 + "\n")

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Cloudbeds enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent browser contexts")

    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
