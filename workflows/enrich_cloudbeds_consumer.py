"""Cloudbeds enrichment consumer - uses fast API (no Playwright needed).

Polls SQS and enriches hotels using the property_info API.
Run on multiple EC2 instances for parallel processing.

Usage:
    uv run python -m workflows.enrich_cloudbeds_consumer
    uv run python -m workflows.enrich_cloudbeds_consumer --concurrency 30
    uv run python -m workflows.enrich_cloudbeds_consumer --legacy  # Use Playwright
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


async def run_consumer(concurrency: int = 20, use_legacy: bool = False, use_brightdata: bool = True):
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
        
        if use_legacy:
            # Legacy Playwright-based consumer
            logger.info("Using legacy Playwright consumer")
            result = await service.consume_cloudbeds_queue(
                queue_url=QUEUE_URL,
                concurrency=concurrency,
                should_stop=lambda: shutdown_requested,
            )
        else:
            # Fast API-based consumer (default)
            logger.info("Using fast API consumer")
            result = await service.consume_cloudbeds_queue_api(
                queue_url=QUEUE_URL,
                concurrency=concurrency,
                use_brightdata=use_brightdata,
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
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent requests (default 20 for API, 6 for legacy)")
    parser.add_argument("--legacy", action="store_true", help="Use legacy Playwright-based consumer")
    parser.add_argument("--no-proxy", action="store_true", help="Disable Brightdata proxy")

    args = parser.parse_args()
    
    # Adjust default concurrency for legacy mode
    concurrency = args.concurrency
    if args.legacy and args.concurrency == 20:
        concurrency = 6  # Lower for Playwright
    
    asyncio.run(run_consumer(
        concurrency=concurrency,
        use_legacy=args.legacy,
        use_brightdata=not args.no_proxy,
    ))


if __name__ == "__main__":
    main()
