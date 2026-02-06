#!/usr/bin/env python3
"""SiteMinder enrichment worker - Polls SQS and enriches hotels using SiteMinder API.

Two modes:
1. SQS mode (default): Polls SQS queue for hotel enrichment tasks
2. Direct mode (--direct): Polls database directly (legacy mode)

Usage:
    # SQS consumer mode (production)
    uv run python -m workflows.enrich_siteminder_consumer
    uv run python -m workflows.enrich_siteminder_consumer --concurrency 20
    
    # Direct DB polling mode (legacy)
    uv run python -m workflows.enrich_siteminder_consumer --direct --limit 1000
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
from services.enrichment import repo
from services.enrichment.service import Service, SiteMinderEnrichmentResult
from infra.sqs import receive_messages, delete_messages_batch, get_queue_attributes
from lib.siteminder.api_client import SiteMinderClient

QUEUE_URL = os.getenv("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 600  # 10 minutes per batch

shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def run_sqs_consumer(concurrency: int = 20, use_brightdata: bool = False):
    """Run the SQS consumer loop."""
    if not QUEUE_URL:
        logger.error("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL not set in .env")
        return

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    await init_db()
    try:
        service = Service()
        logger.info(f"Starting SiteMinder SQS consumer (concurrency={concurrency})")

        total_processed = 0
        total_enriched = 0
        batch_results = []
        failed_ids = []

        async with SiteMinderClient(use_brightdata=use_brightdata) as client:
            semaphore = asyncio.Semaphore(concurrency)

            while not shutdown_requested:
                messages = receive_messages(
                    QUEUE_URL, max_messages=10,
                    visibility_timeout=VISIBILITY_TIMEOUT, wait_time_seconds=20,
                )
                if not messages:
                    continue

                valid_messages = []
                receipt_handles = []
                for msg in messages:
                    receipt_handles.append(msg["receipt_handle"])
                    body = msg["body"]
                    if body.get("hotel_id") and body.get("booking_url"):
                        valid_messages.append((body["hotel_id"], body["booking_url"]))

                if not valid_messages:
                    delete_messages_batch(QUEUE_URL, receipt_handles)
                    continue

                async def process(hotel_id, url):
                    async with semaphore:
                        return await service.process_siteminder_hotel(hotel_id, url, client=client)

                results = await asyncio.gather(*[process(h, u) for h, u in valid_messages])
                delete_messages_batch(QUEUE_URL, receipt_handles)

                for result in results:
                    total_processed += 1
                    if result.success and result.name:
                        batch_results.append(result.to_update_dict())
                        total_enriched += 1
                        logger.info(f"  Hotel {result.hotel_id}: {result.name[:40]}")
                    else:
                        failed_ids.append(result.hotel_id)

                if len(batch_results) >= 50:
                    await repo.batch_update_siteminder_enrichment(batch_results)
                    batch_results = []
                if len(failed_ids) >= 50:
                    await repo.batch_set_siteminder_enrichment_failed(failed_ids)
                    failed_ids = []

                attrs = get_queue_attributes(QUEUE_URL)
                remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, ~{remaining} remaining")

        if batch_results:
            await repo.batch_update_siteminder_enrichment(batch_results)
        if failed_ids:
            await repo.batch_set_siteminder_enrichment_failed(failed_ids)

        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
    finally:
        await close_db()


async def run_direct(limit: int = 1000, concurrency: int = 20, use_brightdata: bool = False):
    """Run direct DB polling mode (legacy)."""
    await init_db()
    try:
        service = Service()
        hotels = await repo.get_siteminder_hotels_needing_enrichment(limit=limit)
        if not hotels:
            logger.info("No SiteMinder hotels pending enrichment")
            return

        logger.info(f"Found {len(hotels)} SiteMinder hotels to enrich")
        enriched = 0
        failed = 0

        async with SiteMinderClient(use_brightdata=use_brightdata) as client:
            semaphore = asyncio.Semaphore(concurrency)

            for i in range(0, len(hotels), 50):
                batch = hotels[i:i + 50]

                async def process(h):
                    async with semaphore:
                        return await service.process_siteminder_hotel(h.id, h.booking_url, client=client)

                results = await asyncio.gather(*[process(h) for h in batch])

                ok = [r.to_update_dict() for r in results if r.success and r.name]
                fail = [r.hotel_id for r in results if not r.success]
                if ok:
                    await repo.batch_update_siteminder_enrichment(ok)
                    enriched += len(ok)
                if fail:
                    await repo.batch_set_siteminder_enrichment_failed(fail)
                    failed += len(fail)

                logger.info(f"Progress: {min(i + 50, len(hotels))}/{len(hotels)} (enriched: {enriched}, failed: {failed})")

        logger.info(f"Done. Enriched: {enriched}, Failed: {failed}")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="SiteMinder enrichment consumer")
    parser.add_argument("--direct", action="store_true", help="Use direct DB polling instead of SQS")
    parser.add_argument("--limit", "-l", type=int, default=1000, help="Max hotels (direct mode)")
    parser.add_argument("--concurrency", "-c", type=int, default=20, help="Concurrent API calls")
    parser.add_argument("--brightdata", action="store_true", help="Use Brightdata proxy")
    args = parser.parse_args()

    if args.direct:
        asyncio.run(run_direct(limit=args.limit, concurrency=args.concurrency, use_brightdata=args.brightdata))
    else:
        asyncio.run(run_sqs_consumer(concurrency=args.concurrency, use_brightdata=args.brightdata))


if __name__ == "__main__":
    main()
