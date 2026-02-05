#!/usr/bin/env python3
"""SiteMinder enrichment worker - Polls SQS and enriches hotels using SiteMinder API.

Two modes:
1. SQS mode (default): Polls SQS queue for hotel enrichment tasks
2. Direct mode (--direct): Polls database directly (legacy mode)

Extracts hotel data:
- Name, website
- Address, city, state, country (TODO: requires API integration)
- Email, phone (TODO: requires API integration)

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
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from loguru import logger

from db.client import init_db, close_db
from services.enrichment import repo
from infra.sqs import receive_messages, delete_messages_batch, get_queue_attributes
from lib.siteminder.api_client import SiteMinderClient, extract_channel_code

QUEUE_URL = os.getenv("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 600  # 10 minutes per batch

shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


@dataclass
class EnrichmentResult:
    """Result of enriching a hotel."""
    hotel_id: int
    success: bool
    name: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for batch update."""
        return {
            "hotel_id": self.hotel_id,
            "name": self.name,
            "website": self.website,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "email": self.email,
            "phone": self.phone,
        }


async def process_hotel(
    client: SiteMinderClient,
    hotel_id: int,
    booking_url: str,
) -> EnrichmentResult:
    """Process a single hotel using the SiteMinder property API.
    
    Uses the full 'property' endpoint which returns address, contact, and coordinates.
    """
    try:
        data = await client.get_property_data_from_url(booking_url)
        
        if not data or not data.name:
            return EnrichmentResult(hotel_id=hotel_id, success=False, error="no_data")
        
        return EnrichmentResult(
            hotel_id=hotel_id,
            success=True,
            name=data.name,
            website=data.website,
            address=data.address,
            city=data.city,
            state=data.state,
            country=data.country,
            email=data.email,
            phone=data.phone,
        )
    except Exception as e:
        return EnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])


async def run_sqs_consumer(
    concurrency: int = 20,
    use_brightdata: bool = False,
    force_overwrite: bool = False,
):
    """Run the SQS consumer with parallel processing."""
    if not QUEUE_URL:
        logger.error("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL not set in .env")
        return
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    await init_db()
    
    try:
        logger.info(f"Starting SiteMinder SQS consumer (concurrency={concurrency})")
        if use_brightdata:
            logger.info("Using Brightdata proxy")
        
        total_processed = 0
        total_enriched = 0
        batch_results = []
        failed_ids = []
        
        async with SiteMinderClient(use_brightdata=use_brightdata) as client:
            semaphore = asyncio.Semaphore(concurrency)
            
            while not shutdown_requested:
                # Receive batch of messages
                messages = receive_messages(
                    QUEUE_URL,
                    max_messages=10,
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    wait_time_seconds=20,
                )
                
                if not messages:
                    logger.debug("No messages, waiting...")
                    continue
                
                # Collect valid messages and receipt handles
                valid_messages = []
                receipt_handles = []
                
                for msg in messages:
                    receipt_handles.append(msg["receipt_handle"])
                    body = msg["body"]
                    hotel_id = body.get("hotel_id")
                    booking_url = body.get("booking_url")
                    if hotel_id and booking_url:
                        valid_messages.append((hotel_id, booking_url))
                
                if not valid_messages:
                    delete_messages_batch(QUEUE_URL, receipt_handles)
                    continue
                
                # Process hotels concurrently
                async def process_with_semaphore(hotel_id, booking_url):
                    async with semaphore:
                        return await process_hotel(client, hotel_id, booking_url)
                
                results = await asyncio.gather(*[
                    process_with_semaphore(h_id, url)
                    for h_id, url in valid_messages
                ])
                
                # Delete messages after processing
                delete_messages_batch(QUEUE_URL, receipt_handles)
                
                # Collect results
                for result in results:
                    total_processed += 1
                    if result.success and result.name:
                        batch_results.append(result.to_dict())
                        total_enriched += 1
                        logger.info(f"  Hotel {result.hotel_id}: {result.name[:40] if result.name else 'Unknown'}")
                    else:
                        failed_ids.append(result.hotel_id)
                        logger.debug(f"  Hotel {result.hotel_id}: {result.error}")
                
                # Batch update every 50 results
                if len(batch_results) >= 50:
                    updated = await repo.batch_update_siteminder_enrichment(
                        batch_results, force_overwrite=force_overwrite
                    )
                    logger.info(f"Batch update: {updated} hotels")
                    batch_results = []
                
                # Mark failed
                if len(failed_ids) >= 50:
                    await repo.batch_set_siteminder_enrichment_failed(failed_ids)
                    failed_ids = []
                
                # Log progress
                attrs = get_queue_attributes(QUEUE_URL)
                remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, ~{remaining} remaining")
        
        # Final flush
        if batch_results:
            updated = await repo.batch_update_siteminder_enrichment(
                batch_results, force_overwrite=force_overwrite
            )
            logger.info(f"Final batch: {updated} hotels")
        if failed_ids:
            await repo.batch_set_siteminder_enrichment_failed(failed_ids)
        
        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
        
    finally:
        await close_db()


async def run_direct(
    limit: int = 1000,
    concurrency: int = 20,
    batch_size: int = 50,
    use_brightdata: bool = False,
    force_overwrite: bool = False,
):
    """Run direct DB polling mode (legacy)."""
    await init_db()
    
    try:
        # Get pending hotels
        hotels = await repo.get_siteminder_hotels_needing_enrichment(limit=limit)
        
        if not hotels:
            logger.info("No SiteMinder hotels pending enrichment")
            return
        
        logger.info(f"Found {len(hotels)} SiteMinder hotels to enrich")
        
        enriched = 0
        failed = 0
        batch_results = []
        failed_ids = []
        
        async with SiteMinderClient(use_brightdata=use_brightdata) as client:
            semaphore = asyncio.Semaphore(concurrency)
            
            async def process_with_semaphore(hotel):
                async with semaphore:
                    return await process_hotel(client, hotel.id, hotel.booking_url)
            
            # Process in batches
            for i in range(0, len(hotels), batch_size):
                batch = hotels[i:i + batch_size]
                
                results = await asyncio.gather(*[
                    process_with_semaphore(h)
                    for h in batch
                ])
                
                for result in results:
                    if result.success and result.name:
                        batch_results.append(result.to_dict())
                        enriched += 1
                    else:
                        failed_ids.append(result.hotel_id)
                        failed += 1
                
                # Flush updates
                if batch_results:
                    await repo.batch_update_siteminder_enrichment(
                        batch_results, force_overwrite=force_overwrite
                    )
                    batch_results = []
                if failed_ids:
                    await repo.batch_set_siteminder_enrichment_failed(failed_ids)
                    failed_ids = []
                
                processed = min(i + batch_size, len(hotels))
                logger.info(f"Progress: {processed}/{len(hotels)} (enriched: {enriched}, failed: {failed})")
        
        logger.info("=" * 60)
        logger.info("SITEMINDER ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total processed: {len(hotels)}")
        logger.info(f"Enriched: {enriched}")
        logger.info(f"Failed: {failed}")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="SiteMinder enrichment consumer")
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Use direct DB polling instead of SQS"
    )
    parser.add_argument("--limit", "-l", type=int, default=1000, help="Max hotels (direct mode)")
    parser.add_argument("--concurrency", "-c", type=int, default=20, help="Concurrent API calls")
    parser.add_argument("--batch-size", "-b", type=int, default=50, help="Batch size for DB updates")
    parser.add_argument("--brightdata", action="store_true", help="Use Brightdata proxy")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing data")
    
    args = parser.parse_args()
    
    if args.direct:
        asyncio.run(run_direct(
            limit=args.limit,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            use_brightdata=args.brightdata,
            force_overwrite=args.force,
        ))
    else:
        asyncio.run(run_sqs_consumer(
            concurrency=args.concurrency,
            use_brightdata=args.brightdata,
            force_overwrite=args.force,
        ))


if __name__ == "__main__":
    main()
