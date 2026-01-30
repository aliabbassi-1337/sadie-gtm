"""Mews enrichment worker - Polls SQS and enriches hotels using Mews API.

Uses lib/mews/api_client.py to intercept Mews's hidden API (configurations/get)
and extract full hotel data:
- Name, address, city, country
- Email, phone
- Lat/lon coordinates

Usage:
    uv run python -m workflows.enrich_mews_consumer
    uv run python -m workflows.enrich_mews_consumer --concurrency 6
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import signal
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from loguru import logger
# Playwright is used internally by MewsApiClient

from db.client import init_db, close_db
from services.enrichment import repo
from infra.sqs import receive_messages, delete_message, get_queue_attributes
from lib.mews.api_client import MewsApiClient

QUEUE_URL = os.getenv("SQS_MEWS_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 300  # 5 minutes per batch (handles slow pages)

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
    address: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for batch update."""
        return {
            "hotel_id": self.hotel_id,
            "name": self.name,
            "address": self.address,
            "city": self.city,
            "country": self.country,
            "email": self.email,
            "phone": self.phone,
            "lat": self.lat,
            "lon": self.lon,
        }


async def process_hotel_with_client(client: MewsApiClient, hotel_id: int, booking_url: str) -> EnrichmentResult:
    """Process a single hotel using the Mews API client."""
    # Extract slug from booking URL
    # URL format: https://app.mews.com/distributor/{uuid}
    try:
        slug = booking_url.rstrip("/").split("/")[-1]
    except Exception:
        return EnrichmentResult(hotel_id=hotel_id, success=False, error="invalid_url")
    
    try:
        data = await client.extract(slug)
        
        if not data or not data.is_valid:
            return EnrichmentResult(hotel_id=hotel_id, success=False, error="no_data")
        
        return EnrichmentResult(
            hotel_id=hotel_id,
            success=True,
            name=data.name,
            address=data.address,
            city=data.city,
            country=data.country,
            email=data.email,
            phone=data.phone,
            lat=data.lat,
            lon=data.lon,
        )
    except Exception as e:
        return EnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])


async def run_consumer(concurrency: int = 5):
    """Run the SQS consumer with Mews API clients."""
    if not QUEUE_URL:
        logger.error("SQS_MEWS_ENRICHMENT_QUEUE_URL not set in .env")
        return
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    await init_db()
    
    # Create pool of API clients for concurrent processing
    clients: List[MewsApiClient] = []
    
    try:
        logger.info(f"Starting Mews enrichment consumer (concurrency={concurrency})")
        
        # Initialize API clients
        for i in range(concurrency):
            client = MewsApiClient(timeout=30.0)
            await client.initialize()
            clients.append(client)
        
        logger.info(f"Created {concurrency} Mews API clients")
        
        total_processed = 0
        total_enriched = 0
        batch_results = []
        
        while not shutdown_requested:
            # Receive messages
            messages = receive_messages(
                QUEUE_URL,
                max_messages=min(concurrency, 10),
                visibility_timeout=VISIBILITY_TIMEOUT,
                wait_time_seconds=10,
            )
            
            if not messages:
                logger.debug("No messages, waiting...")
                continue
            
            # Process messages - one per client
            valid_messages = []
            for msg in messages:
                body = msg["body"]
                hotel_id = body.get("hotel_id")
                booking_url = body.get("booking_url")
                
                if not hotel_id or not booking_url:
                    delete_message(QUEUE_URL, msg["receipt_handle"])
                    continue
                
                valid_messages.append((msg, hotel_id, booking_url))
            
            # Process batch
            tasks = []
            message_map = {}
            
            for i, (msg, hotel_id, booking_url) in enumerate(valid_messages[:concurrency]):
                message_map[hotel_id] = msg
                tasks.append(process_hotel_with_client(clients[i], hotel_id, booking_url))
            
            if not tasks:
                continue
            
            # Wait for all to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Task error: {result}")
                    continue
                
                msg = message_map.get(result.hotel_id)
                if not msg:
                    continue
                
                if result.success and result.name:
                    batch_results.append(result.to_dict())
                    total_enriched += 1
                    logger.info(f"  Hotel {result.hotel_id}: {result.name[:30]} | {result.city}, {result.country}")
                elif result.error:
                    logger.debug(f"  Hotel {result.hotel_id}: {result.error}")
                
                # Delete from queue
                delete_message(QUEUE_URL, msg["receipt_handle"])
                total_processed += 1
            
            # Batch update every 50 results
            if len(batch_results) >= 50:
                updated = await repo.batch_update_mews_enrichment(batch_results)
                logger.info(f"Batch update: {updated} hotels")
                batch_results = []
            
            # Log progress
            attrs = get_queue_attributes(QUEUE_URL)
            remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
            logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, ~{remaining} remaining")
        
        # Final batch
        if batch_results:
            updated = await repo.batch_update_mews_enrichment(batch_results)
            logger.info(f"Final batch update: {updated} hotels")
        
        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
        
    finally:
        # Cleanup clients
        for client in clients:
            await client.close()
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Mews enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
