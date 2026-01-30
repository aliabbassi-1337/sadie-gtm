"""Mews enrichment worker - Polls SQS and enriches hotels using Playwright.

Uses Mews's hidden API (configurations/get) to extract full hotel data:
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
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from loguru import logger
from playwright.async_api import async_playwright, Page, BrowserContext

from db.client import init_db, close_db
from services.enrichment import repo
from infra.sqs import receive_messages, delete_message, get_queue_attributes

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


def parse_mews_response(data: dict) -> dict:
    """Parse Mews API response into hotel data."""
    result = {}
    
    # Get enterprise (property) data - keys can be camelCase or PascalCase
    enterprises = data.get("enterprises") or data.get("Enterprises", [])
    if enterprises:
        enterprise = enterprises[0]
        
        # Name - can be dict with language codes
        name = enterprise.get("name") or enterprise.get("Name")
        if isinstance(name, dict):
            # Prefer English, fall back to first available
            result["name"] = name.get("en-US") or name.get("en-GB") or next(iter(name.values()), None)
        elif isinstance(name, str):
            result["name"] = name
        
        # Address
        address = enterprise.get("address") or enterprise.get("Address", {})
        if address:
            line1 = address.get("line1") or address.get("Line1")
            line2 = address.get("line2") or address.get("Line2")
            result["address"] = line1
            if line2:
                result["address"] = f"{result['address']}, {line2}"
            result["city"] = address.get("city") or address.get("City")
            result["country"] = address.get("countryCode") or address.get("CountryCode")
            result["lat"] = address.get("latitude") or address.get("Latitude")
            result["lon"] = address.get("longitude") or address.get("Longitude")
        
        # Contact info
        result["email"] = enterprise.get("email") or enterprise.get("Email")
        result["phone"] = enterprise.get("telephone") or enterprise.get("Telephone")
    
    # Get chain name as fallback
    chains = data.get("chains") or data.get("Chains", [])
    if chains and not result.get("name"):
        result["name"] = chains[0].get("name") or chains[0].get("Name")
    
    return result


async def process_hotel(context: BrowserContext, hotel_id: int, booking_url: str) -> EnrichmentResult:
    """Process a single hotel by loading page and intercepting API call."""
    page = await context.new_page()
    captured_data = {}
    response_tasks = []
    api_urls_seen = []
    
    try:
        def handle_response(response):
            """Sync handler that creates async task for JSON parsing."""
            url = response.url
            # Log API calls we're interested in
            if "mews.com" in url and ("api" in url.lower() or "bookingEngine" in url):
                api_urls_seen.append(url[:100])
            
            # Match the configurations/get API endpoint
            if "configurations/get" in url:
                async def fetch_json():
                    try:
                        data = await response.json()
                        captured_data["config"] = data
                    except Exception as e:
                        captured_data["error"] = str(e)
                task = asyncio.create_task(fetch_json())
                response_tasks.append(task)
        
        page.on("response", handle_response)
        
        # Load page - use commit to get callback early, then wait for API calls
        await page.goto(booking_url, timeout=45000, wait_until="commit")
        
        # Wait for configurations/get API call (can take up to 15-20s on slow connections)
        for _ in range(40):  # 20 second max wait
            if response_tasks:
                await asyncio.gather(*response_tasks, return_exceptions=True)
            if "config" in captured_data:
                break
            await asyncio.sleep(0.5)
        
        if "config" not in captured_data:
            # Include debug info about what APIs we saw
            if api_urls_seen:
                debug_info = f"no_config (apis: {api_urls_seen[0][:60]})"
            else:
                debug_info = "no_api_calls_seen"
            if captured_data.get("error"):
                debug_info = f"json_error: {captured_data['error'][:50]}"
            return EnrichmentResult(hotel_id=hotel_id, success=False, error=debug_info)
        
        # Parse the API response
        parsed = parse_mews_response(captured_data["config"])
        
        if not parsed.get("name"):
            return EnrichmentResult(hotel_id=hotel_id, success=False, error="no_name_in_response")
        
        return EnrichmentResult(
            hotel_id=hotel_id,
            success=True,
            name=parsed.get("name"),
            address=parsed.get("address"),
            city=parsed.get("city"),
            country=parsed.get("country"),
            email=parsed.get("email"),
            phone=parsed.get("phone"),
            lat=parsed.get("lat"),
            lon=parsed.get("lon"),
        )
        
    except Exception as e:
        return EnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])
    finally:
        await page.close()


async def run_consumer(concurrency: int = 5):
    """Run the SQS consumer with Playwright."""
    if not QUEUE_URL:
        logger.error("SQS_MEWS_ENRICHMENT_QUEUE_URL not set in .env")
        return
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    await init_db()
    
    try:
        logger.info(f"Starting Mews enrichment consumer (concurrency={concurrency})")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create browser contexts pool
            contexts = []
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                )
                contexts.append(ctx)
            
            logger.info(f"Created {concurrency} browser contexts")
            
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
                
                # Process messages - one per context
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
                    tasks.append(process_hotel(contexts[i], hotel_id, booking_url))
                
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
            
            # Cleanup
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Mews enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
