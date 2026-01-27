"""Cloudbeds enrichment worker - Polls SQS and enriches hotels using Playwright.

Run on multiple EC2 instances for parallel processing.
Each instance can run multiple concurrent browser contexts.

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
import re
from typing import Dict, Any, List
from dataclasses import dataclass
from loguru import logger
from playwright.async_api import async_playwright, Page, Browser
from playwright_stealth import Stealth

from db.client import init_db, close_db
from services.enrichment import repo
from infra.sqs import receive_messages, delete_message, get_queue_attributes

QUEUE_URL = os.getenv("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", "")
VISIBILITY_TIMEOUT = 300  # 5 minutes per hotel

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
    name: str = None
    address: str = None
    city: str = None
    state: str = None
    country: str = None
    phone: str = None
    email: str = None
    error: str = None


async def extract_from_page(page: Page) -> Dict[str, Any]:
    """Extract name and address/contact from Cloudbeds page."""
    result = {}
    
    # Extract name and location from title
    try:
        title_data = await page.evaluate("""
            () => {
                const title = document.querySelector('title');
                if (!title) return null;
                
                const text = title.textContent.trim();
                const parts = text.split(/\\s*-\\s*/);
                
                if (parts.length >= 2) {
                    const name = parts[0].trim();
                    const locationPart = parts[1].trim();
                    const locParts = locationPart.split(',').map(p => p.trim());
                    
                    return {
                        name: name,
                        city: locParts[0] || null,
                        state: locParts.length === 3 ? locParts[1] : null,
                        country: locParts[locParts.length - 1] || null
                    };
                }
                
                return { name: parts[0].trim() };
            }
        """)
        if title_data:
            if title_data.get('name') and title_data['name'] not in ['Book Now', 'Reservation', 'Booking', 'Home']:
                result['name'] = title_data['name']
            if title_data.get('city'):
                result['city'] = title_data['city']
            if title_data.get('state'):
                result['state'] = title_data['state']
            if title_data.get('country'):
                country = title_data['country']
                if country in ['United States of America', 'United States', 'US', 'USA']:
                    result['country'] = 'USA'
                else:
                    result['country'] = country
    except Exception:
        pass
    
    # Extract from Cloudbeds widget (if present)
    try:
        widget_data = await page.evaluate("""
            () => {
                const container = document.querySelector('[data-testid="property-address-and-contact"]') 
                               || document.querySelector('.cb-address-and-contact');
                if (!container) return null;
                
                const lines = Array.from(container.querySelectorAll('p[data-be-text="true"]'))
                    .map(p => p.textContent?.trim() || '');
                
                const mailtoLink = container.querySelector('a[href^="mailto:"]');
                const email = mailtoLink ? mailtoLink.href.replace('mailto:', '').split('?')[0] : '';
                
                return { lines, email };
            }
        """)
        
        if widget_data and widget_data.get('lines') and len(widget_data['lines']) >= 3:
            lines = widget_data['lines']
            
            if len(lines) > 0:
                result['address'] = lines[0]
            if len(lines) > 1:
                result['city'] = lines[1]
            if len(lines) > 2:
                state_country = lines[2].strip()
                parts = state_country.rsplit(' ', 1)
                if len(parts) == 2:
                    result['state'] = parts[0].strip()
                    country = parts[1].strip().upper()
                    result['country'] = 'USA' if country in ['US', 'USA'] else country
                else:
                    result['state'] = state_country
            
            phone_pattern = re.compile(r'^[\d\-\(\)\s\+\.]{7,20}$')
            for line in lines[3:]:
                if phone_pattern.match(line) and 'phone' not in result:
                    result['phone'] = line
            
            if widget_data.get('email'):
                result['email'] = widget_data['email']
    except Exception:
        pass
    
    # Fallback: phone from tel: links
    if 'phone' not in result:
        try:
            phone = await page.evaluate("""
                () => {
                    const tel = document.querySelector('a[href^="tel:"]');
                    if (tel) return tel.href.replace('tel:', '').replace(/[^0-9+()-]/g, '');
                    return null;
                }
            """)
            if phone and len(phone) >= 10:
                result['phone'] = phone
        except Exception:
            pass
    
    # Fallback: email from mailto: links
    if 'email' not in result:
        try:
            email = await page.evaluate("""
                () => {
                    const mailto = document.querySelector('a[href^="mailto:"]');
                    if (mailto) return mailto.href.replace('mailto:', '').split('?')[0];
                    return null;
                }
            """)
            if email and '@' in email:
                result['email'] = email
        except Exception:
            pass
    
    return result


async def process_hotel(page: Page, hotel_id: int, booking_url: str) -> EnrichmentResult:
    """Process a single hotel."""
    try:
        await page.goto(booking_url, timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(4)  # Wait for React and address widget to load
        
        data = await extract_from_page(page)
        
        if not data:
            return EnrichmentResult(hotel_id=hotel_id, success=False, error="no_data")
        
        return EnrichmentResult(
            hotel_id=hotel_id,
            success=True,
            name=data.get('name'),
            address=data.get('address'),
            city=data.get('city'),
            state=data.get('state'),
            country=data.get('country'),
            phone=data.get('phone'),
            email=data.get('email'),
        )
    except Exception as e:
        return EnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])


async def run_consumer(concurrency: int = 5):
    """Run the SQS consumer with Playwright."""
    if not QUEUE_URL:
        logger.error("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL not set in .env")
        return
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    await init_db()
    
    try:
        logger.info(f"Starting Cloudbeds enrichment consumer (concurrency={concurrency})")
        
        async with Stealth().use_async(async_playwright()) as p:
            # playwright-stealth bypasses headless detection
            browser = await p.chromium.launch(headless=True)
            
            # Create browser contexts pool
            contexts = []
            pages = []
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                pages.append(page)
            
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
                
                # Process messages - one per page to avoid conflicts
                # Only process up to concurrency messages at a time
                valid_messages = []
                for msg in messages:
                    body = msg["body"]
                    hotel_id = body.get("hotel_id")
                    booking_url = body.get("booking_url")
                    
                    if not hotel_id or not booking_url:
                        delete_message(QUEUE_URL, msg["receipt_handle"])
                        continue
                    
                    valid_messages.append((msg, hotel_id, booking_url))
                
                # Process in batches of concurrency (one message per page)
                tasks = []
                message_map = {}  # hotel_id -> message
                
                for i, (msg, hotel_id, booking_url) in enumerate(valid_messages[:concurrency]):
                    message_map[hotel_id] = msg
                    tasks.append(process_hotel(pages[i], hotel_id, booking_url))
                
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
                    
                    if result.success and (result.city or result.name):
                        batch_results.append({
                            "hotel_id": result.hotel_id,
                            "name": result.name,
                            "address": result.address,
                            "city": result.city,
                            "state": result.state,
                            "country": result.country,
                            "phone": result.phone,
                            "email": result.email,
                        })
                        total_enriched += 1
                        
                        parts = []
                        if result.name:
                            parts.append(f"name={result.name[:20]}")
                        if result.city:
                            parts.append(f"city={result.city}")
                        logger.info(f"  Hotel {result.hotel_id}: {', '.join(parts)}")
                    elif result.error:
                        logger.warning(f"  Hotel {result.hotel_id}: {result.error}")
                    
                    # Delete from queue
                    delete_message(QUEUE_URL, msg["receipt_handle"])
                    total_processed += 1
                
                # Batch update every 50 results
                if len(batch_results) >= 50:
                    updated = await repo.batch_update_cloudbeds_enrichment(batch_results)
                    logger.info(f"Batch update: {updated} hotels")
                    batch_results = []
                
                # Log progress
                attrs = get_queue_attributes(QUEUE_URL)
                remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Progress: {total_processed} processed, {total_enriched} enriched, ~{remaining} remaining")
            
            # Final batch
            if batch_results:
                updated = await repo.batch_update_cloudbeds_enrichment(batch_results)
                logger.info(f"Final batch update: {updated} hotels")
            
            # Cleanup
            for page in pages:
                await page.close()
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        logger.info(f"Consumer stopped. Total: {total_processed} processed, {total_enriched} enriched")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Cloudbeds enrichment consumer")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    asyncio.run(run_consumer(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
