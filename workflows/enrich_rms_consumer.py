#!/usr/bin/env python3
"""
RMS Enrichment Consumer

Consumes RMS hotels from SQS and enriches them by scraping the RMS booking pages.
Extracts: name, address, city, state, country, phone, email, website.

Usage:
    python workflows/enrich_rms_consumer.py --concurrency 6
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import re
import signal
from dataclasses import dataclass
from typing import Optional, Dict, Any

from loguru import logger
from playwright.async_api import async_playwright, Page, Browser

from db.client import init_db, close_db
from infra.sqs import receive_messages, delete_message, get_queue_url, get_queue_attributes

QUEUE_NAME = "sadie-gtm-rms-enrichment"
PAGE_TIMEOUT = 20000  # 20 seconds
VISIBILITY_TIMEOUT = 3600  # 1 hour

# Global shutdown flag
shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


@dataclass
class ExtractedRMSData:
    """Data extracted from RMS booking page."""
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    def has_data(self) -> bool:
        """Check if we extracted any useful data."""
        return bool(self.name or self.address or self.phone or self.email)


def decode_cloudflare_email(encoded: str) -> str:
    """Decode Cloudflare-protected email addresses."""
    try:
        r = int(encoded[:2], 16)
        return ''.join(
            chr(int(encoded[i:i+2], 16) ^ r)
            for i in range(2, len(encoded), 2)
        )
    except Exception:
        return ""


def normalize_country(country: str) -> str:
    """Normalize country names to ISO codes."""
    if not country:
        return ""
    
    country_map = {
        "united states": "USA",
        "united states of america": "USA",
        "us": "USA",
        "usa": "USA",
        "australia": "AU",
        "canada": "CA",
        "new zealand": "NZ",
        "united kingdom": "GB",
        "uk": "GB",
        "mexico": "MX",
    }
    
    normalized = country_map.get(country.lower().strip(), country.upper()[:2])
    return normalized


async def extract_rms_data(page: Page, url: str) -> Optional[ExtractedRMSData]:
    """Extract data from RMS booking page using Playwright."""
    data = ExtractedRMSData()
    
    try:
        # Navigate with domcontentloaded (faster than networkidle for SPAs)
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        
        # Wait for React to render
        await asyncio.sleep(3)
        
        # Get page content
        content = await page.content()
        body_text = await page.evaluate("document.body.innerText")
        
        # Check for error pages
        if "Error" in content[:500] and "application issues" in content:
            return None
        if "Page Not Found" in content or "404" in content[:1000]:
            return None
        
        # Extract property name from various sources
        # 1. Try specific selectors first
        name_selectors = [
            'h1', 
            '.property-name',
            '[data-testid="property-name"]',
            '.header-title',
        ]
        for selector in name_selectors:
            try:
                el = await page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and len(text) > 2 and len(text) < 100:
                        if text.lower() not in ['online bookings', 'search', 'book now', 'cart']:
                            data.name = text
                            break
            except Exception:
                pass
        
        # 2. Try page title if no name found
        if not data.name:
            title = await page.title()
            if title and title.lower() not in ['online bookings', 'search', '']:
                # Clean up title
                title = re.sub(r'\s*[-|]\s*RMS.*$', '', title, flags=re.IGNORECASE)
                if title and len(title) > 2:
                    data.name = title.strip()
        
        # Extract contact info from page content
        # Phone - look for patterns with digits
        phone_patterns = [
            r'(?:tel|phone|call)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
            r'(?<!\d)(\d{2,4}[\s\-]\d{3,4}[\s\-]\d{3,4})(?!\d)',
        ]
        for pattern in phone_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                # Validate phone (at least 7 digits)
                if len(re.sub(r'\D', '', phone)) >= 7:
                    data.phone = phone
                    break
        
        # Email - check for Cloudflare protection first
        cf_match = re.search(r'data-cfemail="([a-f0-9]+)"', content)
        if cf_match:
            data.email = decode_cloudflare_email(cf_match.group(1))
        else:
            # Direct email pattern
            email_match = re.search(r'[\w\.\-+]+@[\w\.-]+\.\w{2,}', body_text)
            if email_match:
                email = email_match.group(0)
                # Filter out common non-hotel emails
                if not any(x in email.lower() for x in ['rmscloud', 'example', 'test', 'noreply']):
                    data.email = email
        
        # Website - look for external links
        try:
            links = await page.query_selector_all('a[href^="http"]')
            for link in links[:10]:  # Check first 10 links
                href = await link.get_attribute('href')
                if href and 'rmscloud' not in href and 'google' not in href:
                    # Check if it looks like a hotel website
                    if any(x in href.lower() for x in ['.com', '.com.au', '.co.nz', '.co.uk', '.ca']):
                        data.website = href
                        break
        except Exception:
            pass
        
        # Try to extract address from page text
        # Look for common address patterns
        address_patterns = [
            r'(?:address|location)[:\s]*([^\n]{10,100})',
            r'(\d+\s+[A-Za-z]+\s+(?:St|Street|Rd|Road|Ave|Avenue|Blvd|Boulevard|Dr|Drive)[^\n]{0,50})',
        ]
        for pattern in address_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                addr = match.group(1).strip()
                if len(addr) > 10:
                    data.address = addr
                    break
        
        # Parse address for city/state/country
        if data.address:
            # Try to extract state from address (e.g., ", NSW " or ", CA ")
            state_match = re.search(r',\s*([A-Z]{2,3})\s*(?:\d|$)', data.address)
            if state_match:
                data.state = state_match.group(1)
            
            # Try to extract country
            country_match = re.search(r'(?:Australia|USA|Canada|New Zealand|UK)', data.address, re.IGNORECASE)
            if country_match:
                data.country = normalize_country(country_match.group(0))
        
        return data if data.has_data() else None
        
    except Exception as e:
        logger.debug(f"Error extracting from {url}: {e}")
        return None


async def update_hotel(pool, hotel_id: int, data: ExtractedRMSData, booking_url: str):
    """Update hotel with extracted data."""
    updates = []
    values = []
    param_idx = 1
    
    if data.name:
        updates.append(f"name = ${param_idx}")
        values.append(data.name)
        param_idx += 1
    
    if data.address:
        updates.append(f"address = ${param_idx}")
        values.append(data.address)
        param_idx += 1
    
    if data.city:
        updates.append(f"city = ${param_idx}")
        values.append(data.city)
        param_idx += 1
    
    if data.state:
        updates.append(f"state = ${param_idx}")
        values.append(data.state)
        param_idx += 1
    
    if data.country:
        updates.append(f"country = ${param_idx}")
        values.append(data.country)
        param_idx += 1
    
    if data.phone:
        updates.append(f"phone = ${param_idx}")
        values.append(data.phone)
        param_idx += 1
    
    if data.email:
        updates.append(f"email = ${param_idx}")
        values.append(data.email)
        param_idx += 1
    
    if data.website:
        updates.append(f"website = ${param_idx}")
        values.append(data.website)
        param_idx += 1
    
    if not updates:
        return
    
    # Update hotels table
    values.append(hotel_id)
    query = f"""
        UPDATE sadie_gtm.hotels 
        SET {', '.join(updates)}, updated_at = NOW()
        WHERE id = ${param_idx}
    """
    
    async with pool.acquire() as conn:
        await conn.execute(query, *values)
        
        # Update enrichment status
        await conn.execute("""
            UPDATE sadie_gtm.hotel_booking_engines
            SET enrichment_status = 'enriched',
                last_enrichment_attempt = NOW()
            WHERE booking_url = $1
        """, booking_url)


async def mark_enrichment_failed(pool, booking_url: str, status: str = "no_data"):
    """Mark hotel enrichment as failed."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE sadie_gtm.hotel_booking_engines
            SET enrichment_status = $1,
                last_enrichment_attempt = NOW()
            WHERE booking_url = $2
        """, status, booking_url)


async def process_message(
    pool,
    browser: Browser,
    message: Dict[str, Any],
    queue_url: str,
) -> tuple[int, int, int]:
    """Process a single SQS message.
    
    Returns: (processed, enriched, failed)
    """
    receipt_handle = message["receipt_handle"]
    hotels = message["body"].get("hotels", [])
    
    if not hotels:
        delete_message(queue_url, receipt_handle)
        return (0, 0, 0)
    
    processed = 0
    enriched = 0
    failed = 0
    
    # Create a new page for this batch
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    )
    page = await context.new_page()
    
    try:
        for hotel in hotels:
            hotel_id = hotel["hotel_id"]
            booking_url = hotel["booking_url"]
            
            # Ensure URL has protocol
            if not booking_url.startswith("http"):
                booking_url = f"https://{booking_url}"
            
            try:
                data = await extract_rms_data(page, booking_url)
                
                if data and data.has_data():
                    await update_hotel(pool, hotel_id, data, hotel["booking_url"])
                    enriched += 1
                    logger.info(f"Enriched hotel {hotel_id}: {data.name}")
                else:
                    await mark_enrichment_failed(pool, hotel["booking_url"], "no_data")
                    failed += 1
                    logger.debug(f"No data for hotel {hotel_id}")
                
            except Exception as e:
                logger.error(f"Error processing hotel {hotel_id}: {e}")
                await mark_enrichment_failed(pool, hotel["booking_url"], "error")
                failed += 1
            
            processed += 1
        
        # Message processed successfully
        delete_message(queue_url, receipt_handle)
        
    finally:
        await context.close()
    
    return (processed, enriched, failed)


async def run_consumer(concurrency: int):
    """Run the consumer loop."""
    global shutdown_requested
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    pool = await init_db()
    queue_url = get_queue_url(QUEUE_NAME)
    
    total_processed = 0
    total_enriched = 0
    total_failed = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        logger.info(f"Starting RMS enrichment consumer (concurrency={concurrency})")
        logger.info(f"Queue: {QUEUE_NAME}")
        
        while not shutdown_requested:
            try:
                # Check queue stats
                attrs = get_queue_attributes(queue_url)
                pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                
                if pending == 0 and in_flight == 0:
                    logger.info("Queue empty, waiting...")
                    await asyncio.sleep(30)
                    continue
                
                # Receive messages
                messages = receive_messages(
                    queue_url,
                    max_messages=min(concurrency, 10),
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    wait_time=20,
                )
                
                if not messages:
                    continue
                
                logger.info(f"Processing {len(messages)} messages ({pending} pending, {in_flight} in flight)")
                
                # Process messages concurrently
                tasks = [
                    process_message(pool, browser, msg, queue_url)
                    for msg in messages
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Aggregate results
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Message processing error: {result}")
                    else:
                        p, e, f = result
                        total_processed += p
                        total_enriched += e
                        total_failed += f
                
                logger.info(
                    f"Progress: {total_processed} processed, "
                    f"{total_enriched} enriched, {total_failed} failed"
                )
                
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)
        
        await browser.close()
    
    await close_db()
    
    logger.success(
        f"Consumer stopped. Total: {total_processed} processed, "
        f"{total_enriched} enriched, {total_failed} failed"
    )


def main():
    parser = argparse.ArgumentParser(description="RMS Enrichment Consumer")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent messages to process")
    args = parser.parse_args()
    
    asyncio.run(run_consumer(args.concurrency))


if __name__ == "__main__":
    main()
