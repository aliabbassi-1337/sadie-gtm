#!/usr/bin/env python3
"""
RMS Booking Engine Ingestor

Scans RMS booking engine IDs to discover valid hotels and saves them directly to the database.
Uses Playwright since the RMS new engine (ibe*.rmscloud.com) is a JavaScript SPA.

Usage:
    # Scan range and ingest
    python workflows/ingest_rms.py --start 0 --end 10000
    
    # Distributed scanning (run on different servers)
    python workflows/ingest_rms.py --start 0 --end 5000
    python workflows/ingest_rms.py --start 5000 --end 10000
    
    # Dry run (don't save to DB)
    python workflows/ingest_rms.py --start 0 --end 100 --dry-run
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import asyncio
import re
import signal
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List

from loguru import logger
from playwright.async_api import async_playwright, Page, Browser

from db.client import init_db, close_db

# RMS subdomain variations for new engine
RMS_SUBDOMAINS = ["ibe12", "ibe"]

# Configuration
PAGE_TIMEOUT = 20000  # 20 seconds
MAX_CONSECUTIVE_FAILURES = 30
BATCH_SAVE_SIZE = 50

# Booking engine ID for RMS Cloud
RMS_BOOKING_ENGINE_ID = 4  # Verify this matches your DB

# Global shutdown flag
shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


@dataclass
class RMSHotel:
    """Extracted hotel data from RMS page."""
    slug: str
    booking_url: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    def has_data(self) -> bool:
        """Check if we extracted useful data."""
        return bool(self.name and self.name.lower() not in ['online bookings', 'search', 'error', 'loading', ''])


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
    
    return country_map.get(country.lower().strip(), country.upper()[:2])


async def extract_rms_data(page: Page, url: str, slug: str) -> Optional[RMSHotel]:
    """Extract data from RMS booking page using Playwright."""
    hotel = RMSHotel(slug=slug, booking_url=url)
    
    try:
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        await asyncio.sleep(3)  # Wait for React to render
        
        content = await page.content()
        body_text = await page.evaluate("document.body.innerText")
        
        # Check for error pages
        if "Error" in content[:500] and "application issues" in content:
            return None
        if "Page Not Found" in content or "404" in content[:1000]:
            return None
        if not body_text or len(body_text) < 100:
            return None
        
        # Extract property name
        name_selectors = ['h1', '.property-name', '[data-testid="property-name"]', '.header-title']
        for selector in name_selectors:
            try:
                el = await page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and 2 < len(text) < 100 and text.lower() not in ['online bookings', 'search', 'book now', 'cart']:
                        hotel.name = text
                        break
            except Exception:
                pass
        
        # Fallback to page title
        if not hotel.name:
            title = await page.title()
            if title and title.lower() not in ['online bookings', 'search', '']:
                title = re.sub(r'\s*[-|]\s*RMS.*$', '', title, flags=re.IGNORECASE)
                if title and len(title) > 2:
                    hotel.name = title.strip()
        
        # Extract phone
        phone_patterns = [
            r'(?:tel|phone|call)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
            r'(?<!\d)(\d{2,4}[\s\-]\d{3,4}[\s\-]\d{3,4})(?!\d)',
        ]
        for pattern in phone_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                if len(re.sub(r'\D', '', phone)) >= 7:
                    hotel.phone = phone
                    break
        
        # Extract email
        cf_match = re.search(r'data-cfemail="([a-f0-9]+)"', content)
        if cf_match:
            hotel.email = decode_cloudflare_email(cf_match.group(1))
        else:
            email_match = re.search(r'[\w\.\-+]+@[\w\.-]+\.\w{2,}', body_text)
            if email_match:
                email = email_match.group(0)
                if not any(x in email.lower() for x in ['rmscloud', 'example', 'test', 'noreply']):
                    hotel.email = email
        
        # Extract website
        try:
            links = await page.query_selector_all('a[href^="http"]')
            for link in links[:10]:
                href = await link.get_attribute('href')
                if href and 'rmscloud' not in href and 'google' not in href:
                    if any(x in href.lower() for x in ['.com', '.com.au', '.co.nz', '.co.uk', '.ca']):
                        hotel.website = href
                        break
        except Exception:
            pass
        
        # Extract address
        address_patterns = [
            r'(?:address|location)[:\s]*([^\n]{10,100})',
            r'(\d+\s+[A-Za-z]+\s+(?:St|Street|Rd|Road|Ave|Avenue|Blvd|Boulevard|Dr|Drive)[^\n]{0,50})',
        ]
        for pattern in address_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                addr = match.group(1).strip()
                if len(addr) > 10:
                    hotel.address = addr
                    break
        
        # Parse state/country from address
        if hotel.address:
            state_match = re.search(r',\s*([A-Z]{2,3})\s*(?:\d|$)', hotel.address)
            if state_match:
                hotel.state = state_match.group(1)
            
            country_match = re.search(r'(?:Australia|USA|Canada|New Zealand|UK)', hotel.address, re.IGNORECASE)
            if country_match:
                hotel.country = normalize_country(country_match.group(0))
        
        return hotel if hotel.has_data() else None
        
    except Exception as e:
        logger.debug(f"Error extracting {url}: {e}")
        return None


async def save_hotels_batch(pool, hotels: List[RMSHotel], booking_engine_id: int):
    """Save a batch of hotels to the database."""
    if not hotels:
        return 0
    
    saved = 0
    async with pool.acquire() as conn:
        for hotel in hotels:
            try:
                # Insert hotel
                hotel_id = await conn.fetchval("""
                    INSERT INTO sadie_gtm.hotels (
                        name, address, city, state, country, phone, email, website,
                        source, status, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
                    ON CONFLICT (name, COALESCE(city, ''), COALESCE(state, '')) 
                    DO UPDATE SET
                        address = COALESCE(EXCLUDED.address, sadie_gtm.hotels.address),
                        phone = COALESCE(EXCLUDED.phone, sadie_gtm.hotels.phone),
                        email = COALESCE(EXCLUDED.email, sadie_gtm.hotels.email),
                        website = COALESCE(EXCLUDED.website, sadie_gtm.hotels.website),
                        updated_at = NOW()
                    RETURNING id
                """, hotel.name, hotel.address, hotel.city, hotel.state, 
                    hotel.country, hotel.phone, hotel.email, hotel.website,
                    'rms_scan', 1)
                
                if hotel_id:
                    # Insert booking engine relation
                    await conn.execute("""
                        INSERT INTO sadie_gtm.hotel_booking_engines (
                            hotel_id, booking_engine_id, booking_url, enrichment_status,
                            last_enrichment_attempt, created_at
                        ) VALUES ($1, $2, $3, 'enriched', NOW(), NOW())
                        ON CONFLICT (hotel_id, booking_engine_id) 
                        DO UPDATE SET
                            booking_url = EXCLUDED.booking_url,
                            enrichment_status = 'enriched',
                            last_enrichment_attempt = NOW()
                    """, hotel_id, booking_engine_id, hotel.booking_url)
                    
                    saved += 1
                    
            except Exception as e:
                logger.error(f"Error saving hotel {hotel.name}: {e}")
    
    return saved


async def scan_and_ingest(
    pool,
    start_id: int,
    end_id: int,
    concurrency: int,
    booking_engine_id: int,
    dry_run: bool = False,
):
    """Scan RMS IDs and ingest valid hotels to database."""
    global shutdown_requested
    
    found_hotels: List[RMSHotel] = []
    total_saved = 0
    consecutive_failures = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # Create browser contexts for concurrency
        contexts = []
        pages = []
        for _ in range(concurrency):
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await ctx.new_page()
            contexts.append(ctx)
            pages.append(page)
        
        semaphore = asyncio.Semaphore(concurrency)
        
        async def check_id(id_num: int, page_idx: int) -> Optional[RMSHotel]:
            nonlocal consecutive_failures
            
            async with semaphore:
                page = pages[page_idx % len(pages)]
                
                # Try different slug formats
                formats = [str(id_num), f"{id_num:04d}", f"{id_num:05d}"]
                
                for fmt in formats:
                    for subdomain in RMS_SUBDOMAINS:
                        if shutdown_requested:
                            return None
                        
                        url = f"https://{subdomain}.rmscloud.com/{fmt}"
                        hotel = await extract_rms_data(page, url, fmt)
                        
                        if hotel:
                            consecutive_failures = 0
                            return hotel
                
                consecutive_failures += 1
                return None
        
        # Process in batches
        batch_size = concurrency * 2
        for batch_start in range(start_id, end_id, batch_size):
            if shutdown_requested:
                break
            
            batch_end = min(batch_start + batch_size, end_id)
            
            logger.info(f"Scanning IDs {batch_start} - {batch_end}...")
            
            # Run batch
            tasks = [
                check_id(id_num, i) 
                for i, id_num in enumerate(range(batch_start, batch_end))
            ]
            results = await asyncio.gather(*tasks)
            
            # Collect found hotels
            for hotel in results:
                if hotel:
                    found_hotels.append(hotel)
                    logger.success(f"Found: {hotel.name} ({hotel.booking_url})")
            
            # Save batch to DB
            if len(found_hotels) >= BATCH_SAVE_SIZE and not dry_run:
                saved = await save_hotels_batch(pool, found_hotels, booking_engine_id)
                total_saved += saved
                logger.info(f"Saved {saved} hotels to database (total: {total_saved})")
                found_hotels = []
            
            # Progress
            progress = (batch_end - start_id) / (end_id - start_id) * 100
            logger.info(f"Progress: {progress:.1f}% ({batch_end}/{end_id}), Found: {total_saved + len(found_hotels)}")
            
            # Check for sparse region
            if consecutive_failures > MAX_CONSECUTIVE_FAILURES:
                logger.warning(f"Many failures at {batch_end}, region may be sparse")
                consecutive_failures = 0
            
            await asyncio.sleep(0.5)
        
        # Save remaining hotels
        if found_hotels and not dry_run:
            saved = await save_hotels_batch(pool, found_hotels, booking_engine_id)
            total_saved += saved
        
        # Cleanup
        for ctx in contexts:
            await ctx.close()
        await browser.close()
    
    return total_saved, found_hotels if dry_run else []


async def get_booking_engine_id(pool) -> int:
    """Get RMS Cloud booking engine ID from database."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id FROM sadie_gtm.booking_engines WHERE name = 'RMS Cloud'
        """)
        if row:
            return row['id']
        raise ValueError("RMS Cloud booking engine not found in database")


async def main():
    parser = argparse.ArgumentParser(description="Ingest RMS hotels by scanning booking engine IDs")
    parser.add_argument("--start", type=int, default=0, help="Start ID")
    parser.add_argument("--end", type=int, default=10000, help="End ID")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent pages")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    logger.info(f"Starting RMS ingestion: IDs {args.start} - {args.end}")
    logger.info(f"Concurrency: {args.concurrency}, Dry run: {args.dry_run}")
    
    pool = await init_db()
    
    try:
        booking_engine_id = await get_booking_engine_id(pool)
        logger.info(f"RMS Cloud booking engine ID: {booking_engine_id}")
        
        total_saved, dry_run_hotels = await scan_and_ingest(
            pool,
            args.start,
            args.end,
            args.concurrency,
            booking_engine_id,
            args.dry_run,
        )
        
        print("\n" + "=" * 50)
        print("INGESTION SUMMARY")
        print("=" * 50)
        print(f"Range scanned: {args.start} - {args.end}")
        print(f"Hotels {'found' if args.dry_run else 'saved'}: {total_saved if not args.dry_run else len(dry_run_hotels)}")
        
        if args.dry_run and dry_run_hotels:
            print("\nSample hotels (dry run):")
            for hotel in dry_run_hotels[:10]:
                print(f"  - {hotel.name}")
                if hotel.phone:
                    print(f"    Phone: {hotel.phone}")
                if hotel.email:
                    print(f"    Email: {hotel.email}")
        
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
