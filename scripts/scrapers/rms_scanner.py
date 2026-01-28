#!/usr/bin/env python3
"""
RMS Booking Engine Slug Scanner

Scans RMS booking engine URLs to discover valid hotel slugs.
Uses Playwright for the new engine (ibe*.rmscloud.com) since it's a JavaScript SPA.

This script is for DISCOVERING new RMS slugs, not for enriching existing hotels.
For enriching existing hotels, use workflows/enrich_rms_consumer.py.

Usage:
    # Scan for new slugs (ibe* engine)
    python scripts/scrapers/rms_scanner.py --start 0 --end 1000 --output rms_found.json
    
    # For distributed scanning across servers:
    python scripts/scrapers/rms_scanner.py --start 0 --end 5000 --output rms_1.json
    python scripts/scrapers/rms_scanner.py --start 5000 --end 10000 --output rms_2.json
    
    # Output URLs for ingestion
    python scripts/scrapers/rms_scanner.py --start 0 --end 10000 --output-urls rms_urls.txt
"""

import argparse
import asyncio
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

# RMS subdomain variations for new engine
NEW_ENGINE_SUBDOMAINS = [
    "ibe12",
    "ibe",
]

# Timeouts
PAGE_TIMEOUT = 15000  # 15 seconds
MAX_CONSECUTIVE_FAILURES = 20


@dataclass
class RMSHotel:
    """Extracted hotel data from RMS page."""
    slug: str
    url: str
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)
    
    def is_valid(self):
        """Check if we got meaningful data."""
        return bool(self.name and self.name.lower() not in ['online bookings', 'search', 'error', 'loading'])


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


async def extract_new_engine_data(page: Page, url: str, slug: str) -> Optional[RMSHotel]:
    """Extract data from new RMS booking engine page (ibe*) using Playwright."""
    hotel = RMSHotel(slug=slug, url=url)
    
    try:
        # Wait for the page to load content
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="networkidle")
        
        # Wait a bit more for React to render
        await asyncio.sleep(2)
        
        # Check if error page
        content = await page.content()
        if "Error" in content and "application issues" in content:
            return None
        if "Page Not Found" in content or "404" in content:
            return None
        
        # Get page title
        title = await page.title()
        if title and title.lower() not in ['online bookings', 'search', '', 'error']:
            hotel.name = title.strip()
        
        # Try to find property name in the page
        # Look for common patterns in RMS new engine
        try:
            # Try header/logo area
            name_selectors = [
                'h1',
                '.property-name',
                '.hotel-name', 
                '[data-testid="property-name"]',
                '.header-title',
            ]
            for selector in name_selectors:
                el = await page.query_selector(selector)
                if el:
                    text = await el.inner_text()
                    if text and len(text) > 2 and len(text) < 100:
                        hotel.name = text.strip()
                        break
        except Exception:
            pass
        
        # Try to find contact info
        try:
            # Look for phone patterns
            phone_match = re.search(r'[\+\d][\d\s\-\(\)]{7,15}', content)
            if phone_match:
                phone = phone_match.group(0).strip()
                # Validate it looks like a phone
                if len(re.sub(r'\D', '', phone)) >= 7:
                    hotel.phone = phone
        except Exception:
            pass
        
        # Try to find email
        try:
            # Cloudflare protected
            cf_match = re.search(r'data-cfemail="([a-f0-9]+)"', content)
            if cf_match:
                hotel.email = decode_cloudflare_email(cf_match.group(1))
            else:
                # Direct email
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', content)
                if email_match:
                    hotel.email = email_match.group(0)
        except Exception:
            pass
        
        # Try to find address
        try:
            # Look for address patterns
            address_selectors = [
                '.address',
                '.property-address',
                '[data-testid="address"]',
            ]
            for selector in address_selectors:
                el = await page.query_selector(selector)
                if el:
                    text = await el.inner_text()
                    if text and len(text) > 5:
                        hotel.address = text.strip()
                        break
        except Exception:
            pass
        
        return hotel if hotel.is_valid() else None
        
    except PlaywrightTimeout:
        logger.debug(f"Timeout for {url}")
        return None
    except Exception as e:
        logger.debug(f"Error extracting from {url}: {e}")
        return None


async def scan_new_engine(
    start_id: int,
    end_id: int,
    concurrency: int = 6,
    output_file: str = "rms_found.json",
    output_urls: Optional[str] = None,
):
    """Scan new booking engine URLs with Playwright."""
    found_hotels = []
    consecutive_failures = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # Create multiple browser contexts for concurrency
        contexts = []
        pages = []
        for _ in range(concurrency):
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = await ctx.new_page()
            contexts.append(ctx)
            pages.append(page)
        
        async def check_id(page_idx: int, id_num: int):
            nonlocal consecutive_failures
            page = pages[page_idx % len(pages)]
            
            # Format ID - try different formats
            formats = [str(id_num), f"{id_num:04d}", f"{id_num:05d}"]
            
            for fmt in formats:
                for subdomain in NEW_ENGINE_SUBDOMAINS:
                    url = f"https://{subdomain}.rmscloud.com/{fmt}"
                    
                    hotel = await extract_new_engine_data(page, url, fmt)
                    
                    if hotel:
                        consecutive_failures = 0
                        logger.success(f"Found hotel: {hotel.name} at {url}")
                        return hotel
            
            consecutive_failures += 1
            return None
        
        # Process in batches
        batch_size = concurrency * 2
        for batch_start in range(start_id, end_id, batch_size):
            batch_end = min(batch_start + batch_size, end_id)
            
            logger.info(f"Scanning IDs {batch_start} - {batch_end}...")
            
            # Run batch with semaphore for concurrency control
            tasks = []
            for i, id_num in enumerate(range(batch_start, batch_end)):
                tasks.append(check_id(i, id_num))
            
            results = await asyncio.gather(*tasks)
            
            for hotel in results:
                if hotel:
                    found_hotels.append(hotel)
            
            # Progress update
            logger.info(f"Progress: {batch_end}/{end_id} ({len(found_hotels)} hotels found)")
            
            # Save intermediate results
            if len(found_hotels) > 0 and batch_end % 100 == 0:
                save_results(found_hotels, output_file, output_urls, start_id, batch_end)
            
            # Check for too many consecutive failures (sparse region)
            if consecutive_failures > MAX_CONSECUTIVE_FAILURES:
                logger.warning(f"Many consecutive failures at ID {batch_end}, region may be sparse")
                # Don't stop, just log and reset
                consecutive_failures = 0
            
            # Small delay between batches
            await asyncio.sleep(0.5)
        
        # Cleanup
        for ctx in contexts:
            await ctx.close()
        await browser.close()
    
    return found_hotels


def save_results(hotels: list, output_file: str, output_urls: Optional[str], start_id: int, end_id: int):
    """Save results to files."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump({
            "scan_time": datetime.now().isoformat(),
            "range": {"start": start_id, "end": end_id},
            "total_found": len(hotels),
            "hotels": [h.to_dict() for h in hotels]
        }, f, indent=2)
    
    if output_urls:
        urls_path = Path(output_urls)
        with open(urls_path, "w") as f:
            for hotel in hotels:
                url = hotel.url.replace("https://", "").replace("http://", "")
                f.write(f"{url}\n")


async def main():
    parser = argparse.ArgumentParser(description="Scan RMS booking engine for valid hotel slugs")
    parser.add_argument("--start", type=int, default=0, help="Start ID")
    parser.add_argument("--end", type=int, default=10000, help="End ID")
    parser.add_argument("--output", type=str, default="rms_found.json", help="Output file")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent browser pages")
    parser.add_argument("--output-urls", type=str, help="Also output URLs to text file")
    
    args = parser.parse_args()
    
    logger.info(f"Starting RMS scan: IDs {args.start} - {args.end}")
    logger.info(f"Concurrency: {args.concurrency}")
    
    hotels = await scan_new_engine(
        args.start, 
        args.end, 
        args.concurrency,
        args.output,
        args.output_urls,
    )
    
    # Final save
    save_results(hotels, args.output, args.output_urls, args.start, args.end)
    
    logger.success(f"Scan complete: {len(hotels)} hotels found")
    
    # Print summary
    print("\n" + "="*50)
    print("SCAN SUMMARY")
    print("="*50)
    print(f"Range scanned: {args.start} - {args.end}")
    print(f"Total hotels found: {len(hotels)}")
    
    if hotels:
        with_email = sum(1 for h in hotels if h.email)
        with_phone = sum(1 for h in hotels if h.phone)
        with_address = sum(1 for h in hotels if h.address)
        
        print(f"  - With email: {with_email}")
        print(f"  - With phone: {with_phone}")
        print(f"  - With address: {with_address}")
        
        print("\nSample hotels:")
        for hotel in hotels[:5]:
            print(f"  - {hotel.name} ({hotel.url})")
            if hotel.phone:
                print(f"    Phone: {hotel.phone}")
            if hotel.email:
                print(f"    Email: {hotel.email}")


if __name__ == "__main__":
    asyncio.run(main())
