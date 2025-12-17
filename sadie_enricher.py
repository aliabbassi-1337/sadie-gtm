#!/usr/bin/env python3
"""
Sadie Enricher - Find Missing Hotel Websites via DuckDuckGo Search
===================================================================
Uses Playwright to search DuckDuckGo for hotels missing websites.
DuckDuckGo is much more lenient with automated searches (no CAPTCHAs!).

Usage:
    python3 sadie_enricher.py --input hotels.csv --output enriched_hotels.csv
    python3 sadie_enricher.py --input hotels.csv --concurrency 5 --location "Ocean City MD"
"""

import csv
import os
import sys
import argparse
import asyncio
import random
import time
from urllib.parse import urlparse, quote_plus, unquote
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_CONCURRENCY = 5  # Number of parallel browser contexts (DuckDuckGo handles more)

# Delays between searches (DuckDuckGo is friendlier, can go faster)
MIN_DELAY = 0.8
MAX_DELAY = 1.5

# User agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

# Domains to skip (not real hotel websites)
SKIP_DOMAINS = [
    "booking.com", "expedia.com", "hotels.com", "tripadvisor.com",
    "kayak.com", "trivago.com", "priceline.com", "agoda.com",
    "google.com", "yelp.com", "facebook.com", "instagram.com",
    "twitter.com", "linkedin.com", "youtube.com", "tiktok.com",
    "wikipedia.org", "wikitravel.org", "airbnb.com", "vrbo.com",
    "marriott.com", "hilton.com", "ihg.com", "hyatt.com", "wyndham.com",
    "oyorooms.com", "redawning.com",
]

# Progress file to resume from
PROGRESS_FILE = "enricher_progress.txt"

# Thread-safe counter
_stats = {"processed": 0, "found": 0}
_stats_lock = asyncio.Lock()

def log(msg: str):
    """Simple logging with timestamp."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


def is_valid_hotel_domain(url: str) -> bool:
    """Check if URL looks like a real hotel website (not an OTA)."""
    if not url:
        return False
    try:
        domain = urlparse(url).netloc.lower()
        return not any(skip in domain for skip in SKIP_DOMAINS)
    except Exception:
        return False


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


@dataclass
class SearchResult:
    """Result of a DuckDuckGo search."""
    hotel_name: str
    website: str
    success: bool
    captcha: bool = False


async def search_duckduckgo(page, hotel_name: str, location: str = "") -> SearchResult:
    """
    Search DuckDuckGo HTML version for a hotel's official website.
    Uses the lite/HTML version which doesn't need JavaScript rendering.
    """
    # Build search query
    query = f'{hotel_name}'
    if location:
        query += f" {location}"
    query += " hotel official website"
    
    # Use DuckDuckGo HTML version - no JavaScript needed, much more reliable
    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    
    try:
        await page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(0.3)
        
        # HTML version uses simple class-based selectors
        results = await page.locator("a.result__a").all()
        
        for result in results[:10]:
            try:
                href = await result.get_attribute("href")
                if not href:
                    continue
                
                # DuckDuckGo HTML wraps URLs - extract actual URL
                # Format: //duckduckgo.com/l/?uddg=ENCODED_URL&...
                if "//duckduckgo.com/l/" in href and "uddg=" in href:
                    # Extract the uddg parameter
                    uddg_start = href.find("uddg=") + 5
                    uddg_end = href.find("&", uddg_start)
                    if uddg_end == -1:
                        uddg_end = len(href)
                    href = unquote(href[uddg_start:uddg_end])
                
                # Skip internal links
                if href.startswith("/") or "duckduckgo.com" in href:
                    continue
                
                # Check if it's a valid hotel domain
                if is_valid_hotel_domain(href):
                    return SearchResult(hotel_name, href, True)
                    
            except Exception:
                continue
        
        return SearchResult(hotel_name, "", False)
        
    except PWTimeoutError:
        return SearchResult(hotel_name, "", False)
    except Exception:
        return SearchResult(hotel_name, "", False)


async def worker(
    worker_id: int,
    browser,
    queue: asyncio.Queue,
    results: dict,
    location: str,
    progress_file: str,
    headed: bool,
):
    """Worker that processes hotels from the queue."""
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1280, "height": 800},
    )
    page = await context.new_page()
    
    searches_count = 0
    
    try:
        while True:
            try:
                # Get next hotel from queue (with timeout to allow clean shutdown)
                hotel = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if queue.empty():
                    break
                continue
            
            hotel_name = hotel.get("hotel", "")
            if not hotel_name:
                queue.task_done()
                continue
            
            # Search DuckDuckGo (no CAPTCHAs!)
            result = await search_duckduckgo(page, hotel_name, location)
            
            async with _stats_lock:
                _stats["processed"] += 1
            
            if result.success:
                hotel["website"] = result.website
                results[hotel_name] = result.website
                async with _stats_lock:
                    _stats["found"] += 1
                log(f"  [W{worker_id}] ✓ {hotel_name[:30]} -> {extract_domain(result.website)}")
            else:
                log(f"  [W{worker_id}] ✗ {hotel_name[:30]}")
            
            # Save progress
            with open(progress_file, "a") as f:
                f.write(f"{hotel_name}\n")
            
            # Random delay
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            await asyncio.sleep(delay)
            
            # Rotate user agent occasionally (DuckDuckGo is friendlier, less frequent needed)
            searches_count += 1
            if searches_count % 50 == 0:
                await context.close()
                context = await browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={"width": 1280, "height": 800},
                )
                page = await context.new_page()
            
            queue.task_done()
            
    finally:
        try:
            await context.close()
        except Exception:
            pass  # Context may already be closed


async def enrich_hotels(
    input_csv: str,
    output_csv: str,
    location: str = "",
    headed: bool = False,
    concurrency: int = DEFAULT_CONCURRENCY,
):
    """Main enrichment loop with concurrent workers."""
    
    # Load input CSV
    hotels = []
    with open(input_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        hotels = list(reader)
    
    if not hotels:
        log("No hotels found in input file")
        return
    
    # Find hotels missing websites
    missing_website = [h for h in hotels if not h.get("website", "").strip()]
    has_website = [h for h in hotels if h.get("website", "").strip()]
    
    log(f"Loaded {len(hotels)} hotels")
    log(f"  - {len(has_website)} already have websites")
    log(f"  - {len(missing_website)} need enrichment")
    
    if not missing_website:
        log("All hotels already have websites!")
        return
    
    # Load progress (already processed hotels)
    processed = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            processed = set(line.strip() for line in f)
        log(f"Resuming: {len(processed)} already processed")
    
    # Filter out already processed
    to_process = [h for h in missing_website if h.get("hotel", "") not in processed]
    log(f"  - {len(to_process)} remaining to process")
    log(f"  - Using {concurrency} concurrent workers")
    
    if not to_process:
        log("All hotels already processed!")
        _write_output(hotels, output_csv, fieldnames)
        return
    
    # Reset stats
    global _stats
    _stats = {"processed": 0, "found": 0}
    
    start_time = time.time()
    
    # Create queue and results dict
    queue = asyncio.Queue()
    results = {}
    
    # Add hotels to queue
    for hotel in to_process:
        await queue.put(hotel)
    
    # Start browser and workers
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headed)
        
        # Create worker tasks
        workers = [
            asyncio.create_task(
                worker(i + 1, browser, queue, results, location, PROGRESS_FILE, headed)
            )
            for i in range(min(concurrency, len(to_process)))
        ]
        
        # Wait for queue to be processed
        await queue.join()
        
        # Cancel workers
        for w in workers:
            w.cancel()
        
        await browser.close()
    
    elapsed = time.time() - start_time
    
    # Update hotels with results
    for hotel in missing_website:
        hotel_name = hotel.get("hotel", "")
        if hotel_name in results:
            hotel["website"] = results[hotel_name]
    
    # Write enriched output
    _write_output(hotels, output_csv, fieldnames)
    
    # Summary
    log("")
    log("=" * 60)
    log("ENRICHMENT COMPLETE!")
    log("=" * 60)
    log(f"Hotels processed:  {_stats['processed']}")
    log(f"Websites found:    {_stats['found']}")
    log(f"Hit rate:          {_stats['found']/max(_stats['processed'],1)*100:.1f}%")
    log(f"Time elapsed:      {elapsed/60:.1f} minutes")
    log(f"Speed:             {_stats['processed']/max(elapsed,1)*60:.1f} hotels/min")
    log(f"Output:            {output_csv}")
    log("=" * 60)
    
    # Clean up progress file
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


def _write_output(hotels: list, output_csv: str, fieldnames: list):
    """Write enriched hotels to CSV."""
    # Create output directory if needed
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hotels)


def main():
    parser = argparse.ArgumentParser(description="Enrich hotel data with missing websites via Google Search")
    parser.add_argument("--input", "-i", required=True, help="Input CSV file with hotels")
    parser.add_argument("--output", "-o", help="Output CSV file (default: input file with _enriched suffix)")
    parser.add_argument("--location", "-l", default="", help="Location hint for search (e.g., 'Ocean City MD')")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode (visible)")
    parser.add_argument("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY, 
                        help=f"Number of concurrent workers (default: {DEFAULT_CONCURRENCY})")
    parser.add_argument("--debug", action="store_true", help="Run browser in headed mode (visible) for debugging")
    
    args = parser.parse_args()
    
    # --debug implies headed mode
    if args.debug:
        args.headed = True
    
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)
    
    output = args.output
    if not output:
        base = os.path.splitext(args.input)[0]
        output = f"{base}_enriched.csv"
    
    log("Sadie Enricher - DuckDuckGo Website Finder")
    log(f"Input:       {args.input}")
    log(f"Output:      {output}")
    log(f"Location:    {args.location or '(none)'}")
    log(f"Concurrency: {args.concurrency}")
    log("")
    
    asyncio.run(enrich_hotels(args.input, output, args.location, args.headed, args.concurrency))


if __name__ == "__main__":
    main()
