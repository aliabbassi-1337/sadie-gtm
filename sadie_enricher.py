#!/usr/bin/env python3
"""
Sadie Enricher - Find Missing Hotel Websites via Serper.dev
============================================================
Uses Serper.dev Google Search API to find websites for hotels.
Fast, reliable, no CAPTCHAs!

Usage:
    export SERPER_KEY=your_api_key
    python3 sadie_enricher.py --input hotels.csv --output enriched_hotels.csv
    python3 sadie_enricher.py --input hotels.csv --location "Ocean City MD"
"""

import csv
import os
import sys
import argparse
import time
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_CONCURRENCY = 10  # Serper handles high concurrency well
SERPER_API_URL = "https://google.serper.dev/search"

# Domains to skip (not real hotel websites)
SKIP_DOMAINS = [
    # OTAs
    "booking.com", "expedia.com", "hotels.com", "tripadvisor.com",
    "kayak.com", "trivago.com", "priceline.com", "agoda.com",
    "orbitz.com", "travelocity.com", "hotwire.com", "cheaptickets.com",
    "trip.com", "makemytrip.com", "goibibo.com", "hostelworld.com",
    # Social / Info
    "google.com", "yelp.com", "facebook.com", "instagram.com",
    "twitter.com", "linkedin.com", "youtube.com", "tiktok.com",
    "wikipedia.org", "wikitravel.org", "waze.com",
    # Vacation rentals
    "airbnb.com", "vrbo.com", "oyorooms.com", "redawning.com", "evolve.com",
    # Big chains (we filter these out anyway)
    "marriott.com", "hilton.com", "ihg.com", "hyatt.com", "wyndham.com",
    "bestwestern.com", "choicehotels.com", "radissonhotels.com",
    # Location-specific junk
    "gatlinburghomesandproperties.com",
    # Maps/directions
    "mapquest.com", "maps.apple.com",
    # Other junk
    "nextdoor.com", "yellowpages.com", "manta.com", "bbb.org",
]

# Stats
_stats = {"processed": 0, "found": 0}

def log(msg: str):
    """Simple logging with timestamp."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def is_valid_hotel_domain(url: str) -> bool:
    """Check if URL looks like a real hotel website (not an OTA)."""
    if not url:
        return False
    try:
        # Skip file downloads
        lower_url = url.lower()
        bad_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.jpg', '.png', '.gif']
        if any(lower_url.endswith(ext) for ext in bad_extensions):
            return False
        
        # Skip government and educational sites
        if '.gov' in lower_url or '.edu' in lower_url or '.mil' in lower_url:
            return False
        
        # Skip state parks and similar
        if 'dnr.' in lower_url or 'parks.' in lower_url or 'recreation.' in lower_url:
            return False
        
        domain = extract_domain(url)
        return not any(skip in domain for skip in SKIP_DOMAINS)
    except Exception:
        return False


def search_serper(hotel_name: str, location: str, api_key: str, debug: bool = False) -> str:
    """
    Search Google via Serper.dev for a hotel's official website.
    Returns the website URL or empty string if not found.
    """
    query = f'{hotel_name}'
    if location:
        query += f" {location}"
    query += " hotel official website"
    
    if debug:
        log(f"    [SEARCH] Query: {query[:60]}...")
    
    try:
        response = requests.post(
            SERPER_API_URL,
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json"
            },
            json={"q": query, "num": 10},
            timeout=15
        )
        
        if response.status_code != 200:
            if debug:
                log(f"    [SEARCH] API error: {response.status_code} - {response.text[:100]}")
            return ""
        
        data = response.json()
        
        # Check organic results
        organic = data.get("organic", [])
        if debug:
            log(f"    [SEARCH] Got {len(organic)} organic results")
        
        for i, result in enumerate(organic[:10]):
            link = result.get("link", "")
            title = result.get("title", "")[:40]
            
            if debug:
                log(f"    [RESULT {i}] '{title}' -> {link[:50]}...")
            
            if is_valid_hotel_domain(link):
                if debug:
                    log(f"    [RESULT {i}] ✓ VALID: {extract_domain(link)}")
                return link
            elif debug:
                log(f"    [RESULT {i}] ✗ Blocked: {extract_domain(link)}")
        
        if debug:
            log(f"    [SEARCH] No valid website found")
        return ""
        
    except requests.Timeout:
        if debug:
            log(f"    [SEARCH] Timeout")
        return ""
    except Exception as e:
        if debug:
            log(f"    [SEARCH] Error: {e}")
        return ""


def process_hotel(hotel: dict, location: str, api_key: str, debug: bool) -> tuple:
    """Process a single hotel, return (hotel_name, website_found)."""
    hotel_name = hotel.get("hotel", "")
    if not hotel_name:
        return (None, None)
    
    website = search_serper(hotel_name, location, api_key, debug)
    
    if website:
        log(f"  ✓ {hotel_name[:35]} -> {extract_domain(website)}")
    else:
        log(f"  ✗ {hotel_name[:35]}")
    
    return (hotel_name, website)


def enrich_hotels(
    input_csv: str,
    output_csv: str,
    location: str = "",
    concurrency: int = DEFAULT_CONCURRENCY,
    api_key: str = "",
    debug: bool = False,
):
    """Main enrichment with concurrent API calls."""
    
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
    
    log(f"  - Using {concurrency} concurrent workers")
    log("")
    
    global _stats
    _stats = {"processed": 0, "found": 0}
    
    start_time = time.time()
    results = {}
    
    # Process with thread pool
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(process_hotel, hotel, location, api_key, debug): hotel
            for hotel in missing_website
        }
        
        for future in as_completed(futures):
            hotel_name, website = future.result()
            _stats["processed"] += 1
            
            if hotel_name and website:
                results[hotel_name] = website
                _stats["found"] += 1
    
    elapsed = time.time() - start_time
    
    # Update hotels with results
    for hotel in missing_website:
        hotel_name = hotel.get("hotel", "")
        if hotel_name in results:
            hotel["website"] = results[hotel_name]
    
    # Write enriched output
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        # Filter out any None keys from dicts
        clean_hotels = []
        for h in hotels:
            clean_hotels.append({k: v for k, v in h.items() if k is not None})
        writer.writerows(clean_hotels)
    
    # Summary
    log("")
    log("=" * 60)
    log("ENRICHMENT COMPLETE!")
    log("=" * 60)
    log(f"Hotels processed:  {_stats['processed']}")
    log(f"Websites found:    {_stats['found']}")
    log(f"Hit rate:          {_stats['found']/max(_stats['processed'],1)*100:.1f}%")
    log(f"Time elapsed:      {elapsed:.1f} seconds")
    log(f"Speed:             {_stats['processed']/max(elapsed,1):.1f} hotels/sec")
    log(f"Output:            {output_csv}")
    log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Enrich hotel data with missing websites via Serper.dev")
    parser.add_argument("--input", "-i", required=True, help="Input CSV file with hotels")
    parser.add_argument("--output", "-o", help="Output CSV file (default: input file with _enriched suffix)")
    parser.add_argument("--location", "-l", default="", help="Location hint for search (e.g., 'Ocean City MD')")
    parser.add_argument("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY, 
                        help=f"Number of concurrent API calls (default: {DEFAULT_CONCURRENCY})")
    parser.add_argument("--debug", action="store_true", help="Show detailed search results")
    
    args = parser.parse_args()
    
    # Get API key from environment
    api_key = os.environ.get("SERPER_KEY", "")
    if not api_key:
        print("Error: SERPER_KEY environment variable not set")
        print("Get your API key from https://serper.dev and run:")
        print("  export SERPER_KEY=your_api_key")
        sys.exit(1)
    
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)
    
    output = args.output
    if not output:
        base = os.path.splitext(args.input)[0]
        output = f"{base}_enriched.csv"
    
    log("Sadie Enricher - Serper.dev Website Finder")
    log(f"Input:       {args.input}")
    log(f"Output:      {output}")
    log(f"Location:    {args.location or '(none)'}")
    log(f"Concurrency: {args.concurrency}")
    log("")
    
    enrich_hotels(args.input, output, args.location, args.concurrency, api_key, args.debug)


if __name__ == "__main__":
    main()
