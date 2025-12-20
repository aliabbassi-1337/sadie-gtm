#!/usr/bin/env python3
"""
Sadie Scraper SerpAPI - Hotel Scraper using SerpAPI Google Maps
================================================================
Uses SerpAPI's Google Maps endpoint to find hotels.
Free tier: 100 searches/month per account

Usage:
    python3 sadie_scraper_serpapi.py --query "Sydney hotels" --api-key YOUR_KEY
    python3 sadie_scraper_serpapi.py --query "Bondi hotels" --api-key YOUR_KEY -o output.csv
"""

import csv
import os
import sys
import argparse
import time
import requests
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

SERPAPI_URL = "https://serpapi.com/search.json"

# Big chains to filter out
SKIP_CHAIN_NAMES = [
    "marriott", "hilton", "hyatt", "sheraton", "westin", "w hotel",
    "intercontinental", "holiday inn", "crowne plaza", "ihg",
    "best western", "choice hotels", "comfort inn", "quality inn",
    "radisson", "wyndham", "ramada", "days inn", "super 8", "motel 6",
    "la quinta", "travelodge", "ibis", "novotel", "mercure", "accor",
    "four seasons", "ritz-carlton", "st. regis", "fairmont",
]

# Stats
_stats = {"found": 0, "skipped_chains": 0, "api_calls": 0}
_out_of_credits = False


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


# ============================================================================
# SERPAPI GOOGLE MAPS API
# ============================================================================

def search_serpapi_maps(query: str, api_key: str) -> list:
    """
    Search Google Maps via SerpAPI.
    Returns list of place results.
    """
    global _out_of_credits
    
    if _out_of_credits:
        return []
    
    _stats["api_calls"] += 1
    
    try:
        response = requests.get(
            SERPAPI_URL,
            params={
                "engine": "google_maps",
                "q": query,
                "type": "search",
                "api_key": api_key,
            },
            timeout=30
        )
        
        if response.status_code == 401:
            log("Invalid API key or out of credits")
            _out_of_credits = True
            return []
        
        if response.status_code == 429:
            log("Rate limited - waiting...")
            time.sleep(5)
            return search_serpapi_maps(query, api_key)
        
        if response.status_code != 200:
            log(f"API error: {response.status_code} - {response.text[:200]}")
            return []
        
        data = response.json()
        
        # Check for errors in response
        if "error" in data:
            error_msg = data.get("error", "")
            if "limit" in error_msg.lower() or "credit" in error_msg.lower():
                log(f"OUT OF CREDITS: {error_msg}")
                _out_of_credits = True
                return []
            log(f"API error: {error_msg}")
            return []
        
        return data.get("local_results", [])
        
    except Exception as e:
        log(f"Error: {e}")
        return []


def is_chain_hotel(name: str) -> bool:
    """Check if hotel name matches a big chain."""
    name_lower = name.lower()
    return any(chain in name_lower for chain in SKIP_CHAIN_NAMES)


def extract_hotel_info(place: dict) -> dict:
    """Extract hotel info from SerpAPI place result."""
    name = place.get("title", "")
    
    # Get website - SerpAPI uses different field names
    website = place.get("website", "") or place.get("link", "")
    
    # Get phone
    phone = place.get("phone", "")
    
    # Get coordinates
    gps = place.get("gps_coordinates", {})
    lat = gps.get("latitude", "")
    lng = gps.get("longitude", "")
    
    return {
        "hotel": name,
        "website": website,
        "phone": phone,
        "lat": lat,
        "long": lng,
    }


def run_scraper(query: str, output_csv: str, api_key: str):
    """Run the scraper for a single query."""
    log(f"Searching: {query}")
    
    start_time = time.time()
    
    # Search
    places = search_serpapi_maps(query, api_key)
    log(f"  Raw results: {len(places)}")
    
    hotels = []
    seen_names = set()
    
    for place in places:
        name = place.get("title", "")
        
        if not name:
            continue
        
        # Skip duplicates
        name_key = name.lower().strip()
        if name_key in seen_names:
            continue
        seen_names.add(name_key)
        
        # Skip chains
        if is_chain_hotel(name):
            _stats["skipped_chains"] += 1
            continue
        
        hotel = extract_hotel_info(place)
        hotels.append(hotel)
        _stats["found"] += 1
    
    # Write output
    fieldnames = ["hotel", "website", "phone", "lat", "long"]
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hotels)
    
    elapsed = time.time() - start_time
    with_website = sum(1 for h in hotels if h.get("website"))
    
    log(f"  Hotels found: {_stats['found']}, Chains skipped: {_stats['skipped_chains']}, API calls: {_stats['api_calls']}")
    log(f"  Output: {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Sadie Scraper SerpAPI - Google Maps scraper")
    
    parser.add_argument("--query", "-q", type=str, required=True, help="Search query")
    parser.add_argument("--output", "-o", default="scraper_output/hotels_serpapi.csv")
    parser.add_argument("--api-key", type=str, help="SerpAPI key (or set SERPAPI_KEY env var)")
    
    args = parser.parse_args()
    
    # Get API key
    api_key = args.api_key or os.environ.get("SERPAPI_KEY", "")
    if not api_key:
        log("ERROR: No API key provided")
        log("Use --api-key or set SERPAPI_KEY environment variable")
        sys.exit(1)
    
    run_scraper(
        query=args.query,
        output_csv=args.output,
        api_key=api_key,
    )


if __name__ == "__main__":
    main()

