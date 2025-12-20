#!/usr/bin/env python3
"""
Sadie Scraper Google Search - Scrape Google Travel via site: operator
======================================================================
Uses Serper's regular search endpoint with site:google.com/travel

Usage:
    python3 sadie_scraper_google_search.py --query "Sydney hotels" --api-key KEY
"""

import csv
import os
import sys
import argparse
import re
import requests
from datetime import datetime
from urllib.parse import unquote, urlparse, parse_qs

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Stats
_stats = {"found": 0, "api_calls": 0}

def log(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")


def search_google(query: str, api_key: str) -> dict:
    """Regular Google search via Serper. Returns ~10 results per query."""
    _stats["api_calls"] += 1
    
    try:
        response = requests.post(
            SERPER_SEARCH_URL,
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json"
            },
            json={"q": query},
            timeout=30
        )
        
        if response.status_code != 200:
            log(f"API error: {response.status_code} - {response.text[:200]}")
            return {}
        
        return response.json()
        
    except Exception as e:
        log(f"Error: {e}")
        return {}


def extract_hotels_from_results(data: dict) -> list:
    """Extract hotel info from Google search results."""
    hotels = []
    seen = set()
    
    # Process organic results
    for result in data.get("organic", []):
        title = result.get("title", "")
        link = result.get("link", "")
        snippet = result.get("snippet", "")
        
        # Skip non-travel results
        if "google.com/travel" not in link:
            continue
        
        # Try to extract hotel name from title
        # Google Travel titles are like "Hotel Name - Google Hotel Search"
        hotel_name = title.replace(" - Google Hotel Search", "").replace(" - Google Travel", "").strip()
        
        if not hotel_name or hotel_name.lower() in seen:
            continue
        seen.add(hotel_name.lower())
        
        # Try to extract info from snippet
        phone = ""
        phone_match = re.search(r'\+?\d[\d\s\-\(\)]{8,}', snippet)
        if phone_match:
            phone = phone_match.group().strip()
        
        hotels.append({
            "hotel": hotel_name,
            "website": "",  # Would need to visit the page to get this
            "phone": phone,
            "source_url": link,
        })
        _stats["found"] += 1
    
    # Also check "places" if present (sometimes Google returns these)
    for place in data.get("places", []):
        name = place.get("title", "")
        if name and name.lower() not in seen:
            seen.add(name.lower())
            hotels.append({
                "hotel": name,
                "website": place.get("website", ""),
                "phone": place.get("phone", ""),
                "source_url": place.get("link", ""),
            })
            _stats["found"] += 1
    
    return hotels


def run_scraper(base_query: str, output_csv: str, api_key: str):
    """Run the scraper."""
    log(f"Query: {base_query} site:google.com/travel")
    
    # Search with site: operator
    query = f"{base_query} site:google.com/travel"
    data = search_google(query, api_key)
    
    hotels = extract_hotels_from_results(data)
    
    log(f"Found {len(hotels)} hotels")
    
    # Write output
    fieldnames = ["hotel", "website", "phone", "source_url"]
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hotels)
    
    log(f"Output: {output_csv}")
    log(f"API calls: {_stats['api_calls']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", "-q", required=True, help="Search query (e.g., 'Sydney hotels')")
    parser.add_argument("--output", "-o", default="scraper_output/google_search.csv")
    parser.add_argument("--api-key", help="Serper API key")
    
    args = parser.parse_args()
    
    api_key = args.api_key or os.environ.get("SERPER_KEY", "")
    if not api_key:
        log("ERROR: No API key")
        sys.exit(1)
    
    run_scraper(args.query, args.output, api_key)


if __name__ == "__main__":
    main()

