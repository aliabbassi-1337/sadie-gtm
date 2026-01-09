#!/usr/bin/env python3
"""
City Scraper - Scrape hotels for a city by name
================================================
Loads zip codes from data/us_zipcodes.csv and searches each one.

Usage:
    python3 scripts/scrapers/city.py --city "Miami Beach" --state FL
    python3 scripts/scrapers/city.py --city "Key West" --state FL
"""

import csv
import os
import sys
import argparse
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ZIPCODE_FILE = os.path.join(PROJECT_ROOT, "data", "us_zipcodes.csv")

SERPER_MAPS_URL = "https://google.serper.dev/maps"
SEARCH_TERMS = ["hotels", "motels", "inns", "resorts", "boutique hotel"]

SKIP_CHAINS = [
    "marriott", "hilton", "hyatt", "sheraton", "westin", "w hotel",
    "intercontinental", "holiday inn", "crowne plaza", "ihg",
    "best western", "choice hotels", "comfort inn", "quality inn",
    "radisson", "wyndham", "ramada", "days inn", "super 8", "motel 6",
    "la quinta", "travelodge", "ibis", "novotel", "mercure", "accor",
    "four seasons", "ritz-carlton", "st. regis", "fairmont",
]

_stats = {"api_calls": 0, "hotels_found": 0}


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_city_zipcodes(city: str, state: str) -> list:
    """Get all zip codes for a city from the CSV."""
    zips = []
    city_lower = city.lower()

    with open(ZIPCODE_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['state'] == state and row['city'].lower() == city_lower:
                zips.append(row['code'])

    return sorted(set(zips))


def search_serper(query: str, api_key: str) -> list:
    """Search Google Maps via Serper."""
    _stats["api_calls"] += 1
    try:
        resp = requests.post(
            SERPER_MAPS_URL,
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={"q": query, "num": 40},
            timeout=30
        )
        if resp.status_code == 400 and "credits" in resp.text.lower():
            log("OUT OF CREDITS!")
            return []
        if resp.status_code != 200:
            return []
        return resp.json().get("places", [])
    except:
        return []


def scrape_city(city: str, state: str, api_key: str) -> list:
    """Scrape hotels for a city."""
    zip_codes = get_city_zipcodes(city, state)
    log(f"Found {len(zip_codes)} zip codes for {city}, {state}")

    hotels = []
    seen = set()

    # Search by city name
    for term in SEARCH_TERMS:
        query = f"{term} in {city}, {state}"
        log(f"Searching: {query}")

        for place in search_serper(query, api_key):
            name = place.get("title", "").strip()
            if not name:
                continue

            name_lower = name.lower()
            if any(chain in name_lower for chain in SKIP_CHAINS):
                continue
            if name_lower in seen:
                continue
            seen.add(name_lower)

            hotels.append({
                "hotel": name,
                "website": place.get("website", ""),
                "phone": place.get("phoneNumber", ""),
                "address": place.get("address", ""),
                "lat": place.get("latitude", ""),
                "long": place.get("longitude", ""),
                "rating": place.get("rating", ""),
                "city": city,
            })
            _stats["hotels_found"] += 1

    # Search by zip codes
    for zipcode in zip_codes:
        for term in SEARCH_TERMS:
            query = f"{term} in {zipcode}"
            log(f"Searching: {query}")

            for place in search_serper(query, api_key):
                name = place.get("title", "").strip()
                if not name:
                    continue

                name_lower = name.lower()
                if any(chain in name_lower for chain in SKIP_CHAINS):
                    continue
                if name_lower in seen:
                    continue
                seen.add(name_lower)

                hotels.append({
                    "hotel": name,
                    "website": place.get("website", ""),
                    "phone": place.get("phoneNumber", ""),
                    "address": place.get("address", ""),
                    "lat": place.get("latitude", ""),
                    "long": place.get("longitude", ""),
                    "rating": place.get("rating", ""),
                    "city": city,
                })
                _stats["hotels_found"] += 1

    return hotels


def main():
    parser = argparse.ArgumentParser(description="Scrape hotels for a city")
    parser.add_argument("--city", required=True, help="City name (e.g. 'Miami Beach')")
    parser.add_argument("--state", default="FL", help="State abbreviation (default: FL)")
    parser.add_argument("--output", "-o", default="scraper_output", help="Output directory")
    args = parser.parse_args()

    api_key = os.environ.get("SERPER_SAMI") or os.environ.get("SERPER_KEY")
    if not api_key:
        log("ERROR: Set SERPER_SAMI or SERPER_KEY")
        sys.exit(1)

    log(f"Scraping: {args.city}, {args.state}")
    hotels = scrape_city(args.city, args.state, api_key)

    # Save output
    os.makedirs(args.output, exist_ok=True)
    slug = args.city.lower().replace(" ", "_")
    output_file = os.path.join(args.output, f"{slug}_hotels.csv")

    if hotels:
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=hotels[0].keys())
            writer.writeheader()
            writer.writerows(hotels)

    log(f"Done: {_stats['hotels_found']} hotels, {_stats['api_calls']} API calls")
    log(f"Output: {output_file}")


if __name__ == "__main__":
    main()
