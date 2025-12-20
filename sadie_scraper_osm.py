#!/usr/bin/env python3
"""
Sadie Scraper OSM - Free Hotel Scraper using OpenStreetMap
===========================================================
Uses OpenStreetMap Overpass API (FREE) to find hotels.
No API keys required!

Usage:
    python3 sadie_scraper_osm.py --city "Sydney, Australia" --radius-km 50
    python3 sadie_scraper_osm.py --center-lat -33.8688 --center-lng 151.2093 --radius-km 30
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

OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
NOMINATIM_API_URL = "https://nominatim.openstreetmap.org/search"

DEFAULT_RADIUS_KM = 30

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
_stats = {"found": 0, "skipped_chains": 0}


def log(msg: str):
    """Logging with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


# ============================================================================
# GEOCODING
# ============================================================================

def geocode_city(city_name: str) -> tuple:
    """Convert city name to lat/lng using Nominatim (free)."""
    try:
        response = requests.get(
            NOMINATIM_API_URL,
            params={
                "q": city_name,
                "format": "json",
                "limit": 1,
            },
            headers={"User-Agent": "SadieScraper/1.0"},
            timeout=10,
        )
        data = response.json()
        if data:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            display_name = data[0].get("display_name", city_name)
            return lat, lng, display_name
    except Exception as e:
        log(f"Geocoding error: {e}")
    return None, None, None


# ============================================================================
# OPENSTREETMAP OVERPASS API
# ============================================================================

def query_osm_hotels(center_lat: float, center_lng: float, radius_km: float) -> list:
    """
    Query OpenStreetMap for hotels using Overpass API.
    Returns list of hotel dicts.
    """
    radius_m = int(radius_km * 1000)
    
    query = f"""
    [out:json][timeout:60];
    (
      node["tourism"="hotel"](around:{radius_m},{center_lat},{center_lng});
      way["tourism"="hotel"](around:{radius_m},{center_lat},{center_lng});
      node["tourism"="motel"](around:{radius_m},{center_lat},{center_lng});
      way["tourism"="motel"](around:{radius_m},{center_lat},{center_lng});
      node["tourism"="guest_house"](around:{radius_m},{center_lat},{center_lng});
      way["tourism"="guest_house"](around:{radius_m},{center_lat},{center_lng});
      node["amenity"="hotel"](around:{radius_m},{center_lat},{center_lng});
      way["amenity"="hotel"](around:{radius_m},{center_lat},{center_lng});
      node["tourism"="resort"](around:{radius_m},{center_lat},{center_lng});
      way["tourism"="resort"](around:{radius_m},{center_lat},{center_lng});
    );
    out center tags;
    """
    
    log(f"Querying OSM Overpass API (radius: {radius_km}km)...")
    
    try:
        response = requests.post(
            OVERPASS_API_URL,
            data={"data": query},
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
    except requests.Timeout:
        log("ERROR: Overpass API timeout. Try a smaller radius.")
        return []
    except Exception as e:
        log(f"ERROR: Overpass API error: {e}")
        return []
    
    elements = data.get("elements", [])
    log(f"OSM returned {len(elements)} raw elements")
    
    hotels = []
    seen_names = set()
    
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name", "").strip()
        
        if not name:
            continue
        
        # Skip duplicates
        name_lower = name.lower()
        if name_lower in seen_names:
            continue
        seen_names.add(name_lower)
        
        # Skip big chains
        if any(chain in name_lower for chain in SKIP_CHAIN_NAMES):
            _stats["skipped_chains"] += 1
            continue
        
        # Get coordinates
        if el.get("type") == "node":
            lat = el.get("lat")
            lng = el.get("lon")
        else:
            center = el.get("center", {})
            lat = center.get("lat")
            lng = center.get("lon")
        
        hotel = {
            "hotel": name,
            "website": tags.get("website", "") or tags.get("contact:website", ""),
            "phone": tags.get("phone", "") or tags.get("contact:phone", ""),
            "lat": lat or "",
            "long": lng or "",
        }
        
        hotels.append(hotel)
        _stats["found"] += 1
    
    return hotels


# ============================================================================
# MAIN
# ============================================================================

def run_scraper(
    center_lat: float,
    center_lng: float,
    radius_km: float,
    location_label: str,
    output_csv: str,
):
    """Main scraper function."""
    global _stats
    _stats = {"found": 0, "skipped_chains": 0}
    
    log("Sadie Scraper OSM - Free Hotel Scraper")
    log(f"Center: {center_lat:.4f}, {center_lng:.4f}")
    log(f"Radius: {radius_km}km")
    log(f"Location: {location_label}")
    
    start_time = time.time()
    
    # Query OSM
    hotels = query_osm_hotels(center_lat, center_lng, radius_km)
    
    if not hotels:
        log("No hotels found. Try a larger radius.")
        return
    
    # Save to CSV
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    fieldnames = ["hotel", "website", "phone", "lat", "long"]
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hotels)
    
    elapsed = time.time() - start_time
    with_website = sum(1 for h in hotels if h.get("website"))
    
    log("")
    log("=" * 60)
    log("COMPLETE!")
    log(f"Hotels found:      {_stats['found']}")
    log(f"Skipped (chains):  {_stats['skipped_chains']}")
    log(f"With website:      {with_website}")
    log(f"Without website:   {_stats['found'] - with_website}")
    log(f"Time:              {elapsed:.1f}s")
    log(f"Output:            {output_csv}")
    log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Sadie Scraper OSM - Free hotel scraper")
    
    parser.add_argument("--center-lat", type=float, help="Center latitude")
    parser.add_argument("--center-lng", type=float, help="Center longitude")
    parser.add_argument("--city", type=str, help="City name (alternative to lat/lng)")
    parser.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM)
    parser.add_argument("--output", "-o", default="scraper_output/hotels_osm.csv")
    
    args = parser.parse_args()
    
    # Determine coordinates
    if args.city:
        log(f"Geocoding: {args.city}")
        lat, lng, display_name = geocode_city(args.city)
        if lat is None:
            log(f"ERROR: Could not geocode '{args.city}'")
            sys.exit(1)
        log(f"Found: {display_name}")
        center_lat, center_lng = lat, lng
        location_label = args.city
    elif args.center_lat is not None and args.center_lng is not None:
        center_lat = args.center_lat
        center_lng = args.center_lng
        location_label = f"{center_lat:.2f}, {center_lng:.2f}"
    else:
        parser.error("Either --city or both --center-lat and --center-lng required")
    
    run_scraper(
        center_lat=center_lat,
        center_lng=center_lng,
        radius_km=args.radius_km,
        location_label=location_label,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()
