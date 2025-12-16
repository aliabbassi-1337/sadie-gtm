#!/usr/bin/env python3
"""
Sadie Scraper - Google Places API Hotel Scraper
================================================
Scrapes hotels from Google Places API by geographic area.
Filters out OTAs, apartments, and bad leads.

Usage:
    python3 sadie_scraper.py --center-lat 25.7617 --center-lng -80.1918 --overall-radius-km 35

Requires:
    - GOOGLE_PLACES_API_KEY in .env file
"""

import csv
import os
import math
import time
import argparse
from datetime import datetime
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

DEFAULT_CENTER_LAT = 25.7617
DEFAULT_CENTER_LNG = -80.1918
DEFAULT_OVERALL_RADIUS_KM = 35.0
DEFAULT_GRID_ROWS = 5
DEFAULT_GRID_COLS = 5
DEFAULT_MAX_PAGES_PER_CENTER = 3

OUTPUT_CSV = "hotels_scraped.csv"
LOG_FILE = "sadie_scraper.log"

# OTA domains to skip
OTA_DOMAINS_BLACKLIST = [
    "booking.com", "expedia.com", "hotels.com", "airbnb.com",
    "tripadvisor.com", "priceline.com", "agoda.com", "orbitz.com",
    "kayak.com", "travelocity.com", "hostelworld.com", "vrbo.com",
    "ebookers.com", "lastminute.com", "trivago.com", "hotwire.com", "travelzoo.com",
]

# Bad lead keywords
BAD_LEAD_KEYWORDS = [
    "apartment", "apartments", "condo", "condos", "condominium", "condominiums",
    "vacation rental", "vacation rentals", "holiday rental", "holiday home",
    "townhouse", "townhome", "villa rental", "private home",
    "hostel", "hostels", "backpacker",
    "timeshare", "time share", "fractional ownership",
    "extended stay", "corporate housing", "furnished apartment",
    "rv park", "rv resort", "campground", "camping", "glamping",
    "day spa", "wellness center",
    "event venue", "wedding venue", "banquet hall", "conference center",
]

# Search modes
SEARCH_MODES = [
    {"label": "lodging_type", "type": "lodging", "keyword": None},
    {"label": "hotel_keyword", "type": None, "keyword": "hotel"},
    {"label": "motel_keyword", "type": None, "keyword": "motel"},
    {"label": "resort_keyword", "type": None, "keyword": "resort"},
    {"label": "inn_keyword", "type": None, "keyword": "inn"},
    {"label": "boutique_hotel", "type": None, "keyword": "boutique hotel"},
]


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

_log_file = None

def init_log_file():
    global _log_file
    _log_file = open(LOG_FILE, "w", encoding="utf-8")

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if _log_file:
        _log_file.write(line + "\n")
        _log_file.flush()


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def is_ota_domain(website: str) -> bool:
    domain = extract_domain(website)
    for ota in OTA_DOMAINS_BLACKLIST:
        if domain.endswith(ota):
            return True
    return False


def is_bad_lead(name: str, website: str = "") -> bool:
    text = f"{name} {website}".lower()
    for kw in BAD_LEAD_KEYWORDS:
        if kw in text:
            return True
    return False


def deg_per_km_lat():
    return 1.0 / 111.0


def deg_per_km_lng(lat_deg: float) -> float:
    return 1.0 / (111.0 * math.cos(math.radians(lat_deg)))


def build_grid_centers(center_lat, center_lng, overall_radius_km, rows, cols):
    lat_span_deg = overall_radius_km * deg_per_km_lat()
    lng_span_deg = overall_radius_km * deg_per_km_lng(center_lat)

    min_lat = center_lat - lat_span_deg
    max_lat = center_lat + lat_span_deg
    min_lng = center_lng - lng_span_deg
    max_lng = center_lng + lng_span_deg

    centers = []
    for i in range(rows):
        row_frac = 0.0 if rows == 1 else i / (rows - 1)
        lat = min_lat + (max_lat - min_lat) * row_frac
        for j in range(cols):
            col_frac = 0.0 if cols == 1 else j / (cols - 1)
            lng = min_lng + (max_lng - min_lng) * col_frac
            centers.append((lat, lng))
    return centers


# ------------------------------------------------------------
# Google Places API
# ------------------------------------------------------------

def places_nearby(api_key, lat, lng, radius_m, place_type=None, keyword=None, page_token=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "key": api_key,
        "location": f"{lat},{lng}",
        "radius": radius_m,
    }
    if place_type:
        params["type"] = place_type
    if keyword:
        params["keyword"] = keyword
    if page_token:
        params["pagetoken"] = page_token

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def place_details(api_key, place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": api_key,
        "place_id": place_id,
        "fields": "name,geometry,website,business_status,formatted_phone_number,international_phone_number,rating,user_ratings_total,formatted_address,types",
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def run_scraper(
    api_key: str,
    center_lat: float,
    center_lng: float,
    overall_radius_km: float,
    grid_rows: int,
    grid_cols: int,
    max_pages_per_center: int,
    max_results: int,
    output_csv: str,
):
    log("Sadie Scraper - Google Places Hotel Scraper")
    
    spacing_km = (overall_radius_km * 2) / max(grid_rows - 1, grid_cols - 1, 1)
    search_radius_km = max(3.0, spacing_km * 0.75)
    radius_m = int(search_radius_km * 1000)
    
    centers = build_grid_centers(center_lat, center_lng, overall_radius_km, grid_rows, grid_cols)
    
    log(f"Center: {center_lat:.4f}, {center_lng:.4f} | Radius: {overall_radius_km}km")
    log(f"Grid: {grid_rows}x{grid_cols} ({len(centers)} centers) | Search radius: {search_radius_km:.1f}km")
    
    seen_place_ids = set()
    hotels = []
    stats = {"candidates": 0, "kept": 0, "no_website": 0, "ota": 0, "bad_lead": 0, "duplicates": 0}
    
    for center_idx, (lat, lng) in enumerate(centers, 1):
        log(f"Center {center_idx}/{len(centers)}: {lat:.4f}, {lng:.4f}")
        
        for mode in SEARCH_MODES:
            page_token = None
            page_count = 0
            
            while page_count < max_pages_per_center:
                page_count += 1
                try:
                    nearby = places_nearby(
                        api_key, lat, lng, radius_m,
                        place_type=mode["type"],
                        keyword=mode["keyword"],
                        page_token=page_token
                    )
                except Exception as e:
                    log(f"  API error: {e}")
                    break
                
                status = nearby.get("status")
                if status not in ("OK", "ZERO_RESULTS"):
                    if status == "OVER_QUERY_LIMIT":
                        log("  OVER_QUERY_LIMIT - stopping")
                    break
                
                results = nearby.get("results", [])
                if not results:
                    break
                
                for r in results:
                    place_id = r.get("place_id")
                    name = r.get("name", "").strip()
                    
                    if not place_id or not name:
                        continue
                    
                    if place_id in seen_place_ids:
                        stats["duplicates"] += 1
                        continue
                    seen_place_ids.add(place_id)
                    
                    try:
                        details = place_details(api_key, place_id)
                    except Exception:
                        continue
                    
                    if details.get("status") != "OK":
                        continue
                    
                    result = details.get("result", {})
                    website = (result.get("website") or "").strip()
                    
                    stats["candidates"] += 1
                    
                    if not website:
                        stats["no_website"] += 1
                        continue
                    
                    if is_ota_domain(website):
                        stats["ota"] += 1
                        continue
                    
                    if is_bad_lead(name, website):
                        stats["bad_lead"] += 1
                        continue
                    
                    geometry = result.get("geometry", {}).get("location", {})
                    hotels.append({
                        "name": name,
                        "website": website,
                        "latitude": geometry.get("lat", ""),
                        "longitude": geometry.get("lng", ""),
                        "phone": result.get("international_phone_number") or result.get("formatted_phone_number", ""),
                        "address": result.get("formatted_address", ""),
                        "rating": result.get("rating", ""),
                        "review_count": result.get("user_ratings_total", ""),
                        "place_id": place_id,
                    })
                    stats["kept"] += 1
                    log(f"  + {name}")
                    
                    if max_results > 0 and stats["kept"] >= max_results:
                        break
                
                if max_results > 0 and stats["kept"] >= max_results:
                    break
                
                page_token = nearby.get("next_page_token")
                if not page_token:
                    break
                time.sleep(2.0)
            
            if max_results > 0 and stats["kept"] >= max_results:
                break
        
        if max_results > 0 and stats["kept"] >= max_results:
            break
    
    # Save to CSV
    if hotels:
        fieldnames = ["name", "website", "latitude", "longitude", "phone", "address", "rating", "review_count", "place_id"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(hotels)
    
    log(f"\n{'='*60}")
    log(f"COMPLETE!")
    log(f"Hotels found: {stats['kept']}")
    log(f"Skipped: {stats['no_website']} no website, {stats['ota']} OTA, {stats['bad_lead']} bad leads, {stats['duplicates']} duplicates")
    log(f"Output: {output_csv}")
    log(f"{'='*60}")


def main():
    load_dotenv()
    init_log_file()
    
    parser = argparse.ArgumentParser(description="Sadie Scraper - Google Places Hotel Scraper")
    
    parser.add_argument("--center-lat", type=float, default=DEFAULT_CENTER_LAT)
    parser.add_argument("--center-lng", type=float, default=DEFAULT_CENTER_LNG)
    parser.add_argument("--overall-radius-km", type=float, default=DEFAULT_OVERALL_RADIUS_KM)
    parser.add_argument("--grid-rows", type=int, default=DEFAULT_GRID_ROWS)
    parser.add_argument("--grid-cols", type=int, default=DEFAULT_GRID_COLS)
    parser.add_argument("--max-pages-per-center", type=int, default=DEFAULT_MAX_PAGES_PER_CENTER)
    parser.add_argument("--max-results", type=int, default=0)
    parser.add_argument("--output", default=OUTPUT_CSV)
    
    args = parser.parse_args()
    
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        raise SystemExit("Missing GOOGLE_PLACES_API_KEY in .env")
    
    run_scraper(
        api_key=api_key,
        center_lat=args.center_lat,
        center_lng=args.center_lng,
        overall_radius_km=args.overall_radius_km,
        grid_rows=args.grid_rows,
        grid_cols=args.grid_cols,
        max_pages_per_center=args.max_pages_per_center,
        max_results=args.max_results,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()

