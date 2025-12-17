#!/usr/bin/env python3
"""
Sadie Scraper - Google Places API Hotel Scraper
================================================
Scrapes hotels from Google Places API by geographic area.
Filters out OTAs, apartments, and bad leads.
Uses concurrent API calls for speed.

Usage:
    python3 sadie_scraper.py --center-lat 25.7617 --center-lng -80.1918 --overall-radius-km 35
    python3 sadie_scraper.py --center-lat 38.3886 --center-lng -75.0735 --overall-radius-km 20 --concurrency 20

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Optional

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
DEFAULT_CONCURRENCY = 15  # Parallel API calls

OUTPUT_DIR = "hotel_scraper_output"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "hotels_scraped.csv")
LOG_FILE = "sadie_scraper.log"

# OTA domains to skip
OTA_DOMAINS_BLACKLIST = [
    "booking.com", "expedia.com", "hotels.com", "airbnb.com",
    "tripadvisor.com", "priceline.com", "agoda.com", "orbitz.com",
    "kayak.com", "travelocity.com", "hostelworld.com", "vrbo.com",
    "ebookers.com", "lastminute.com", "trivago.com", "hotwire.com", "travelzoo.com",
]

# Big hotel chains to skip - they have their own booking systems, not good leads
SKIP_CHAIN_DOMAINS = [
    "marriott.com", "hilton.com", "ihg.com", "hyatt.com", "wyndham.com",
    "choicehotels.com", "bestwestern.com", "radissonhotels.com", "accor.com",
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
# Logging (thread-safe)
# ------------------------------------------------------------

_log_file = None
_log_lock = Lock()

def init_log_file():
    global _log_file
    _log_file = open(LOG_FILE, "w", encoding="utf-8")

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    with _log_lock:
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


def is_big_chain(website: str) -> bool:
    """Check if website belongs to a big hotel chain."""
    domain = extract_domain(website)
    for chain in SKIP_CHAIN_DOMAINS:
        if chain in domain.lower():
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

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def place_details(api_key, place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": api_key,
        "place_id": place_id,
        "fields": "name,geometry,website,business_status,formatted_phone_number,international_phone_number,rating,user_ratings_total,formatted_address,types",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ------------------------------------------------------------
# Concurrent Processing
# ------------------------------------------------------------

def fetch_place_details_batch(api_key: str, place_ids: list, max_workers: int = 15) -> dict:
    """Fetch place details concurrently."""
    results = {}
    
    def fetch_one(place_id):
        try:
            return place_id, place_details(api_key, place_id)
        except Exception as e:
            return place_id, {"status": "ERROR", "error": str(e)}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, pid): pid for pid in place_ids}
        for future in as_completed(futures):
            place_id, details = future.result()
            results[place_id] = details
    
    return results


def search_center(api_key: str, lat: float, lng: float, radius_m: int, 
                  max_pages: int, seen_place_ids: set, lock: Lock) -> list:
    """Search all modes for a single center. Returns list of (place_id, name) tuples."""
    results = []
    
    for mode in SEARCH_MODES:
        page_token = None
        page_count = 0
        
        while page_count < max_pages:
            page_count += 1
            try:
                nearby = places_nearby(
                    api_key, lat, lng, radius_m,
                    place_type=mode["type"],
                    keyword=mode["keyword"],
                    page_token=page_token
                )
            except Exception as e:
                break
            
            status = nearby.get("status")
            if status not in ("OK", "ZERO_RESULTS"):
                break
            
            places = nearby.get("results", [])
            
            for r in places:
                place_id = r.get("place_id")
                name = r.get("name", "").strip()
                
                if not place_id or not name:
                    continue
                
                with lock:
                    if place_id in seen_place_ids:
                        continue
                    seen_place_ids.add(place_id)
                
                results.append((place_id, name))
            
            page_token = nearby.get("next_page_token")
            if not page_token:
                break
            time.sleep(2.0)  # Required by Google for pagination
    
    return results


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
    concurrency: int,
):
    log("Sadie Scraper - Google Places Hotel Scraper")
    
    spacing_km = (overall_radius_km * 2) / max(grid_rows - 1, grid_cols - 1, 1)
    search_radius_km = max(3.0, spacing_km * 0.75)
    radius_m = int(search_radius_km * 1000)
    
    centers = build_grid_centers(center_lat, center_lng, overall_radius_km, grid_rows, grid_cols)
    
    log(f"Center: {center_lat:.4f}, {center_lng:.4f} | Radius: {overall_radius_km}km")
    log(f"Grid: {grid_rows}x{grid_cols} ({len(centers)} centers) | Search radius: {search_radius_km:.1f}km")
    log(f"Concurrency: {concurrency} parallel requests")
    
    seen_place_ids = set()
    seen_lock = Lock()
    place_ids_to_fetch = []
    hotels = []
    stats = {"candidates": 0, "kept": 0, "no_website": 0, "ota": 0, "bad_lead": 0, "big_chain": 0, "duplicates": 0, "existing": 0}
    
    # Load existing hotels from output file to avoid duplicates across runs
    existing_hotels = {}
    if os.path.exists(output_csv):
        try:
            with open(output_csv, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row.get("hotel", "").strip().lower(), row.get("website", "").strip().lower())
                    if key[0]:
                        existing_hotels[key] = row
            if existing_hotels:
                log(f"Loaded {len(existing_hotels)} existing hotels from {output_csv}")
        except Exception as e:
            log(f"Warning: Could not read existing file: {e}")
    
    # Phase 1: Search all centers concurrently
    log(f"\nPhase 1: Searching {len(centers)} grid centers...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=min(concurrency, len(centers))) as executor:
        futures = {
            executor.submit(
                search_center, api_key, lat, lng, radius_m, 
                max_pages_per_center, seen_place_ids, seen_lock
            ): (i, lat, lng)
            for i, (lat, lng) in enumerate(centers, 1)
        }
        
        for future in as_completed(futures):
            center_idx, lat, lng = futures[future]
            try:
                results = future.result()
                place_ids_to_fetch.extend(results)
                log(f"  Center {center_idx}/{len(centers)}: {len(results)} places found")
            except Exception as e:
                log(f"  Center {center_idx} error: {e}")
    
    search_time = time.time() - start_time
    log(f"  Search complete: {len(place_ids_to_fetch)} unique places in {search_time:.1f}s")
    
    # Phase 2: Fetch all place details concurrently
    if place_ids_to_fetch:
        log(f"\nPhase 2: Fetching details for {len(place_ids_to_fetch)} places...")
        start_time = time.time()
        
        place_id_map = {pid: name for pid, name in place_ids_to_fetch}
        place_ids = [pid for pid, _ in place_ids_to_fetch]
        
        # Fetch in batches
        batch_size = 100
        all_details = {}
        for i in range(0, len(place_ids), batch_size):
            batch = place_ids[i:i+batch_size]
            batch_details = fetch_place_details_batch(api_key, batch, max_workers=concurrency)
            all_details.update(batch_details)
            log(f"  Fetched {min(i+batch_size, len(place_ids))}/{len(place_ids)} details")
        
        fetch_time = time.time() - start_time
        log(f"  Fetch complete in {fetch_time:.1f}s")
        
        # Phase 3: Process all fetched details
        log(f"\nPhase 3: Processing results...")
        for place_id, details in all_details.items():
            if details.get("status") != "OK":
                continue
            
            name = place_id_map.get(place_id, "")
            result = details.get("result", {})
            website = (result.get("website") or "").strip()
            
            stats["candidates"] += 1
            
            # Keep hotels without websites for later enrichment
            if not website:
                stats["no_website"] += 1
            
            if is_ota_domain(website):
                stats["ota"] += 1
                continue
            
            if is_big_chain(website):
                stats["big_chain"] += 1
                continue
            
            if is_bad_lead(name, website):
                stats["bad_lead"] += 1
                continue
            
            geometry = result.get("geometry", {}).get("location", {})
            phone = result.get("international_phone_number") or result.get("formatted_phone_number", "")
            
            # Check if this hotel already exists in output file
            key = (name.lower(), website.lower())
            if key in existing_hotels:
                stats["existing"] += 1
                continue
            
            hotels.append({
                "hotel": name,
                "website": website,
                "phone": phone,
                "lat": geometry.get("lat", ""),
                "long": geometry.get("lng", ""),
            })
            stats["kept"] += 1
            
            if max_results > 0 and stats["kept"] >= max_results:
                break
    
    # Merge new hotels with existing ones
    all_hotels = list(existing_hotels.values()) + hotels
    
    # Save to CSV
    if all_hotels or existing_hotels:
        output_dir = os.path.dirname(output_csv)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        fieldnames = ["hotel", "website", "phone", "lat", "long"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_hotels)
    
    log(f"\n{'='*60}")
    log("COMPLETE!")
    log(f"Candidates found: {stats['candidates']}")
    log(f"New hotels added: {stats['kept']}")
    log(f"Already existed:  {stats['existing']}")
    log(f"Without website:  {stats['no_website']} (kept for enrichment)")
    log(f"Skipped: {stats['ota']} OTA, {stats['big_chain']} big chains, {stats['bad_lead']} bad leads")
    if all_hotels:
        log(f"Total in file:    {len(all_hotels)}")
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
    parser.add_argument("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
                        help=f"Number of concurrent API requests (default: {DEFAULT_CONCURRENCY})")
    
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
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
