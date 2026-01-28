#!/usr/bin/env python3
"""
Enrich Maryland hotel data with lat/lon coordinates from MD iMap Parcel Points API.
"""

import csv
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

INPUT_FILE = Path(__file__).parent.parent / "hotel-sources/us/maryland/maryland_hotels_statewide.csv"
OUTPUT_FILE = Path(__file__).parent.parent / "hotel-sources/us/maryland/maryland_hotels_statewide.csv"

API_URL = "https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_PropertyData/MapServer/0/query"
BATCH_SIZE = 100  # API allows 2000, but URLs get too long

def fetch_coordinates(account_ids: list[str]) -> dict[str, tuple[float, float]]:
    """Query MD iMap API for parcel coordinates."""
    # Build WHERE clause: ACCTID IN ('id1','id2',...)
    ids_str = ",".join(f"'{aid}'" for aid in account_ids)
    where_clause = f"ACCTID IN ({ids_str})"

    params = {
        "where": where_clause,
        "outFields": "ACCTID",
        "f": "geojson",
        "outSR": "4326",
        "returnGeometry": "true"
    }

    url = f"{API_URL}?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode())
    except Exception as e:
        print(f"  Error fetching batch: {e}")
        return {}

    coords = {}
    for feature in data.get("features", []):
        acctid = feature["properties"]["ACCTID"]
        geom = feature.get("geometry")
        if geom and geom.get("coordinates"):
            lon, lat = geom["coordinates"]
            coords[acctid] = (lat, lon)

    return coords

def main():
    # Read existing data
    print(f"Reading {INPUT_FILE}...")
    with open(INPUT_FILE, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    print(f"Found {len(rows)} hotels")

    # Get all account IDs
    account_ids = [row["account_id"] for row in rows]

    # Fetch coordinates in batches
    all_coords = {}
    total_batches = (len(account_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(account_ids), BATCH_SIZE):
        batch = account_ids[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"Fetching batch {batch_num}/{total_batches} ({len(batch)} parcels)...")

        coords = fetch_coordinates(batch)
        all_coords.update(coords)

        # Rate limit
        if i + BATCH_SIZE < len(account_ids):
            time.sleep(0.5)

    print(f"\nGot coordinates for {len(all_coords)}/{len(account_ids)} parcels")

    # Add lat/lon columns
    if "lat" not in fieldnames:
        fieldnames = list(fieldnames) + ["lat", "lon"]

    matched = 0
    for row in rows:
        acctid = row["account_id"]
        if acctid in all_coords:
            row["lat"], row["lon"] = all_coords[acctid]
            matched += 1
        else:
            row["lat"] = ""
            row["lon"] = ""

    # Write output
    print(f"\nWriting {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done! {matched}/{len(rows)} hotels have coordinates")

if __name__ == "__main__":
    main()
