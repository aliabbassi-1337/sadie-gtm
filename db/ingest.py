#!/usr/bin/env python3
"""
Sadie GTM - Data Ingestion Script
==================================
Loads existing CSV files into PostgreSQL database.

Usage:
    python3 db/ingest.py --scraper scraper_output/florida
    python3 db/ingest.py --detector detector_output/florida
    python3 db/ingest.py --all
"""

import os
import csv
import argparse
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

# Database connection
DB_CONFIG = {
    "host": os.getenv("SADIE_DB_HOST", "sadieconcierge-prod-instance-1.ch8ycoa6ur9a.eu-north-1.rds.amazonaws.com"),
    "port": os.getenv("SADIE_DB_PORT", "5432"),
    "database": os.getenv("SADIE_DB_NAME", "sadieconcierge"),
    "user": os.getenv("SADIE_DB_USER", ""),
    "password": os.getenv("SADIE_DB_PASSWORD", ""),
}

SCHEMA = "sadie_gtm"

# Known booking engines for tier classification
TIER1_ENGINES = {
    "cloudbeds", "synxis", "travelclick", "siteminder", "mews", "rms", "rmscloud",
    "resnexus", "thinkreservations", "innroad", "webrez", "guesty", "lodgify",
    "little hotelier", "eviivo", "beds24", "hostaway", "bookassist", "avvio",
    "net affinity", "profitroom", "d-edge", "sabre", "amadeus", "opera",
    "protel", "apaleo", "clock pms", "hotelogix", "roomracoon", "sirvoy",
    "stayntouch", "oracle hospitality", "infor hms"
}


def get_connection():
    """Get database connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def parse_float(val):
    """Safely parse float."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def parse_int(val):
    """Safely parse integer."""
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def classify_tier(booking_engine: str) -> int:
    """Classify booking engine as tier 1 or 2."""
    if not booking_engine:
        return 2
    engine_lower = booking_engine.lower()
    for known in TIER1_ENGINES:
        if known in engine_lower:
            return 1
    if "unknown" in engine_lower:
        return 2
    return 1  # Default to tier 1 if it's a named engine


def extract_city_from_filename(filepath: Path) -> str:
    """Extract city name from filename."""
    name = filepath.stem
    # Remove common suffixes
    for suffix in ["_leads", "_hotels", "_osm", "_serper", "_grid", "_zipcode", "_scraped"]:
        name = name.replace(suffix, "")
    return name.replace("_", " ").title()


def extract_state_from_path(filepath: Path) -> str:
    """Extract state from directory path."""
    parent = filepath.parent.name
    return parent.replace("_", " ").title()


def ingest_scraper_file(conn, filepath: Path, state: str = None):
    """Ingest a single scraper CSV file."""
    city = extract_city_from_filename(filepath)
    state = state or extract_state_from_path(filepath)

    rows = []
    with open(filepath, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Handle different column names
            name = row.get("hotel") or row.get("name") or row.get("Hotel") or row.get("Name")
            website = row.get("website") or row.get("Website")
            phone = row.get("phone") or row.get("phone_google") or row.get("Phone")
            address = row.get("address") or row.get("Address")
            lat = row.get("lat") or row.get("latitude") or row.get("Latitude")
            lng = row.get("long") or row.get("longitude") or row.get("lng") or row.get("Longitude")
            rating = row.get("rating") or row.get("Rating")
            review_count = row.get("review_count") or row.get("reviews")

            if not name:
                continue

            rows.append((
                name.strip(),
                website.strip() if website else None,
                phone.strip() if phone else None,
                address.strip() if address else None,
                parse_float(lat),
                parse_float(lng),
                city,
                state,
                "USA",
                parse_float(rating),
                parse_int(review_count),
                filepath.stem,  # source
            ))

    if not rows:
        return 0

    cursor = conn.cursor()
    cursor.execute(f"SET search_path TO {SCHEMA}")

    # Upsert hotels
    execute_values(
        cursor,
        """
        INSERT INTO hotels (name, website, phone, address, latitude, longitude, city, state, country, rating, review_count, source)
        VALUES %s
        ON CONFLICT (name, COALESCE(website, '')) DO UPDATE SET
            phone = COALESCE(EXCLUDED.phone, hotels.phone),
            address = COALESCE(EXCLUDED.address, hotels.address),
            latitude = COALESCE(EXCLUDED.latitude, hotels.latitude),
            longitude = COALESCE(EXCLUDED.longitude, hotels.longitude),
            rating = COALESCE(EXCLUDED.rating, hotels.rating),
            review_count = COALESCE(EXCLUDED.review_count, hotels.review_count)
        """,
        rows,
        page_size=1000
    )

    conn.commit()
    return len(rows)


def ingest_detector_file(conn, filepath: Path, state: str = None):
    """Ingest a single detector CSV file (leads)."""
    city = extract_city_from_filename(filepath)
    state = state or extract_state_from_path(filepath)

    rows = []
    with open(filepath, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name") or row.get("Name")
            website = row.get("website") or row.get("Website")
            booking_url = row.get("booking_url")
            booking_engine = row.get("booking_engine")
            booking_engine_domain = row.get("booking_engine_domain")

            if not name:
                continue

            tier = classify_tier(booking_engine)

            rows.append((
                name.strip(),
                website.strip() if website else None,
                booking_url.strip() if booking_url else None,
                booking_engine.strip() if booking_engine else None,
                booking_engine_domain.strip() if booking_engine_domain else None,
                row.get("detection_method"),
                tier,
                row.get("phone_google"),
                row.get("phone_website"),
                row.get("email"),
                row.get("address"),
                parse_float(row.get("latitude")),
                parse_float(row.get("longitude")),
                city,
                state,
                "USA",
                parse_float(row.get("rating")),
                parse_int(row.get("review_count")),
                parse_int(row.get("room_count")),
                "enriched" if row.get("room_count") else "detected",
            ))

    if not rows:
        return 0

    cursor = conn.cursor()
    cursor.execute(f"SET search_path TO {SCHEMA}")

    # Upsert leads
    execute_values(
        cursor,
        """
        INSERT INTO leads (
            name, website, booking_url, booking_engine, booking_engine_domain,
            detection_method, tier, phone_google, phone_website, email, address,
            latitude, longitude, city, state, country, rating, review_count,
            room_count, status
        )
        VALUES %s
        ON CONFLICT (name, COALESCE(website, '')) DO UPDATE SET
            booking_url = COALESCE(EXCLUDED.booking_url, leads.booking_url),
            booking_engine = COALESCE(EXCLUDED.booking_engine, leads.booking_engine),
            booking_engine_domain = COALESCE(EXCLUDED.booking_engine_domain, leads.booking_engine_domain),
            phone_google = COALESCE(EXCLUDED.phone_google, leads.phone_google),
            phone_website = COALESCE(EXCLUDED.phone_website, leads.phone_website),
            email = COALESCE(EXCLUDED.email, leads.email),
            room_count = COALESCE(EXCLUDED.room_count, leads.room_count),
            status = CASE WHEN EXCLUDED.room_count IS NOT NULL THEN 'enriched' ELSE leads.status END,
            updated_at = CURRENT_TIMESTAMP
        """,
        rows,
        page_size=1000
    )

    conn.commit()
    return len(rows)


def ingest_scraper_directory(conn, directory: Path):
    """Ingest all scraper CSVs from a directory."""
    state = extract_state_from_path(directory / "dummy.csv")
    total = 0

    csv_files = list(directory.glob("*.csv"))
    csv_files = [f for f in csv_files if "stats" not in f.name.lower()]

    print(f"Found {len(csv_files)} CSV files in {directory}")

    for filepath in sorted(csv_files):
        try:
            count = ingest_scraper_file(conn, filepath, state)
            print(f"  {filepath.name}: {count} hotels")
            total += count
        except Exception as e:
            print(f"  {filepath.name}: ERROR - {e}")

    return total


def ingest_detector_directory(conn, directory: Path):
    """Ingest all detector CSVs from a directory."""
    state = extract_state_from_path(directory / "dummy.csv")
    total = 0

    csv_files = list(directory.glob("*_leads.csv"))
    # Skip junk files
    csv_files = [f for f in csv_files if not any(x in f.name for x in
                 ["funnel", "checkpoint", "backup", "_post", "_old"])]

    print(f"Found {len(csv_files)} leads files in {directory}")

    for filepath in sorted(csv_files):
        try:
            count = ingest_detector_file(conn, filepath, state)
            print(f"  {filepath.name}: {count} leads")
            total += count
        except Exception as e:
            print(f"  {filepath.name}: ERROR - {e}")

    return total


def ingest_all(conn, scraper_base: Path, detector_base: Path):
    """Ingest all data from scraper and detector directories."""
    total_hotels = 0
    total_leads = 0

    # Ingest scraper data
    print("\n=== INGESTING SCRAPER DATA ===")
    for state_dir in sorted(scraper_base.iterdir()):
        if state_dir.is_dir() and not state_dir.name.startswith("."):
            print(f"\nState: {state_dir.name}")
            total_hotels += ingest_scraper_directory(conn, state_dir)

    # Ingest detector data
    print("\n=== INGESTING DETECTOR DATA ===")
    for state_dir in sorted(detector_base.iterdir()):
        if state_dir.is_dir() and not state_dir.name.startswith("."):
            if "backup" in state_dir.name.lower():
                continue
            print(f"\nState: {state_dir.name}")
            total_leads += ingest_detector_directory(conn, state_dir)

    return total_hotels, total_leads


def main():
    parser = argparse.ArgumentParser(description="Ingest Sadie GTM data into PostgreSQL")
    parser.add_argument("--scraper", type=Path, help="Scraper output directory to ingest")
    parser.add_argument("--detector", type=Path, help="Detector output directory to ingest")
    parser.add_argument("--all", action="store_true", help="Ingest all scraper and detector data")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually write to database")

    args = parser.parse_args()

    if not any([args.scraper, args.detector, args.all]):
        parser.print_help()
        return

    if args.dry_run:
        print("DRY RUN - No data will be written")
        return

    # Check required env vars
    if not DB_CONFIG["user"] or not DB_CONFIG["password"]:
        print("Error: Set SADIE_DB_USER and SADIE_DB_PASSWORD environment variables")
        print("Example:")
        print("  export SADIE_DB_USER=your_user")
        print("  export SADIE_DB_PASSWORD=your_password")
        return

    print(f"Connecting to {DB_CONFIG['host']}...")
    conn = get_connection()

    try:
        if args.all:
            total_hotels, total_leads = ingest_all(
                conn,
                Path("scraper_output"),
                Path("detector_output")
            )
            print(f"\n=== COMPLETE ===")
            print(f"Total hotels ingested: {total_hotels:,}")
            print(f"Total leads ingested: {total_leads:,}")

        elif args.scraper:
            if args.scraper.is_dir():
                total = ingest_scraper_directory(conn, args.scraper)
            else:
                total = ingest_scraper_file(conn, args.scraper)
            print(f"\nTotal hotels ingested: {total:,}")

        elif args.detector:
            if args.detector.is_dir():
                total = ingest_detector_directory(conn, args.detector)
            else:
                total = ingest_detector_file(conn, args.detector)
            print(f"\nTotal leads ingested: {total:,}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
