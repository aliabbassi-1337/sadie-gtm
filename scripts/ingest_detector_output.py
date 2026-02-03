#!/usr/bin/env python3
"""
Ingest detector output CSVs into the database with deduplication.

Usage:
    uv run python scripts/ingest_detector_output.py /path/to/detector_output/maryland
    uv run python scripts/ingest_detector_output.py /path/to/detector_output/maryland --dry-run
    uv run python scripts/ingest_detector_output.py /path/to/detector_output/maryland --state-filter MD
"""

import asyncio
import csv
import re
import sys
from pathlib import Path
from typing import Optional
import argparse

import asyncpg
from loguru import logger

# Booking engine name to ID mapping (from booking_engines table)
BOOKING_ENGINE_IDS = {
    "triptease": 1,
    "synxis": 2,
    "synxis / travelclick": 2,
    "travelclick": 2,
    "cloudbeds": 3,
    "mews": 4,
    "siteminder": 14,
    "thinkreservations": 24,
    "reseze": 29,
    "revinate": 95,
    "fareharbor": 33,
    "iqwebbook": 26,
    "innroad": 9,
    "rezstream": 28,
    "newbook": 11,
    "asi web reservations": 25,
    "jehs / ipms": 22,
    "vertical booking": 78,
    "windsurfer crs": 23,
}

DB_URL = "postgresql://postgres.yunairadgmaqesxejqap:SadieGTM321-@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"


def parse_address(address: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse address like '1317 Beach Ave, Cape May, NJ 08204' into components."""
    if not address:
        return None, None, None, None
    
    # Try to match: Street, City, ST ZIP
    match = re.match(r'^(.+?),\s*([^,]+),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)$', address.strip())
    if match:
        street, city, state, zip_code = match.groups()
        return street.strip(), city.strip(), state.strip(), zip_code.strip()
    
    # Try without zip: Street, City, ST
    match = re.match(r'^(.+?),\s*([^,]+),\s*([A-Z]{2})$', address.strip())
    if match:
        street, city, state = match.groups()
        return street.strip(), city.strip(), state.strip(), None
    
    return address, None, None, None


def get_booking_engine_id(engine_name: str) -> Optional[int]:
    """Get booking engine ID from name."""
    if not engine_name:
        return None
    name_lower = engine_name.lower().strip()
    return BOOKING_ENGINE_IDS.get(name_lower)


def clean_phone(phone: str) -> Optional[str]:
    """Clean phone number to digits only, format as XXX-XXX-XXXX."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return phone if phone else None


def normalize_name(name: str) -> str:
    """Normalize hotel name for comparison."""
    if not name:
        return ""
    # Remove common prefixes/suffixes, lowercase, strip whitespace
    normalized = name.lower().strip()
    # Remove "the " prefix
    if normalized.startswith("the "):
        normalized = normalized[4:]
    # Remove punctuation
    normalized = re.sub(r'[^\w\s]', '', normalized)
    # Collapse whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized


async def find_existing_hotel(conn: asyncpg.Connection, name: str, city: str, state: str, website: str) -> Optional[int]:
    """Find existing hotel by name+city+state or by website."""
    
    # First try exact match on name + city + state
    if name and city and state:
        existing = await conn.fetchrow("""
            SELECT id FROM sadie_gtm.hotels
            WHERE LOWER(name) = LOWER($1) AND LOWER(city) = LOWER($2) AND state = $3
        """, name, city, state)
        if existing:
            return existing['id']
    
    # Try matching by website domain
    if website:
        # Extract domain from website
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', website)
        if domain_match:
            domain = domain_match.group(1).lower()
            existing = await conn.fetchrow("""
                SELECT id FROM sadie_gtm.hotels
                WHERE website ILIKE $1
            """, f"%{domain}%")
            if existing:
                return existing['id']
    
    return None


async def ingest_csv(
    conn: asyncpg.Connection,
    csv_path: Path,
    source_name: str,
    state_filter: Optional[str] = None,
    dry_run: bool = False,
) -> tuple[int, int, int, int]:
    """Ingest a single CSV file.
    
    Returns: (processed, inserted, updated, skipped)
    """
    processed = inserted = updated = skipped = 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            processed += 1
            
            name = row.get('name', '').strip()
            if not name:
                skipped += 1
                continue
            
            # Parse address
            address_full = row.get('address', '').strip()
            street, city, state, zip_code = parse_address(address_full)
            
            # Apply state filter
            if state_filter and state != state_filter:
                logger.debug(f"Skipping {name} - state {state} != {state_filter}")
                skipped += 1
                continue
            
            # Get other fields
            website = row.get('website', '').strip() or None
            booking_url = row.get('booking_url', '').strip() or None
            booking_engine = row.get('booking_engine', '').strip() or None
            phone_google = clean_phone(row.get('phone_google', ''))
            phone_website = clean_phone(row.get('phone_website', ''))
            email = row.get('email', '').strip() or None
            
            try:
                lat = float(row.get('latitude', '')) if row.get('latitude') else None
            except ValueError:
                lat = None
            
            try:
                lon = float(row.get('longitude', '')) if row.get('longitude') else None
            except ValueError:
                lon = None
            
            try:
                rating = float(row.get('rating', '')) if row.get('rating') else None
            except ValueError:
                rating = None
            
            try:
                review_count = int(row.get('review_count', '')) if row.get('review_count') else None
            except ValueError:
                review_count = None
            
            try:
                room_count = int(row.get('room_count', '')) if row.get('room_count') else None
            except ValueError:
                room_count = None
            
            if dry_run:
                # Check if would be duplicate
                existing_id = await find_existing_hotel(conn, name, city, state, website)
                if existing_id:
                    logger.info(f"[DRY RUN] Would UPDATE: {name} | {city}, {state} | {booking_engine} (existing id={existing_id})")
                    updated += 1
                else:
                    logger.info(f"[DRY RUN] Would INSERT: {name} | {city}, {state} | {booking_engine}")
                    inserted += 1
                continue
            
            # Check for existing hotel
            hotel_id = await find_existing_hotel(conn, name, city, state, website)
            
            if hotel_id:
                # Update existing hotel with any missing data
                await conn.execute("""
                    UPDATE sadie_gtm.hotels SET
                        phone_google = COALESCE(phone_google, $2),
                        phone_website = COALESCE(phone_website, $3),
                        email = COALESCE(email, $4),
                        rating = COALESCE(rating, $5),
                        review_count = COALESCE(review_count, $6),
                        location = COALESCE(location, ST_SetSRID(ST_MakePoint($7, $8), 4326)),
                        updated_at = NOW()
                    WHERE id = $1
                """, hotel_id, phone_google, phone_website, email, rating, review_count, lon, lat)
                logger.debug(f"Updated hotel: {name} | {city}, {state} (id={hotel_id})")
                updated += 1
            else:
                # Generate external_id from website or name
                external_id = website or f"{name}_{city}_{state}"
                external_id_type = "detector_output"
                
                # Insert new hotel
                hotel_id = await conn.fetchval("""
                    INSERT INTO sadie_gtm.hotels (
                        external_id, external_id_type, name, address, city, state,
                        country, location, phone_google, phone_website, email, website,
                        rating, review_count, source, status
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7, ST_SetSRID(ST_MakePoint($8, $9), 4326), $10, $11, $12, $13,
                        $14, $15, $16, $17
                    )
                    RETURNING id
                """,
                    external_id, external_id_type, name, street, city, state,
                    "USA", lon, lat, phone_google, phone_website, email, website,
                    rating, review_count, source_name, 1
                )
                logger.info(f"Inserted hotel: {name} | {city}, {state} (id={hotel_id})")
                inserted += 1
            
            # Insert booking engine if detected (table allows only one per hotel)
            if booking_url and booking_engine and hotel_id:
                engine_id = get_booking_engine_id(booking_engine)
                if engine_id:
                    # Check if already exists for this hotel
                    existing_be = await conn.fetchrow("""
                        SELECT hotel_id, booking_engine_id FROM sadie_gtm.hotel_booking_engines
                        WHERE hotel_id = $1
                    """, hotel_id)
                    
                    if not existing_be:
                        await conn.execute("""
                            INSERT INTO sadie_gtm.hotel_booking_engines (
                                hotel_id, booking_engine_id, booking_url, detection_method
                            ) VALUES ($1, $2, $3, $4)
                        """, hotel_id, engine_id, booking_url, "detector_import")
                        logger.debug(f"  -> Added booking engine: {booking_engine}")
                    else:
                        logger.debug(f"  -> Hotel already has booking engine (id={existing_be['booking_engine_id']}), skipping")
                else:
                    logger.warning(f"  -> Unknown booking engine: {booking_engine}")
            
            # Insert room count if available
            if room_count and hotel_id:
                existing_rc = await conn.fetchrow("""
                    SELECT hotel_id FROM sadie_gtm.hotel_room_count WHERE hotel_id = $1
                """, hotel_id)
                
                if not existing_rc:
                    await conn.execute("""
                        INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, status)
                        VALUES ($1, $2, $3, 1)
                    """, hotel_id, room_count, "detector_output")
                    logger.debug(f"  -> Added room count: {room_count}")
    
    return processed, inserted, updated, skipped


async def main():
    parser = argparse.ArgumentParser(description="Ingest detector output CSVs")
    parser.add_argument("directory", type=Path, help="Directory containing *_leads.csv files")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually insert, just show what would be done")
    parser.add_argument("--state-filter", type=str, help="Only ingest hotels in this state (e.g., MD)")
    parser.add_argument("--source", type=str, default="detector_output", help="Source name for the data")
    args = parser.parse_args()
    
    if not args.directory.exists():
        logger.error(f"Directory not found: {args.directory}")
        sys.exit(1)
    
    csv_files = list(args.directory.glob("*_leads.csv"))
    if not csv_files:
        logger.error(f"No *_leads.csv files found in {args.directory}")
        sys.exit(1)
    
    logger.info(f"Found {len(csv_files)} CSV files in {args.directory}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - no changes will be made")
    
    conn = await asyncpg.connect(DB_URL, ssl='require', statement_cache_size=0)
    
    total_processed = total_inserted = total_updated = total_skipped = 0
    
    try:
        for csv_file in sorted(csv_files):
            logger.info(f"Processing: {csv_file.name}")
            source_name = f"{args.source}_{csv_file.stem}"
            
            processed, inserted, updated, skipped = await ingest_csv(
                conn, csv_file, source_name, args.state_filter, args.dry_run
            )
            
            total_processed += processed
            total_inserted += inserted
            total_updated += updated
            total_skipped += skipped
            
            logger.info(f"  -> {processed} rows: {inserted} new, {updated} updated, {skipped} skipped")
    
    finally:
        await conn.close()
    
    logger.info(f"\n=== TOTALS ===")
    logger.info(f"Processed: {total_processed}")
    logger.info(f"New:       {total_inserted}")
    logger.info(f"Updated:   {total_updated}")
    logger.info(f"Skipped:   {total_skipped}")


if __name__ == "__main__":
    asyncio.run(main())
