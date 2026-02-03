"""
Workflow: Backfill Missing Location Data
=========================================
Fills in missing city/state for hotels using multiple strategies:

1. Reverse geocoding - for hotels WITH coordinates but missing city/state
2. Forward geocoding - for hotels WITHOUT coordinates (uses name + partial address)
3. State normalization - ensures all state names are full names (not abbreviations)

USAGE:
    # Check status
    uv run python workflows/backfill_locations.py status

    # Reverse geocode hotels with coordinates (1 req/sec rate limit)
    uv run python workflows/backfill_locations.py reverse --limit 100

    # Forward geocode hotels by name (requires Google Places API)
    uv run python workflows/backfill_locations.py forward --limit 100

    # Normalize state names
    uv run python workflows/backfill_locations.py normalize
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from typing import Optional
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.leadgen.geocoding import reverse_geocode


# US state abbreviations to full names
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
}

VALID_STATES = set(US_STATES.values())


async def show_status():
    """Show current location data status."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            logger.info("=" * 60)
            logger.info("LOCATION DATA STATUS")
            logger.info("=" * 60)
            
            # USA hotels overview
            total = await conn.fetchval('''
                SELECT COUNT(*) FROM sadie_gtm.hotels 
                WHERE status != -1 AND country = 'United States'
            ''')
            logger.info(f"Total USA hotels: {total:,}")
            
            # With booking engine (leads)
            leads = await conn.fetchval('''
                SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                WHERE h.status != -1 AND h.country = 'United States'
            ''')
            logger.info(f"USA leads (with booking engine): {leads:,}")
            
            # Missing state
            missing_state = await conn.fetchval('''
                SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                WHERE h.status != -1 AND h.country = 'United States' AND h.state IS NULL
            ''')
            logger.info(f"USA leads missing state: {missing_state:,}")
            
            # Can fix with reverse geocoding
            can_reverse = await conn.fetchval('''
                SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                WHERE h.status != -1 AND h.country = 'United States' 
                AND h.state IS NULL AND h.location IS NOT NULL
            ''')
            logger.info(f"  - Can fix (have coords): {can_reverse:,}")
            
            # Need forward geocoding
            need_forward = await conn.fetchval('''
                SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                WHERE h.status != -1 AND h.country = 'United States' 
                AND h.state IS NULL AND h.location IS NULL
            ''')
            logger.info(f"  - Need forward geocoding: {need_forward:,}")
            
            # State normalization needed
            abbrev_count = await conn.fetchval('''
                SELECT COUNT(*) FROM sadie_gtm.hotels 
                WHERE status != -1 AND country = 'United States'
                AND state IS NOT NULL AND LENGTH(state) = 2
            ''')
            logger.info(f"States needing normalization: {abbrev_count:,}")
            
            logger.info("=" * 60)
            
    finally:
        await close_db()


async def reverse_geocode_batch(limit: int = 100):
    """Reverse geocode hotels that have coordinates but missing city/state."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            # Get hotels with coords but missing state
            hotels = await conn.fetch('''
                SELECT h.id, h.name,
                       ST_Y(h.location::geometry) as lat,
                       ST_X(h.location::geometry) as lng
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                WHERE h.status != -1 
                  AND h.country = 'United States'
                  AND h.state IS NULL 
                  AND h.location IS NOT NULL
                LIMIT $1
            ''', limit)
            
            if not hotels:
                logger.info("No hotels pending reverse geocoding")
                return
            
            logger.info(f"Reverse geocoding {len(hotels)} hotels...")
            
            success = 0
            failed = 0
            
            for hotel in hotels:
                hotel_id = hotel['id']
                name = hotel['name']
                lat = hotel['lat']
                lng = hotel['lng']
                
                # Rate limit: 1 req/sec for Nominatim
                await asyncio.sleep(1.1)
                
                result = await reverse_geocode(lat, lng)
                
                if result and result.state:
                    # Normalize state to full name
                    state = US_STATES.get(result.state, result.state)
                    
                    await conn.execute('''
                        UPDATE sadie_gtm.hotels
                        SET city = COALESCE($2, city),
                            state = $3,
                            address = COALESCE($4, address),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = $1
                    ''', hotel_id, result.city, state, result.address)
                    
                    logger.info(f"  {name[:40]:40} -> {result.city}, {state}")
                    success += 1
                else:
                    logger.warning(f"  {name[:40]:40} -> FAILED")
                    failed += 1
            
            logger.info("=" * 60)
            logger.info(f"Reverse geocoding complete: {success} success, {failed} failed")
            
    finally:
        await close_db()


async def normalize_states():
    """Normalize state abbreviations to full names."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            # Get all unique state values that need normalization
            rows = await conn.fetch('''
                SELECT DISTINCT state FROM sadie_gtm.hotels 
                WHERE status != -1 AND country = 'United States'
                AND state IS NOT NULL
            ''')
            
            total_fixed = 0
            
            for row in rows:
                state = row['state']
                
                # Skip if already a valid full name
                if state in VALID_STATES:
                    continue
                
                # Check if it's an abbreviation
                if state.upper() in US_STATES:
                    full_name = US_STATES[state.upper()]
                    
                    result = await conn.execute('''
                        UPDATE sadie_gtm.hotels
                        SET state = $2, updated_at = CURRENT_TIMESTAMP
                        WHERE status != -1 AND country = 'United States' AND state = $1
                    ''', state, full_name)
                    
                    count = int(result.split()[-1]) if result else 0
                    if count > 0:
                        logger.info(f"  {state} -> {full_name} ({count} hotels)")
                        total_fixed += count
            
            logger.info("=" * 60)
            logger.info(f"State normalization complete: {total_fixed} hotels updated")
            
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Backfill missing location data")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Status command
    subparsers.add_parser("status", help="Show location data status")
    
    # Reverse geocode command
    reverse_parser = subparsers.add_parser("reverse", help="Reverse geocode hotels with coordinates")
    reverse_parser.add_argument("--limit", "-l", type=int, default=100, help="Max hotels to process")
    
    # Forward geocode command (placeholder)
    forward_parser = subparsers.add_parser("forward", help="Forward geocode hotels by name (requires API)")
    forward_parser.add_argument("--limit", "-l", type=int, default=100, help="Max hotels to process")
    
    # Normalize command
    subparsers.add_parser("normalize", help="Normalize state abbreviations to full names")
    
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.command == "status":
        asyncio.run(show_status())
    elif args.command == "reverse":
        asyncio.run(reverse_geocode_batch(limit=args.limit))
    elif args.command == "forward":
        logger.error("Forward geocoding not yet implemented - requires Google Places API")
    elif args.command == "normalize":
        asyncio.run(normalize_states())


if __name__ == "__main__":
    main()
