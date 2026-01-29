"""Parse city/state from RMS Cloud hotel addresses.

RMS booking pages don't have location data, but some hotels have addresses
in the database from other sources. This workflow parses structured addresses
to extract city, state, and country.

Supported patterns:
- Australian: "Street, City STATE Postcode, Australia" 
  e.g., "40 Ragonesi Rd, Alice Springs NT 0870, Australia"
- US: "Street, City, STATE ZIP"
  e.g., "123 Main St, Austin, TX 78701"

Usage:
    # Check status
    uv run python -m workflows.parse_rms_addresses --status
    
    # Dry run
    uv run python -m workflows.parse_rms_addresses --dry-run --limit 20
    
    # Run parser
    uv run python -m workflows.parse_rms_addresses
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import re
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from loguru import logger

from db.client import init_db, close_db, get_conn


# Australian states
AU_STATES = {'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT'}

# US states
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
}

# Australian pattern: "City STATE Postcode , Australia" or "City STATE Postcode, Australia"
AU_PATTERN = re.compile(
    r',\s*([A-Za-z\s\-\']+)\s+(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)\s+(\d{4})\s*,?\s*Australia',
    re.IGNORECASE
)

# US pattern: "City, STATE ZIP" or "City STATE ZIP"
US_PATTERN = re.compile(
    r',\s*([A-Za-z\s\-\'\.]+)[,\s]+(' + '|'.join(US_STATES) + r')\s+(\d{5}(?:-\d{4})?)',
    re.IGNORECASE
)

# New Zealand pattern: "City Postcode, New Zealand"
NZ_PATTERN = re.compile(
    r',\s*([A-Za-z\s\-\']+)\s+(\d{4})\s*,?\s*New Zealand',
    re.IGNORECASE
)

# Singapore pattern: "Street, Postcode, Singapore" 
SG_PATTERN = re.compile(
    r',\s*(\d{6})\s*,?\s*Singapore',
    re.IGNORECASE
)


@dataclass
class ParseResult:
    """Result of parsing an address."""
    hotel_id: int
    success: bool
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    original_address: Optional[str] = None
    error: Optional[str] = None


def parse_address(address: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse address to extract city, state, country.
    
    Returns (city, state, country) or (None, None, None) if no match.
    """
    if not address:
        return None, None, None
    
    # Try Australian pattern
    match = AU_PATTERN.search(address)
    if match:
        city = match.group(1).strip()
        state = match.group(2).upper()
        return city, state, 'AU'
    
    # Try US pattern
    match = US_PATTERN.search(address)
    if match:
        city = match.group(1).strip().rstrip(',')
        state = match.group(2).upper()
        return city, state, 'USA'
    
    # Try NZ pattern
    match = NZ_PATTERN.search(address)
    if match:
        city = match.group(1).strip()
        return city, None, 'NZ'
    
    # Try Singapore pattern
    match = SG_PATTERN.search(address)
    if match:
        return 'Singapore', None, 'SG'
    
    return None, None, None


async def get_rms_hotels_with_parseable_addresses(limit: int = 1000) -> List[Dict[str, Any]]:
    """Get RMS hotels that have addresses but missing city/state."""
    async with get_conn() as conn:
        rows = await conn.fetch("""
            SELECT h.id, h.name, h.address, h.city, h.state, h.country
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            WHERE hbe.booking_engine_id = 12  -- RMS Cloud
            AND h.address IS NOT NULL AND h.address != ''
            AND (h.city IS NULL OR h.city = '' OR h.state IS NULL OR h.state = '')
            ORDER BY h.id
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]


async def get_rms_hotels_needing_parse_count() -> int:
    """Count RMS hotels with addresses but missing city/state."""
    async with get_conn() as conn:
        return await conn.fetchval("""
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            WHERE hbe.booking_engine_id = 12  -- RMS Cloud
            AND h.address IS NOT NULL AND h.address != ''
            AND (h.city IS NULL OR h.city = '' OR h.state IS NULL OR h.state = '')
        """) or 0


async def batch_update_parsed_locations(results: List[ParseResult]) -> int:
    """Batch update hotels with parsed location data."""
    successful = [r for r in results if r.success and (r.city or r.state)]
    
    if not successful:
        return 0
    
    hotel_ids = [r.hotel_id for r in successful]
    cities = [r.city for r in successful]
    states = [r.state for r in successful]
    countries = [r.country for r in successful]
    
    sql = """
    UPDATE sadie_gtm.hotels h
    SET 
        city = CASE WHEN v.city IS NOT NULL AND v.city != '' AND (h.city IS NULL OR h.city = '')
                    THEN v.city ELSE h.city END,
        state = CASE WHEN v.state IS NOT NULL AND v.state != '' AND (h.state IS NULL OR h.state = '')
                     THEN v.state ELSE h.state END,
        country = CASE WHEN v.country IS NOT NULL AND v.country != '' AND (h.country IS NULL OR h.country = '')
                       THEN v.country ELSE h.country END,
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT * FROM unnest(
            $1::integer[],
            $2::text[],
            $3::text[],
            $4::text[]
        ) AS t(hotel_id, city, state, country)
    ) v
    WHERE h.id = v.hotel_id
    """
    
    async with get_conn() as conn:
        result = await conn.execute(sql, hotel_ids, cities, states, countries)
        count = int(result.split()[-1]) if result else len(successful)
    
    return count


async def run_status():
    """Show status of RMS address parsing."""
    await init_db()
    
    try:
        count = await get_rms_hotels_needing_parse_count()
        
        # Get sample to estimate parseability
        hotels = await get_rms_hotels_with_parseable_addresses(limit=200)
        
        parseable = 0
        unparseable = 0
        for h in hotels:
            city, state, country = parse_address(h['address'])
            if city or state:
                parseable += 1
            else:
                unparseable += 1
        
        pct = 100 * parseable // len(hotels) if hotels else 0
        
        print("\n" + "=" * 60)
        print("RMS ADDRESS PARSING STATUS")
        print("=" * 60)
        print(f"  Hotels with address but missing city/state: {count:,}")
        print(f"  Sample analyzed: {len(hotels)}")
        print(f"  Parseable: {parseable} ({pct}%)")
        print(f"  Unparseable: {unparseable} ({100-pct}%)")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


async def run_dry_run(limit: int):
    """Show what would be parsed."""
    await init_db()
    
    try:
        hotels = await get_rms_hotels_with_parseable_addresses(limit=limit)
        
        parseable = []
        unparseable = []
        
        for h in hotels:
            city, state, country = parse_address(h['address'])
            result = ParseResult(
                hotel_id=h['id'],
                success=bool(city or state),
                city=city,
                state=state,
                country=country,
                original_address=h['address'],
            )
            if result.success:
                parseable.append(result)
            else:
                unparseable.append(result)
        
        print(f"\n=== DRY RUN: {len(hotels)} hotels analyzed ===\n")
        
        print(f"PARSEABLE ({len(parseable)}):\n")
        for r in parseable[:15]:
            print(f"  ID={r.hotel_id}")
            print(f"    Address: {r.original_address[:70]}...")
            print(f"    → City: {r.city} | State: {r.state} | Country: {r.country}")
            print()
        if len(parseable) > 15:
            print(f"  ... and {len(parseable) - 15} more\n")
        
        print(f"UNPARSEABLE ({len(unparseable)}):\n")
        for r in unparseable[:10]:
            print(f"  ID={r.hotel_id}")
            print(f"    Address: {r.original_address[:70]}...")
            print()
        if len(unparseable) > 10:
            print(f"  ... and {len(unparseable) - 10} more\n")
            
    finally:
        await close_db()


async def run_parse():
    """Run the address parsing workflow."""
    await init_db()
    
    try:
        hotels = await get_rms_hotels_with_parseable_addresses(limit=10000)
        
        if not hotels:
            logger.info("No RMS hotels need address parsing")
            return
        
        logger.info(f"Found {len(hotels)} RMS hotels to parse")
        
        results = []
        parsed_count = 0
        
        for h in hotels:
            city, state, country = parse_address(h['address'])
            
            result = ParseResult(
                hotel_id=h['id'],
                success=bool(city or state),
                city=city,
                state=state,
                country=country,
                original_address=h['address'],
            )
            results.append(result)
            
            if result.success:
                parsed_count += 1
                logger.info(f"  Hotel {h['id']}: {city}, {state}, {country}")
        
        # Batch update
        updated = await batch_update_parsed_locations(results)
        
        print("\n" + "=" * 60)
        print("RMS ADDRESS PARSING COMPLETE")
        print("=" * 60)
        print(f"  Analyzed:   {len(hotels)}")
        print(f"  Parseable:  {parsed_count}")
        print(f"  Updated:    {updated}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Parse RMS hotel addresses")
    parser.add_argument("--status", action="store_true", help="Show status")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be parsed")
    parser.add_argument("--limit", type=int, default=200, help="Limit for dry run")
    
    args = parser.parse_args()
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit))
    else:
        asyncio.run(run_parse())


if __name__ == "__main__":
    main()
