"""Enrich Texas Cloudbeds leads with room count and customer proximity.

This workflow targets Cloudbeds (booking_engine_id=3) hotels in Texas that need:
1. Location data fixes (state inference from city/address)
2. Room count enrichment (via Cloudbeds API, website scraping, or LLM)
3. Customer proximity calculation

USAGE:
    # Check status
    uv run python workflows/enrich_texas_cloudbeds.py status

    # Fix location data (state inference)
    uv run python workflows/enrich_texas_cloudbeds.py fix-locations

    # Enrich room counts via Cloudbeds API (most accurate, requires Brightdata proxy)
    uv run python workflows/enrich_texas_cloudbeds.py room-counts-api --limit 100

    # Enrich room counts via website scraping + LLM fallback
    uv run python workflows/enrich_texas_cloudbeds.py room-counts --limit 50

    # Calculate customer proximity
    uv run python workflows/enrich_texas_cloudbeds.py proximity --limit 50
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import re
from loguru import logger
from typing import List, Dict, Any, Optional, Tuple

import httpx
from dotenv import load_dotenv

from db.client import init_db, close_db, get_conn
from services.enrichment.service import Service

load_dotenv()


CLOUDBEDS_ENGINE_ID = 3

# Texas cities for state inference
TEXAS_CITIES = {c.lower() for c in [
    # Major cities
    'Houston', 'Dallas', 'San Antonio', 'Austin', 'Fort Worth', 'El Paso',
    'Arlington', 'Corpus Christi', 'Plano', 'Laredo', 'Lubbock', 'Garland',
    'Irving', 'Amarillo', 'Grand Prairie', 'Brownsville', 'McKinney', 'Frisco',
    'Pasadena', 'Mesquite', 'Killeen', 'McAllen', 'Waco', 'Denton', 'Carrollton',
    'Midland', 'Abilene', 'Beaumont', 'Round Rock', 'Odessa', 'Pearland',
    'Richardson', 'College Station', 'League City', 'Lewisville', 'Tyler',
    'San Marcos', 'Sugar Land', 'The Woodlands', 'Edinburg', 'Mission',
    'Conroe', 'New Braunfels', 'Allen', 'Flower Mound', 'Longview', 'Temple',
    'Pharr', 'Bryan', 'Galveston', 'Baytown', 'Mansfield', 'Cedar Park',
    'Georgetown', 'Pflugerville', 'Victoria', 'San Angelo', 'Rockwall',
    'Fredericksburg', 'Boerne', 'Kerrville', 'Dripping Springs', 'Wimberley',
    'Marble Falls', 'Bandera', 'Port Aransas', 'South Padre Island', 'Rockport',
    # Smaller towns common in hotel data
    'Concan', 'Canyon Lake', 'Jefferson', 'Surfside Beach', 'Burnet', 'Terlingua',
    'Kingsland', 'Leakey', 'Alpine', 'Johnson City', 'Spring', 'Montgomery',
    'Glen Rose', 'Hemphill', 'Seguin', 'Crystal Beach', 'Brookeland', 'Buchanan Dam',
    'Marfa', 'Rio Frio', 'Round Top', 'Ingram', 'Llano', 'Jamaica Beach', 'Winnie',
    'San Saba', 'Big Spring', 'Freeport', 'Matagorda', 'Port Isabel', 'Blanco',
    'Comfort', 'Hunt', 'Medina', 'Pipe Creek', 'Utopia', 'Vanderpool', 'Camp Wood',
    'Granbury', 'Mineral Wells', 'Graham', 'Jacksboro', 'Decatur', 'Gainesville',
    'Sherman', 'Denison', 'Paris', 'Greenville', 'Sulphur Springs', 'Mount Pleasant',
    'Marshall', 'Carthage', 'Henderson', 'Kilgore', 'Gladewater', 'Mineola',
    'Canton', 'Athens', 'Mabank', 'Gun Barrel City', 'Crockett', 'Huntsville',
    'Livingston', 'Jasper', 'Woodville', 'Silsbee', 'Orange', 'Port Arthur',
]}


async def get_texas_cloudbeds_stats(conn) -> Dict[str, int]:
    """Get statistics for Texas Cloudbeds leads."""
    result = await conn.fetch('''
        SELECT 
            COUNT(*) as total,
            COUNT(hrc.room_count) as has_rooms,
            COUNT(hcp.distance_km) as has_proximity
        FROM sadie_gtm.hotel_booking_engines hbe
        JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
        LEFT JOIN sadie_gtm.hotel_room_count hrc ON hrc.hotel_id = hbe.hotel_id
        LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON hcp.hotel_id = hbe.hotel_id
        WHERE hbe.booking_engine_id = $1
          AND hbe.status = 1
          AND h.status = 1
          AND h.email IS NOT NULL AND h.email != ''
          AND h.country = 'United States'
          AND h.state = 'Texas'
    ''', CLOUDBEDS_ENGINE_ID)
    
    row = result[0]
    return {
        'total': row['total'],
        'has_rooms': row['has_rooms'],
        'need_rooms': row['total'] - row['has_rooms'],
        'has_proximity': row['has_proximity'],
        'need_proximity': row['total'] - row['has_proximity'],
    }


async def get_hotels_needing_state_fix(conn) -> List[Dict[str, Any]]:
    """Find US hotels with Texas cities but empty state."""
    results = await conn.fetch('''
        SELECT h.id, h.name, h.city, h.address
        FROM sadie_gtm.hotels h
        WHERE (h.state IS NULL OR h.state = '')
          AND h.city IS NOT NULL AND h.city != ''
          AND h.country IN ('United States', 'USA')
    ''')
    
    texas_hotels = []
    for r in results:
        city = (r['city'] or '').strip().lower()
        address = (r['address'] or '').lower()
        
        is_texas = False
        if city in TEXAS_CITIES:
            is_texas = True
        elif ', tx' in address or ' tx ' in address or address.endswith(' tx'):
            is_texas = True
        elif 'texas' in address:
            is_texas = True
        
        if is_texas:
            texas_hotels.append(dict(r))
    
    return texas_hotels


async def fix_texas_locations(conn) -> int:
    """Fix hotels with Texas cities but empty state."""
    hotels = await get_hotels_needing_state_fix(conn)
    
    if not hotels:
        return 0
    
    hotel_ids = [h['id'] for h in hotels]
    await conn.execute('''
        UPDATE sadie_gtm.hotels
        SET state = 'Texas', updated_at = NOW()
        WHERE id = ANY($1)
    ''', hotel_ids)
    
    return len(hotels)


async def get_texas_cloudbeds_needing_rooms(conn, limit: int) -> List[Dict[str, Any]]:
    """Get Texas Cloudbeds hotels that need room count enrichment."""
    return await conn.fetch('''
        SELECT h.id, h.name, h.website, hbe.booking_url
        FROM sadie_gtm.hotel_booking_engines hbe
        JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
        LEFT JOIN sadie_gtm.hotel_room_count hrc ON hrc.hotel_id = hbe.hotel_id
        WHERE hbe.booking_engine_id = $1
          AND hbe.status = 1
          AND h.status = 1
          AND h.state = 'Texas'
          AND hrc.hotel_id IS NULL
        LIMIT $2
    ''', CLOUDBEDS_ENGINE_ID, limit)


def _get_brightdata_proxy() -> Optional[str]:
    """Get Brightdata datacenter proxy URL."""
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
    dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
    if customer_id and dc_zone and dc_password:
        username = f"brd-customer-{customer_id}-zone-{dc_zone}"
        return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
    return None


def _extract_property_code(url: str) -> Optional[str]:
    """Extract property code from Cloudbeds booking URL."""
    match = re.search(r'/(?:reservation|booking)/([a-zA-Z0-9]+)', url)
    return match.group(1) if match else None


async def _get_cloudbeds_room_count(
    client: httpx.AsyncClient, 
    booking_url: str
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """Get room count from Cloudbeds API.
    
    Returns (room_count, email, phone).
    """
    property_code = _extract_property_code(booking_url)
    if not property_code:
        return None, None, None
    
    try:
        # Step 1: Get property_id from property_info API
        resp = await client.post(
            'https://hotels.cloudbeds.com/booking/property_info',
            data={
                'booking_engine_source': 'hosted',
                'iframe': 'false',
                'lang': 'en',
                'property_code': property_code,
            },
            timeout=15.0,
        )
        
        if resp.status_code != 200:
            return None, None, None
        
        data = resp.json()
        if not data.get('success'):
            return None, None, None
        
        props = data.get('data', {})
        property_id = props.get('property_id')
        email = props.get('hotel_email')
        phone = props.get('hotel_phone')
        
        if not property_id:
            return None, email, phone
        
        # Step 2: Get rooms from rooms API
        rooms_resp = await client.post(
            'https://hotels.cloudbeds.com/booking/rooms',
            data={
                'checkin': '1970-01-01',
                'checkout': '1970-01-02',
                'widget_property': property_id,
            },
            timeout=15.0,
        )
        
        if rooms_resp.status_code != 200:
            return None, email, phone
        
        rooms_data = rooms_resp.json()
        
        # Sum up max_rooms from all accommodation types
        total_rooms = sum(
            int(rt.get('max_rooms', 0) or 0) 
            for rt in rooms_data.get('accomodation_types', [])
        )
        
        return total_rooms if total_rooms > 0 else None, email, phone
        
    except Exception as e:
        logger.debug(f"Cloudbeds API error for {property_code}: {e}")
        return None, None, None


async def get_texas_cloudbeds_needing_proximity(conn, limit: int) -> List[Dict[str, Any]]:
    """Get Texas Cloudbeds hotels that need proximity calculation."""
    return await conn.fetch('''
        SELECT h.id, h.name, h.latitude, h.longitude
        FROM sadie_gtm.hotel_booking_engines hbe
        JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
        LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON hcp.hotel_id = hbe.hotel_id
        WHERE hbe.booking_engine_id = $1
          AND hbe.status = 1
          AND h.status = 1
          AND h.email IS NOT NULL AND h.email != ''
          AND h.country = 'United States'
          AND h.state = 'Texas'
          AND h.latitude IS NOT NULL
          AND h.longitude IS NOT NULL
          AND hcp.hotel_id IS NULL
        LIMIT $2
    ''', CLOUDBEDS_ENGINE_ID, limit)


async def show_status() -> None:
    """Show enrichment status for Texas Cloudbeds leads."""
    await init_db()
    try:
        async with get_conn() as conn:
            stats = await get_texas_cloudbeds_stats(conn)
            
            # Check for location fixes needed
            location_fixes = await get_hotels_needing_state_fix(conn)
            
            logger.info("=" * 60)
            logger.info("TEXAS CLOUDBEDS ENRICHMENT STATUS")
            logger.info("=" * 60)
            logger.info(f"Total Texas Cloudbeds leads: {stats['total']}")
            logger.info("")
            logger.info("Room Count:")
            logger.info(f"  Has room count:  {stats['has_rooms']}")
            logger.info(f"  Needs enrichment: {stats['need_rooms']}")
            logger.info("")
            logger.info("Customer Proximity:")
            logger.info(f"  Has proximity:   {stats['has_proximity']}")
            logger.info(f"  Needs enrichment: {stats['need_proximity']}")
            logger.info("")
            logger.info("Location Fixes:")
            logger.info(f"  US hotels with Texas city but empty state: {len(location_fixes)}")
            logger.info("=" * 60)
    finally:
        await close_db()


async def run_fix_locations() -> None:
    """Fix location data for Texas hotels."""
    await init_db()
    try:
        async with get_conn() as conn:
            count = await fix_texas_locations(conn)
            
            logger.info("=" * 60)
            logger.info("LOCATION FIX COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Hotels fixed (state set to Texas): {count}")
            logger.info("=" * 60)
    finally:
        await close_db()


async def run_room_counts_api(limit: int) -> None:
    """Run room count enrichment via Cloudbeds API (most accurate method).
    
    Uses the Cloudbeds /booking/rooms API to get exact room counts.
    Requires Brightdata proxy to avoid rate limiting.
    
    Room count source: 'cloudbeds_api'
    """
    proxy = _get_brightdata_proxy()
    if not proxy:
        logger.error("Brightdata proxy not configured. Set BRIGHTDATA_CUSTOMER_ID, BRIGHTDATA_DC_ZONE, BRIGHTDATA_DC_PASSWORD")
        return
    
    await init_db()
    try:
        async with get_conn() as conn:
            hotels = await get_texas_cloudbeds_needing_rooms(conn, limit)
        
        if not hotels:
            logger.info("No Texas Cloudbeds hotels need room count enrichment")
            return
        
        logger.info(f"Enriching room counts for {len(hotels)} hotels via Cloudbeds API...")
        
        client_kwargs = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://hotels.cloudbeds.com',
            },
            'timeout': 30.0,
            'proxy': proxy,
        }
        
        enriched = 0
        async with get_conn() as conn:
            async with httpx.AsyncClient(**client_kwargs) as client:
                for i, h in enumerate(hotels):
                    room_count, email, phone = await _get_cloudbeds_room_count(
                        client, h['booking_url']
                    )
                    
                    if room_count:
                        await conn.execute('''
                            INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, status)
                            VALUES ($1, $2, 'cloudbeds_api', 1)
                            ON CONFLICT (hotel_id) DO UPDATE SET room_count = $2, source = 'cloudbeds_api'
                        ''', h['id'], room_count)
                        enriched += 1
                        logger.info(f"[{i+1}/{len(hotels)}] {h['name'][:30]} | {room_count} rooms")
                    else:
                        logger.debug(f"[{i+1}/{len(hotels)}] {h['name'][:30]} | no data")
                    
                    # Rate limit
                    await asyncio.sleep(0.3)
        
        logger.info("=" * 60)
        logger.info("ROOM COUNT ENRICHMENT COMPLETE (Cloudbeds API)")
        logger.info("=" * 60)
        logger.info(f"Hotels enriched: {enriched}/{len(hotels)}")
        logger.info("=" * 60)
    finally:
        await close_db()


async def run_room_counts(limit: int) -> None:
    """Run room count enrichment via website scraping + LLM fallback.
    
    Uses regex extraction from hotel websites first, then falls back to
    Groq LLM estimation if regex fails.
    
    Room count sources: 'regex' or 'groq'
    """
    await init_db()
    try:
        service = Service()
        
        async with get_conn() as conn:
            hotels = await conn.fetch('''
                SELECT h.id, h.name, h.website
                FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                LEFT JOIN sadie_gtm.hotel_room_count hrc ON hrc.hotel_id = hbe.hotel_id
                WHERE hbe.booking_engine_id = $1
                  AND hbe.status = 1
                  AND h.status = 1
                  AND h.state = 'Texas'
                  AND h.website IS NOT NULL AND h.website != ''
                  AND hrc.hotel_id IS NULL
                LIMIT $2
            ''', CLOUDBEDS_ENGINE_ID, limit)
        
        if not hotels:
            logger.info("No Texas Cloudbeds hotels need room count enrichment")
            return
        
        logger.info(f"Enriching room counts for {len(hotels)} Texas Cloudbeds hotels via website...")
        
        # Use the enrichment service
        count = await service.enrich_room_counts_for_hotels(
            hotel_ids=[h['id'] for h in hotels],
            concurrency=10,
        )
        
        logger.info("=" * 60)
        logger.info("ROOM COUNT ENRICHMENT COMPLETE (Website)")
        logger.info("=" * 60)
        logger.info(f"Hotels enriched: {count}")
        logger.info("=" * 60)
    finally:
        await close_db()


async def run_proximity(limit: int) -> None:
    """Run customer proximity calculation for Texas Cloudbeds hotels."""
    await init_db()
    try:
        service = Service()
        
        async with get_conn() as conn:
            hotels = await get_texas_cloudbeds_needing_proximity(conn, limit)
        
        if not hotels:
            logger.info("No Texas Cloudbeds hotels need proximity calculation")
            return
        
        logger.info(f"Calculating proximity for {len(hotels)} Texas Cloudbeds hotels...")
        
        count = await service.calculate_proximity_for_hotels(
            hotel_ids=[h['id'] for h in hotels],
            max_distance_km=100.0,
        )
        
        logger.info("=" * 60)
        logger.info("PROXIMITY CALCULATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Hotels processed: {count}")
        logger.info("=" * 60)
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Enrich Texas Cloudbeds leads"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Status command
    subparsers.add_parser("status", help="Show enrichment status")
    
    # Fix locations command
    subparsers.add_parser("fix-locations", help="Fix state for Texas hotels")
    
    # Room counts via Cloudbeds API (recommended)
    room_api_parser = subparsers.add_parser(
        "room-counts-api", 
        help="Enrich room counts via Cloudbeds API (most accurate)"
    )
    room_api_parser.add_argument("--limit", type=int, default=100, help="Max hotels to process")
    
    # Room counts via website scraping
    room_parser = subparsers.add_parser(
        "room-counts", 
        help="Enrich room counts via website scraping + LLM"
    )
    room_parser.add_argument("--limit", type=int, default=50, help="Max hotels to process")
    
    # Proximity command
    prox_parser = subparsers.add_parser("proximity", help="Calculate customer proximity")
    prox_parser.add_argument("--limit", type=int, default=50, help="Max hotels to process")
    
    args = parser.parse_args()
    
    if args.command == "status":
        asyncio.run(show_status())
    elif args.command == "fix-locations":
        asyncio.run(run_fix_locations())
    elif args.command == "room-counts-api":
        asyncio.run(run_room_counts_api(args.limit))
    elif args.command == "room-counts":
        asyncio.run(run_room_counts(args.limit))
    elif args.command == "proximity":
        asyncio.run(run_proximity(args.limit))


if __name__ == "__main__":
    main()
