"""Unified enrichment workflow for Cloudbeds, RMS, and SiteMinder hotels.

Enriches hotels with:
1. Location data (address, city, state, country, lat/lng)
2. Contact info (email, phone, website)
3. Room count (via booking engine APIs)
4. Customer proximity (distance to nearest existing customer)

USAGE:
    # Show status for all booking engines
    uv run python workflows/enrich_booking_engines.py status
    
    # Enrich Cloudbeds hotels
    uv run python workflows/enrich_booking_engines.py cloudbeds --limit 100
    
    # Enrich RMS hotels
    uv run python workflows/enrich_booking_engines.py rms --limit 100
    
    # Enrich SiteMinder hotels
    uv run python workflows/enrich_booking_engines.py siteminder --limit 100
    
    # Calculate proximity for all hotels with location
    uv run python workflows/enrich_booking_engines.py proximity --limit 500
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import re
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal

import httpx
from loguru import logger
from dotenv import load_dotenv

from db.client import init_db, close_db, get_conn

load_dotenv()


# Booking engine IDs
CLOUDBEDS_ID = 3
RMS_ID = 12
SITEMINDER_ID = 14
IPMS247_ID = 22


def _get_brightdata_proxy() -> Optional[str]:
    """Get Brightdata datacenter proxy URL."""
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
    dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
    if customer_id and dc_zone and dc_password:
        username = f"brd-customer-{customer_id}-zone-{dc_zone}"
        return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
    return None


# =============================================================================
# STATUS
# =============================================================================


async def show_status() -> None:
    """Show enrichment status for all booking engines."""
    await init_db()
    try:
        async with get_conn() as conn:
            # Get stats for each booking engine
            stats = await conn.fetch('''
                SELECT 
                    be.id,
                    be.name,
                    COUNT(DISTINCT h.id) as total,
                    COUNT(DISTINCT CASE WHEN h.email IS NOT NULL AND h.email != '' THEN h.id END) as has_email,
                    COUNT(DISTINCT CASE WHEN h.location IS NOT NULL THEN h.id END) as has_location,
                    COUNT(DISTINCT CASE WHEN hrc.room_count IS NOT NULL THEN h.id END) as has_rooms,
                    COUNT(DISTINCT CASE WHEN hcp.distance_km IS NOT NULL THEN h.id END) as has_proximity
                FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
                LEFT JOIN sadie_gtm.hotel_room_count hrc ON hrc.hotel_id = h.id
                LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON hcp.hotel_id = h.id
                WHERE hbe.status = 1
                  AND h.status != 99  -- Exclude dead ends only
                  AND be.id IN ($1, $2, $3, $4)
                GROUP BY be.id, be.name
                ORDER BY total DESC
            ''', CLOUDBEDS_ID, RMS_ID, SITEMINDER_ID, IPMS247_ID)
            
            logger.info("=" * 70)
            logger.info("BOOKING ENGINE ENRICHMENT STATUS")
            logger.info("=" * 70)
            logger.info("")
            logger.info(f"{'Engine':<15} | {'Total':>8} | {'Email':>8} | {'Location':>8} | {'Rooms':>8} | {'Prox':>8}")
            logger.info("-" * 70)
            
            for s in stats:
                logger.info(
                    f"{s['name']:<15} | {s['total']:>8} | "
                    f"{s['has_email']:>8} | {s['has_location']:>8} | "
                    f"{s['has_rooms']:>8} | {s['has_proximity']:>8}"
                )
            
            logger.info("=" * 70)
    finally:
        await close_db()


# =============================================================================
# CLOUDBEDS ENRICHMENT
# =============================================================================


def _extract_cloudbeds_property_code(url: str) -> Optional[str]:
    """Extract property code from Cloudbeds booking URL."""
    match = re.search(r'/(?:reservation|booking)/([a-zA-Z0-9]+)', url)
    return match.group(1) if match else None


async def _enrich_cloudbeds_hotel(
    client: httpx.AsyncClient,
    hotel_id: int,
    booking_url: str,
) -> Dict[str, Any]:
    """Enrich a single Cloudbeds hotel via API.
    
    Returns dict with enriched data.
    """
    result = {
        'hotel_id': hotel_id,
        'success': False,
        'email': None,
        'phone': None,
        'lat': None,
        'lng': None,
        'room_count': None,
    }
    
    property_code = _extract_cloudbeds_property_code(booking_url)
    if not property_code:
        return result
    
    try:
        # Step 1: Get property_info (has lat/lng, email, phone)
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
            return result
        
        data = resp.json()
        if not data.get('success'):
            return result
        
        props = data.get('data', {})
        property_id = props.get('property_id')
        hotel_address = props.get('hotel_address', {})
        
        result['email'] = props.get('hotel_email') or None
        result['phone'] = props.get('hotel_phone') or None
        
        # Extract lat/lng
        if hotel_address.get('lat'):
            try:
                result['lat'] = float(hotel_address['lat'])
            except:
                pass
        if hotel_address.get('lng'):
            try:
                result['lng'] = float(hotel_address['lng'])
            except:
                pass
        
        result['success'] = True
        
        # Step 2: Get room count from /booking/rooms API
        if property_id:
            try:
                rooms_resp = await client.post(
                    'https://hotels.cloudbeds.com/booking/rooms',
                    data={
                        'checkin': '1970-01-01',
                        'checkout': '1970-01-02',
                        'widget_property': property_id,
                    },
                    timeout=15.0,
                )
                
                if rooms_resp.status_code == 200:
                    rooms_data = rooms_resp.json()
                    total_rooms = sum(
                        int(rt.get('max_rooms', 0) or 0)
                        for rt in rooms_data.get('accomodation_types', [])
                    )
                    if total_rooms > 0:
                        result['room_count'] = total_rooms
            except Exception as e:
                logger.debug(f"Rooms API error for {property_code}: {e}")
        
        return result
        
    except Exception as e:
        logger.debug(f"Cloudbeds API error for {property_code}: {e}")
        return result


async def enrich_cloudbeds(limit: int, concurrency: int = 20) -> None:
    """Enrich Cloudbeds hotels via API."""
    proxy = _get_brightdata_proxy()
    if not proxy:
        logger.warning("Brightdata proxy not configured, using direct connection")
    
    await init_db()
    try:
        async with get_conn() as conn:
            # Get hotels needing enrichment
            # Priority: location > email > room_count (need location for proximity)
            # Include all hotels with booking engine (status >= 0 or -1 crawled)
            hotels = await conn.fetch('''
                SELECT h.id, h.name, hbe.booking_url,
                       h.email as existing_email,
                       h.location IS NOT NULL as has_location,
                       hrc.hotel_id IS NOT NULL as has_room_count
                FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                LEFT JOIN sadie_gtm.hotel_room_count hrc ON hrc.hotel_id = h.id
                WHERE hbe.booking_engine_id = $1
                  AND hbe.status = 1
                  AND h.status != 99  -- Exclude dead ends only
                  AND (
                      h.location IS NULL
                      OR (h.email IS NULL OR h.email = '')
                      OR hrc.hotel_id IS NULL
                  )
                ORDER BY 
                    CASE WHEN h.location IS NULL THEN 0 ELSE 1 END,  -- Location first
                    h.id
                LIMIT $2
            ''', CLOUDBEDS_ID, limit)
        
        if not hotels:
            logger.info("No Cloudbeds hotels need enrichment")
            return
        
        logger.info(f"Enriching {len(hotels)} Cloudbeds hotels...")
        
        client_kwargs = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://hotels.cloudbeds.com',
            },
            'timeout': 30.0,
        }
        if proxy:
            client_kwargs['proxy'] = proxy
        
        semaphore = asyncio.Semaphore(concurrency)
        stats = {'location': 0, 'email': 0, 'room_count': 0, 'failed': 0}
        results_to_save = []
        
        async def process_hotel(h):
            async with semaphore:
                result = await _enrich_cloudbeds_hotel(client, h['id'], h['booking_url'])
                result['hotel'] = h  # Include original hotel data
                results_to_save.append(result)
                await asyncio.sleep(0.2)  # Rate limit
        
        async with httpx.AsyncClient(**client_kwargs) as client:
            await asyncio.gather(*[process_hotel(h) for h in hotels])
        
        # Now save all results in one connection
        async with get_conn() as conn:
            for result in results_to_save:
                h = result['hotel']
                
                if not result['success']:
                    stats['failed'] += 1
                    continue
                
                updates = []
                
                # Update location if missing
                if not h['has_location'] and result['lat'] and result['lng']:
                    await conn.execute('''
                        UPDATE sadie_gtm.hotels 
                        SET location = ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                            updated_at = NOW()
                        WHERE id = $3
                    ''', result['lng'], result['lat'], h['id'])
                    stats['location'] += 1
                    updates.append('loc')
                
                # Update email if missing
                if not h['existing_email'] and result['email']:
                    await conn.execute('''
                        UPDATE sadie_gtm.hotels SET email = $1, updated_at = NOW() WHERE id = $2
                    ''', result['email'], h['id'])
                    stats['email'] += 1
                    updates.append('email')
                
                # Update room count if missing
                if not h['has_room_count'] and result['room_count']:
                    await conn.execute('''
                        INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source)
                        VALUES ($1, $2, 'cloudbeds_api')
                        ON CONFLICT (hotel_id) DO UPDATE SET room_count = $2, source = 'cloudbeds_api'
                    ''', h['id'], result['room_count'])
                    stats['room_count'] += 1
                    updates.append(f'{result["room_count"]}rm')
                
                if updates:
                    logger.debug(f"{h['name'][:30]} | +{' +'.join(updates)}")
        
        logger.info("=" * 60)
        logger.info("CLOUDBEDS ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Locations added: {stats['location']}")
        logger.info(f"Emails added: {stats['email']}")
        logger.info(f"Room counts added: {stats['room_count']}")
        logger.info(f"Failed: {stats['failed']}")
        
    finally:
        await close_db()


# =============================================================================
# RMS ENRICHMENT
# =============================================================================


async def enrich_rms(limit: int, concurrency: int = 10) -> None:
    """Enrich RMS hotels via API."""
    from lib.rms.api_client import AdaptiveRMSApiClient
    
    await init_db()
    try:
        async with get_conn() as conn:
            # Get hotels needing enrichment
            # Priority: location > city > email (need location for proximity)
            # Include all hotels with booking engine (exclude dead ends only)
            hotels = await conn.fetch('''
                SELECT h.id, h.name, hbe.booking_url,
                       h.email as existing_email,
                       h.location IS NOT NULL as has_location,
                       hrc.hotel_id IS NOT NULL as has_room_count
                FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                LEFT JOIN sadie_gtm.hotel_room_count hrc ON hrc.hotel_id = h.id
                WHERE hbe.booking_engine_id = $1
                  AND hbe.status = 1
                  AND h.status != 99  -- Exclude dead ends only
                  AND (
                      h.location IS NULL
                      OR h.city IS NULL OR h.city = ''
                      OR (h.email IS NULL OR h.email = '')
                  )
                ORDER BY 
                    CASE WHEN h.location IS NULL THEN 0 ELSE 1 END,  -- Location first
                    h.id
                LIMIT $2
            ''', RMS_ID, limit)
        
        if not hotels:
            logger.info("No RMS hotels need enrichment")
            return
        
        logger.info(f"Enriching {len(hotels)} RMS hotels...")
        
        semaphore = asyncio.Semaphore(concurrency)
        stats = {'enriched': 0, 'failed': 0}
        results_to_save = []
        
        async with AdaptiveRMSApiClient() as api_client:
            async def process_hotel(h):
                async with semaphore:
                    url = h['booking_url']
                    if not url.startswith('http'):
                        url = f'https://{url}'
                    
                    # Parse clientId and server from URL
                    # Format 1: /Search/Index/{id}/90/ (bookings.rmscloud.com)
                    # Format 2: ibe12.rmscloud.com/{id} (IBE servers)
                    # Format 3: external website with /reservation/ path
                    
                    slug = None
                    server = 'bookings.rmscloud.com'
                    
                    # Try /Search/Index/ format first
                    match = re.search(r'/Search/Index/([^/]+)/\d+/?', url)
                    if match:
                        slug = match.group(1)
                        if 'bookings12' in url:
                            server = 'bookings12.rmscloud.com'
                        elif 'bookings10' in url:
                            server = 'bookings10.rmscloud.com'
                        elif 'bookings8' in url:
                            server = 'bookings8.rmscloud.com'
                    else:
                        # Try IBE format: ibe12.rmscloud.com/{numeric_id}
                        ibe_match = re.search(r'(ibe\d+\.rmscloud\.com)/(\d+)', url)
                        if ibe_match:
                            server = ibe_match.group(1)
                            slug = ibe_match.group(2)
                        else:
                            # External website - skip (can't extract from API)
                            results_to_save.append({'hotel': h, 'data': None})
                            return
                    
                    if not slug:
                        results_to_save.append({'hotel': h, 'data': None})
                        return
                    
                    data = await api_client.extract(slug, server)
                    
                    if not data or not data.has_data():
                        data = await api_client.extract_from_html(slug, server)
                    
                    results_to_save.append({
                        'hotel': h,
                        'data': data,
                    })
                    
                    await asyncio.sleep(0.3)  # Rate limit
            
            await asyncio.gather(*[process_hotel(h) for h in hotels])
        
        # Save all results in one connection
        async with get_conn() as conn:
            for r in results_to_save:
                h = r['hotel']
                data = r['data']
                
                if data and data.has_data():
                    # Update hotel data
                    await conn.execute('''
                        UPDATE sadie_gtm.hotels SET
                            name = COALESCE(NULLIF($2, ''), name),
                            address = COALESCE(NULLIF($3, ''), address),
                            city = COALESCE(NULLIF($4, ''), city),
                            state = COALESCE(NULLIF($5, ''), state),
                            country = COALESCE(NULLIF($6, ''), country),
                            email = COALESCE(NULLIF($7, ''), email),
                            phone_website = COALESCE(NULLIF($8, ''), phone_website),
                            website = COALESCE(NULLIF($9, ''), website),
                            updated_at = NOW()
                        WHERE id = $1
                    ''', h['id'], data.name, data.address, data.city,
                        data.state, data.country, data.email, data.phone, data.website)
                    
                    # Update location if available
                    if data.latitude and data.longitude:
                        await conn.execute('''
                            UPDATE sadie_gtm.hotels
                            SET location = ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                            WHERE id = $3 AND location IS NULL
                        ''', data.longitude, data.latitude, h['id'])
                    
                    stats['enriched'] += 1
                    logger.debug(f"{h['name'][:30]} | {data.city or ''}, {data.state or ''}")
                else:
                    stats['failed'] += 1
        
        logger.info("=" * 60)
        logger.info("RMS ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Enriched: {stats['enriched']}")
        logger.info(f"Failed: {stats['failed']}")
        
    finally:
        await close_db()


# =============================================================================
# SITEMINDER ENRICHMENT
# =============================================================================


async def enrich_siteminder(limit: int, concurrency: int = 20) -> None:
    """Enrich SiteMinder hotels via GraphQL API."""
    from lib.siteminder.api_client import SiteMinderClient
    
    await init_db()
    try:
        async with get_conn() as conn:
            # Get hotels needing enrichment
            hotels = await conn.fetch('''
                SELECT h.id, h.name, hbe.booking_url,
                       h.website as existing_website
                FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                WHERE hbe.booking_engine_id = $1
                  AND hbe.status = 1
                  AND h.status != 99  -- Exclude dead ends only
                  AND hbe.booking_url LIKE '%direct-book.com%'
                  AND (
                      h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%'
                      OR h.website IS NULL OR h.website = ''
                  )
                LIMIT $2
            ''', SITEMINDER_ID, limit)
        
        if not hotels:
            logger.info("No SiteMinder hotels need enrichment")
            return
        
        logger.info(f"Enriching {len(hotels)} SiteMinder hotels...")
        
        semaphore = asyncio.Semaphore(concurrency)
        stats = {'enriched': 0, 'failed': 0}
        results_to_save = []
        
        async with SiteMinderClient(use_brightdata=True) as client:
            async def process_hotel(h):
                async with semaphore:
                    data = await client.get_hotel_data_from_url(h['booking_url'])
                    results_to_save.append({'hotel': h, 'data': data})
            
            await asyncio.gather(*[process_hotel(h) for h in hotels])
        
        # Save all results in one connection
        async with get_conn() as conn:
            for r in results_to_save:
                h = r['hotel']
                data = r['data']
                
                if data and (data.name or data.website):
                    await conn.execute('''
                        UPDATE sadie_gtm.hotels SET
                            name = COALESCE(NULLIF($2, ''), name),
                            website = COALESCE(NULLIF($3, ''), website),
                            updated_at = NOW()
                        WHERE id = $1
                    ''', h['id'], data.name, data.website)
                    
                    stats['enriched'] += 1
                    logger.debug(f"{h['name'][:30] if h['name'] else 'Unknown'} | {data.name or ''}")
                else:
                    stats['failed'] += 1
        
        logger.info("=" * 60)
        logger.info("SITEMINDER ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Enriched: {stats['enriched']}")
        logger.info(f"Failed: {stats['failed']}")
        
    finally:
        await close_db()


# =============================================================================
# IPMS247 / EZEE ENRICHMENT
# =============================================================================


async def enrich_ipms247(limit: int, concurrency: int = 10) -> None:
    """Enrich IPMS247/eZee hotels by scraping booking pages."""
    from lib.ipms247.scraper import IPMS247Scraper
    
    await init_db()
    try:
        async with get_conn() as conn:
            # Get hotels needing enrichment
            hotels = await conn.fetch('''
                SELECT h.id, h.name, hbe.booking_url, hbe.engine_property_id as slug,
                       h.address, h.city, h.state, h.country, h.phone_website, h.email, h.location
                FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                WHERE hbe.booking_engine_id = $1
                  AND hbe.status = 1
                  AND h.status != 99  -- Exclude dead ends only
                  AND (
                      h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%'
                      OR h.email IS NULL OR h.email = ''
                      OR h.location IS NULL
                  )
                ORDER BY CASE WHEN h.location IS NULL THEN 0 ELSE 1 END
                LIMIT $2
            ''', IPMS247_ID, limit)
        
        if not hotels:
            logger.info("No IPMS247 hotels need enrichment")
            return
        
        logger.info(f"Enriching {len(hotels)} IPMS247 hotels...")
        
        semaphore = asyncio.Semaphore(concurrency)
        stats = {'enriched': 0, 'failed': 0}
        results_to_save = []
        
        scraper = IPMS247Scraper(use_proxy=True)
        
        async def process_hotel(h):
            async with semaphore:
                slug = h['slug'] or h['booking_url'].split('book-rooms-')[-1].split('/')[0].split('?')[0]
                # Use Playwright to bypass 403 blocks and get full modal data
                data = await scraper.extract_with_playwright(slug)
                results_to_save.append({'hotel': h, 'data': data})
        
        await asyncio.gather(*[process_hotel(h) for h in hotels])
        
        # Save all results in one connection
        async with get_conn() as conn:
            for r in results_to_save:
                h = r['hotel']
                data = r['data']
                
                if data and data.has_data():
                    # Build location point if we have lat/lng
                    location_update = ""
                    params = [h['id']]
                    param_idx = 2
                    
                    updates = []
                    if data.name:
                        updates.append(f"name = COALESCE(NULLIF(${param_idx}, ''), name)")
                        params.append(data.name)
                        param_idx += 1
                    if data.address:
                        updates.append(f"address = COALESCE(NULLIF(${param_idx}, ''), address)")
                        params.append(data.address)
                        param_idx += 1
                    if data.city:
                        updates.append(f"city = COALESCE(NULLIF(${param_idx}, ''), city)")
                        params.append(data.city)
                        param_idx += 1
                    if data.state:
                        updates.append(f"state = COALESCE(NULLIF(${param_idx}, ''), state)")
                        params.append(data.state)
                        param_idx += 1
                    if data.country:
                        updates.append(f"country = COALESCE(NULLIF(${param_idx}, ''), country)")
                        params.append(data.country)
                        param_idx += 1
                    if data.phone:
                        updates.append(f"phone_website = COALESCE(NULLIF(${param_idx}, ''), phone_website)")
                        params.append(data.phone)
                        param_idx += 1
                    if data.email:
                        updates.append(f"email = COALESCE(NULLIF(${param_idx}, ''), email)")
                        params.append(data.email)
                        param_idx += 1
                    if data.latitude and data.longitude:
                        updates.append(f"location = COALESCE(location, ST_SetSRID(ST_MakePoint(${param_idx}, ${param_idx + 1}), 4326))")
                        params.append(data.longitude)
                        params.append(data.latitude)
                        param_idx += 2
                    
                    updates.append("updated_at = NOW()")
                    
                    if updates:
                        await conn.execute(f'''
                            UPDATE sadie_gtm.hotels SET
                                {', '.join(updates)}
                            WHERE id = $1
                        ''', *params)
                        
                        stats['enriched'] += 1
                        logger.debug(f"{h['name'][:30] if h['name'] else 'Unknown'} | {data.name or ''} | {data.email or ''}")
                else:
                    stats['failed'] += 1
        
        logger.info("=" * 60)
        logger.info("IPMS247 ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Enriched: {stats['enriched']}")
        logger.info(f"Failed: {stats['failed']}")
        
    finally:
        await close_db()


# =============================================================================
# PROXIMITY CALCULATION
# =============================================================================


async def calculate_proximity(limit: int, concurrency: int = 20) -> None:
    """Calculate customer proximity for hotels with location but missing proximity."""
    await init_db()
    try:
        async with get_conn() as conn:
            # Get hotels needing proximity
            hotels = await conn.fetch('''
                SELECT h.id, h.name,
                       ST_Y(h.location::geometry) as lat,
                       ST_X(h.location::geometry) as lng
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
                LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON hcp.hotel_id = h.id
                WHERE hbe.booking_engine_id IN ($1, $2, $3)
                  AND hbe.status = 1
                  AND h.status != 99  -- Exclude dead ends only
                  AND h.location IS NOT NULL
                  AND (hcp.hotel_id IS NULL OR hcp.distance_km IS NULL)
                LIMIT $4
            ''', CLOUDBEDS_ID, RMS_ID, SITEMINDER_ID, limit)
            
            if not hotels:
                logger.info("No hotels need proximity calculation")
                return
            
            logger.info(f"Calculating proximity for {len(hotels)} hotels...")
            
            enriched = 0
            for h in hotels:
                nearest = await conn.fetchrow('''
                    SELECT 
                        ec.id, ec.name,
                        ST_Distance(
                            ec.location::geography,
                            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                        ) / 1000 as distance_km
                    FROM sadie_gtm.existing_customers ec
                    WHERE ec.location IS NOT NULL
                    ORDER BY ec.location <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
                    LIMIT 1
                ''', h['lng'], h['lat'])
                
                if nearest:
                    await conn.execute('''
                        INSERT INTO sadie_gtm.hotel_customer_proximity 
                            (hotel_id, existing_customer_id, distance_km, computed_at)
                        VALUES ($1, $2, $3, NOW())
                        ON CONFLICT (hotel_id) DO UPDATE 
                            SET existing_customer_id = $2, distance_km = $3, computed_at = NOW()
                    ''', h['id'], nearest['id'], nearest['distance_km'])
                    enriched += 1
            
            logger.info("=" * 60)
            logger.info("PROXIMITY CALCULATION COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Enriched: {enriched}/{len(hotels)}")
        
    finally:
        await close_db()


# =============================================================================
# MAIN
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Enrich hotels for Cloudbeds, RMS, and SiteMinder"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Status
    subparsers.add_parser("status", help="Show enrichment status")
    
    # Cloudbeds
    cb_parser = subparsers.add_parser("cloudbeds", help="Enrich Cloudbeds hotels")
    cb_parser.add_argument("--limit", type=int, default=100, help="Max hotels")
    cb_parser.add_argument("--concurrency", type=int, default=20, help="Concurrent requests")
    
    # RMS
    rms_parser = subparsers.add_parser("rms", help="Enrich RMS hotels")
    rms_parser.add_argument("--limit", type=int, default=100, help="Max hotels")
    rms_parser.add_argument("--concurrency", type=int, default=10, help="Concurrent requests")
    
    # SiteMinder
    sm_parser = subparsers.add_parser("siteminder", help="Enrich SiteMinder hotels")
    sm_parser.add_argument("--limit", type=int, default=100, help="Max hotels")
    sm_parser.add_argument("--concurrency", type=int, default=20, help="Concurrent requests")
    
    # IPMS247
    ipms_parser = subparsers.add_parser("ipms247", help="Enrich IPMS247/eZee hotels")
    ipms_parser.add_argument("--limit", type=int, default=100, help="Max hotels")
    ipms_parser.add_argument("--concurrency", type=int, default=10, help="Concurrent requests")
    
    # Proximity
    prox_parser = subparsers.add_parser("proximity", help="Calculate customer proximity")
    prox_parser.add_argument("--limit", type=int, default=500, help="Max hotels")
    prox_parser.add_argument("--concurrency", type=int, default=20, help="Concurrent calculations")
    
    args = parser.parse_args()
    
    if args.command == "status":
        asyncio.run(show_status())
    elif args.command == "cloudbeds":
        asyncio.run(enrich_cloudbeds(args.limit, args.concurrency))
    elif args.command == "rms":
        asyncio.run(enrich_rms(args.limit, args.concurrency))
    elif args.command == "siteminder":
        asyncio.run(enrich_siteminder(args.limit, args.concurrency))
    elif args.command == "ipms247":
        asyncio.run(enrich_ipms247(args.limit, args.concurrency))
    elif args.command == "proximity":
        asyncio.run(calculate_proximity(args.limit, args.concurrency))


if __name__ == "__main__":
    main()
