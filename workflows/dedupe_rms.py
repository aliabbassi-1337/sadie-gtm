"""Deduplicate RMS hotels by client ID and name.

RMS has ~7k unique properties but we have ~17k records due to:
1. Multiple URL formats (numeric vs hex client IDs) for the same property
2. Same property ingested from different sources

This workflow:

PHASE 1 - Numeric Client ID Deduplication:
- Extracts numeric RMS client ID from booking URLs
- Groups hotels by client ID to find duplicates
- Keeps the "best" record (has email, city, etc.)
- Merges data from duplicates into the keeper
- Updates external_id to rms_client_id
- Deletes duplicate records

PHASE 2 - Hex URL Deduplication (by name matching):
- Finds hex URLs that match numeric hotels by name
- Merges data from hex record into numeric record (if hex has better data)
- Deletes the hex duplicate
- Hex-only records (no numeric match) are KEPT

USAGE:
    # Dry run - see what would be done
    uv run python workflows/dedupe_rms.py --dry-run

    # Execute deduplication
    uv run python workflows/dedupe_rms.py --execute
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import re
from typing import Optional, Dict, List, Any, Tuple
from loguru import logger

from db.client import init_db, close_db, get_conn


RMS_BOOKING_ENGINE_ID = 12


def extract_rms_client_id(url: str) -> Optional[str]:
    """Extract numeric RMS client ID from booking URL.
    
    RMS URLs have multiple formats:
    - /search/index/{client_id}/...
    - /rates/index/{client_id}/...
    - rmscloud.com/{client_id}/1 or rmscloud.com/{client_id}
    - ibe*.rmscloud.com/{client_id}
    
    Only numeric client IDs are valid. Hex IDs are legacy format.
    """
    if not url:
        return None
    
    # Format 1: /search/index/{numeric_id}/ (case insensitive)
    match = re.search(r'/search/index/(\d+)(?:/|$)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Format 2: /rates/index/{numeric_id}/ (beta format, numeric only)
    match = re.search(r'/rates/index/(\d+)(?:/|$)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Format 3: rmscloud.com/{numeric_id} (with or without trailing path)
    match = re.search(r'rmscloud\.com/(\d+)(?:/|$)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Format 4: ibe*.rmscloud.com/{numeric_id}
    match = re.search(r'ibe\d*\.rmscloud\.com/(\d+)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None


def is_hex_url(url: str) -> bool:
    """Check if URL uses hex format (not numeric)."""
    if not url:
        return False
    
    # Hex in /search/index/ format
    match = re.search(r'/search/index/([A-Fa-f0-9]+)', url, re.IGNORECASE)
    if match and not match.group(1).isdigit():
        return True
    
    # Hex in /rates/index/ format
    match = re.search(r'/rates/index/([A-Fa-f0-9]+)', url, re.IGNORECASE)
    if match and not match.group(1).isdigit():
        return True
    
    # Short hex format: rmscloud.com/{hex}
    match = re.search(r'rmscloud\.com/([A-Fa-f0-9]+)(?:/|$)', url, re.IGNORECASE)
    if match and not match.group(1).isdigit() and len(match.group(1)) >= 8:
        return True
    
    return False


def normalize_name(name: str) -> str:
    """Normalize hotel name for matching."""
    if not name:
        return ''
    return name.strip().lower()


def is_garbage_name(name: str) -> bool:
    """Check if name is garbage/placeholder."""
    if not name:
        return True
    garbage = ('search', 'error', 'online bookings', 'unknown', 'rms')
    return normalize_name(name) in garbage


def is_garbage_city(city: str) -> bool:
    """Check if city is garbage/placeholder."""
    if not city:
        return True
    return city in ('RMS Online Booking', 'Online Bookings', '')


def score_record(r: Dict[str, Any]) -> int:
    """Score a hotel record - higher is better quality data."""
    score = 0
    
    # Email is most valuable
    if r.get('email'):
        score += 10
    
    # Valid city (not garbage)
    if not is_garbage_city(r.get('city')):
        score += 5
    
    # State
    if r.get('state'):
        score += 3
    
    # Valid name (not garbage)
    if not is_garbage_name(r.get('name')):
        score += 5
    
    # Phone
    if r.get('phone_website'):
        score += 2
    
    return score


async def get_rms_hotels(conn) -> List[Dict[str, Any]]:
    """Fetch all RMS hotels with their booking URLs."""
    records = await conn.fetch('''
        SELECT 
            hbe.hotel_id,
            hbe.booking_url,
            h.name,
            h.email,
            h.city,
            h.state,
            h.country,
            h.address,
            h.phone_website,
            h.external_id,
            h.external_id_type
        FROM sadie_gtm.hotel_booking_engines hbe
        JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
        WHERE hbe.booking_engine_id = $1
    ''', RMS_BOOKING_ENGINE_ID)
    return [dict(r) for r in records]


def separate_by_url_type(records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """Separate records into numeric and hex URL types."""
    numeric = []
    hex_urls = []
    
    for r in records:
        if extract_rms_client_id(r['booking_url']):
            numeric.append(r)
        elif is_hex_url(r['booking_url']):
            hex_urls.append(r)
        # Skip any other URL formats
    
    return numeric, hex_urls


def group_by_client_id(records: List[Dict]) -> Dict[str, List[Dict]]:
    """Group numeric records by RMS client ID."""
    by_client_id = {}
    for r in records:
        client_id = extract_rms_client_id(r['booking_url'])
        if client_id:
            if client_id not in by_client_id:
                by_client_id[client_id] = []
            by_client_id[client_id].append(r)
    return by_client_id


def plan_numeric_deduplication(by_client_id: Dict[str, List[Dict]]) -> Tuple[List[Dict], List[int]]:
    """
    Plan deduplication for numeric client IDs.
    
    Returns:
        (to_keep, to_delete) where:
        - to_keep: list of dicts with hotel_id, client_id, and merged fields
        - to_delete: list of hotel_ids to delete
    """
    to_keep = []
    to_delete = []
    
    for client_id, recs in by_client_id.items():
        if len(recs) == 1:
            # Single record - just update external_id
            r = recs[0]
            to_keep.append({
                'hotel_id': r['hotel_id'],
                'client_id': client_id,
                'name': r['name'],
                'email': r['email'],
                'city': r['city'],
                'state': r['state'],
                'country': r['country'],
                'phone_website': r['phone_website'],
            })
        else:
            # Multiple records - keep best, merge data
            recs.sort(key=score_record, reverse=True)
            keeper = recs[0].copy()
            
            # Merge data from others
            for other in recs[1:]:
                if not keeper.get('email') and other.get('email'):
                    keeper['email'] = other['email']
                if is_garbage_city(keeper.get('city')) and not is_garbage_city(other.get('city')):
                    keeper['city'] = other['city']
                if not keeper.get('state') and other.get('state'):
                    keeper['state'] = other['state']
                if not keeper.get('phone_website') and other.get('phone_website'):
                    keeper['phone_website'] = other['phone_website']
                if is_garbage_name(keeper.get('name')) and not is_garbage_name(other.get('name')):
                    keeper['name'] = other['name']
                
                # Mark for deletion
                to_delete.append(other['hotel_id'])
            
            to_keep.append({
                'hotel_id': keeper['hotel_id'],
                'client_id': client_id,
                'name': keeper.get('name'),
                'email': keeper.get('email'),
                'city': keeper.get('city'),
                'state': keeper.get('state'),
                'country': keeper.get('country'),
                'phone_website': keeper.get('phone_website'),
            })
    
    return to_keep, to_delete


def plan_hex_deduplication(
    hex_records: List[Dict],
    numeric_keepers: List[Dict],
) -> Tuple[List[Dict], List[int], int]:
    """
    Plan deduplication for hex URLs by matching to numeric hotels.
    
    Returns:
        (to_update, to_delete, kept_count) where:
        - to_update: numeric records to update with merged hex data
        - to_delete: hex hotel_ids to delete (matched to numeric)
        - kept_count: number of hex-only records kept (no match)
    """
    # Build name -> numeric keeper mapping
    numeric_by_name = {}
    for r in numeric_keepers:
        name = normalize_name(r.get('name'))
        if name and not is_garbage_name(r.get('name')):
            numeric_by_name[name] = r
    
    to_update = []
    to_delete = []
    kept_count = 0
    
    for hex_r in hex_records:
        hex_name = normalize_name(hex_r.get('name'))
        
        if hex_name in numeric_by_name:
            # Match found - merge hex data into numeric
            numeric_r = numeric_by_name[hex_name]
            updated = False
            
            # Check if hex has better data
            if not numeric_r.get('email') and hex_r.get('email'):
                numeric_r['email'] = hex_r['email']
                updated = True
            if is_garbage_city(numeric_r.get('city')) and not is_garbage_city(hex_r.get('city')):
                numeric_r['city'] = hex_r['city']
                updated = True
            if not numeric_r.get('state') and hex_r.get('state'):
                numeric_r['state'] = hex_r['state']
                updated = True
            if not numeric_r.get('phone_website') and hex_r.get('phone_website'):
                numeric_r['phone_website'] = hex_r['phone_website']
                updated = True
            
            if updated:
                to_update.append(numeric_r)
            
            # Delete the hex record (it's a duplicate)
            to_delete.append(hex_r['hotel_id'])
        else:
            # No match - keep the hex record
            kept_count += 1
    
    return to_update, to_delete, kept_count


async def execute_deduplication(
    conn,
    numeric_keepers: List[Dict],
    numeric_deletes: List[int],
    hex_updates: List[Dict],
    hex_deletes: List[int],
) -> Dict[str, int]:
    """Execute the deduplication in a transaction."""
    stats = {
        'numeric_updated': 0,
        'numeric_deleted': 0,
        'hex_merged': 0,
        'hex_deleted': 0,
    }
    
    async with conn.transaction():
        # Phase 1: Update numeric keepers with merged data and external_id
        for r in numeric_keepers:
            await conn.execute('''
                UPDATE sadie_gtm.hotels
                SET 
                    external_id = $2,
                    external_id_type = 'rms_client_id',
                    name = COALESCE(NULLIF($3, ''), name),
                    email = COALESCE(NULLIF($4, ''), email),
                    city = CASE 
                        WHEN $5 IS NOT NULL AND $5 != '' AND $5 != 'RMS Online Booking' 
                        THEN $5 
                        ELSE city 
                    END,
                    state = COALESCE(NULLIF($6, ''), state),
                    phone_website = COALESCE(NULLIF($7, ''), phone_website),
                    updated_at = NOW()
                WHERE id = $1
            ''', r['hotel_id'], r['client_id'], r.get('name'), r.get('email'),
                r.get('city'), r.get('state'), r.get('phone_website'))
            stats['numeric_updated'] += 1
        
        # Phase 2: Update numeric records with hex data (where hex had better data)
        for r in hex_updates:
            await conn.execute('''
                UPDATE sadie_gtm.hotels
                SET 
                    email = COALESCE(NULLIF($2, ''), email),
                    city = CASE 
                        WHEN $3 IS NOT NULL AND $3 != '' AND $3 != 'RMS Online Booking' 
                        THEN $3 
                        ELSE city 
                    END,
                    state = COALESCE(NULLIF($4, ''), state),
                    phone_website = COALESCE(NULLIF($5, ''), phone_website),
                    updated_at = NOW()
                WHERE id = $1
            ''', r['hotel_id'], r.get('email'), r.get('city'),
                r.get('state'), r.get('phone_website'))
            stats['hex_merged'] += 1
        
        # Phase 3: Delete numeric duplicates
        if numeric_deletes:
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_booking_engines
                WHERE hotel_id = ANY($1)
            ''', numeric_deletes)
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_room_count
                WHERE hotel_id = ANY($1)
            ''', numeric_deletes)
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_customer_proximity
                WHERE hotel_id = ANY($1)
            ''', numeric_deletes)
            await conn.execute('''
                DELETE FROM sadie_gtm.hotels
                WHERE id = ANY($1)
            ''', numeric_deletes)
            stats['numeric_deleted'] = len(numeric_deletes)
        
        # Phase 4: Delete hex duplicates (that matched numeric by name)
        if hex_deletes:
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_booking_engines
                WHERE hotel_id = ANY($1)
            ''', hex_deletes)
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_room_count
                WHERE hotel_id = ANY($1)
            ''', hex_deletes)
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_customer_proximity
                WHERE hotel_id = ANY($1)
            ''', hex_deletes)
            await conn.execute('''
                DELETE FROM sadie_gtm.hotels
                WHERE id = ANY($1)
            ''', hex_deletes)
            stats['hex_deleted'] = len(hex_deletes)
    
    return stats


async def run_dedupe(dry_run: bool = True) -> None:
    """Run the deduplication workflow."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            logger.info("Fetching RMS hotels...")
            records = await get_rms_hotels(conn)
            logger.info(f"Total RMS records: {len(records)}")
            
            # Separate by URL type
            numeric_records, hex_records = separate_by_url_type(records)
            logger.info(f"Numeric URL records: {len(numeric_records)}")
            logger.info(f"Hex URL records: {len(hex_records)}")
            
            # Phase 1: Plan numeric deduplication
            logger.info("")
            logger.info("=" * 60)
            logger.info("PHASE 1: Numeric Client ID Deduplication")
            logger.info("=" * 60)
            
            by_client_id = group_by_client_id(numeric_records)
            logger.info(f"Unique numeric client IDs: {len(by_client_id)}")
            
            numeric_keepers, numeric_deletes = plan_numeric_deduplication(by_client_id)
            logger.info(f"Records to keep/update: {len(numeric_keepers)}")
            logger.info(f"Duplicate records to delete: {len(numeric_deletes)}")
            
            # Phase 2: Plan hex deduplication
            logger.info("")
            logger.info("=" * 60)
            logger.info("PHASE 2: Hex URL Deduplication (by name)")
            logger.info("=" * 60)
            
            hex_updates, hex_deletes, hex_kept = plan_hex_deduplication(
                hex_records, numeric_keepers
            )
            logger.info(f"Hex records matching numeric (to delete): {len(hex_deletes)}")
            logger.info(f"Hex records with better data (to merge): {len(hex_updates)}")
            logger.info(f"Hex-only records (KEPT): {hex_kept}")
            
            # Summary
            logger.info("")
            logger.info("=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            total_deletes = len(numeric_deletes) + len(hex_deletes)
            final_count = len(records) - total_deletes
            logger.info(f"Current records: {len(records)}")
            logger.info(f"To delete: {total_deletes}")
            logger.info(f"Final count: {final_count}")
            
            if dry_run:
                logger.info("")
                logger.info("DRY RUN - No changes made")
                logger.info("")
                
                # Show sample numeric merges
                logger.info("Sample numeric merges:")
                shown = 0
                for r in numeric_keepers:
                    if shown >= 3:
                        break
                    client_recs = by_client_id.get(r['client_id'], [])
                    if len(client_recs) > 1:
                        logger.info(f"  Client {r['client_id']}: Keep hotel {r['hotel_id']}")
                        logger.info(f"    -> {r['name']} | {r['city']}, {r['state']} | {r['email']}")
                        for orig in client_recs:
                            if orig['hotel_id'] != r['hotel_id']:
                                logger.info(f"    Delete hotel {orig['hotel_id']}: {orig['name']} | {orig['city']}")
                        shown += 1
                
                # Show sample hex merges
                if hex_updates:
                    logger.info("")
                    logger.info("Sample hex->numeric merges (hex had better data):")
                    for r in hex_updates[:3]:
                        logger.info(f"  Updated hotel {r['hotel_id']} with: {r['city']} | {r['email']}")
                
                logger.info("")
                logger.info("Run with --execute to apply changes")
            else:
                logger.info("")
                logger.info("Executing deduplication...")
                stats = await execute_deduplication(
                    conn, numeric_keepers, numeric_deletes, hex_updates, hex_deletes
                )
                
                logger.info("")
                logger.info("=" * 60)
                logger.info("DEDUPLICATION COMPLETE")
                logger.info("=" * 60)
                logger.info(f"Numeric hotels updated: {stats['numeric_updated']}")
                logger.info(f"Numeric duplicates deleted: {stats['numeric_deleted']}")
                logger.info(f"Hex data merged into numeric: {stats['hex_merged']}")
                logger.info(f"Hex duplicates deleted: {stats['hex_deleted']}")
                
                # Verify final count
                final = await conn.fetchval('''
                    SELECT COUNT(DISTINCT h.id)
                    FROM sadie_gtm.hotels h
                    JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                    WHERE hbe.booking_engine_id = $1
                ''', RMS_BOOKING_ENGINE_ID)
                logger.info(f"Final RMS hotel count: {final}")
    
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Deduplicate RMS hotels by client ID and name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes (default)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the deduplication",
    )
    
    args = parser.parse_args()
    
    # Default to dry-run if neither specified
    dry_run = not args.execute
    
    asyncio.run(run_dedupe(dry_run=dry_run))


if __name__ == "__main__":
    main()
