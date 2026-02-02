"""Deduplicate RMS hotels by client ID.

RMS has ~7k unique properties but we have ~17k records due to multiple URL formats
pointing to the same property. This workflow:

1. Extracts numeric RMS client ID from booking URLs
2. Groups hotels by client ID
3. For duplicates, keeps the "best" record (has email, city, etc.)
4. Merges data from duplicates into the keeper
5. Updates external_id to rms_client_id for dedup on future ingests
6. Deletes duplicate hotel records

USAGE:
    # Dry run - see what would happen
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
from typing import Optional, Dict, List, Any
from loguru import logger

from db.client import init_db, close_db, get_conn


RMS_BOOKING_ENGINE_ID = 12


def extract_rms_client_id(url: str) -> Optional[str]:
    """Extract numeric RMS client ID from booking URL.
    
    RMS URLs have format: /Search/Index/{client_id}/...
    Only numeric client IDs are valid. Hex IDs are legacy format.
    """
    if not url:
        return None
    match = re.search(r'/Search/Index/(\d+)/', url)
    return match.group(1) if match else None


def score_record(r: Dict[str, Any]) -> int:
    """Score a hotel record - higher is better quality data."""
    score = 0
    
    # Email is most valuable
    if r.get('email'):
        score += 10
    
    # Valid city (not garbage)
    city = r.get('city')
    if city and city not in ('RMS Online Booking', 'Online Bookings', ''):
        score += 5
    
    # State
    if r.get('state'):
        score += 3
    
    # Valid name (not garbage)
    name = r.get('name') or ''
    garbage_names = ('Search', 'Error', 'Online Bookings', 'Unknown', 'RMS')
    if name and not any(g in name for g in garbage_names):
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


def group_by_client_id(records: List[Dict]) -> Dict[str, List[Dict]]:
    """Group records by RMS client ID."""
    by_client_id = {}
    for r in records:
        client_id = extract_rms_client_id(r['booking_url'])
        if client_id:
            if client_id not in by_client_id:
                by_client_id[client_id] = []
            by_client_id[client_id].append(r)
    return by_client_id


def plan_deduplication(by_client_id: Dict[str, List[Dict]]) -> tuple:
    """
    Plan which records to keep, update, and delete.
    
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
                if (not keeper.get('city') or keeper['city'] in ('RMS Online Booking', '')) and other.get('city') and other['city'] not in ('RMS Online Booking', ''):
                    keeper['city'] = other['city']
                if not keeper.get('state') and other.get('state'):
                    keeper['state'] = other['state']
                if not keeper.get('phone_website') and other.get('phone_website'):
                    keeper['phone_website'] = other['phone_website']
                if (not keeper.get('name') or any(g in keeper['name'] for g in ('Unknown', 'Search', 'Error'))) and other.get('name'):
                    if not any(g in other['name'] for g in ('Unknown', 'Search', 'Error')):
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


async def execute_deduplication(conn, to_keep: List[Dict], to_delete: List[int]) -> tuple:
    """Execute the deduplication in a transaction."""
    updated = 0
    deleted = 0
    
    async with conn.transaction():
        # First, update keepers with merged data and external_id
        for r in to_keep:
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
            updated += 1
        
        # Delete duplicates - first remove from hotel_booking_engines, then hotels
        if to_delete:
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_booking_engines
                WHERE hotel_id = ANY($1)
            ''', to_delete)
            
            # Also delete from other related tables
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_room_count
                WHERE hotel_id = ANY($1)
            ''', to_delete)
            
            await conn.execute('''
                DELETE FROM sadie_gtm.hotel_customer_proximity
                WHERE hotel_id = ANY($1)
            ''', to_delete)
            
            result = await conn.execute('''
                DELETE FROM sadie_gtm.hotels
                WHERE id = ANY($1)
            ''', to_delete)
            deleted = len(to_delete)
    
    return updated, deleted


async def run_dedupe(dry_run: bool = True) -> None:
    """Run the deduplication workflow."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            logger.info("Fetching RMS hotels...")
            records = await get_rms_hotels(conn)
            logger.info(f"Total RMS records: {len(records)}")
            
            # Group by client ID
            by_client_id = group_by_client_id(records)
            logger.info(f"Unique numeric client IDs: {len(by_client_id)}")
            
            # Records without numeric ID (hex/legacy)
            no_client_id = len(records) - sum(len(v) for v in by_client_id.values())
            logger.info(f"Records without numeric ID (legacy): {no_client_id}")
            
            # Plan deduplication
            to_keep, to_delete = plan_deduplication(by_client_id)
            
            duplicates = len(to_delete)
            logger.info(f"Duplicate records to delete: {duplicates}")
            logger.info(f"Records to keep/update: {len(to_keep)}")
            
            if dry_run:
                logger.info("")
                logger.info("=" * 60)
                logger.info("DRY RUN - No changes made")
                logger.info("=" * 60)
                
                # Show sample merges
                logger.info("")
                logger.info("Sample merges (keeping best record):")
                shown = 0
                for r in to_keep:
                    if shown >= 5:
                        break
                    # Check if this was a merge (had duplicates)
                    client_recs = by_client_id.get(r['client_id'], [])
                    if len(client_recs) > 1:
                        logger.info(f"  Client {r['client_id']}: Keep hotel {r['hotel_id']}")
                        logger.info(f"    -> {r['name']} | {r['city']}, {r['state']} | {r['email']}")
                        for orig in client_recs:
                            if orig['hotel_id'] != r['hotel_id']:
                                logger.info(f"    Delete hotel {orig['hotel_id']}: {orig['name']} | {orig['city']}")
                        shown += 1
                
                logger.info("")
                logger.info("Run with --execute to apply changes")
            else:
                logger.info("")
                logger.info("Executing deduplication...")
                updated, deleted = await execute_deduplication(conn, to_keep, to_delete)
                
                logger.info("")
                logger.info("=" * 60)
                logger.info("DEDUPLICATION COMPLETE")
                logger.info("=" * 60)
                logger.info(f"Hotels updated with external_id: {updated}")
                logger.info(f"Duplicate hotels deleted: {deleted}")
                
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
    parser = argparse.ArgumentParser(description="Deduplicate RMS hotels by client ID")
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
