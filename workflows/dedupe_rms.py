"""Deduplicate RMS hotels in two stages.

RMS has ~7k unique properties but we have ~17k records due to:
1. Multiple URL formats (numeric vs hex client IDs) for the same property
2. Same property with different client IDs (RMS re-assigns client IDs)
3. Same property ingested from different sources
4. Junk "United States" entries (Australian WA parsed as Washington)

This workflow marks duplicates with status = -1 (preserves data, won't be launched).

TWO-STAGE DEDUPLICATION:

STAGE 1 - BY CLIENT ID (100% accurate, zero false positives):
- Extracts numeric RMS client ID from booking URLs
- Groups hotels by client ID - same client ID = definitely same hotel
- Keeps the "best" record, merges data from duplicates
- Marks duplicates with status = -1

STAGE 2 - BY NAME + CITY (catches remaining dupes):
- Only processes survivors from Stage 1
- Groups by normalized (name + city)
- Handles cases where RMS reassigned client IDs
- Keeps best record, marks remaining duplicates

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

# Garbage names to exclude from deduplication (these are placeholders, not real hotels)
GARBAGE_NAMES = {
    'online bookings', 'error', 'search', 'unknown', 'rms', '', ' ',
    'rates', 'book now', 'reservation', 'reservations', 'hotel',
    'an unhandled exception occurred while processing the request.',
}


def extract_rms_client_id(url: str) -> Optional[str]:
    """Extract numeric RMS client ID from booking URL."""
    if not url:
        return None
    
    match = re.search(r'/search/index/(\d+)(?:/|$)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    match = re.search(r'/rates/index/(\d+)(?:/|$)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    match = re.search(r'rmscloud\.com/(\d+)(?:/|$)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    match = re.search(r'ibe\d*\.rmscloud\.com/(\d+)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None


def normalize_text(text: str) -> str:
    """Normalize text for matching."""
    if not text:
        return ''
    return text.strip().lower()


def is_garbage_name(name: str) -> bool:
    """Check if name is garbage/placeholder."""
    if not name:
        return True
    return normalize_text(name) in GARBAGE_NAMES


def is_garbage_city(city: str) -> bool:
    """Check if city is garbage/placeholder."""
    if not city:
        return True
    return city.strip() in ('RMS Online Booking', 'Online Bookings', '', 'its bubbling')


def country_matches_address(country: str, address: str) -> bool:
    """Check if country field matches what's in the address."""
    if not country or not address:
        return False
    
    addr_lower = address.lower()
    country_lower = country.lower()
    
    # Check for obvious mismatches
    if 'united states' in country_lower:
        # US shouldn't have Australian addresses
        if 'australia' in addr_lower or ', au' in addr_lower:
            return False
        # WA in address with US country is usually wrong (Western Australia)
        if ' wa ' in addr_lower or addr_lower.endswith(' wa'):
            return False
        # Other Australian state abbreviations in address
        if any(f' {st} ' in addr_lower or addr_lower.endswith(f' {st}') 
               for st in ['nsw', 'qld', 'vic', 'tas', 'sa', 'nt', 'act']):
            return False
    
    if 'australia' in country_lower:
        if 'australia' in addr_lower:
            return True
    
    if 'canada' in country_lower:
        if 'canada' in addr_lower or any(f' {prov} ' in addr_lower 
               for prov in ['on', 'bc', 'ab', 'mb', 'sk', 'qc', 'ns', 'nb', 'nl', 'pe']):
            return True
    
    # Default: can't determine, treat as neutral
    return True


def score_record(r: Dict[str, Any]) -> int:
    """Score a hotel record - higher is better quality data."""
    score = 0
    
    # Email is most valuable
    if r.get('email'):
        score += 15
    
    # Address that matches country (penalize mismatches)
    if r.get('address') and r.get('country'):
        if country_matches_address(r['country'], r['address']):
            score += 10
        else:
            score -= 20  # Heavy penalty for country/address mismatch
    
    # Has address at all
    if r.get('address'):
        score += 5
    
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
    
    # Country (non-null is better)
    if r.get('country'):
        score += 2
    
    return score


async def get_rms_hotels(conn) -> List[Dict[str, Any]]:
    """Fetch all RMS hotels with their booking URLs, excluding garbage names."""
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
            h.status
        FROM sadie_gtm.hotel_booking_engines hbe
        JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
        WHERE hbe.booking_engine_id = $1
          AND h.name IS NOT NULL
          AND h.name != ''
          AND h.status >= 0  -- Only active hotels
    ''', RMS_BOOKING_ENGINE_ID)
    
    # Filter out garbage names in Python (more flexible than SQL)
    valid = []
    garbage_count = 0
    for r in records:
        if is_garbage_name(r['name']):
            garbage_count += 1
        else:
            valid.append(dict(r))
    
    logger.info(f"Excluded {garbage_count} records with garbage names")
    return valid


def group_by_client_id(records: List[Dict]) -> Tuple[Dict[str, List[Dict]], List[Dict]]:
    """
    Group records by RMS client ID.
    
    Returns:
        (by_client_id, no_client_id) where:
        - by_client_id: dict mapping client_id -> list of records
        - no_client_id: list of records without extractable client ID (hex URLs, etc.)
    """
    by_client_id = {}
    no_client_id = []
    
    for r in records:
        client_id = extract_rms_client_id(r['booking_url'])
        if client_id:
            if client_id not in by_client_id:
                by_client_id[client_id] = []
            by_client_id[client_id].append(r)
        else:
            no_client_id.append(r)
    
    return by_client_id, no_client_id


def group_by_name_city(records: List[Dict]) -> Dict[Tuple[str, str], List[Dict]]:
    """Group records by normalized (name + city)."""
    groups = {}
    for r in records:
        key = (normalize_text(r.get('name', '')), normalize_text(r.get('city', '')))
        if key not in groups:
            groups[key] = []
        groups[key].append(r)
    return groups


def merge_records(recs: List[Dict]) -> Tuple[Dict, List[int]]:
    """
    Merge a group of duplicate records.
    
    Returns:
        (keeper, dupe_ids) where:
        - keeper: the merged record to keep
        - dupe_ids: list of hotel_ids to mark as duplicates
    """
    if len(recs) == 1:
        return recs[0].copy(), []
    
    # Sort by score, best first
    recs_sorted = sorted(recs, key=score_record, reverse=True)
    keeper = recs_sorted[0].copy()
    dupe_ids = []
    
    # Merge data from duplicates
    for other in recs_sorted[1:]:
        if not keeper.get('email') and other.get('email'):
            keeper['email'] = other['email']
        if is_garbage_city(keeper.get('city')) and not is_garbage_city(other.get('city')):
            keeper['city'] = other['city']
        if not keeper.get('state') and other.get('state'):
            keeper['state'] = other['state']
        if not keeper.get('address') and other.get('address'):
            keeper['address'] = other['address']
        if not keeper.get('phone_website') and other.get('phone_website'):
            keeper['phone_website'] = other['phone_website']
        if is_garbage_name(keeper.get('name')) and not is_garbage_name(other.get('name')):
            keeper['name'] = other['name']
        # Fix country if keeper has address/country mismatch
        if keeper.get('address') and other.get('country'):
            if not country_matches_address(keeper.get('country', ''), keeper['address']):
                if country_matches_address(other['country'], keeper['address']):
                    keeper['country'] = other['country']
        
        dupe_ids.append(other['hotel_id'])
    
    return keeper, dupe_ids


def plan_stage1_deduplication(by_client_id: Dict[str, List[Dict]]) -> Tuple[List[Dict], List[int]]:
    """
    Plan Stage 1 deduplication by client ID.
    
    Returns:
        (keepers, dupe_ids) - keepers have client_id set
    """
    keepers = []
    all_dupes = []
    
    for client_id, recs in by_client_id.items():
        keeper, dupe_ids = merge_records(recs)
        keeper['client_id'] = client_id  # Store for external_id
        keepers.append(keeper)
        all_dupes.extend(dupe_ids)
    
    return keepers, all_dupes


def plan_stage2_deduplication(
    stage1_keepers: List[Dict],
    no_client_id: List[Dict],
) -> Tuple[List[Dict], List[int]]:
    """
    Plan Stage 2 deduplication by name + city.
    
    Processes Stage 1 keepers + records without client ID.
    
    Returns:
        (keepers, dupe_ids)
    """
    # Combine stage 1 keepers with no-client-id records
    all_records = stage1_keepers + no_client_id
    
    # Group by name + city
    groups = group_by_name_city(all_records)
    
    keepers = []
    all_dupes = []
    
    for (name_norm, city_norm), recs in groups.items():
        keeper, dupe_ids = merge_records(recs)
        keepers.append(keeper)
        all_dupes.extend(dupe_ids)
    
    return keepers, all_dupes


async def execute_deduplication(
    conn,
    keepers: List[Dict],
    dupes: List[int],
) -> Dict[str, int]:
    """Execute the deduplication using batch updates."""
    stats = {
        'updated': 0,
        'marked_dupe': 0,
    }
    
    async with conn.transaction():
        # Step 1: Clear external_id from ALL RMS hotels to avoid unique constraint issues
        await conn.execute('''
            UPDATE sadie_gtm.hotels
            SET external_id = NULL, external_id_type = NULL
            WHERE external_id_type = 'rms_client_id'
        ''')
        logger.info("Cleared existing rms_client_id external_ids")
        
        # Step 2: Mark duplicates as status = -1 (batch)
        if dupes:
            await conn.execute('''
                UPDATE sadie_gtm.hotels
                SET status = -1, updated_at = NOW()
                WHERE id = ANY($1)
            ''', dupes)
            stats['marked_dupe'] = len(dupes)
            logger.info(f"Marked {len(dupes)} duplicates as status=-1")
        
        # Step 3: Batch update keepers using temp table
        # Prepare data for batch insert
        keeper_data = []
        for r in keepers:
            client_id = r.get('client_id') or extract_rms_client_id(r.get('booking_url', ''))
            keeper_data.append((
                r['hotel_id'],
                client_id or '',
                r.get('name') or '',
                r.get('email') or '',
                r.get('city') or '',
                r.get('state') or '',
                r.get('country') or '',
                r.get('phone_website') or '',
            ))
        
        # Create temp table
        await conn.execute('''
            CREATE TEMP TABLE temp_keeper_updates (
                hotel_id INTEGER PRIMARY KEY,
                client_id TEXT,
                name TEXT,
                email TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                phone_website TEXT
            ) ON COMMIT DROP
        ''')
        
        # Batch insert into temp table
        await conn.executemany('''
            INSERT INTO temp_keeper_updates 
            (hotel_id, client_id, name, email, city, state, country, phone_website)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ''', keeper_data)
        logger.info(f"Loaded {len(keeper_data)} keepers into temp table")
        
        # Single UPDATE from temp table
        result = await conn.execute('''
            UPDATE sadie_gtm.hotels h
            SET 
                external_id = NULLIF(t.client_id, ''),
                external_id_type = CASE WHEN t.client_id != '' THEN 'rms_client_id' ELSE h.external_id_type END,
                name = COALESCE(NULLIF(t.name, ''), h.name),
                email = COALESCE(NULLIF(t.email, ''), h.email),
                city = CASE 
                    WHEN t.city != '' AND t.city != 'RMS Online Booking' 
                    THEN t.city 
                    ELSE h.city 
                END,
                state = COALESCE(NULLIF(t.state, ''), h.state),
                country = COALESCE(NULLIF(t.country, ''), h.country),
                phone_website = COALESCE(NULLIF(t.phone_website, ''), h.phone_website),
                updated_at = NOW()
            FROM temp_keeper_updates t
            WHERE h.id = t.hotel_id
        ''')
        stats['updated'] = len(keeper_data)
        logger.info(f"Updated {stats['updated']} keeper hotels")
    
    return stats


async def run_dedupe(dry_run: bool = True) -> None:
    """Run the two-stage deduplication workflow."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            logger.info("Fetching RMS hotels...")
            records = await get_rms_hotels(conn)
            logger.info(f"Valid RMS records (non-garbage names, active): {len(records)}")
            
            # ================================================================
            # STAGE 1: Deduplicate by Client ID
            # ================================================================
            logger.info("")
            logger.info("=" * 60)
            logger.info("STAGE 1: DEDUPLICATE BY CLIENT ID")
            logger.info("(Same client ID = definitely same hotel, 100% accurate)")
            logger.info("=" * 60)
            
            by_client_id, no_client_id = group_by_client_id(records)
            logger.info(f"Records with numeric client ID: {sum(len(v) for v in by_client_id.values())}")
            logger.info(f"Records without client ID (hex URLs): {len(no_client_id)}")
            logger.info(f"Unique client IDs: {len(by_client_id)}")
            
            # Plan stage 1
            stage1_keepers, stage1_dupes = plan_stage1_deduplication(by_client_id)
            dup_groups_s1 = sum(1 for recs in by_client_id.values() if len(recs) > 1)
            logger.info(f"Client ID groups with duplicates: {dup_groups_s1}")
            logger.info(f"Stage 1 keepers: {len(stage1_keepers)}")
            logger.info(f"Stage 1 duplicates: {len(stage1_dupes)}")
            
            # ================================================================
            # STAGE 2: Deduplicate by Name + City
            # ================================================================
            logger.info("")
            logger.info("=" * 60)
            logger.info("STAGE 2: DEDUPLICATE BY NAME + CITY")
            logger.info("(Catches hotels with different client IDs)")
            logger.info("=" * 60)
            
            # Stage 2 processes stage 1 keepers + no-client-id records
            stage2_input = len(stage1_keepers) + len(no_client_id)
            logger.info(f"Stage 2 input: {stage2_input} records")
            
            final_keepers, stage2_dupes = plan_stage2_deduplication(stage1_keepers, no_client_id)
            
            # Count stage 2 duplicate groups
            groups = group_by_name_city(stage1_keepers + no_client_id)
            dup_groups_s2 = sum(1 for recs in groups.values() if len(recs) > 1)
            logger.info(f"Name+City groups with duplicates: {dup_groups_s2}")
            logger.info(f"Stage 2 keepers: {len(final_keepers)}")
            logger.info(f"Stage 2 duplicates: {len(stage2_dupes)}")
            
            # ================================================================
            # SUMMARY
            # ================================================================
            logger.info("")
            logger.info("=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            total_dupes = len(stage1_dupes) + len(stage2_dupes)
            logger.info(f"Original records: {len(records)}")
            logger.info(f"Stage 1 duplicates (by client ID): {len(stage1_dupes)}")
            logger.info(f"Stage 2 duplicates (by name+city): {len(stage2_dupes)}")
            logger.info(f"Total duplicates: {total_dupes}")
            logger.info(f"Final unique hotels: {len(final_keepers)}")
            logger.info(f"Reduction: {total_dupes} ({100*total_dupes/len(records):.1f}%)")
            
            if dry_run:
                logger.info("")
                logger.info("DRY RUN - No changes made")
                logger.info("")
                
                # Show sample Stage 1 merges
                logger.info("Sample Stage 1 merges (by client ID):")
                shown = 0
                for client_id, recs in by_client_id.items():
                    if shown >= 3 and len(recs) > 1:
                        break
                    if len(recs) > 1:
                        recs_sorted = sorted(recs, key=score_record, reverse=True)
                        keeper = recs_sorted[0]
                        logger.info(f"  Client {client_id}: {len(recs)} records")
                        logger.info(f"    KEEP [{keeper['hotel_id']}]: {keeper['name'][:30] if keeper['name'] else 'NULL'} | {keeper['country']}")
                        for orig in recs_sorted[1:2]:
                            logger.info(f"    DUPE [{orig['hotel_id']}]: {orig['name'][:30] if orig['name'] else 'NULL'} | {orig['country']}")
                        if len(recs) > 2:
                            logger.info(f"    ... and {len(recs) - 2} more")
                        shown += 1
                
                # Show sample Stage 2 merges
                logger.info("")
                logger.info("Sample Stage 2 merges (by name+city):")
                shown = 0
                for (name_norm, city_norm), recs in groups.items():
                    if shown >= 3:
                        break
                    if len(recs) > 1:
                        recs_sorted = sorted(recs, key=score_record, reverse=True)
                        keeper = recs_sorted[0]
                        # Check if these have different client IDs
                        client_ids = set(r.get('client_id') or extract_rms_client_id(r.get('booking_url', '')) for r in recs)
                        if len(client_ids) > 1:
                            logger.info(f"  '{name_norm[:30]}' | city='{city_norm}' | {len(client_ids)} different client IDs")
                            logger.info(f"    KEEP [{keeper['hotel_id']}]: {keeper['country']} | score={score_record(keeper)}")
                            for orig in recs_sorted[1:2]:
                                logger.info(f"    DUPE [{orig['hotel_id']}]: {orig['country']} | score={score_record(orig)}")
                            shown += 1
                
                logger.info("")
                logger.info("Run with --execute to apply changes")
            else:
                logger.info("")
                logger.info("Executing deduplication...")
                
                # Combine all duplicates
                all_dupes = stage1_dupes + stage2_dupes
                
                stats = await execute_deduplication(conn, final_keepers, all_dupes)
                
                logger.info("")
                logger.info("=" * 60)
                logger.info("DEDUPLICATION COMPLETE")
                logger.info("=" * 60)
                logger.info(f"Hotels updated: {stats['updated']}")
                logger.info(f"Duplicates marked (status=-1): {stats['marked_dupe']}")
                
                # Verify final count
                active = await conn.fetchval('''
                    SELECT COUNT(DISTINCT h.id)
                    FROM sadie_gtm.hotels h
                    JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                    WHERE hbe.booking_engine_id = $1 AND h.status >= 0
                ''', RMS_BOOKING_ENGINE_ID)
                logger.info(f"Active RMS hotels (status >= 0): {active}")
    
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate RMS hotels in two stages: (1) by client ID, (2) by name+city"
    )
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
    dry_run = not args.execute
    asyncio.run(run_dedupe(dry_run=dry_run))


if __name__ == "__main__":
    main()
