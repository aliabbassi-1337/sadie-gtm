#!/usr/bin/env python3
"""
Deduplicate hotels by merging scattered data before marking duplicates.

This workflow:
1. Finds duplicate hotels (same name + email within a booking engine)
2. Merges the best data from all duplicates into one "keeper" row
3. Marks the other rows as status=-1

Usage:
    # Dry run - see what would be merged
    uv run python -m workflows.deduplicate_hotels --engine cloudbeds --dry-run
    
    # Actually deduplicate
    uv run python -m workflows.deduplicate_hotels --engine rms
    
    # Deduplicate all engines
    uv run python -m workflows.deduplicate_hotels --all
"""

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from loguru import logger

from db.client import close_db, get_conn, init_db


# Fields that can be merged (in priority order for picking best value)
MERGEABLE_FIELDS = [
    'city', 'state', 'address', 'phone_google', 'phone_website', 
    'website', 'rating', 'review_count', 'category'
]

# Garbage values to ignore
GARBAGE_VALUES = {
    'rms online booking', 'none', 'null', 'n/a', '', ' ',
    'noemail@noemail.com', 'donotreply@', 'test@test.com'
}


def is_real_data(val) -> bool:
    """Check if a value contains real data (not garbage)."""
    if val is None:
        return False
    val_str = str(val).strip().lower()
    if val_str in GARBAGE_VALUES:
        return False
    # Check for partial matches (like DONOTREPLY@)
    for garbage in GARBAGE_VALUES:
        if garbage in val_str:
            return False
    return True


def pick_best_value(values: list):
    """Pick the best value from a list, preferring non-null real data."""
    for val in values:
        if is_real_data(val):
            return val
    return values[0] if values else None


@dataclass
class MergeResult:
    keeper_id: int
    name: str
    duplicate_ids: list[int]
    merged_fields: dict
    

async def find_duplicates(conn, booking_engine_id: int, country: str = 'United States') -> list[MergeResult]:
    """Find duplicate hotels and determine what to merge."""
    
    # Get all hotels for this engine, grouped by name+email
    rows = await conn.fetch("""
        SELECT h.id, h.name, LOWER(COALESCE(h.email, '')) as email_lower,
               h.city, h.state, h.address, h.phone_google, h.phone_website,
               h.website, h.rating, h.review_count, h.category, h.created_at, h.status
        FROM sadie_gtm.hotel_booking_engines hbe
        JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
        WHERE hbe.booking_engine_id = $1
          AND h.country = $2
          AND h.status != -1
          AND (h.email != '' AND h.email IS NOT NULL AND h.email != ' ')
        ORDER BY h.name, h.email, h.created_at
    """, booking_engine_id, country)
    
    # Group by name + email
    groups = defaultdict(list)
    for row in rows:
        key = (row['name'], row['email_lower'])
        groups[key].append(dict(row))
    
    # Find groups with duplicates
    results = []
    for (name, email), hotels in groups.items():
        if len(hotels) < 2:
            continue
        
        # Keeper is the oldest (first created)
        keeper = hotels[0]
        duplicates = hotels[1:]
        
        # Determine what fields to merge
        merged_fields = {}
        for field in MERGEABLE_FIELDS:
            keeper_val = keeper.get(field)
            if is_real_data(keeper_val):
                continue  # Keeper already has good data
            
            # Check duplicates for good data
            for dupe in duplicates:
                dupe_val = dupe.get(field)
                if is_real_data(dupe_val):
                    merged_fields[field] = dupe_val
                    break
        
        results.append(MergeResult(
            keeper_id=keeper['id'],
            name=name,
            duplicate_ids=[d['id'] for d in duplicates],
            merged_fields=merged_fields
        ))
    
    return results


async def apply_merge(conn, result: MergeResult, dry_run: bool = False) -> None:
    """Apply the merge - update keeper with merged data, mark duplicates as -1."""
    
    if result.merged_fields:
        if dry_run:
            logger.info(f"Would merge into {result.keeper_id} ({result.name}): {result.merged_fields}")
        else:
            # Build dynamic UPDATE
            set_clauses = []
            values = []
            for i, (field, value) in enumerate(result.merged_fields.items(), start=2):
                set_clauses.append(f"{field} = ${i}")
                values.append(value)
            
            if set_clauses:
                query = f"UPDATE sadie_gtm.hotels SET {', '.join(set_clauses)} WHERE id = $1"
                await conn.execute(query, result.keeper_id, *values)
                logger.info(f"Merged into {result.keeper_id} ({result.name}): {result.merged_fields}")
    
    # Mark duplicates
    if dry_run:
        logger.info(f"Would mark {len(result.duplicate_ids)} duplicates as -1: {result.duplicate_ids}")
    else:
        await conn.execute("""
            UPDATE sadie_gtm.hotels SET status = -1 WHERE id = ANY($1)
        """, result.duplicate_ids)
        logger.debug(f"Marked {len(result.duplicate_ids)} duplicates as -1")


async def deduplicate_engine(booking_engine_id: int, engine_name: str, dry_run: bool = False):
    """Deduplicate hotels for a specific booking engine."""
    
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Deduplicating {engine_name} (ID: {booking_engine_id})...")
    
    async with get_conn() as conn:
        results = await find_duplicates(conn, booking_engine_id)
        
        if not results:
            logger.info(f"No duplicates found for {engine_name}")
            return
        
        total_dupes = sum(len(r.duplicate_ids) for r in results)
        total_merges = sum(1 for r in results if r.merged_fields)
        
        logger.info(f"Found {len(results)} duplicate groups ({total_dupes} records to mark)")
        logger.info(f"Data to merge in {total_merges} hotels")
        
        for result in results:
            await apply_merge(conn, result, dry_run)
        
        if not dry_run:
            # Verify
            remaining = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT h.name
                    FROM sadie_gtm.hotel_booking_engines hbe
                    JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
                    WHERE hbe.booking_engine_id = $1
                      AND h.country = 'United States'
                      AND h.status != -1
                      AND (h.email != '' AND h.email IS NOT NULL)
                    GROUP BY h.name
                    HAVING COUNT(*) > 1
                ) x
            """, booking_engine_id)
            logger.info(f"Remaining duplicate names after dedup: {remaining}")


async def main():
    parser = argparse.ArgumentParser(description='Deduplicate hotels by merging data')
    parser.add_argument('--engine', type=str, help='Booking engine name (e.g., cloudbeds, rms)')
    parser.add_argument('--engine-id', type=int, help='Booking engine ID')
    parser.add_argument('--all', action='store_true', help='Deduplicate all engines')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    
    args = parser.parse_args()
    
    if not args.engine and not args.engine_id and not args.all:
        parser.error('Must specify --engine, --engine-id, or --all')
    
    await init_db()
    
    try:
        async with get_conn() as conn:
            if args.all:
                # Get all engines
                engines = await conn.fetch("""
                    SELECT id, name FROM sadie_gtm.booking_engines WHERE status = 1 ORDER BY name
                """)
                for engine in engines:
                    await deduplicate_engine(engine['id'], engine['name'], args.dry_run)
            else:
                # Get specific engine
                if args.engine_id:
                    engine = await conn.fetchrow("""
                        SELECT id, name FROM sadie_gtm.booking_engines WHERE id = $1
                    """, args.engine_id)
                else:
                    engine = await conn.fetchrow("""
                        SELECT id, name FROM sadie_gtm.booking_engines WHERE LOWER(name) = LOWER($1)
                    """, args.engine)
                
                if not engine:
                    logger.error(f"Engine not found: {args.engine or args.engine_id}")
                    return
                
                await deduplicate_engine(engine['id'], engine['name'], args.dry_run)
    finally:
        await close_db()


if __name__ == '__main__':
    asyncio.run(main())
