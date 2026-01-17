#!/usr/bin/env python3
"""
Deduplicate hotels in the database.

Uses 3-tier deduplication logic:
1. Google Place ID (primary - globally unique)
2. Location (secondary - ~11m precision)
3. Name + Website (tertiary - fallback)

Marks duplicates with status=-3 (DUPLICATE).

Usage:
    # Dry run - show what would be marked as duplicates
    uv run python -m workflows.deduplicate --dry-run

    # Run deduplication
    uv run python -m workflows.deduplicate

    # Only deduplicate specific state
    uv run python -m workflows.deduplicate --state FL

    # Show stats only
    uv run python -m workflows.deduplicate --stats
"""

import argparse
import asyncio
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.client import init_db, close_db, get_conn


# New status for duplicates
STATUS_DUPLICATE = -3


async def get_duplicate_stats() -> Dict:
    """Get statistics about potential duplicates."""
    async with get_conn() as conn:
        # Total hotels
        total = await conn.fetchval("SELECT COUNT(*) FROM hotels")
        
        # Already marked as duplicates
        duplicates_marked = await conn.fetchval(
            "SELECT COUNT(*) FROM hotels WHERE status = $1", STATUS_DUPLICATE
        )
        
        # Hotels with google_place_id
        with_place_id = await conn.fetchval(
            "SELECT COUNT(*) FROM hotels WHERE google_place_id IS NOT NULL"
        )
        
        # Duplicate place IDs (same place_id appears multiple times)
        dup_place_ids = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT google_place_id, COUNT(*) as cnt
                FROM hotels
                WHERE google_place_id IS NOT NULL
                  AND status != $1
                GROUP BY google_place_id
                HAVING COUNT(*) > 1
            ) t
        """, STATUS_DUPLICATE)
        
        # Hotels affected by duplicate place IDs
        hotels_with_dup_place_id = await conn.fetchval("""
            SELECT COUNT(*) FROM hotels
            WHERE google_place_id IN (
                SELECT google_place_id
                FROM hotels
                WHERE google_place_id IS NOT NULL
                  AND status != $1
                GROUP BY google_place_id
                HAVING COUNT(*) > 1
            )
            AND status != $1
        """, STATUS_DUPLICATE)
        
        # Duplicate locations (same lat/lng rounded to 4 decimals)
        dup_locations = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT 
                    ROUND(ST_Y(location::geometry)::numeric, 4) as lat,
                    ROUND(ST_X(location::geometry)::numeric, 4) as lng,
                    COUNT(*) as cnt
                FROM hotels
                WHERE location IS NOT NULL
                  AND google_place_id IS NULL
                  AND status != $1
                GROUP BY lat, lng
                HAVING COUNT(*) > 1
            ) t
        """, STATUS_DUPLICATE)
        
        return {
            "total_hotels": total,
            "already_marked_duplicate": duplicates_marked,
            "with_google_place_id": with_place_id,
            "duplicate_place_id_groups": dup_place_ids,
            "hotels_with_duplicate_place_id": hotels_with_dup_place_id,
            "duplicate_location_groups": dup_locations,
        }


async def find_duplicates_by_place_id(
    state: Optional[str] = None,
    limit: int = 10000,
) -> List[Tuple[int, str, str]]:
    """Find duplicate hotels by Google Place ID.
    
    Returns list of (hotel_id, google_place_id, name) for duplicates to mark.
    Keeps the hotel with highest rating (or earliest created_at if tied).
    """
    state_filter = "AND state = $2" if state else ""
    params = [STATUS_DUPLICATE, state] if state else [STATUS_DUPLICATE]
    
    query = f"""
        WITH ranked AS (
            SELECT 
                id,
                google_place_id,
                name,
                rating,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY google_place_id 
                    ORDER BY 
                        COALESCE(rating, 0) DESC,
                        created_at ASC
                ) as rn
            FROM hotels
            WHERE google_place_id IS NOT NULL
              AND status != $1
              {state_filter}
        )
        SELECT id, google_place_id, name
        FROM ranked
        WHERE rn > 1
        LIMIT {limit}
    """
    
    async with get_conn() as conn:
        rows = await conn.fetch(query, *params)
        return [(r['id'], r['google_place_id'], r['name']) for r in rows]


async def find_duplicates_by_location(
    state: Optional[str] = None,
    limit: int = 10000,
) -> List[Tuple[int, float, float, str]]:
    """Find duplicate hotels by location (for hotels without google_place_id).
    
    Returns list of (hotel_id, lat, lng, name) for duplicates to mark.
    Keeps the hotel with highest rating (or earliest created_at if tied).
    """
    state_filter = "AND state = $2" if state else ""
    params = [STATUS_DUPLICATE, state] if state else [STATUS_DUPLICATE]
    
    query = f"""
        WITH ranked AS (
            SELECT 
                id,
                name,
                rating,
                created_at,
                ROUND(ST_Y(location::geometry)::numeric, 4) as lat,
                ROUND(ST_X(location::geometry)::numeric, 4) as lng,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        ROUND(ST_Y(location::geometry)::numeric, 4),
                        ROUND(ST_X(location::geometry)::numeric, 4)
                    ORDER BY 
                        COALESCE(rating, 0) DESC,
                        created_at ASC
                ) as rn
            FROM hotels
            WHERE location IS NOT NULL
              AND google_place_id IS NULL
              AND status != $1
              {state_filter}
        )
        SELECT id, lat, lng, name
        FROM ranked
        WHERE rn > 1
        LIMIT {limit}
    """
    
    async with get_conn() as conn:
        rows = await conn.fetch(query, *params)
        return [(r['id'], float(r['lat']), float(r['lng']), r['name']) for r in rows]


async def find_duplicates_by_name(
    state: Optional[str] = None,
    limit: int = 10000,
) -> List[Tuple[int, str, str]]:
    """Find duplicate hotels by name+website (for hotels without place_id or location).
    
    Returns list of (hotel_id, name, website) for duplicates to mark.
    Keeps the hotel with highest rating (or earliest created_at if tied).
    """
    state_filter = "AND state = $2" if state else ""
    params = [STATUS_DUPLICATE, state] if state else [STATUS_DUPLICATE]
    
    query = f"""
        WITH ranked AS (
            SELECT 
                id,
                name,
                COALESCE(website, '') as website,
                rating,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY LOWER(name), LOWER(COALESCE(website, ''))
                    ORDER BY 
                        COALESCE(rating, 0) DESC,
                        created_at ASC
                ) as rn
            FROM hotels
            WHERE google_place_id IS NULL
              AND location IS NULL
              AND status != $1
              {state_filter}
        )
        SELECT id, name, website
        FROM ranked
        WHERE rn > 1
        LIMIT {limit}
    """
    
    async with get_conn() as conn:
        rows = await conn.fetch(query, *params)
        return [(r['id'], r['name'], r['website']) for r in rows]


async def mark_duplicates(hotel_ids: List[int]) -> int:
    """Mark hotels as duplicates (status=-3)."""
    if not hotel_ids:
        return 0
    
    async with get_conn() as conn:
        result = await conn.execute(
            "UPDATE hotels SET status = $1, updated_at = NOW() WHERE id = ANY($2)",
            STATUS_DUPLICATE, hotel_ids
        )
        # Extract count from "UPDATE N"
        return int(result.split()[-1])


async def run_deduplication(
    state: Optional[str] = None,
    dry_run: bool = False,
    batch_size: int = 1000,
) -> Dict:
    """Run full deduplication process.
    
    Returns stats about what was deduplicated.
    """
    stats = {
        "place_id_duplicates": 0,
        "location_duplicates": 0,
        "name_duplicates": 0,
        "total_marked": 0,
    }
    
    state_msg = f" for {state}" if state else ""
    
    # Tier 1: Google Place ID duplicates
    logger.info(f"Finding duplicates by Google Place ID{state_msg}...")
    place_id_dups = await find_duplicates_by_place_id(state)
    stats["place_id_duplicates"] = len(place_id_dups)
    
    if place_id_dups:
        logger.info(f"  Found {len(place_id_dups)} duplicates by Place ID")
        if not dry_run:
            for i in range(0, len(place_id_dups), batch_size):
                batch = place_id_dups[i:i + batch_size]
                ids = [d[0] for d in batch]
                marked = await mark_duplicates(ids)
                stats["total_marked"] += marked
                logger.info(f"  Marked batch {i//batch_size + 1}: {marked} hotels")
        else:
            # Show sample in dry run
            for hotel_id, place_id, name in place_id_dups[:10]:
                logger.info(f"    [DRY RUN] Would mark #{hotel_id}: {name} (placeId: {place_id[:20]}...)")
            if len(place_id_dups) > 10:
                logger.info(f"    ... and {len(place_id_dups) - 10} more")
    
    # Tier 2: Location duplicates (for hotels without place_id)
    logger.info(f"Finding duplicates by location{state_msg}...")
    location_dups = await find_duplicates_by_location(state)
    stats["location_duplicates"] = len(location_dups)
    
    if location_dups:
        logger.info(f"  Found {len(location_dups)} duplicates by location")
        if not dry_run:
            for i in range(0, len(location_dups), batch_size):
                batch = location_dups[i:i + batch_size]
                ids = [d[0] for d in batch]
                marked = await mark_duplicates(ids)
                stats["total_marked"] += marked
                logger.info(f"  Marked batch {i//batch_size + 1}: {marked} hotels")
        else:
            for hotel_id, lat, lng, name in location_dups[:10]:
                logger.info(f"    [DRY RUN] Would mark #{hotel_id}: {name} ({lat:.4f}, {lng:.4f})")
            if len(location_dups) > 10:
                logger.info(f"    ... and {len(location_dups) - 10} more")
    
    # Tier 3: Name duplicates (for hotels without place_id or location)
    logger.info(f"Finding duplicates by name{state_msg}...")
    name_dups = await find_duplicates_by_name(state)
    stats["name_duplicates"] = len(name_dups)
    
    if name_dups:
        logger.info(f"  Found {len(name_dups)} duplicates by name")
        if not dry_run:
            for i in range(0, len(name_dups), batch_size):
                batch = name_dups[i:i + batch_size]
                ids = [d[0] for d in batch]
                marked = await mark_duplicates(ids)
                stats["total_marked"] += marked
                logger.info(f"  Marked batch {i//batch_size + 1}: {marked} hotels")
        else:
            for hotel_id, name, website in name_dups[:10]:
                logger.info(f"    [DRY RUN] Would mark #{hotel_id}: {name} ({website or 'no website'})")
            if len(name_dups) > 10:
                logger.info(f"    ... and {len(name_dups) - 10} more")
    
    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate hotels in the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be deduplicated
  uv run python -m workflows.deduplicate --dry-run

  # Run deduplication
  uv run python -m workflows.deduplicate

  # Only deduplicate Florida hotels
  uv run python -m workflows.deduplicate --state FL

  # Just show stats
  uv run python -m workflows.deduplicate --stats
        """
    )
    
    parser.add_argument("--state", help="Only deduplicate hotels in this state")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deduplicated without making changes")
    parser.add_argument("--stats", action="store_true", help="Just show duplicate statistics")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    await init_db()
    
    try:
        if args.stats:
            stats = await get_duplicate_stats()
            logger.info("=" * 60)
            logger.info("Duplicate Statistics")
            logger.info("=" * 60)
            logger.info(f"Total hotels:                    {stats['total_hotels']:,}")
            logger.info(f"Already marked as duplicate:     {stats['already_marked_duplicate']:,}")
            logger.info(f"With Google Place ID:            {stats['with_google_place_id']:,}")
            logger.info(f"Duplicate Place ID groups:       {stats['duplicate_place_id_groups']:,}")
            logger.info(f"Hotels with duplicate Place ID:  {stats['hotels_with_duplicate_place_id']:,}")
            logger.info(f"Duplicate location groups:       {stats['duplicate_location_groups']:,}")
            return
        
        state = args.state.upper() if args.state else None
        
        logger.info("=" * 60)
        if args.dry_run:
            logger.info("Deduplication DRY RUN")
        else:
            logger.info("Running Deduplication")
        logger.info("=" * 60)
        logger.info(f"State filter: {state or 'All states'}")
        logger.info(f"Duplicate status: {STATUS_DUPLICATE}")
        logger.info("")
        
        stats = await run_deduplication(
            state=state,
            dry_run=args.dry_run,
        )
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("Summary")
        logger.info("=" * 60)
        logger.info(f"Place ID duplicates found:  {stats['place_id_duplicates']:,}")
        logger.info(f"Location duplicates found:  {stats['location_duplicates']:,}")
        logger.info(f"Name duplicates found:      {stats['name_duplicates']:,}")
        if not args.dry_run:
            logger.info(f"Total marked as duplicate:  {stats['total_marked']:,}")
        else:
            total = stats['place_id_duplicates'] + stats['location_duplicates'] + stats['name_duplicates']
            logger.info(f"Would mark as duplicate:    {total:,}")
        
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
