"""Fix external_id values that contain URLs instead of just the ID.

Usage:
    # Dry run - show what would be fixed
    uv run python -m workflows.fix_external_ids --dry-run
    
    # Run the fix
    uv run python -m workflows.fix_external_ids
"""

import asyncio
import argparse
import re
from typing import Optional
from loguru import logger

from db.client import init_db, close_db, get_conn


def extract_cloudbeds_code(url_or_id: str) -> Optional[str]:
    """Extract property code from Cloudbeds URL or return as-is if already a code.
    
    Examples:
        hotels.cloudbeds.com/reservation/ddku4z -> ddku4z
        https://hotels.cloudbeds.com/reservation/ddku4z -> ddku4z
        ddku4z -> ddku4z
    """
    if not url_or_id:
        return None
    
    # If it contains cloudbeds.com, extract the code
    if 'cloudbeds.com' in url_or_id:
        match = re.search(r'/(?:reservation|booking)/([a-zA-Z0-9]{2,10})(?:/|$|\?)', url_or_id)
        if match:
            code = match.group(1)
            # Skip invalid codes
            if code.lower() in ('hotels', 'www', 'booking', 'reservation'):
                return None
            return code
        return None
    
    # If it contains a slash, it's probably a URL path - extract last segment
    if '/' in url_or_id:
        parts = url_or_id.rstrip('/').split('/')
        code = parts[-1]
        if code and re.match(r'^[a-zA-Z0-9]{2,10}$', code):
            return code
        return None
    
    # Already a code
    return url_or_id


async def get_bad_external_ids():
    """Find external_ids that look like URLs."""
    async with get_conn() as conn:
        rows = await conn.fetch("""
            SELECT id, external_id, external_id_type
            FROM sadie_gtm.hotels 
            WHERE external_id LIKE '%/%'
              AND external_id_type = 'cloudbeds_crawl'
        """)
        return rows


async def fix_external_id(hotel_id: int, new_external_id: str, external_id_type: str) -> bool:
    """Update external_id for a hotel. Returns False if duplicate exists."""
    async with get_conn() as conn:
        # Check if the new ID already exists for this type
        existing = await conn.fetchrow("""
            SELECT id FROM sadie_gtm.hotels 
            WHERE external_id = $1 AND external_id_type = $2 AND id != $3
        """, new_external_id, external_id_type, hotel_id)
        
        if existing:
            # Duplicate - mark this one as -1 (it's a dupe)
            await conn.execute("""
                UPDATE sadie_gtm.hotels 
                SET status = -1, updated_at = NOW()
                WHERE id = $1
            """, hotel_id)
            return False
        
        await conn.execute("""
            UPDATE sadie_gtm.hotels 
            SET external_id = $1, updated_at = NOW()
            WHERE id = $2
        """, new_external_id, hotel_id)
        return True


async def run(dry_run: bool = False):
    """Run the external_id fix."""
    await init_db()
    
    print("=" * 60)
    print("FIX EXTERNAL IDS")
    print("=" * 60)
    
    rows = await get_bad_external_ids()
    print(f"\nFound {len(rows)} external_ids that look like URLs")
    
    if not rows:
        print("Nothing to fix!")
        await close_db()
        return
    
    fixed = 0
    unfixable = 0
    duplicates = 0
    
    for row in rows:
        hotel_id = row['id']
        old_id = row['external_id']
        id_type = row['external_id_type']
        new_id = extract_cloudbeds_code(old_id)
        
        if new_id and new_id != old_id:
            if dry_run:
                print(f"  {hotel_id}: '{old_id}' -> '{new_id}'")
                fixed += 1
            else:
                success = await fix_external_id(hotel_id, new_id, id_type)
                if success:
                    fixed += 1
                else:
                    duplicates += 1
                    print(f"  {hotel_id}: duplicate, marked as status=-1")
        else:
            unfixable += 1
            if dry_run:
                print(f"  {hotel_id}: '{old_id}' -> UNFIXABLE")
    
    print(f"\n{'Would fix' if dry_run else 'Fixed'}: {fixed}")
    print(f"Unfixable (invalid codes): {unfixable}")
    if not dry_run:
        print(f"Duplicates (marked status=-1): {duplicates}")
    
    await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix external_id URLs to just IDs")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed")
    
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))
