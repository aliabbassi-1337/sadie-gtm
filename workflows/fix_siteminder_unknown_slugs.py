#!/usr/bin/env python3
"""
Fix SiteMinder hotel names with "Unknown (slug)" format.

These hotels have names like "Unknown (1028kerlerecstreetdirect)" where the slug
contains the actual hotel name that can be parsed.

This script:
1. Fetches all SiteMinder hotels with "Unknown (...)" names
2. Extracts and parses the slug from the name
3. Updates the database with the parsed names

Usage:
    uv run python workflows/fix_siteminder_unknown_slugs.py [--dry-run] [--limit N]
"""
import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

from db.client import get_conn

SITEMINDER_BOOKING_ENGINE_ID = 14


def parse_slug_to_name(slug: str) -> Optional[str]:
    """
    Parse a hotel name from a slug.
    
    Examples:
        1028kerlerecstreetdirect -> "1028 Kerlerec Street"
        thebellinndirect -> "The Bell Inn"
        chaletsavoydirect -> "Chalet Savoy"
    """
    if not slug or len(slug) <= 3:
        return None
    
    slug = slug.lower().strip()
    
    # Remove common suffixes
    for suffix in ["direct", "tac", "prpl", "bookings"]:
        if slug.endswith(suffix):
            slug = slug[:-len(suffix)]
    
    # Skip if slug is now too short
    if len(slug) <= 2:
        return None
    
    name = slug
    
    # Insert spaces around common hotel words
    replacements = [
        # Hotel types
        (r"(boutique)", r" \1 "),
        (r"(hotel)", r" \1 "),
        (r"(inn)(?=[a-z])", r" \1 "),
        (r"(inn)$", r" \1"),
        (r"(lodge)", r" \1 "),
        (r"(motel)", r" \1 "),
        (r"(hostel)", r" \1 "),
        (r"(house)", r" \1 "),
        (r"(resort)", r" \1 "),
        (r"(suites?)", r" \1 "),
        (r"(manor)", r" \1 "),
        (r"(castle)", r" \1 "),
        (r"(palace)", r" \1 "),
        (r"(villa)", r" \1 "),
        (r"(chalet)", r"\1 "),
        (r"(cabins?)", r" \1 "),
        (r"(cottages?)", r" \1 "),
        (r"(bnb|b&b)", r" B&B "),
        (r"(apartments?)", r" \1 "),
        (r"(studios?)", r" \1 "),
        (r"(guest)", r" \1 "),
        (r"(beach)", r" \1 "),
        (r"(lake)", r" \1 "),
        (r"(mountain)", r" \1 "),
        (r"(river)", r" \1 "),
        (r"(view)", r" \1 "),
        (r"(park)", r" \1 "),
        (r"(street)", r" \1 "),
        (r"(avenue)", r" \1 "),
        (r"(road)", r" \1 "),
        # Common prefixes
        (r"^the", "the "),
        # Connectors
        (r"(&)", r" & "),
        # Numbers (like "1028kerlerec")
        (r"(\d+)", r"\1 "),
    ]
    
    for pattern, replacement in replacements:
        name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)
    
    # Clean up multiple spaces
    name = " ".join(name.split())
    
    # Title case
    name = name.title()
    
    # Fix common title case issues
    name = name.replace(" And ", " and ")
    name = name.replace("B&B", "B&B")
    
    # Final validation
    if len(name) <= 3:
        return None
    if name.lower() in ["unknown", "book online now", "hotel website builder"]:
        return None
        
    return name


def extract_slug_from_name(name: str) -> Optional[str]:
    """
    Extract slug from "Unknown (slug)" format.
    
    Example: "Unknown (1028kerlerecstreetdirect)" -> "1028kerlerecstreetdirect"
    """
    if not name:
        return None
    
    match = re.match(r"Unknown\s*\(([^)]+)\)", name, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


async def get_unknown_slug_hotels(limit: Optional[int] = None) -> List[dict]:
    """Fetch SiteMinder hotels with Unknown (slug) names."""
    async with get_conn() as conn:
        limit_clause = f" LIMIT {limit}" if limit else ""
        query = f"""
            SELECT 
                h.id AS hotel_id,
                h.name
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            WHERE hbe.booking_engine_id = $1
            AND h.name LIKE 'Unknown (%'
            AND h.status = 1
            {limit_clause}
        """
        rows = await conn.fetch(query, SITEMINDER_BOOKING_ENGINE_ID)
        return [dict(r) for r in rows]


async def batch_update_hotel_names(updates: List[Tuple[str, int]]) -> int:
    """Batch update hotel names."""
    if not updates:
        return 0
    
    async with get_conn() as conn:
        await conn.executemany(
            """
            UPDATE sadie_gtm.hotels
            SET name = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
            """,
            updates,
        )
        return len(updates)


async def main(dry_run: bool = False, limit: Optional[int] = None):
    """Main entry point."""
    logger.info("Fetching SiteMinder hotels with 'Unknown (slug)' names...")
    
    hotels = await get_unknown_slug_hotels(limit)
    logger.info(f"Found {len(hotels)} hotels to process")
    
    if not hotels:
        logger.info("No hotels to fix!")
        return
    
    updates = []
    skipped = 0
    
    for hotel in hotels:
        hotel_id = hotel["hotel_id"]
        current_name = hotel["name"]
        
        # Extract slug from name
        slug = extract_slug_from_name(current_name)
        if not slug:
            skipped += 1
            continue
        
        # Parse slug to get real name
        new_name = parse_slug_to_name(slug)
        if not new_name:
            skipped += 1
            continue
        
        updates.append((new_name, hotel_id))
        
        if len(updates) <= 5:
            logger.info(f"Sample: {current_name!r} -> {new_name!r}")
    
    logger.info(f"Parsed {len(updates)} names, {skipped} skipped")
    
    if dry_run:
        for new_name, hotel_id in updates[:10]:
            logger.info(f"[DRY RUN] Would update hotel {hotel_id} -> {new_name!r}")
        if len(updates) > 10:
            logger.info(f"... and {len(updates) - 10} more")
        logger.success(f"Done! Would fix: {len(updates)}, Skipped: {skipped}")
        return
    
    # Batch update
    BATCH_SIZE = 1000
    total_updated = 0
    
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        count = await batch_update_hotel_names(batch)
        total_updated += count
        logger.info(f"Progress: {total_updated}/{len(updates)} updated")
    
    logger.success(f"Done! Fixed: {total_updated}, Skipped: {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix SiteMinder Unknown (slug) names")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument("--limit", type=int, help="Limit number of hotels")
    args = parser.parse_args()
    
    asyncio.run(main(dry_run=args.dry_run, limit=args.limit))
