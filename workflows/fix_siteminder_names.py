#!/usr/bin/env python3
"""
Fix SiteMinder hotel names by parsing them from URL slugs.

SiteMinder hotels were ingested from Common Crawl with garbage names like "Book Online Now"
because the pages are SPAs that load hotel names via JavaScript. The actual hotel name
can be derived from the URL slug (e.g., chaletsavoydirect.book-onlinenow.net -> "Chalet Savoy").

This script:
1. Fetches all SiteMinder hotels with garbage names
2. Parses the hotel name from the URL slug
3. Updates the database with the parsed names

Usage:
    uv run python workflows/fix_siteminder_names.py [--dry-run] [--limit N]
"""
import argparse
import asyncio
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

from typing import Optional

from loguru import logger

from db.client import get_conn

# SiteMinder booking engine ID
SITEMINDER_BOOKING_ENGINE_ID = 14

# Garbage names to fix
GARBAGE_NAMES = [
    "Book Online Now",
    "Hotel Website Builder",
]


def parse_siteminder_slug(url: str) -> Optional[str]:
    """
    Extract and parse hotel name from SiteMinder URL slug.
    
    Examples:
        chaletsavoydirect.book-onlinenow.net -> "Chalet Savoy"
        zumhoteldirect.siteminder.com -> "Zum Hotel"
        thebellinndirect.book-onlinenow.net -> "The Bell Inn"
    """
    if not url:
        return None
        
    parsed = urlparse(url)
    parts = parsed.netloc.split(".")
    if not parts:
        return None
    
    slug = parts[0].lower()
    
    # Skip single-letter or very short slugs
    if len(slug) <= 2:
        return None
    
    # Remove common suffixes
    for suffix in ["direct", "tac", "prpl"]:
        if slug.endswith(suffix):
            slug = slug[:-len(suffix)]
    
    # Skip if slug is now too short
    if len(slug) <= 2:
        return None
    
    name = slug
    
    # Insert spaces around common hotel words (case insensitive)
    # Order matters - more specific patterns first
    replacements = [
        # Hotel types - add spaces around
        (r"(boutique)", r" \1 "),
        (r"(hotel)", r" \1 "),
        (r"(inn)(?=[a-z])", r" \1 "),  # inn followed by letter
        (r"(inn)$", r" \1"),  # inn at end
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
        # Common prefixes
        (r"^the", "the "),
        # Connectors
        (r"(&)", r" & "),
        # Numbers (like "1872riverhousedirect")
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
    name = name.replace("B&B", "B&B")  # Keep B&B uppercase
    
    # Final validation - skip if result is too short or same as garbage
    if len(name) <= 3:
        return None
    if name in GARBAGE_NAMES:
        return None
        
    return name


async def get_hotels_with_garbage_names(limit: Optional[int] = None) -> list[dict]:
    """Fetch SiteMinder hotels with garbage names."""
    async with get_conn() as conn:
        # Build query with proper parameterization
        limit_clause = f" LIMIT {limit}" if limit else ""
        query = f"""
            SELECT 
                h.id AS hotel_id,
                h.name,
                hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            WHERE hbe.booking_engine_id = $1
            AND (h.name = 'Book Online Now' OR h.name = 'Hotel Website Builder')
            AND hbe.booking_url IS NOT NULL
            AND hbe.booking_url != ''
            -- Exclude generic URLs that don't have hotel slugs
            AND hbe.booking_url NOT LIKE '%www.siteminder.com%'
            AND hbe.booking_url NOT LIKE '%/canvas%'
            {limit_clause}
        """
        rows = await conn.fetch(query, SITEMINDER_BOOKING_ENGINE_ID)
        return [dict(r) for r in rows]


async def update_hotel_name(hotel_id: int, new_name: str) -> bool:
    """Update hotel name in database."""
    async with get_conn() as conn:
        await conn.execute(
            """
            UPDATE sadie_gtm.hotels
            SET name = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
            """,
            new_name,
            hotel_id,
        )
        return True


async def main(dry_run: bool = False, limit: Optional[int] = None):
    """Main entry point."""
    logger.info(f"Fetching SiteMinder hotels with garbage names...")
    
    hotels = await get_hotels_with_garbage_names(limit)
    logger.info(f"Found {len(hotels)} hotels to fix")
    
    if not hotels:
        logger.info("No hotels to fix!")
        return
    
    fixed = 0
    skipped = 0
    
    for hotel in hotels:
        hotel_id = hotel["hotel_id"]
        old_name = hotel["name"]
        booking_url = hotel["booking_url"]
        
        new_name = parse_siteminder_slug(booking_url)
        
        if not new_name:
            skipped += 1
            continue
        
        if dry_run:
            logger.info(f"[DRY RUN] Would update hotel {hotel_id}: {old_name!r} -> {new_name!r}")
        else:
            await update_hotel_name(hotel_id, new_name)
            if fixed < 10:  # Only log first 10
                logger.info(f"Updated hotel {hotel_id}: {old_name!r} -> {new_name!r}")
        
        fixed += 1
        
        if fixed % 500 == 0:
            logger.info(f"Progress: {fixed}/{len(hotels)} fixed")
    
    logger.success(f"Done! Fixed: {fixed}, Skipped: {skipped}")
    
    if dry_run:
        logger.info("This was a dry run. Run without --dry-run to apply changes.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix SiteMinder hotel names")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update database")
    parser.add_argument("--limit", type=int, help="Limit number of hotels to process")
    args = parser.parse_args()
    
    asyncio.run(main(dry_run=args.dry_run, limit=args.limit))
