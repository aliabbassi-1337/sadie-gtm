"""Recover hotel data from Wayback Machine for 404 booking URLs.

For Cloudbeds URLs that return 404, check if Wayback Machine has an
archived version and extract hotel name/city/country from it.

Usage:
    uv run python -m workflows.enrich_wayback --limit 100
    uv run python -m workflows.enrich_wayback --dry-run
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import httpx
import re
from typing import Optional, Dict
from loguru import logger

from db.client import init_db, close_db, get_conn


async def get_wayback_url(url: str, client: httpx.AsyncClient) -> Optional[str]:
    """Check if URL has a Wayback Machine archive."""
    try:
        resp = await client.get(
            "https://archive.org/wayback/available",
            params={"url": url},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            snapshots = data.get("archived_snapshots", {})
            closest = snapshots.get("closest", {})
            if closest.get("available"):
                return closest.get("url")
    except Exception as e:
        logger.debug(f"Wayback API error for {url}: {e}")
    return None


async def extract_from_wayback(wayback_url: str, client: httpx.AsyncClient) -> Dict[str, str]:
    """Extract hotel data from Wayback Machine page with retry for rate limits."""
    result = {}
    resp = None
    
    # Retry with backoff for 503 errors
    for attempt in range(3):
        try:
            resp = await client.get(wayback_url, timeout=30, follow_redirects=True)
            if resp.status_code == 503:
                wait_time = 2 ** attempt  # 1, 2, 4 seconds
                logger.debug(f"Rate limited, waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            if resp.status_code != 200:
                logger.debug(f"Wayback returned {resp.status_code}")
                return result
            break
        except Exception as e:
            logger.debug(f"Request error: {e}")
            if attempt < 2:
                await asyncio.sleep(1)
                continue
            return result
    else:
        return result  # All retries failed
    
    # Extract from HTML
    try:
        html = resp.text
        
        # Extract from title: "Hotel Name - City, Country - Best Price Guarantee"
        title_match = re.search(r'<title>([^<]+)</title>', html)
        if title_match:
            title = title_match.group(1).strip()
            
            # Skip garbage titles
            if 'cloudbeds' in title.lower() or 'soluções' in title.lower():
                logger.debug(f"Garbage title: {title}")
                return result
            
            # Remove "- Best Price Guarantee" suffix
            title = re.sub(r'\s*-\s*Best Price Guarantee.*$', '', title, flags=re.I)
            
            parts = title.split(' - ')
            if len(parts) >= 2:
                result['name'] = parts[0].strip()
                location = parts[1].strip()
                loc_parts = location.split(',')
                if len(loc_parts) >= 2:
                    result['city'] = loc_parts[0].strip()
                    result['country'] = loc_parts[-1].strip()
                elif len(loc_parts) == 1:
                    result['city'] = loc_parts[0].strip()
            elif len(parts) == 1 and title:
                result['name'] = title
        else:
            logger.debug(f"No title found in HTML")
                
    except Exception as e:
        logger.debug(f"Error extracting from {wayback_url}: {e}")
    
    return result


async def run(limit: int = 100, dry_run: bool = False):
    """Recover hotel data from Wayback Machine."""
    await init_db()
    
    try:
        async with get_conn() as conn:
            # Get hotels with garbage Cloudbeds data
            rows = await conn.fetch('''
                SELECT h.id, hbe.booking_url
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                WHERE h.name LIKE '%Cloudbeds%'
                   OR h.name LIKE '%cloudbeds%'
                ORDER BY h.id
                LIMIT $1
            ''', limit)
            
            if not rows:
                logger.info("No hotels need Wayback recovery")
                return 0
            
            logger.info(f"Found {len(rows)} hotels to check in Wayback Machine")
            
            if dry_run:
                logger.info("Dry run - not updating database")
            
            recovered = 0
            not_archived = 0
            
            async with httpx.AsyncClient() as client:
                for row in rows:
                    hotel_id = row['id']
                    booking_url = row['booking_url']
                    
                    # Check Wayback
                    wayback_url = await get_wayback_url(booking_url, client)
                    
                    if not wayback_url:
                        not_archived += 1
                        logger.debug(f"  Hotel {hotel_id}: no archive")
                        continue
                    
                    # Extract data
                    data = await extract_from_wayback(wayback_url, client)
                    
                    if not data.get('name'):
                        not_archived += 1
                        logger.debug(f"  Hotel {hotel_id}: archive found but no data")
                        continue
                    
                    logger.info(f"  Hotel {hotel_id}: recovered name={data.get('name')}, city={data.get('city')}")
                    recovered += 1
                    
                    if not dry_run:
                        await conn.execute('''
                            UPDATE sadie_gtm.hotels
                            SET name = $1,
                                city = COALESCE($2, city),
                                country = COALESCE($3, country),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = $4
                        ''', data.get('name'), data.get('city'), data.get('country'), hotel_id)
                    
                    # Rate limit - be nice to Wayback (they rate limit aggressively)
                    await asyncio.sleep(2)
            
            logger.info(f"Done: {recovered} recovered, {not_archived} not archived")
            return recovered
            
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Recover hotel data from Wayback Machine")
    parser.add_argument("--limit", type=int, default=100, help="Max hotels to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
