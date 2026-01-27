"""Recover hotel data from web archives for 404 booking URLs.

For Cloudbeds URLs that return 404, check Common Crawl and Wayback Machine
for archived versions and extract hotel name/city/country from them.

Uses multiple sources:
1. Common Crawl - Fast, no rate limiting (primary) - uses older indexes
2. Wayback Machine - Larger archive (fallback)

Reuses CommonCrawlEnumerator from services/leadgen/booking_engines.py

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
import gzip
import json
import re
from typing import Optional, Dict, Tuple
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.leadgen.booking_engines import CommonCrawlEnumerator, CommonCrawlRecord

# Older indexes for recovering dead URLs (active 2019-2022)
ARCHIVE_INDEXES = [
    "CC-MAIN-2020-34",
    "CC-MAIN-2021-04",
    "CC-MAIN-2019-51",
    "CC-MAIN-2022-05",
    "CC-MAIN-2020-16",
]


async def get_common_crawl_html(url: str, client: httpx.AsyncClient) -> Optional[str]:
    """Fetch HTML from Common Crawl older archives (for dead URLs)."""
    for crawl_id in ARCHIVE_INDEXES:
        try:
            resp = await client.get(
                f"https://index.commoncrawl.org/{crawl_id}-index",
                params={"url": url, "output": "json"},
                timeout=15,
            )
            if resp.status_code != 200 or not resp.text.strip():
                continue
            
            data = json.loads(resp.text.strip().split("\n")[0])
            if data.get("status") != "200":
                continue
            
            # Fetch WARC - reuse CommonCrawlEnumerator logic
            warc_url = f"https://data.commoncrawl.org/{data['filename']}"
            offset = int(data["offset"])
            length = int(data["length"])
            headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
            
            resp2 = await client.get(warc_url, headers=headers, timeout=60)
            if resp2.status_code == 206:
                content = gzip.decompress(resp2.content)
                return content.decode("utf-8", errors="ignore")
                
        except Exception as e:
            logger.debug(f"CC {crawl_id} error: {e}")
            continue
    
    return None


async def get_wayback_url(url: str, client: httpx.AsyncClient) -> Optional[str]:
    """Check Wayback Machine for archive."""
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
        logger.debug(f"Wayback error: {e}")
    return None


def extract_from_html(html: str) -> Dict[str, str]:
    """Extract hotel data from HTML content."""
    result = {}
    
    try:
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
            logger.debug("No title found in HTML")
        
        # Try to extract address from older Cloudbeds format
        # Format: "Address 1:</span> Street Address</p>"
        addr_match = re.search(r'Address\s*\d?:</span>\s*([^<]+)</p>', html)
        if addr_match:
            addr = addr_match.group(1).strip()
            if addr and len(addr) > 3:
                result['address'] = addr
        
        # Extract city from older format: "City:</span> Milan - Milano</p>"  
        city_match = re.search(r'City\s*:</span>\s*([^<]+)</p>', html)
        if city_match and not result.get('city'):
            city_text = city_match.group(1).strip()
            # Handle "Milan - Milano" format, take first part
            city_text = city_text.split(' - ')[0].strip()
            if city_text:
                result['city'] = city_text
        
        # Fallback: Try data-be-text for newer archives
        if not result.get('address') and 'data-be-text="true"' in html:
            be_texts = re.findall(r'data-be-text="true"[^>]*>([^<]+)<', html)
            be_texts = [t.strip() for t in be_texts if t.strip() and len(t.strip()) > 3 
                       and t.strip() not in ['*', 'Address', 'Contact', 'Phone', 'Email']]
            if be_texts:
                result['address'] = be_texts[0]
                
    except Exception as e:
        logger.debug(f"Error extracting from HTML: {e}")
    
    return result


async def get_archived_html(url: str, client: httpx.AsyncClient) -> Tuple[Optional[str], str]:
    """Try to get archived HTML from Common Crawl or Wayback Machine.
    
    Returns (html, source) where source is 'commoncrawl', 'wayback', or 'none'.
    """
    # Try Common Crawl first (faster, no rate limiting)
    html = await get_common_crawl_html(url, client)
    if html:
        return html, "commoncrawl"
    
    # Fall back to Wayback Machine
    wayback_url = await get_wayback_url(url, client)
    if wayback_url:
        # Fetch with retry for rate limits
        for attempt in range(3):
            try:
                resp = await client.get(wayback_url, timeout=30, follow_redirects=True)
                if resp.status_code == 503:
                    wait_time = 2 ** attempt
                    logger.debug(f"Wayback rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                if resp.status_code == 200:
                    return resp.text, "wayback"
                break
            except Exception as e:
                logger.debug(f"Wayback fetch error: {e}")
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
    
    return None, "none"


async def run(limit: int = 100, dry_run: bool = False):
    """Recover hotel data from web archives (Common Crawl + Wayback)."""
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
                logger.info("No hotels need archive recovery")
                return 0
            
            logger.info(f"Found {len(rows)} hotels to check in web archives")
            
            if dry_run:
                logger.info("Dry run - not updating database")
            
            recovered = 0
            not_archived = 0
            from_cc = 0
            from_wb = 0
            
            async with httpx.AsyncClient() as client:
                for row in rows:
                    hotel_id = row['id']
                    booking_url = row['booking_url']
                    
                    # Try Common Crawl first, then Wayback
                    html, source = await get_archived_html(booking_url, client)
                    
                    if not html:
                        not_archived += 1
                        logger.debug(f"  Hotel {hotel_id}: no archive")
                        continue
                    
                    # Extract data
                    data = extract_from_html(html)
                    
                    if not data.get('name'):
                        not_archived += 1
                        logger.debug(f"  Hotel {hotel_id}: archive found but no data")
                        continue
                    
                    logger.info(f"  Hotel {hotel_id} [{source}]: recovered name={data.get('name')}, city={data.get('city')}, address={data.get('address')}")
                    recovered += 1
                    if source == "commoncrawl":
                        from_cc += 1
                    else:
                        from_wb += 1
                    
                    if not dry_run:
                        await conn.execute('''
                            UPDATE sadie_gtm.hotels
                            SET name = $1,
                                city = COALESCE($2, city),
                                country = COALESCE($3, country),
                                address = COALESCE($4, address),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = $5
                        ''', data.get('name'), data.get('city'), data.get('country'), data.get('address'), hotel_id)
                    
                    # Only rate limit for Wayback (Common Crawl has no limits)
                    if source == "wayback":
                        await asyncio.sleep(2)
            
            logger.info(f"Done: {recovered} recovered ({from_cc} Common Crawl, {from_wb} Wayback), {not_archived} not archived")
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
