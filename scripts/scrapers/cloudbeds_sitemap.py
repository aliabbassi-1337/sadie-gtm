#!/usr/bin/env python3
"""
Cloudbeds Sitemap Scraper - Enumerate all Cloudbeds properties via their sitemap.

The Cloudbeds sitemap at https://hotels.cloudbeds.com/sitemap.xml contains 
2,713 property subdomains. Each subdomain is a hotel or hotel group using Cloudbeds.

This script fetches the sitemap, visits each subdomain, and extracts:
- Hotel/group name from <title> tag
- Booking URL

Usage:
    uv run python scripts/scrapers/cloudbeds_sitemap.py --max 50  # Test with 50
    uv run python scripts/scrapers/cloudbeds_sitemap.py --output cloudbeds_hotels.json
    uv run python scripts/scrapers/cloudbeds_sitemap.py --save-db
"""

import argparse
import asyncio
import json
import re
import sys
import xml.etree.ElementTree as ET
from typing import List, Optional
from dataclasses import dataclass, asdict

import httpx
from loguru import logger


SITEMAP_URL = "https://hotels.cloudbeds.com/sitemap.xml"

# Skip these known non-property subdomains
SKIP_SUBDOMAINS = {
    'hotels', 'api', 'static1', 'static2', 'static3',
    'h-img1', 'h-img2', 'h-img3', 'www', 'mybookings',
    'tracking', 'booking-tracking', 'maps-a', 'maps-b', 'maps-c',
}


@dataclass
class CloudbedsLead:
    """A Cloudbeds hotel/group discovered from sitemap."""
    subdomain: str
    name: str
    booking_url: str
    is_demo: bool = False  # Has [DEMO] or [DoNotUse] in name


async def fetch_sitemap() -> List[str]:
    """Fetch the Cloudbeds sitemap and extract subdomains."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(SITEMAP_URL)
        resp.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(resp.text)
        
        # Extract URLs - namespace handling
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        subdomains = []
        
        for url_elem in root.findall('.//sm:loc', ns):
            url = url_elem.text
            if url:
                match = re.match(r'https://([^.]+)\.cloudbeds\.com', url)
                if match:
                    subdomain = match.group(1).lower()
                    if subdomain not in SKIP_SUBDOMAINS:
                        subdomains.append(subdomain)
        
        return list(set(subdomains))


async def scrape_subdomain(
    subdomain: str,
    client: httpx.AsyncClient,
    delay: float = 0.5,
) -> Optional[CloudbedsLead]:
    """Fetch subdomain page and extract hotel name from title."""
    import random
    
    url = f"https://{subdomain}.cloudbeds.com/"

    try:
        # Add jittered delay to be polite
        await asyncio.sleep(delay + random.uniform(0, delay * 0.5))
        
        resp = await client.get(url, follow_redirects=True, timeout=10.0)

        if resp.status_code == 429:  # Rate limited
            logger.warning(f"Rate limited on {subdomain}, backing off...")
            await asyncio.sleep(30)
            resp = await client.get(url, follow_redirects=True, timeout=10.0)

        if resp.status_code != 200:
            return None

        html = resp.text

        # Extract name from title
        name_match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
        if not name_match:
            return None

        name = name_match.group(1).strip()

        # Skip error pages
        if 'Page Not Found' in name or 'Cloudbeds.com -' in name:
            return None

        # Check if demo/test account
        is_demo = '[DEMO]' in name or '[DoNotUse]' in name or 'demo' in subdomain.lower()

        return CloudbedsLead(
            subdomain=subdomain,
            name=name,
            booking_url=url,
            is_demo=is_demo,
        )

    except Exception as e:
        logger.debug(f"Error scraping {subdomain}: {e}")
        return None


async def main():
    parser = argparse.ArgumentParser(
        description="Scrape Cloudbeds hotels from sitemap (2,700+ leads)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test with 50 subdomains
    uv run python scripts/scrapers/cloudbeds_sitemap.py --max 50

    # Full scrape to JSON
    uv run python scripts/scrapers/cloudbeds_sitemap.py --output cloudbeds_hotels.json

    # Exclude demo accounts
    uv run python scripts/scrapers/cloudbeds_sitemap.py --no-demos --output leads.json
"""
    )
    parser.add_argument("--max", type=int, help="Max subdomains to scrape")
    parser.add_argument("--output", "-o", type=str, help="Output JSON file")
    parser.add_argument("--save-db", action="store_true", help="Save to database")
    parser.add_argument("--no-demos", action="store_true", help="Exclude demo accounts")
    parser.add_argument("--concurrency", "-c", type=int, default=5, help="Concurrent requests (default: 5)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds (default: 0.5)")
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    # Fetch sitemap
    logger.info("Fetching Cloudbeds sitemap...")
    subdomains = await fetch_sitemap()
    logger.info(f"Found {len(subdomains)} subdomains in sitemap")
    
    if args.max:
        subdomains = subdomains[:args.max]
        logger.info(f"Limited to {args.max} subdomains")
    
    # Scrape with concurrency and rate limiting
    semaphore = asyncio.Semaphore(args.concurrency)
    completed = 0
    delay = args.delay

    async def scrape_with_limit(subdomain: str, client: httpx.AsyncClient) -> Optional[CloudbedsLead]:
        nonlocal completed
        async with semaphore:
            result = await scrape_subdomain(subdomain, client, delay=delay)
            completed += 1
            if completed % 100 == 0:
                logger.info(f"Progress: {completed}/{len(subdomains)}")
            return result

    logger.info(f"Scraping {len(subdomains)} subdomains (concurrency: {args.concurrency}, delay: {delay}s)...")
    
    async with httpx.AsyncClient() as client:
        tasks = [scrape_with_limit(s, client) for s in subdomains]
        results = await asyncio.gather(*tasks)
    
    leads = [r for r in results if r is not None]
    
    # Filter demos if requested
    if args.no_demos:
        leads = [l for l in leads if not l.is_demo]
    
    # Stats
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"Subdomains scraped: {len(subdomains)}")
    logger.info(f"Hotels/groups found: {len(leads)}")
    logger.info(f"Demo accounts: {sum(1 for l in leads if l.is_demo)}")
    logger.info(f"Real leads: {sum(1 for l in leads if not l.is_demo)}")
    
    # Output to JSON
    if args.output:
        output_data = [asdict(l) for l in leads]
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Saved {len(leads)} leads to {args.output}")
    
    # Sample output
    logger.info("")
    logger.info("Sample leads:")
    for l in [x for x in leads if not x.is_demo][:15]:
        logger.info(f"  {l.subdomain}.cloudbeds.com → {l.name}")
    
    # Save to DB
    if args.save_db:
        from db.client import init_db, get_conn
        await init_db()
        
        logger.info("Saving to database...")
        inserted = 0
        skipped = 0
        
        for lead in leads:
            if lead.is_demo:
                skipped += 1
                continue
                
            try:
                async with get_conn() as conn:
                    # Check if exists
                    exists = await conn.fetchval(
                        "SELECT 1 FROM hotels WHERE external_id = $1 AND external_id_type = $2",
                        f"cloudbeds_{lead.subdomain}",
                        "cloudbeds_sitemap",
                    )
                    if exists:
                        skipped += 1
                        continue
                    
                    # Insert
                    hotel_id = await conn.fetchval(
                        """
                        INSERT INTO hotels (name, website, source, external_id, external_id_type)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id
                        """,
                        lead.name,
                        lead.booking_url,
                        "cloudbeds_sitemap",
                        f"cloudbeds_{lead.subdomain}",
                        "cloudbeds_sitemap",
                    )
                    
                    # Link to Cloudbeds engine
                    await conn.execute(
                        """
                        INSERT INTO hotel_booking_engines (hotel_id, booking_engine_id, detected_at)
                        SELECT $1, id, NOW() FROM booking_engines WHERE name ILIKE 'cloudbeds'
                        ON CONFLICT DO NOTHING
                        """,
                        hotel_id,
                    )
                    inserted += 1
                    
            except Exception as e:
                logger.error(f"DB error for {lead.subdomain}: {e}")
        
        logger.info(f"Database: {inserted} inserted, {skipped} skipped")


if __name__ == "__main__":
    asyncio.run(main())
