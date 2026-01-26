#!/usr/bin/env python3
"""
Enrich hotels with missing websites by searching Google.

For hotels imported via booking engine crawl (Cloudbeds, Mews, etc.),
we have the booking URL but not the hotel's actual website.

This workflow searches Google for "{hotel_name} {city} official website"
and extracts the most likely hotel website.

Usage:
    # Enrich hotels missing websites (limit 100)
    uv run python -m workflows.enrich_websites --limit 100
    
    # Enrich hotels from a specific source
    uv run python -m workflows.enrich_websites --source commoncrawl --limit 500
    
    # Dry run to see what would be enriched
    uv run python -m workflows.enrich_websites --dry-run --limit 50
"""

import argparse
import asyncio
import logging
import re
from typing import Optional, List, Dict

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Domains to skip (booking engines, social media, directories)
SKIP_DOMAINS = {
    # Booking engines
    'cloudbeds.com', 'mews.com', 'booking.com', 'expedia.com', 'hotels.com',
    'agoda.com', 'trivago.com', 'kayak.com', 'priceline.com', 'orbitz.com',
    'travelocity.com', 'hotwire.com', 'rmscloud.com', 'siteminder.com',
    # Review sites
    'tripadvisor.com', 'yelp.com', 'trustpilot.com',
    # Social media
    'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
    'youtube.com', 'pinterest.com', 'tiktok.com',
    # Maps
    'google.com', 'bing.com', 'yahoo.com', 'maps.apple.com',
    # Directories
    'yellowpages.com', 'whitepages.com', 'bbb.org',
}


def is_likely_hotel_website(url: str, hotel_name: str) -> bool:
    """Check if URL is likely the hotel's official website."""
    if not url:
        return False
    
    url_lower = url.lower()
    
    # Skip known non-hotel domains
    for domain in SKIP_DOMAINS:
        if domain in url_lower:
            return False
    
    # Prefer URLs that contain parts of the hotel name
    name_parts = hotel_name.lower().split()
    for part in name_parts:
        if len(part) > 3 and part in url_lower:
            return True
    
    return True


async def search_hotel_website(
    client: httpx.AsyncClient,
    hotel_name: str,
    city: Optional[str] = None,
    serper_api_key: Optional[str] = None,
) -> Optional[str]:
    """
    Search for hotel's official website using Google (via Serper API).
    
    Returns the most likely hotel website URL, or None.
    """
    query = f"{hotel_name}"
    if city:
        query += f" {city}"
    query += " official website"
    
    if serper_api_key:
        # Use Serper API for reliable Google results
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": serper_api_key},
                json={"q": query, "num": 5},
                timeout=10.0,
            )
            
            if resp.status_code == 200:
                data = resp.json()
                for result in data.get("organic", []):
                    url = result.get("link", "")
                    if is_likely_hotel_website(url, hotel_name):
                        return url
        except Exception as e:
            logger.debug(f"Serper search failed: {e}")
    
    # Fallback: Use DuckDuckGo HTML (no API key needed)
    try:
        resp = await client.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            timeout=10.0,
        )
        
        if resp.status_code == 200:
            # Extract URLs from results
            urls = re.findall(r'href="(https?://[^"]+)"', resp.text)
            for url in urls:
                if is_likely_hotel_website(url, hotel_name):
                    return url
    except Exception as e:
        logger.debug(f"DuckDuckGo search failed: {e}")
    
    return None


async def main():
    parser = argparse.ArgumentParser(
        description="Enrich hotels with missing websites via Google search"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum hotels to process (default: 100)"
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Filter by source (e.g., 'commoncrawl')"
    )
    parser.add_argument(
        "--state",
        type=str,
        help="Filter by state (e.g., 'FL')"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be enriched without making changes"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Concurrent searches (default: 5, be gentle to search APIs)"
    )
    parser.add_argument(
        "--serper-key",
        type=str,
        help="Serper API key for Google search (optional, uses DuckDuckGo otherwise)"
    )
    
    args = parser.parse_args()
    
    # Initialize database
    from db.client import init_db, get_conn
    await init_db()
    
    # Get hotels missing websites
    logger.info("Finding hotels without websites...")
    
    async with get_conn() as conn:
        query = """
            SELECT h.id, h.name, h.city, h.state, h.source
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            WHERE h.website IS NULL OR h.website = ''
        """
        params = []
        
        if args.source:
            query += f" AND h.source LIKE ${len(params) + 1}"
            params.append(f"%{args.source}%")
        
        if args.state:
            query += f" AND h.state = ${len(params) + 1}"
            params.append(args.state)
        
        query += f" LIMIT ${len(params) + 1}"
        params.append(args.limit)
        
        hotels = await conn.fetch(query, *params)
    
    logger.info(f"Found {len(hotels)} hotels without websites")
    
    if not hotels:
        logger.info("No hotels to enrich")
        return
    
    if args.dry_run:
        logger.info("Dry run - would search for these hotels:")
        for h in hotels[:20]:
            logger.info(f"  {h['name']} ({h['city']}, {h['state']})")
        if len(hotels) > 20:
            logger.info(f"  ... and {len(hotels) - 20} more")
        return
    
    # Search for websites
    stats = {
        "total": len(hotels),
        "found": 0,
        "not_found": 0,
        "errors": 0,
    }
    
    semaphore = asyncio.Semaphore(args.concurrency)
    
    async def enrich_one(client: httpx.AsyncClient, hotel: dict) -> Optional[str]:
        async with semaphore:
            website = await search_hotel_website(
                client,
                hotel["name"],
                hotel.get("city"),
                args.serper_key,
            )
            
            # Small delay to be polite
            await asyncio.sleep(0.5)
            
            return website
    
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; HotelBot/1.0)"},
    ) as client:
        # Process in batches
        batch_size = 50
        
        for i in range(0, len(hotels), batch_size):
            batch = hotels[i:i + batch_size]
            tasks = [enrich_one(client, h) for h in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update database
            async with get_conn() as conn:
                for hotel, website in zip(batch, results):
                    if isinstance(website, Exception):
                        stats["errors"] += 1
                        continue
                    
                    if website:
                        await conn.execute(
                            "UPDATE sadie_gtm.hotels SET website = $1, updated_at = NOW() WHERE id = $2",
                            website, hotel["id"]
                        )
                        stats["found"] += 1
                        logger.debug(f"  {hotel['name']}: {website}")
                    else:
                        stats["not_found"] += 1
            
            logger.info(f"Processed {i + len(batch)}/{len(hotels)} - Found: {stats['found']}")
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("ENRICHMENT COMPLETE")
    logger.info("=" * 50)
    logger.info(f"Total processed: {stats['total']}")
    logger.info(f"Websites found: {stats['found']}")
    logger.info(f"Not found: {stats['not_found']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info(f"Success rate: {stats['found'] / max(stats['total'], 1) * 100:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
