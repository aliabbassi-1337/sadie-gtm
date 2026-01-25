#!/usr/bin/env python3
"""
RMS Cloud Property Enumeration - Discover hotels using RMS booking engine.

RMS Cloud uses sequential numeric IDs in their booking URLs:
- https://bookings12.rmscloud.com/search/index/{id}/3
- https://ibe12.rmscloud.com/{id}/3

We can enumerate all valid properties by scanning the ID range.

Usage:
    # Scan ID range 2600-2800
    uv run python scripts/scrapers/rms_enum.py --start 2600 --end 2800

    # Full scan (may take a while)
    uv run python scripts/scrapers/rms_enum.py --start 1 --end 10000 --output rms_hotels.csv
"""

import argparse
import asyncio
import csv
import re
import sys
from dataclasses import dataclass, asdict
from typing import Optional, List

import httpx
from loguru import logger


@dataclass 
class RMSProperty:
    """Property discovered from RMS Cloud."""
    id: int
    name: str
    booking_url: str
    phone: Optional[str] = None
    email: Optional[str] = None


async def check_property(
    client: httpx.AsyncClient,
    property_id: int,
    delay: float = 0.2,
) -> Optional[RMSProperty]:
    """Check if a property ID exists and extract details."""
    import random
    
    url = f"https://bookings12.rmscloud.com/search/index/{property_id}/3"
    
    try:
        await asyncio.sleep(delay + random.uniform(0, delay * 0.3))
        
        resp = await client.get(url, follow_redirects=True, timeout=10.0)
        
        if resp.status_code != 200:
            return None
        
        html = resp.text
        
        # Check if it's an error page
        if '<title>Error</title>' in html:
            return None
        
        # Extract property name - it's in div.prop-name-login
        name_match = re.search(r'class="prop-name-login"[^>]*>\s*([^<]+?)\s*</div>', html, re.IGNORECASE)
        if not name_match:
            # Try H1 tags
            name_match = re.search(r'<h1[^>]*>\s*([^<]+?)\s*</h1>', html, re.IGNORECASE)
        if not name_match:
            # Try the title after "Search - "
            name_match = re.search(r'<h1[^>]*class="[^"]*property[^"]*"[^>]*>\s*([^<]+?)\s*</h1>', html, re.IGNORECASE)
        
        if not name_match:
            return None
        
        name = name_match.group(1).strip()
        
        # Clean name
        name = name.strip()
        if not name or name == 'Search - RMS Online Booking':
            return None
        
        # Extract phone
        phone = None
        phone_match = re.search(r'[\+]?[\d\s\-\(\)]{10,}', html)
        if phone_match:
            phone = phone_match.group(0).strip()
        
        # Extract email
        email = None
        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', html)
        if email_match:
            email = email_match.group(0)
        
        return RMSProperty(
            id=property_id,
            name=name,
            booking_url=url,
            phone=phone,
            email=email,
        )
        
    except Exception as e:
        logger.debug(f"Error checking {property_id}: {e}")
        return None


async def scan_range(
    start_id: int,
    end_id: int,
    concurrency: int = 10,
    delay: float = 0.2,
) -> List[RMSProperty]:
    """Scan a range of IDs for valid properties."""
    properties = []
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0
    total = end_id - start_id + 1
    found = 0
    
    async with httpx.AsyncClient() as client:
        async def check_with_limit(prop_id: int) -> Optional[RMSProperty]:
            nonlocal completed, found
            async with semaphore:
                result = await check_property(client, prop_id, delay)
                completed += 1
                if result:
                    found += 1
                    logger.info(f"[{completed}/{total}] Found: {result.name} (ID: {prop_id})")
                elif completed % 100 == 0:
                    logger.info(f"[{completed}/{total}] Scanned... ({found} found so far)")
                return result
        
        tasks = [check_with_limit(i) for i in range(start_id, end_id + 1)]
        results = await asyncio.gather(*tasks)
        
        properties = [r for r in results if r is not None]
    
    return properties


async def main():
    parser = argparse.ArgumentParser(
        description="Enumerate RMS Cloud properties by scanning ID range",
    )
    parser.add_argument("--start", type=int, default=2600, help="Start ID")
    parser.add_argument("--end", type=int, default=2800, help="End ID")
    parser.add_argument("--output", "-o", type=str, help="Output CSV file")
    parser.add_argument("--concurrency", "-c", type=int, default=10, help="Concurrent requests")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between requests")
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    logger.info(f"Scanning RMS Cloud IDs {args.start} to {args.end}")
    logger.info(f"Concurrency: {args.concurrency}, Delay: {args.delay}s")
    
    properties = await scan_range(
        args.start,
        args.end,
        concurrency=args.concurrency,
        delay=args.delay,
    )
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"IDs scanned: {args.end - args.start + 1}")
    logger.info(f"Properties found: {len(properties)}")
    
    if args.output and properties:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'name', 'booking_url', 'phone', 'email'])
            writer.writeheader()
            for p in properties:
                writer.writerow(asdict(p))
        logger.info(f"Saved to {args.output}")
    
    if properties:
        logger.info("")
        logger.info("Sample properties:")
        for p in properties[:10]:
            logger.info(f"  [{p.id}] {p.name}")


if __name__ == "__main__":
    asyncio.run(main())
