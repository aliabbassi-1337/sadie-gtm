"""Unified Cloudbeds enrichment - extracts name, address, phone, email in one visit.

Cloudbeds pages are JS-rendered (React/Chakra) so we use Playwright.
Visits each booking page once to extract:
- Name (from og:title / JSON-LD)
- Address, City, State, Country (from Cloudbeds widget)
- Phone, Email (from Cloudbeds widget)

Usage:
    # Check status
    uv run python -m workflows.enrich_cloudbeds --status
    
    # Dry run (show what would be enriched)
    uv run python -m workflows.enrich_cloudbeds --dry-run --limit 10
    
    # Run enrichment
    uv run python -m workflows.enrich_cloudbeds --limit 100
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import re
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from loguru import logger
from playwright.async_api import async_playwright, Page

from db.client import init_db, close_db, get_conn
from db import queries


@dataclass
class CloudbedsEnrichmentResult:
    """Result of enriching a hotel from Cloudbeds page."""
    hotel_id: int
    success: bool
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    error: Optional[str] = None


async def get_cloudbeds_hotels_needing_enrichment(limit: int = 100) -> List[Dict]:
    """Get hotels with Cloudbeds booking URLs that need name or location enrichment."""
    async with get_conn() as conn:
        # Hotels with Cloudbeds booking engine that are missing name or city
        rows = await conn.fetch("""
            SELECT h.id, h.name, h.city, h.state, h.country, h.address,
                   hbe.booking_url, hbe.engine_property_id as slug
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%cloudbeds%'
              AND hbe.booking_url IS NOT NULL
              AND hbe.booking_url != ''
              AND (
                  (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%')
                  OR (h.city IS NULL OR h.city = '')
              )
            ORDER BY h.id
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]


async def get_cloudbeds_hotels_count() -> int:
    """Count Cloudbeds hotels needing enrichment."""
    async with get_conn() as conn:
        result = await conn.fetchval("""
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name ILIKE '%cloudbeds%'
              AND hbe.booking_url IS NOT NULL
              AND hbe.booking_url != ''
              AND (
                  (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%')
                  OR (h.city IS NULL OR h.city = '')
              )
        """)
        return result or 0


async def extract_from_cloudbeds_page(page: Page) -> Dict[str, Any]:
    """Extract name and address/contact from Cloudbeds page.
    
    Returns dict with: name, address, city, state, country, phone, email
    """
    result = {}
    
    # Extract name and location from title tag
    # Format: "Hotel Name - City, Country - Best Price Guarantee"
    try:
        title_data = await page.evaluate("""
            () => {
                const title = document.querySelector('title');
                if (!title) return null;
                
                const text = title.textContent.trim();
                // Split on " - " separator
                const parts = text.split(/\\s*-\\s*/);
                
                if (parts.length >= 2) {
                    const name = parts[0].trim();
                    const locationPart = parts[1].trim();
                    
                    // Parse location: "City, Country" or "City, State, Country"
                    const locParts = locationPart.split(',').map(p => p.trim());
                    
                    return {
                        name: name,
                        city: locParts[0] || null,
                        state: locParts.length === 3 ? locParts[1] : null,
                        country: locParts[locParts.length - 1] || null
                    };
                }
                
                return { name: parts[0].trim() };
            }
        """)
        if title_data:
            if title_data.get('name') and title_data['name'] not in ['Book Now', 'Reservation', 'Booking', 'Home']:
                result['name'] = title_data['name']
            if title_data.get('city'):
                result['city'] = title_data['city']
            if title_data.get('state'):
                result['state'] = title_data['state']
            if title_data.get('country'):
                # Normalize country
                country = title_data['country']
                if country in ['United States of America', 'United States', 'US', 'USA']:
                    result['country'] = 'USA'
                else:
                    result['country'] = country
    except Exception:
        pass
    
    # Extract address and contact from Cloudbeds widget (if present)
    # This widget is optional - not all properties enable it
    try:
        widget_data = await page.evaluate("""
            () => {
                const container = document.querySelector('[data-testid="property-address-and-contact"]') 
                               || document.querySelector('.cb-address-and-contact');
                if (!container) return null;
                
                // Get all text lines
                const lines = Array.from(container.querySelectorAll('p[data-be-text="true"]'))
                    .map(p => p.textContent?.trim() || '');
                
                // Get email from mailto link
                const mailtoLink = container.querySelector('a[href^="mailto:"]');
                const email = mailtoLink ? mailtoLink.href.replace('mailto:', '').split('?')[0] : '';
                
                return { lines, email };
            }
        """)
        
        if widget_data and widget_data.get('lines') and len(widget_data['lines']) >= 3:
            lines = widget_data['lines']
            
            # Widget data is more detailed - use it to override/enhance title data
            # Line 0: Street address
            if len(lines) > 0:
                result['address'] = lines[0]
            
            # Line 1: City (override title if widget has it)
            if len(lines) > 1:
                result['city'] = lines[1]
            
            # Line 2: "State Country" e.g. "Texas US"
            if len(lines) > 2:
                state_country = lines[2].strip()
                parts = state_country.rsplit(' ', 1)
                if len(parts) == 2:
                    result['state'] = parts[0].strip()
                    country = parts[1].strip().upper()
                    result['country'] = 'USA' if country in ['US', 'USA'] else country
                else:
                    result['state'] = state_country
            
            # Find phone (digits with common phone chars)
            phone_pattern = re.compile(r'^[\d\-\(\)\s\+\.]{7,20}$')
            for line in lines[3:]:
                if phone_pattern.match(line) and 'phone' not in result:
                    result['phone'] = line
            
            # Email from mailto link
            if widget_data.get('email'):
                result['email'] = widget_data['email']
    except Exception:
        pass
    
    # Also try to get phone from tel: links anywhere on page
    if 'phone' not in result:
        try:
            phone = await page.evaluate("""
                () => {
                    const tel = document.querySelector('a[href^="tel:"]');
                    if (tel) {
                        return tel.href.replace('tel:', '').replace(/[^0-9+()-]/g, '');
                    }
                    return null;
                }
            """)
            if phone and len(phone) >= 10:
                result['phone'] = phone
        except Exception:
            pass
    
    # Also try to get email from mailto: links anywhere on page
    if 'email' not in result:
        try:
            email = await page.evaluate("""
                () => {
                    const mailto = document.querySelector('a[href^="mailto:"]');
                    if (mailto) {
                        return mailto.href.replace('mailto:', '').split('?')[0];
                    }
                    return null;
                }
            """)
            if email and '@' in email:
                result['email'] = email
        except Exception:
            pass
    
    return result


async def enrich_hotel(
    page: Page,
    hotel: Dict,
) -> CloudbedsEnrichmentResult:
    """Enrich a single hotel from its Cloudbeds booking page."""
    hotel_id = hotel['id']
    booking_url = hotel['booking_url']
    
    try:
        # Navigate to booking page
        await page.goto(booking_url, timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(1.5)  # Wait for React to render
        
        # Extract data
        data = await extract_from_cloudbeds_page(page)
        
        if not data:
            return CloudbedsEnrichmentResult(
                hotel_id=hotel_id,
                success=False,
                error="no_data_extracted"
            )
        
        return CloudbedsEnrichmentResult(
            hotel_id=hotel_id,
            success=True,
            name=data.get('name'),
            address=data.get('address'),
            city=data.get('city'),
            state=data.get('state'),
            country=data.get('country'),
            phone=data.get('phone'),
            email=data.get('email'),
        )
        
    except Exception as e:
        return CloudbedsEnrichmentResult(
            hotel_id=hotel_id,
            success=False,
            error=str(e)[:100]
        )


async def batch_update_hotels(results: List[CloudbedsEnrichmentResult]) -> int:
    """Batch update hotels with enrichment results using bulk UPDATE."""
    successful = [r for r in results if r.success and (r.city or r.name)]
    
    if not successful:
        return 0
    
    # Prepare arrays for unnest
    hotel_ids = []
    names = []
    addresses = []
    cities = []
    states = []
    countries = []
    phones = []
    emails = []
    
    for r in successful:
        hotel_ids.append(r.hotel_id)
        names.append(r.name)
        addresses.append(r.address)
        cities.append(r.city)
        states.append(r.state)
        countries.append(r.country)
        phones.append(r.phone)
        emails.append(r.email)
    
    # Bulk UPDATE using unnest
    # For Cloudbeds, the page title/widget is authoritative - scraped data overrides existing
    # This is correct because crawl data sources often have wrong location info
    sql = """
    UPDATE sadie_gtm.hotels h
    SET 
        name = CASE WHEN v.name IS NOT NULL AND v.name != '' 
                    THEN v.name ELSE h.name END,
        address = CASE WHEN v.address IS NOT NULL AND v.address != '' 
                       THEN v.address ELSE h.address END,
        city = CASE WHEN v.city IS NOT NULL AND v.city != '' 
                    THEN v.city ELSE h.city END,
        state = CASE WHEN v.state IS NOT NULL AND v.state != '' 
                     THEN v.state ELSE h.state END,
        country = CASE WHEN v.country IS NOT NULL AND v.country != '' 
                       THEN v.country ELSE h.country END,
        phone_website = CASE WHEN v.phone IS NOT NULL AND v.phone != '' 
                             THEN v.phone ELSE h.phone_website END,
        email = CASE WHEN v.email IS NOT NULL AND v.email != '' 
                     THEN v.email ELSE h.email END,
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT * FROM unnest(
            $1::integer[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::text[]
        ) AS t(hotel_id, name, address, city, state, country, phone, email)
    ) v
    WHERE h.id = v.hotel_id
    """
    
    async with get_conn() as conn:
        result = await conn.execute(
            sql,
            hotel_ids,
            names,
            addresses,
            cities,
            states,
            countries,
            phones,
            emails,
        )
        count = int(result.split()[-1]) if result else len(successful)
    
    return count


async def run_status():
    """Show enrichment status."""
    await init_db()
    
    try:
        count = await get_cloudbeds_hotels_count()
        
        # Get total Cloudbeds hotels
        async with get_conn() as conn:
            total = await conn.fetchval("""
                SELECT COUNT(*)
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
                WHERE be.name ILIKE '%cloudbeds%'
            """)
        
        print("\n" + "=" * 60)
        print("CLOUDBEDS ENRICHMENT STATUS")
        print("=" * 60)
        print(f"  Total Cloudbeds hotels:     {total:,}")
        print(f"  Needing enrichment:         {count:,}")
        print(f"  Already enriched:           {(total or 0) - count:,}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


async def run_dry_run(limit: int):
    """Show what would be enriched without making changes."""
    await init_db()
    
    try:
        hotels = await get_cloudbeds_hotels_needing_enrichment(limit=limit)
        
        print(f"\n=== DRY RUN: Would enrich {len(hotels)} hotels ===\n")
        
        for h in hotels[:20]:  # Show first 20
            needs = []
            if not h['name'] or h['name'].startswith('Unknown'):
                needs.append('name')
            if not h['city']:
                needs.append('location')
            
            print(f"  ID={h['id']}: {h['name'] or 'NO NAME'}")
            print(f"    URL: {h['booking_url'][:60]}...")
            print(f"    Needs: {', '.join(needs)}")
            print()
        
        if len(hotels) > 20:
            print(f"  ... and {len(hotels) - 20} more\n")
            
    finally:
        await close_db()


async def run_enrichment(limit: int, concurrency: int = 3):
    """Run the enrichment workflow."""
    await init_db()
    
    try:
        hotels = await get_cloudbeds_hotels_needing_enrichment(limit=limit)
        
        if not hotels:
            logger.info("No Cloudbeds hotels need enrichment")
            return
        
        logger.info(f"Found {len(hotels)} Cloudbeds hotels to enrich")
        
        total_enriched = 0
        total_errors = 0
        batch_size = 50  # Update DB every 50 hotels
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # Process with limited concurrency
            semaphore = asyncio.Semaphore(concurrency)
            results_buffer = []
            
            async def process_hotel(hotel: Dict) -> CloudbedsEnrichmentResult:
                async with semaphore:
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                    )
                    page = await context.new_page()
                    try:
                        result = await enrich_hotel(page, hotel)
                        return result
                    finally:
                        await page.close()
                        await context.close()
            
            for i, hotel in enumerate(hotels):
                result = await process_hotel(hotel)
                results_buffer.append(result)
                
                if result.success and (result.city or result.name):
                    parts = []
                    if result.name:
                        parts.append(f"name={result.name[:25]}")
                    if result.city:
                        parts.append(f"loc={result.city}, {result.state}")
                    if result.phone:
                        parts.append("phone")
                    if result.email:
                        parts.append("email")
                    logger.info(f"  [{i+1}/{len(hotels)}] Hotel {hotel['id']}: {', '.join(parts)}")
                elif result.error:
                    total_errors += 1
                    logger.warning(f"  [{i+1}/{len(hotels)}] Hotel {hotel['id']}: error - {result.error}")
                
                # Batch update
                if len(results_buffer) >= batch_size:
                    updated = await batch_update_hotels(results_buffer)
                    total_enriched += updated
                    logger.info(f"  Batch update: {updated} hotels")
                    results_buffer = []
            
            # Final batch
            if results_buffer:
                updated = await batch_update_hotels(results_buffer)
                total_enriched += updated
                logger.info(f"  Final batch update: {updated} hotels")
            
            await browser.close()
        
        print("\n" + "=" * 60)
        print("ENRICHMENT COMPLETE")
        print("=" * 60)
        print(f"  Processed:  {len(hotels)}")
        print(f"  Enriched:   {total_enriched}")
        print(f"  Errors:     {total_errors}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Cloudbeds hotel enrichment")
    parser.add_argument("--status", action="store_true", help="Show enrichment status")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enriched")
    parser.add_argument("--limit", type=int, default=100, help="Max hotels to process")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent browser contexts")
    
    args = parser.parse_args()
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit))
    else:
        asyncio.run(run_enrichment(args.limit, args.concurrency))


if __name__ == "__main__":
    main()
