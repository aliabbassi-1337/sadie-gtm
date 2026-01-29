"""Fix Cloudbeds hotels that have city but missing state.

The original scraper missed state because:
1. Title parsing only captures state if format is "City, State, Country" (rare)
2. Widget selector 'p[data-be-text="true"]' doesn't match all Cloudbeds pages

This workflow re-scrapes these hotels with a more robust extraction.

Usage:
    # Check status
    uv run python -m workflows.fix_cloudbeds_state --status
    
    # Dry run
    uv run python -m workflows.fix_cloudbeds_state --dry-run --limit 10
    
    # Run fix
    uv run python -m workflows.fix_cloudbeds_state --limit 100
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
from playwright_stealth import Stealth

from db.client import init_db, close_db, get_conn


@dataclass
class StateFixResult:
    """Result of fixing state for a hotel."""
    hotel_id: int
    success: bool
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    error: Optional[str] = None


async def get_hotels_missing_state(limit: int = 100) -> List[Dict[str, Any]]:
    """Get Cloudbeds hotels that have city but missing state."""
    async with get_conn() as conn:
        rows = await conn.fetch("""
            SELECT h.id, h.name, h.city, h.state, h.country, hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            WHERE hbe.booking_engine_id = 3  -- Cloudbeds
            AND hbe.booking_url IS NOT NULL
            AND hbe.booking_url != ''
            AND h.city IS NOT NULL AND h.city != ''
            AND (h.state IS NULL OR h.state = '')
            ORDER BY h.id
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]


async def get_hotels_missing_state_count() -> int:
    """Count Cloudbeds hotels with city but missing state."""
    async with get_conn() as conn:
        result = await conn.fetchval("""
            SELECT COUNT(*)
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            WHERE hbe.booking_engine_id = 3  -- Cloudbeds
            AND hbe.booking_url IS NOT NULL
            AND hbe.booking_url != ''
            AND h.city IS NOT NULL AND h.city != ''
            AND (h.state IS NULL OR h.state = '')
        """)
        return result or 0


async def extract_state_from_page(page: Page) -> Dict[str, Any]:
    """Extract state and contact from Cloudbeds page using robust selectors.
    
    The "Address and Contact" section has this structure:
    - heading "Address and Contact"
    - paragraph: Street address
    - paragraph: City
    - paragraph: "State Country" (e.g., "California US")
    - paragraph: Zip code
    - paragraph: Phone
    - link: Email (mailto:)
    """
    result = {}
    
    # Find the Address and Contact section and extract all paragraphs
    try:
        data = await page.evaluate("""
            () => {
                // Find the "Address and Contact" heading
                const headings = Array.from(document.querySelectorAll('h3'));
                const addressHeading = headings.find(h => 
                    h.textContent?.toLowerCase().includes('address and contact')
                );
                
                if (!addressHeading) return null;
                
                // Get the parent container
                const container = addressHeading.closest('div')?.parentElement || 
                                  addressHeading.parentElement;
                if (!container) return null;
                
                // Get all paragraphs in this section
                const paragraphs = Array.from(container.querySelectorAll('p'))
                    .map(p => p.textContent?.trim())
                    .filter(t => t && t.length > 0);
                
                // Get email from mailto link
                const mailtoLink = container.querySelector('a[href^="mailto:"]');
                const email = mailtoLink ? 
                    mailtoLink.href.replace('mailto:', '').split('?')[0] : null;
                
                // Get phone from tel link
                const telLink = container.querySelector('a[href^="tel:"]');
                const phone = telLink ? 
                    telLink.href.replace('tel:', '').replace(/[^0-9+()-]/g, '') : null;
                
                return { paragraphs, email, phone };
            }
        """)
        
        if not data or not data.get('paragraphs'):
            return result
        
        paragraphs = data['paragraphs']
        
        # Parse paragraphs to find state
        # Typical order: Street, City, "State Country", Zip, Phone
        # Look for "State Country" pattern (e.g., "California US", "Texas US", "NSW AU")
        state_country_pattern = re.compile(
            r'^([A-Za-z\s]+)\s+(US|USA|AU|UK|CA|NZ|GB|IE|MX|AR|PR|CO|IT|ES|FR|DE|PT|BR|CL|PE|CR|PA|NI|GT|SV|HN|DO|CU|JM|BS|BB|TT|GY|SR|EC|VE|BO|PY|UY|SG|MY|TH|VN|ID|PH|JP|KR|CN|HK|TW|IN|AE|SA|IL|ZA|EG|MA|KE|NG|GH)$',
            re.IGNORECASE
        )
        
        for para in paragraphs:
            match = state_country_pattern.match(para.strip())
            if match:
                state = match.group(1).strip()
                country = match.group(2).strip().upper()
                
                # Normalize country codes
                if country in ['US', 'USA']:
                    result['country'] = 'USA'
                elif country in ['UK', 'GB']:
                    result['country'] = 'UK'
                else:
                    result['country'] = country
                
                result['state'] = state
                break
        
        # Get contact info
        if data.get('email'):
            result['email'] = data['email']
        if data.get('phone') and len(data['phone']) >= 10:
            result['phone'] = data['phone']
        
    except Exception as e:
        logger.warning(f"Error extracting state: {e}")
    
    return result


async def fix_hotel_state(page: Page, hotel: Dict) -> StateFixResult:
    """Fix state for a single hotel."""
    hotel_id = hotel['id']
    booking_url = hotel['booking_url']
    
    try:
        response = await page.goto(booking_url, timeout=30000, wait_until="domcontentloaded")
        
        # Check for 404
        if response and response.status == 404:
            return StateFixResult(hotel_id=hotel_id, success=False, error="404")
        
        # Wait for React to render
        await asyncio.sleep(4)
        
        data = await extract_state_from_page(page)
        
        if not data or not data.get('state'):
            return StateFixResult(hotel_id=hotel_id, success=False, error="no_state_found")
        
        return StateFixResult(
            hotel_id=hotel_id,
            success=True,
            state=data.get('state'),
            country=data.get('country'),
            phone=data.get('phone'),
            email=data.get('email'),
        )
        
    except Exception as e:
        return StateFixResult(hotel_id=hotel_id, success=False, error=str(e)[:100])


async def batch_update_states(results: List[StateFixResult]) -> int:
    """Batch update hotels with extracted state data."""
    successful = [r for r in results if r.success and r.state]
    
    if not successful:
        return 0
    
    hotel_ids = [r.hotel_id for r in successful]
    states = [r.state for r in successful]
    countries = [r.country for r in successful]
    phones = [r.phone for r in successful]
    emails = [r.email for r in successful]
    
    sql = """
    UPDATE sadie_gtm.hotels h
    SET 
        state = CASE WHEN v.state IS NOT NULL AND v.state != '' 
                     THEN v.state ELSE h.state END,
        country = CASE WHEN v.country IS NOT NULL AND v.country != '' 
                       THEN v.country ELSE h.country END,
        phone_website = CASE WHEN v.phone IS NOT NULL AND v.phone != '' AND h.phone_website IS NULL
                             THEN v.phone ELSE h.phone_website END,
        email = CASE WHEN v.email IS NOT NULL AND v.email != '' AND h.email IS NULL
                     THEN v.email ELSE h.email END,
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT * FROM unnest(
            $1::integer[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::text[]
        ) AS t(hotel_id, state, country, phone, email)
    ) v
    WHERE h.id = v.hotel_id
    """
    
    async with get_conn() as conn:
        result = await conn.execute(sql, hotel_ids, states, countries, phones, emails)
        count = int(result.split()[-1]) if result else len(successful)
    
    return count


async def run_status():
    """Show status of hotels needing state fix."""
    await init_db()
    
    try:
        count = await get_hotels_missing_state_count()
        
        print("\n" + "=" * 60)
        print("CLOUDBEDS STATE FIX STATUS")
        print("=" * 60)
        print(f"  Hotels with city but missing state: {count:,}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


async def run_dry_run(limit: int):
    """Show what would be fixed."""
    await init_db()
    
    try:
        hotels = await get_hotels_missing_state(limit=limit)
        
        print(f"\n=== DRY RUN: Would fix {len(hotels)} hotels ===\n")
        
        for h in hotels[:20]:
            print(f"  ID={h['id']}: {h['name'] or 'NO NAME'}")
            print(f"    City: {h['city']} | State: {h['state']} | Country: {h['country']}")
            print(f"    URL: {h['booking_url'][:60]}...")
            print()
        
        if len(hotels) > 20:
            print(f"  ... and {len(hotels) - 20} more\n")
            
    finally:
        await close_db()


async def run_fix(limit: int, concurrency: int = 3):
    """Run the state fix workflow."""
    await init_db()
    
    try:
        hotels = await get_hotels_missing_state(limit=limit)
        
        if not hotels:
            logger.info("No Cloudbeds hotels need state fix")
            return
        
        logger.info(f"Found {len(hotels)} Cloudbeds hotels to fix")
        
        total_fixed = 0
        total_errors = 0
        batch_size = 50
        
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            
            contexts = []
            pages = []
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    viewport={"width": 1280, "height": 800},
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                pages.append(page)
            
            logger.info(f"Created {concurrency} browser contexts")
            
            results_buffer = []
            processed = 0
            
            for batch_start in range(0, len(hotels), concurrency):
                batch = hotels[batch_start:batch_start + concurrency]
                
                tasks = [
                    fix_hotel_state(pages[i], hotel)
                    for i, hotel in enumerate(batch)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for i, result in enumerate(results):
                    processed += 1
                    hotel = batch[i]
                    
                    if isinstance(result, Exception):
                        total_errors += 1
                        logger.warning(f"  [{processed}/{len(hotels)}] Hotel {hotel['id']}: error - {result}")
                        continue
                    
                    results_buffer.append(result)
                    
                    if result.success and result.state:
                        logger.info(f"  [{processed}/{len(hotels)}] Hotel {hotel['id']}: state={result.state}")
                    elif result.error:
                        total_errors += 1
                        logger.warning(f"  [{processed}/{len(hotels)}] Hotel {hotel['id']}: {result.error}")
                
                # Batch update
                if len(results_buffer) >= batch_size:
                    updated = await batch_update_states(results_buffer)
                    total_fixed += updated
                    logger.info(f"  Batch update: {updated} hotels")
                    results_buffer = []
            
            # Final batch
            if results_buffer:
                updated = await batch_update_states(results_buffer)
                total_fixed += updated
                logger.info(f"  Final batch: {updated} hotels")
            
            # Cleanup
            for page in pages:
                await page.close()
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        print("\n" + "=" * 60)
        print("STATE FIX COMPLETE")
        print("=" * 60)
        print(f"  Processed:  {len(hotels)}")
        print(f"  Fixed:      {total_fixed}")
        print(f"  Errors:     {total_errors}")
        print("=" * 60 + "\n")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Fix Cloudbeds state extraction")
    parser.add_argument("--status", action="store_true", help="Show status")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed")
    parser.add_argument("--limit", type=int, default=100, help="Max hotels to process")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent browsers")
    
    args = parser.parse_args()
    
    if args.status:
        asyncio.run(run_status())
    elif args.dry_run:
        asyncio.run(run_dry_run(args.limit))
    else:
        asyncio.run(run_fix(args.limit, args.concurrency))


if __name__ == "__main__":
    main()
