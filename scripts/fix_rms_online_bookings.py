#!/usr/bin/env python3
"""Fix RMS hotels with 'Online Bookings' name by re-scraping with Playwright."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
from typing import List
from loguru import logger
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from db.client import get_conn
from lib.rms.scraper import RMSScraper, convert_to_bookings_url

RMS_BOOKING_ENGINE_ID = 12


async def get_online_bookings_hotels():
    """Get RMS hotels with Online Bookings name."""
    async with get_conn() as conn:
        rows = await conn.fetch("""
            SELECT h.id as hotel_id, hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            WHERE hbe.booking_engine_id = $1 
            AND h.name = $2
            AND h.status = 1
            AND hbe.booking_url IS NOT NULL
        """, RMS_BOOKING_ENGINE_ID, "Online Bookings")
        return [dict(r) for r in rows]


async def batch_update_hotels(updates: List[tuple]) -> int:
    """Batch update hotels with scraped data."""
    if not updates:
        return 0
    async with get_conn() as conn:
        # updates is list of (name, city, state, email, phone, hotel_id)
        await conn.executemany("""
            UPDATE sadie_gtm.hotels SET
                name = COALESCE($1, name),
                city = CASE WHEN city IS NULL OR city = '' THEN COALESCE($2, city) ELSE city END,
                state = CASE WHEN state IS NULL OR state = '' THEN COALESCE($3, state) ELSE state END,
                email = CASE WHEN email IS NULL OR email = '' THEN COALESCE($4, email) ELSE email END,
                phone_website = CASE WHEN phone_website IS NULL OR phone_website = '' THEN COALESCE($5, phone_website) ELSE phone_website END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $6
        """, updates)
        return len(updates)


async def main():
    logger.info("Fetching RMS hotels with 'Online Bookings' name...")
    hotels = await get_online_bookings_hotels()
    logger.info(f"Found {len(hotels)} hotels to fix")
    
    if not hotels:
        return
    
    updates = []
    failed = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        
        scraper = RMSScraper(page)
        
        for i, hotel in enumerate(hotels):
            hotel_id = hotel["hotel_id"]
            url = hotel["booking_url"]
            
            # Convert to correct format
            scrape_url = convert_to_bookings_url(url)
            
            try:
                data = await scraper.extract(scrape_url, str(hotel_id))
                
                if data and data.has_data():
                    updates.append((
                        data.name,
                        data.city,
                        data.state,
                        data.email,
                        data.phone,
                        hotel_id
                    ))
                    if len(updates) <= 10:
                        logger.info(f"Scraped {hotel_id}: {data.name}")
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                if failed <= 5:
                    logger.error(f"Error {hotel_id}: {str(e)[:50]}")
            
            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i+1}/{len(hotels)}, scraped={len(updates)}, failed={failed}")
        
        await browser.close()
    
    # Batch update
    if updates:
        logger.info(f"Updating {len(updates)} hotels in database...")
        count = await batch_update_hotels(updates)
        logger.success(f"Done! Fixed: {count}, Failed: {failed}")
    else:
        logger.warning(f"No hotels fixed. Failed: {failed}")


if __name__ == "__main__":
    asyncio.run(main())
