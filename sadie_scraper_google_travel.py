#!/usr/bin/env python3
"""
Sadie Scraper Google Travel - FREE Hotel Scraper
=================================================
Scrapes hotels from Google Travel (google.com/travel) using Playwright.
FREE - no API credits needed!

Usage:
    python3 sadie_scraper_google_travel.py --city "Sydney"
    python3 sadie_scraper_google_travel.py --city "Sydney" --max-hotels 500
"""

import csv
import os
import argparse
import asyncio
import random
import time
from datetime import datetime
from urllib.parse import quote

from playwright.async_api import async_playwright

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_MAX_HOTELS = 10000  # Effectively unlimited - will stop when no more results
DEFAULT_SCROLL_DELAY = (2, 4)  # Random delay between scrolls (seconds)

# Big chains to filter out
SKIP_CHAIN_NAMES = [
    "marriott", "hilton", "hyatt", "sheraton", "westin", "w hotel",
    "intercontinental", "holiday inn", "crowne plaza", "ihg",
    "best western", "choice hotels", "comfort inn", "quality inn",
    "radisson", "wyndham", "ramada", "days inn", "super 8", "motel 6",
    "la quinta", "travelodge", "ibis", "novotel", "mercure", "accor",
    "four seasons", "ritz-carlton", "st. regis", "fairmont",
]

_stats = {"found": 0, "skipped_chains": 0, "with_website": 0}


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


# ============================================================================
# GOOGLE TRAVEL SCRAPER
# ============================================================================

async def save_progress(hotels: list, output_csv: str):
    """Save current progress to CSV."""
    import csv
    import os
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    fieldnames = ["hotel", "website", "phone", "email", "rating"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(hotels)
    log(f"  [saved {len(hotels)} hotels to {output_csv}]")


async def scrape_google_travel(
    city: str,
    output_csv: str,
    max_hotels: int = DEFAULT_MAX_HOTELS,
    scroll_delay: tuple = DEFAULT_SCROLL_DELAY,
    headless: bool = True,
) -> list:
    """
    Scrape hotels from Google Travel.
    Click each hotel to get official website from sidebar.
    Returns list of hotel dicts.
    """
    url = f"https://www.google.com/travel/search?q={quote(city)}%20hotels"
    
    log(f"Opening Google Travel: {city}")
    log(f"URL: {url}")
    
    hotels = []
    seen_names = set()
    processed_indices = set()
    
    # Resume from existing file if it exists
    import os
    if os.path.exists(output_csv):
        try:
            with open(output_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    hotels.append(row)
                    if row.get('hotel'):
                        seen_names.add(row['hotel'])
            log(f"Resuming from {len(hotels)} existing hotels")
        except Exception as e:
            log(f"Could not read existing file: {e}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )
        page = await context.new_page()
        
        try:
            # Navigate to Google Travel
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1.5)
            
            # Accept cookies if prompted
            try:
                accept_btn = page.locator("button:has-text('Accept all')")
                if await accept_btn.count() > 0:
                    await accept_btn.click()
                    await asyncio.sleep(1)
            except Exception:
                pass
            
            log("Page loaded, starting to process hotels...")
            
            card_index = 0
            no_new_count = 0
            
            # Click first card to open sidebar
            initial_cards = page.locator('div.uaTTDe.BcKagd')
            if await initial_cards.count() > 0:
                try:
                    await initial_cards.first.click(timeout=3000)
                    await asyncio.sleep(1.5)
                    log("  Sidebar opened")
                except Exception as e:
                    log(f"  Failed to open sidebar: {e}")
            
            while len(hotels) < max_hotels:
                # Use sidebar cards
                cards = page.locator('div.XCgKOb[jsname="s57fDc"]')
                card_count = await cards.count()
                log(f"  Found {card_count} sidebar cards, at index {card_index}")
                
                if card_index >= card_count:
                    # Try clicking "More results" button
                    more_btn = page.locator('span.VfPpkd-vQzf8d:has-text("More results")')
                    if await more_btn.count() > 0:
                        try:
                            log(f"Progress: {len(hotels)} hotels, clicking More results...")
                            await more_btn.first.click(timeout=3000)
                            await asyncio.sleep(2.0)
                            
                            # More results closes sidebar - re-open it by clicking first card
                            initial_cards = page.locator('div.uaTTDe.BcKagd')
                            if await initial_cards.count() > 0:
                                await initial_cards.first.click(timeout=3000)
                                await asyncio.sleep(1.5)
                                log("  Sidebar re-opened")
                            
                            card_index = 0  # Reset to check new cards
                            no_new_count = 0
                            continue
                        except Exception as e:
                            log(f"  More results failed: {e}")
                    
                    # Fallback: scroll for more
                    no_new_count += 1
                    if no_new_count >= 3:
                        log("No new hotels after 3 attempts, stopping")
                        break
                    
                    log(f"Progress: {len(hotels)} hotels, scrolling for more...")
                    await page.evaluate("window.scrollBy(0, 600)")
                    delay = random.uniform(scroll_delay[0], scroll_delay[1])
                    await asyncio.sleep(delay)
                    continue
                
                no_new_count = 0
                
                try:
                    card = cards.nth(card_index)
                    card_index += 1
                    
                    # Get hotel name - skip if not found (might be ad or different element)
                    name_el = card.locator('h2.CF94Hd, h2.BgYkof, h2.ogfYpf').first
                    if await name_el.count() == 0:
                        continue
                    
                    try:
                        name = await name_el.text_content(timeout=2000)
                    except:
                        continue
                    
                    name = name.strip() if name else ""
                    
                    if not name or name in seen_names:
                        continue
                    
                    seen_names.add(name)
                    
                    # Skip chains
                    name_lower = name.lower()
                    if any(chain in name_lower for chain in SKIP_CHAIN_NAMES):
                        _stats["skipped_chains"] += 1
                        log(f"  [skip] {name[:50]} (chain)")
                        continue
                    
                    # Get rating
                    rating = ""
                    try:
                        rating_el = card.locator('span.KFi5wf.lA0BZ, span.lA0BZ').first
                        rating = await rating_el.text_content(timeout=2000)
                        rating = rating.strip() if rating else ""
                    except:
                        pass
                    
                    website = ""
                    phone = ""
                    email = ""
                    
                    # Click the card to update sidebar
                    try:
                        await card.click(timeout=3000)
                        await asyncio.sleep(1.5)
                        
                        # Click "About" tab to get contact info
                        about_tab = page.locator('div[aria-label="About"][role="tab"]')
                        if await about_tab.count() > 0:
                            await about_tab.first.click(timeout=2000)
                            await asyncio.sleep(1.5)
                        
                        # Find Website link (globe icon button)
                        website_link = page.locator('a.WpHeLc[aria-label="Website"]')
                        if await website_link.count() > 0:
                            href = await website_link.first.get_attribute('href', timeout=2000)
                            if href:
                                from urllib.parse import urlparse
                                parsed = urlparse(href)
                                extracted = f"{parsed.scheme}://{parsed.netloc}"
                                if 'google.com' not in extracted:
                                    website = extracted
                        
                        # Find phone number from aria-label
                        phone_el = page.locator('span[aria-label*="call this hotel"] span[dir="ltr"]')
                        if await phone_el.count() > 0:
                            phone = await phone_el.first.text_content(timeout=1000)
                            phone = phone.strip() if phone else ""
                        
                        # Find email if present
                        email_link = page.locator('a[href^="mailto:"]')
                        if await email_link.count() > 0:
                            email_href = await email_link.first.get_attribute('href', timeout=1000)
                            if email_href:
                                email = email_href.replace('mailto:', '').split('?')[0]
                    except Exception as e:
                        log(f"    Error: {e}")
                    
                    hotel_data = {
                        "hotel": name,
                        "website": website,
                        "phone": phone,
                        "email": email,
                        "rating": rating,
                    }
                    
                    hotels.append(hotel_data)
                    _stats["found"] += 1
                    
                    if website:
                        _stats["with_website"] += 1
                        log(f"  ✓ {name[:40]} -> {website[:50]}")
                    else:
                        log(f"  ✗ {name[:40]} (no official site)")
                    
                    # Save progress every 50 hotels
                    if len(hotels) % 50 == 0:
                        await save_progress(hotels, output_csv)
                    
                except Exception as e:
                    log(f"  Error processing card {card_index}: {e}")
                    continue
            
        except Exception as e:
            log(f"Error: {e}")
        
        await browser.close()
    
    return hotels




# ============================================================================
# MAIN
# ============================================================================

def run_scraper(
    city: str,
    max_hotels: int,
    output_csv: str,
    headless: bool,
):
    """Main scraper function."""
    global _stats
    _stats = {"found": 0, "skipped_chains": 0, "with_website": 0}
    
    log("=" * 60)
    log("Sadie Scraper Google Travel - FREE Hotel Scraper")
    log("=" * 60)
    log(f"City: {city}")
    log(f"Max hotels: {max_hotels}")
    log("=" * 60)
    
    start_time = time.time()
    
    # Run scraper
    hotels = asyncio.run(scrape_google_travel(
        city=city,
        output_csv=output_csv,
        max_hotels=max_hotels,
        headless=headless,
    ))
    
    if not hotels:
        log("No hotels found.")
        return
    
    # Save to CSV
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    fieldnames = ["hotel", "website", "phone", "email", "rating"]
    
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(hotels)
    
    elapsed = time.time() - start_time
    
    log("")
    log("=" * 60)
    log("COMPLETE!")
    log(f"Hotels found:      {_stats['found']}")
    log(f"Skipped (chains):  {_stats['skipped_chains']}")
    log(f"With website:      {_stats['with_website']}")
    log(f"Without website:   {_stats['found'] - _stats['with_website']}")
    log(f"Time:              {elapsed:.1f}s")
    log(f"Output:            {output_csv}")
    log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Sadie Scraper Google Travel - FREE hotel scraper")
    
    parser.add_argument("--city", required=True, help="City to search")
    parser.add_argument("--max-hotels", type=int, default=DEFAULT_MAX_HOTELS, help=f"Max hotels to scrape (default: {DEFAULT_MAX_HOTELS})")
    parser.add_argument("--output", "-o", default="scraper_output/hotels_google_travel.csv")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode (visible)")
    
    args = parser.parse_args()
    
    run_scraper(
        city=args.city,
        max_hotels=args.max_hotels,
        output_csv=args.output,
        headless=not args.headed,
    )


if __name__ == "__main__":
    main()

