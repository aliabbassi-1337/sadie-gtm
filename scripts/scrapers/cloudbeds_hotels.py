#!/usr/bin/env python3
"""
Cloudbeds Hotels Scraper - Extract individual hotels from all Cloudbeds groups.

Each subdomain (e.g., masreservasonline.cloudbeds.com) can be a hotel GROUP
containing multiple individual hotels. This script uses the existing Playwright
infrastructure to render each page and extract all hotel details.

Usage:
    # Test with 10 groups
    uv run python scripts/scrapers/cloudbeds_hotels.py --max-groups 10

    # Full scrape
    uv run python scripts/scrapers/cloudbeds_hotels.py --output data/cloudbeds_hotels.csv

    # Resume from specific group
    uv run python scripts/scrapers/cloudbeds_hotels.py --start-from 100
"""

import argparse
import asyncio
import csv
import json
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict
import random

from loguru import logger
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright.async_api import TimeoutError as PlaywrightTimeout


# Reuse user agents from detector
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class Hotel:
    """Individual hotel extracted from a Cloudbeds group."""
    name: str
    city: str
    slug: str
    booking_url: str
    group_subdomain: str
    property_id: Optional[int] = None


async def extract_hotels_from_page(page: Page) -> List[dict]:
    """Extract all hotels from the current page using JavaScript."""
    return await page.evaluate("""
        () => {
            const hotels = [];
            
            // Find all hotel cards (they contain h2 with hotel name)
            document.querySelectorAll('h2').forEach(h2 => {
                const name = h2.textContent.trim();
                if (!name) return;
                
                // Find the parent card container
                let card = h2.parentElement;
                for (let i = 0; i < 5 && card; i++) {
                    if (card.querySelector('a[href*="/reservas/"]') || 
                        card.querySelector('a[href*="/connect/"]')) break;
                    card = card.parentElement;
                }
                if (!card) return;
                
                // Extract city (usually in a div before the h2)
                let city = '';
                const prevDiv = h2.previousElementSibling;
                if (prevDiv) {
                    city = prevDiv.textContent.trim().split('\\n')[0].trim();
                }
                
                // Extract booking URL and slug
                let bookingUrl = '';
                let slug = '';
                let propertyId = null;
                
                const reservasLink = card.querySelector('a[href*="/reservas/"]');
                const connectLink = card.querySelector('a[href*="/connect/"]');
                
                if (reservasLink) {
                    bookingUrl = reservasLink.href.split('?')[0].split('#')[0];
                    const match = bookingUrl.match(/\\/reservas\\/(\\w+)/);
                    if (match) slug = match[1];
                } else if (connectLink) {
                    bookingUrl = connectLink.href.split('#')[0];
                    const match = bookingUrl.match(/\\/connect\\/(\\d+)/);
                    if (match) propertyId = parseInt(match[1]);
                }
                
                // Also check for property ID in image URLs
                const img = card.querySelector('img[src*="/uploads/"]');
                if (img && !propertyId) {
                    const match = img.src.match(/\\/uploads\\/(\\d+)\\//);
                    if (match) propertyId = parseInt(match[1]);
                }
                
                hotels.push({
                    name,
                    city,
                    slug,
                    bookingUrl,
                    propertyId
                });
            });
            
            return hotels;
        }
    """)


class CloudbedsGroupScraper:
    """Scraper for Cloudbeds group pages using shared browser context pool."""
    
    def __init__(
        self,
        concurrency: int = 3,
        timeout: int = 30000,
        delay: float = 2.0,
        debug: bool = False,
    ):
        self.concurrency = concurrency
        self.timeout = timeout
        self.delay = delay  # Seconds between requests per context
        self.debug = debug
        self._browser = None
        self._context_queue: asyncio.Queue = asyncio.Queue()
        self._contexts: List[BrowserContext] = []
        
    def _log(self, msg: str) -> None:
        if self.debug:
            logger.debug(msg)
    
    async def __aenter__(self):
        """Start browser and create context pool."""
        pw = await async_playwright().start()
        self._pw = pw
        self._browser = await pw.chromium.launch(headless=True)
        
        # Create reusable context pool (like detector.py)
        for _ in range(self.concurrency):
            ctx = await self._browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                ignore_https_errors=True,
                locale="en-US",
                timezone_id="America/New_York",
            )
            self._contexts.append(ctx)
            await self._context_queue.put(ctx)
        
        logger.info(f"Browser started with {self.concurrency} contexts")
        return self
    
    async def __aexit__(self, *args):
        """Clean up browser."""
        for ctx in self._contexts:
            try:
                await ctx.close()
            except Exception:
                pass
        if self._browser:
            await self._browser.close()
        if hasattr(self, '_pw'):
            await self._pw.stop()
    
    async def scrape_group(self, subdomain: str) -> List[Hotel]:
        """Scrape all hotels from a single group subdomain."""
        import random
        
        url = f"https://{subdomain}.cloudbeds.com/"
        hotels = []
        
        # Get context from pool
        context = await self._context_queue.get()
        page = await context.new_page()
        
        try:
            # Rate limiting with jitter
            await asyncio.sleep(self.delay + random.uniform(0, self.delay * 0.5))
            
            self._log(f"Scraping {subdomain}...")
            await page.goto(url, timeout=self.timeout)
            
            # Wait for hotels to load
            try:
                await page.wait_for_selector('h2', timeout=10000)
            except PlaywrightTimeout:
                # Might be a single property or error page
                await page.close()
                await self._context_queue.put(context)
                return hotels
            
            # Extract from first page
            page_hotels = await extract_hotels_from_page(page)
            for h in page_hotels:
                hotels.append(Hotel(
                    name=h['name'],
                    city=h['city'],
                    slug=h['slug'],
                    booking_url=h['bookingUrl'],
                    group_subdomain=subdomain,
                    property_id=h.get('propertyId'),
                ))
            
            # Check for pagination
            page_count = 1
            try:
                pagination = await page.query_selector_all('[class*="pagination"] li, .paginate li')
                page_count = len(pagination) if pagination else 1
            except Exception:
                pass
            
            # Navigate through additional pages
            if page_count > 1:
                for page_num in range(2, min(page_count + 1, 20)):
                    try:
                        page_btn = await page.query_selector(
                            f'[class*="pagination"] li:has-text("{page_num}"), .paginate li:has-text("{page_num}")'
                        )
                        if page_btn:
                            await page_btn.click()
                            await asyncio.sleep(1)
                            
                            page_hotels = await extract_hotels_from_page(page)
                            for h in page_hotels:
                                hotels.append(Hotel(
                                    name=h['name'],
                                    city=h['city'],
                                    slug=h['slug'],
                                    booking_url=h['bookingUrl'],
                                    group_subdomain=subdomain,
                                    property_id=h.get('propertyId'),
                                ))
                    except Exception as e:
                        self._log(f"Pagination error on page {page_num}: {e}")
                        break
            
            self._log(f"  Found {len(hotels)} hotels")
            
        except Exception as e:
            self._log(f"Error scraping {subdomain}: {e}")
        finally:
            await page.close()
            await self._context_queue.put(context)
        
        return hotels
    
    async def scrape_all(
        self,
        subdomains: List[str],
        progress_callback=None,
    ) -> List[Hotel]:
        """Scrape all groups with concurrency control."""
        all_hotels = []
        semaphore = asyncio.Semaphore(self.concurrency)
        completed = 0
        
        async def scrape_with_limit(subdomain: str) -> List[Hotel]:
            nonlocal completed
            async with semaphore:
                hotels = await self.scrape_group(subdomain)
                completed += 1
                
                if progress_callback:
                    progress_callback(completed, len(subdomains), subdomain, len(hotels))
                elif completed % 50 == 0 or hotels:
                    logger.info(f"[{completed}/{len(subdomains)}] {subdomain}: {len(hotels)} hotels")
                
                return hotels
        
        tasks = [scrape_with_limit(s) for s in subdomains]
        results = await asyncio.gather(*tasks)
        
        for hotel_list in results:
            all_hotels.extend(hotel_list)
        
        return all_hotels


async def main():
    parser = argparse.ArgumentParser(
        description="Scrape individual hotels from Cloudbeds group pages",
    )
    parser.add_argument("--max-groups", type=int, help="Max groups to scrape (for testing)")
    parser.add_argument("--start-from", type=int, default=0, help="Start from group index")
    parser.add_argument("--output", "-o", type=str, default="cloudbeds_hotels.csv", help="Output CSV file")
    parser.add_argument("--groups-file", type=str, help="JSON file with group subdomains")
    parser.add_argument("--concurrency", "-c", type=int, default=3, help="Concurrent browsers (default: 3)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between requests in seconds (default: 2.0)")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    log_level = "DEBUG" if args.debug else "INFO"
    logger.add(sys.stderr, level=log_level, format="<level>{level: <8}</level> | {message}")
    
    # Load groups
    groups_file = args.groups_file or Path(__file__).parent.parent.parent / "data" / "cloudbeds_sitemap_leads.json"
    
    if not Path(groups_file).exists():
        logger.error(f"Groups file not found: {groups_file}")
        logger.info("Run first: uv run python scripts/scrapers/cloudbeds_sitemap.py --output data/cloudbeds_sitemap_leads.json")
        sys.exit(1)
    
    with open(groups_file) as f:
        groups = json.load(f)
    
    subdomains = [g['subdomain'] for g in groups if not g.get('is_demo', False)]
    logger.info(f"Loaded {len(subdomains)} group subdomains")
    
    # Apply limits
    subdomains = subdomains[args.start_from:]
    if args.max_groups:
        subdomains = subdomains[:args.max_groups]
    
    logger.info(f"Scraping {len(subdomains)} groups (starting from index {args.start_from})")
    
    # Scrape with shared browser pool (rate limited)
    logger.info(f"Using concurrency={args.concurrency}, delay={args.delay}s between requests")
    
    async with CloudbedsGroupScraper(
        concurrency=args.concurrency,
        delay=args.delay,
        debug=args.debug,
    ) as scraper:
        all_hotels = await scraper.scrape_all(subdomains)
    
    # Dedupe by (name, slug)
    seen = set()
    unique_hotels = []
    for h in all_hotels:
        key = (h.name, h.slug or h.property_id)
        if key not in seen:
            seen.add(key)
            unique_hotels.append(h)
    
    # Stats
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"Groups scraped: {len(subdomains)}")
    logger.info(f"Total hotels found: {len(all_hotels)}")
    logger.info(f"Unique hotels: {len(unique_hotels)}")
    logger.info(f"With slug: {sum(1 for h in unique_hotels if h.slug)}")
    logger.info(f"With property ID: {sum(1 for h in unique_hotels if h.property_id)}")
    
    # Save to CSV
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'city', 'slug', 'property_id', 'booking_url', 'group_subdomain'])
        writer.writeheader()
        for h in unique_hotels:
            writer.writerow(asdict(h))
    
    logger.info(f"Saved {len(unique_hotels)} hotels to {output_path}")
    
    # Sample
    logger.info("")
    logger.info("Sample hotels:")
    for h in unique_hotels[:15]:
        logger.info(f"  {h.name} ({h.city}) - {h.slug or h.property_id}")


if __name__ == "__main__":
    asyncio.run(main())
