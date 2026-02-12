"""Cloudbeds Tier 2: Playwright-based checkout URL generation.

Cloudbeds is session-based — to reach the /guests checkout page:
  1. Load reservation page with hash params (dates auto-fill, rooms load)
  2. Click "Select Accommodations" on the first room
  3. Click "Add" to add room to cart
  4. Close modal overlay
  5. Click "Book Now" → navigates to /guests checkout URL
"""

import asyncio
import logging
from typing import Optional

from playwright.async_api import Page, async_playwright

from lib.deeplink.engines.cloudbeds import CloudbedsBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

log = logging.getLogger(__name__)

_tier1 = CloudbedsBuilder()

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


async def build_checkout_url(request: DeepLinkRequest, headless: bool = True) -> DeepLinkResult:
    """Automate Cloudbeds booking flow to get a /guests checkout URL."""
    slug = _tier1.extract_slug(request.booking_url)
    if not slug:
        return DeepLinkResult(
            url=request.booking_url,
            engine_name="Cloudbeds",
            confidence=DeepLinkConfidence.NONE,
            dates_prefilled=False,
            original_url=request.booking_url,
        )

    checkin_str = request.checkin.isoformat()
    checkout_str = request.checkout.isoformat()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent=_UA)
        page = await context.new_page()

        try:
            checkout_url = await _automate_booking(
                page, slug, checkin_str, checkout_str, request.adults
            )
            if checkout_url and "/guests" in checkout_url:
                return DeepLinkResult(
                    url=checkout_url,
                    engine_name="Cloudbeds",
                    confidence=DeepLinkConfidence.HIGH,
                    dates_prefilled=True,
                    original_url=request.booking_url,
                )

            log.warning("Cloudbeds automation didn't reach /guests, returning Tier 1 URL")
            return _tier1.build(request)
        finally:
            await page.close()
            await context.close()
            await browser.close()


async def _automate_booking(
    page: Page,
    slug: str,
    checkin: str,
    checkout: str,
    adults: int,
) -> Optional[str]:
    """Drive Cloudbeds: load with hash params → select room → add → book now."""

    # Load with hash params — Cloudbeds auto-searches and shows rooms
    url = (
        f"https://hotels.cloudbeds.com/reservation/{slug}"
        f"#checkin={checkin}&checkout={checkout}&adults={adults}&submit=1"
    )
    await page.goto(url, timeout=30000, wait_until="domcontentloaded")
    await asyncio.sleep(8)  # Wait for React + room results to render

    # 1. Click "Select Accommodations" on first available room
    select_btn = page.locator('[data-testid^="rate-plan-select-individual-button-"]').first
    if not await select_btn.count():
        log.warning("No 'Select Accommodations' button found")
        return None
    await select_btn.click()
    await asyncio.sleep(2)

    # 2. Click "Add" to add room to cart
    add_btn = page.locator('[data-testid^="select-individual-add-button-"]').first
    if not await add_btn.count():
        log.warning("No 'Add' button found")
        return None
    await add_btn.click()
    await asyncio.sleep(2)

    # 3. Close modal overlay
    await page.keyboard.press("Escape")
    await asyncio.sleep(1)

    # 4. Click "Book Now" in shopping cart (force bypasses any remaining overlay)
    book_btn = page.locator('[data-testid="shopping-cart-confirm-button"]')
    if not await book_btn.count():
        log.warning("No 'Book Now' button found in cart")
        return None
    await book_btn.click(force=True)
    await asyncio.sleep(5)

    checkout_url = page.url
    log.info(f"Cloudbeds checkout URL: {checkout_url}")
    return checkout_url
