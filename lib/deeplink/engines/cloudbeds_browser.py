"""Cloudbeds Tier 2: Playwright-based checkout URL generation.

Cloudbeds is session-based — you can't deep-link to checkout with URL params alone.
This module automates the booking flow:
  1. Open reservation page
  2. Fill check-in/check-out dates
  3. Click search
  4. Select first available room
  5. Capture the /guests checkout URL with session cookies
"""

import asyncio
import logging
import re
from typing import Optional

from playwright.async_api import Page, async_playwright

from lib.deeplink.engines.cloudbeds import CloudbedsBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

log = logging.getLogger(__name__)

# Reuse slug extraction from Tier 1 builder
_tier1 = CloudbedsBuilder()


async def build_checkout_url(request: DeepLinkRequest, headless: bool = True) -> DeepLinkResult:
    """Automate Cloudbeds booking flow to get a checkout URL.

    Opens the booking page in Playwright, fills dates, searches for rooms,
    selects the first available room, and captures the checkout URL.
    """
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
    base_url = f"https://hotels.cloudbeds.com/reservation/{slug}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            checkout_url = await _automate_booking(
                page, base_url, slug, checkin_str, checkout_str, request.adults
            )

            if checkout_url:
                return DeepLinkResult(
                    url=checkout_url,
                    engine_name="Cloudbeds",
                    confidence=DeepLinkConfidence.HIGH,
                    dates_prefilled=True,
                    original_url=request.booking_url,
                )

            # Fallback: construct best-guess URL even if automation failed
            log.warning("Cloudbeds automation failed, returning search page URL")
            fallback = f"{base_url}#checkin={checkin_str}&checkout={checkout_str}&adults={request.adults}&submit=1"
            return DeepLinkResult(
                url=fallback,
                engine_name="Cloudbeds",
                confidence=DeepLinkConfidence.LOW,
                dates_prefilled=True,
                original_url=request.booking_url,
            )
        finally:
            await page.close()
            await context.close()
            await browser.close()


async def _automate_booking(
    page: Page,
    base_url: str,
    slug: str,
    checkin: str,
    checkout: str,
    adults: int,
) -> Optional[str]:
    """Drive the Cloudbeds booking flow and return checkout URL."""

    # Load the reservation page
    await page.goto(base_url, timeout=30000, wait_until="domcontentloaded")
    await asyncio.sleep(4)  # Wait for React to render

    # Fill check-in date
    checkin_input = page.locator('input[name="checkin"], input[data-testid="checkin"], #startDate')
    if await checkin_input.count() > 0:
        await checkin_input.first.click()
        await checkin_input.first.fill(checkin)
    else:
        log.warning("Could not find checkin input")
        return None

    # Fill check-out date
    checkout_input = page.locator('input[name="checkout"], input[data-testid="checkout"], #endDate')
    if await checkout_input.count() > 0:
        await checkout_input.first.click()
        await checkout_input.first.fill(checkout)
    else:
        log.warning("Could not find checkout input")
        return None

    # Click search/check availability button
    search_btn = page.locator(
        'button:has-text("Search"), button:has-text("Check Availability"), '
        'button:has-text("search"), button[type="submit"], '
        'input[type="submit"], .search-button, #search-button'
    )
    if await search_btn.count() > 0:
        await search_btn.first.click()
    else:
        log.warning("Could not find search button")
        return None

    # Wait for room results to load
    await asyncio.sleep(5)

    # Look for "Book Now" / "Reserve" / "Select" button on first available room
    book_btn = page.locator(
        'button:has-text("Book Now"), button:has-text("Reserve"), '
        'button:has-text("Select"), button:has-text("Book"), '
        'a:has-text("Book Now"), a:has-text("Reserve"), '
        '.book-button, .reserve-button, .btn-book'
    )
    if await book_btn.count() > 0:
        await book_btn.first.click()
    else:
        log.warning("Could not find book/reserve button")
        return None

    # Wait for navigation to checkout/guests page
    await asyncio.sleep(3)

    # Capture the checkout URL
    current_url = page.url
    if "/guests" in current_url or "/checkout" in current_url:
        log.info(f"Captured Cloudbeds checkout URL: {current_url}")
        return current_url

    # Check if we got redirected somewhere useful
    log.info(f"Cloudbeds landed on: {current_url}")
    return current_url if current_url != base_url else None
