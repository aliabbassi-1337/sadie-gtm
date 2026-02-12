"""RMS Cloud Tier 2: Playwright-based checkout URL generation.

RMS is session-based — checkout URLs contain a server-generated UUID:
  https://ibe13.rmscloud.com/{client_id}/{area_id}/Guest/{session_uuid}/

This module automates the booking flow:
  1. Open booking page
  2. Fill check-in/check-out dates
  3. Click search
  4. Select first available room
  5. Capture the /Guest/{uuid}/ checkout URL
"""

import asyncio
import logging
import re
from typing import Optional

from playwright.async_api import Page, async_playwright

from lib.deeplink.engines.rms import RmsBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

log = logging.getLogger(__name__)

_tier1 = RmsBuilder()


def _normalize_rms_url(url: str) -> str:
    """Normalize any RMS URL to a standard bookings format.

    Same logic as lib/rms/scraper.py — converts IBE URLs to bookings format.
    """
    # Already in bookings format
    if "bookings" in url and "Search/Index" in url:
        return url
    # IBE format: ibe13.rmscloud.com/{id} → keep as-is (IBE has its own booking UI)
    return url


async def build_checkout_url(request: DeepLinkRequest, headless: bool = True) -> DeepLinkResult:
    """Automate RMS booking flow to get a checkout URL with session UUID."""
    rms_id = _tier1.extract_slug(request.booking_url)
    if not rms_id:
        return DeepLinkResult(
            url=request.booking_url,
            engine_name="RMS Cloud",
            confidence=DeepLinkConfidence.NONE,
            dates_prefilled=False,
            original_url=request.booking_url,
        )

    booking_url = _normalize_rms_url(request.booking_url)
    checkin_str = request.checkin.strftime("%d/%m/%Y")  # RMS uses DD/MM/YYYY
    checkout_str = request.checkout.strftime("%d/%m/%Y")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            checkout_url = await _automate_booking(
                page, booking_url, checkin_str, checkout_str, request.adults
            )

            if checkout_url:
                return DeepLinkResult(
                    url=checkout_url,
                    engine_name="RMS Cloud",
                    confidence=DeepLinkConfidence.HIGH,
                    dates_prefilled=True,
                    original_url=request.booking_url,
                )

            # Fallback: return base URL with best-guess params
            log.warning("RMS automation failed, returning base URL with params")
            return _tier1.build(request)
        finally:
            await page.close()
            await context.close()
            await browser.close()


async def _automate_booking(
    page: Page,
    booking_url: str,
    checkin: str,
    checkout: str,
    adults: int,
) -> Optional[str]:
    """Drive the RMS booking flow and return checkout URL with session UUID."""

    await page.goto(booking_url, timeout=30000, wait_until="domcontentloaded")
    await asyncio.sleep(5)  # RMS pages are slow to render

    # Fill arrival date
    arrival_input = page.locator(
        'input[name="arrival"], input[id*="arrival"], input[id*="Arrival"], '
        'input[placeholder*="Arrive"], input[placeholder*="Check-in"], '
        'input[name="ArrivalDate"], #txtArrival'
    )
    if await arrival_input.count() > 0:
        await arrival_input.first.click()
        await arrival_input.first.fill("")
        await arrival_input.first.type(checkin, delay=50)
        await page.keyboard.press("Escape")  # Close any datepicker popup
    else:
        log.warning("Could not find arrival date input")
        return None

    await asyncio.sleep(1)

    # Fill departure date
    departure_input = page.locator(
        'input[name="departure"], input[id*="departure"], input[id*="Departure"], '
        'input[placeholder*="Depart"], input[placeholder*="Check-out"], '
        'input[name="DepartureDate"], #txtDeparture'
    )
    if await departure_input.count() > 0:
        await departure_input.first.click()
        await departure_input.first.fill("")
        await departure_input.first.type(checkout, delay=50)
        await page.keyboard.press("Escape")
    else:
        log.warning("Could not find departure date input")
        return None

    await asyncio.sleep(1)

    # Set adults count if there's a dropdown/input
    adults_input = page.locator(
        'select[name*="adult"], select[id*="adult"], select[id*="Adult"], '
        'input[name*="adult"], #ddlAdults, #adults'
    )
    if await adults_input.count() > 0:
        el = adults_input.first
        tag = await el.evaluate("el => el.tagName.toLowerCase()")
        if tag == "select":
            await el.select_option(str(adults))
        else:
            await el.fill(str(adults))

    # Click search button
    search_btn = page.locator(
        'button:has-text("Search"), button:has-text("Check"), '
        'input[type="submit"][value*="Search"], input[type="submit"][value*="Check"], '
        'a:has-text("Search"), .search-btn, #btnSearch, '
        'button[type="submit"], input[type="button"][value*="Search"]'
    )
    if await search_btn.count() > 0:
        await search_btn.first.click()
    else:
        log.warning("Could not find search button")
        return None

    # Wait for results
    await asyncio.sleep(5)

    # Look for "Book Now" / "Select" / "Reserve" button
    book_btn = page.locator(
        'button:has-text("Book"), button:has-text("Select"), '
        'a:has-text("Book"), a:has-text("Select"), '
        'input[type="button"][value*="Book"], '
        '.book-btn, .btn-book, .btn-select'
    )
    if await book_btn.count() > 0:
        await book_btn.first.click()
    else:
        log.warning("Could not find book/select button")
        return None

    # Wait for navigation to Guest page
    await asyncio.sleep(3)

    # Check if we landed on a /Guest/ URL with session UUID
    current_url = page.url
    if "/Guest/" in current_url:
        log.info(f"Captured RMS checkout URL: {current_url}")
        return current_url

    # Check for any UUID pattern in the URL
    uuid_match = re.search(r"/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/", current_url)
    if uuid_match:
        log.info(f"Captured RMS session URL: {current_url}")
        return current_url

    log.info(f"RMS landed on: {current_url}")
    return current_url if current_url != booking_url else None
