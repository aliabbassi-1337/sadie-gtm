"""Server-side room booking via Playwright.

Loads the booking engine page in a headless browser, clicks "Add Room",
waits for the cart to populate, and returns the session cookies.
The proxy then redirects the user directly to checkout with a pre-filled cart.

This avoids fragile client-side JS injection — the browser handles WAF
challenges, CSRF tokens, and AJAX calls natively.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from playwright.async_api import async_playwright

log = logging.getLogger(__name__)


@dataclass
class BookingResult:
    success: bool
    cookies: dict[str, str]
    cart_count: int = 0
    error: Optional[str] = None


async def book_resnexus_room(
    property_id: str,
    startdate: str,
    nights: int,
    adults: int = 2,
    timeout_ms: int = 30000,
) -> BookingResult:
    """Load ResNexus, click first available BookRoom, return session cookies.

    Args:
        property_id: ResNexus property GUID
        startdate: Check-in date as MM/DD/YYYY
        nights: Number of nights
        adults: Number of adults
        timeout_ms: Max wait for page load + booking

    Returns:
        BookingResult with cookies and cart count
    """
    url = (
        f"https://resnexus.com/resnexus/reservations/book/{property_id}"
        f"?startdate={startdate}&nights={nights}&adults={adults}"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = await context.new_page()

            log.info(f"[browser_book] Loading {url}")
            await page.goto(url, wait_until="load", timeout=timeout_ms)
            # Wait for JS to render room cards
            await page.wait_for_timeout(5000)

            # Find first BookRoom button (actual rooms, not packages)
            btn = await page.query_selector(
                'button.room-action.add-room[onclick^="BookRoom"]'
            )
            if not btn:
                # Fallback: any add-room button
                btn = await page.query_selector("button.room-action.add-room")

            if not btn:
                log.warning("[browser_book] No BookRoom button found")
                return BookingResult(success=False, cookies={}, error="no_rooms_available")

            onclick = await btn.get_attribute("onclick") or ""
            log.info(f"[browser_book] Clicking: {onclick}")

            # Invoke the onclick directly — more reliable than .click() for
            # ResNexus which binds via onclick= attribute
            await page.evaluate(
                """(btn) => {
                    var fn = btn.getAttribute('onclick') || '';
                    if (fn) new Function(fn).call(btn);
                    else btn.click();
                }""",
                btn,
            )

            # Wait for AJAX to complete and cart to update
            await page.wait_for_timeout(5000)

            # Check cart count
            cart_text = await page.evaluate(
                "document.querySelector('.view-cart.topCartIcon')?.textContent?.trim() || '0'"
            )
            cart_count = int(cart_text) if cart_text.isdigit() else 0
            log.info(f"[browser_book] Cart count: {cart_count}")

            if cart_count == 0:
                # Cart didn't populate — try once more with a longer wait
                await page.wait_for_timeout(3000)
                cart_text = await page.evaluate(
                    "document.querySelector('.view-cart.topCartIcon')?.textContent?.trim() || '0'"
                )
                cart_count = int(cart_text) if cart_text.isdigit() else 0

            # Extract all cookies from the browser context
            raw_cookies = await context.cookies()
            cookies = {
                c["name"]: c["value"]
                for c in raw_cookies
                if "resnexus" in c.get("domain", "")
            }

            log.info(
                f"[browser_book] Done: cart={cart_count}, cookies={list(cookies.keys())}"
            )

            return BookingResult(
                success=cart_count > 0,
                cookies=cookies,
                cart_count=cart_count,
                error=None if cart_count > 0 else "cart_empty_after_click",
            )

        finally:
            await browser.close()
