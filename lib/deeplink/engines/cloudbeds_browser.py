"""Cloudbeds Tier 2: Reverse proxy with client-side auto-booking.

Flow:
  1. Create proxy session instantly (no Playwright needed)
  2. User visits /book/{session_id} → cookie set → redirect to proxied search page
  3. Proxy serves Cloudbeds page with dates pre-filled + autobook JS injected
  4. Autobook JS clicks: Select Accommodations → Add → Book Now
  5. React Router navigates to /guests checkout page with cart populated

The proxy handles all cookie/header rewriting. The autobook JS handles room
selection in the user's browser so React cart state is populated naturally.
"""

import logging
from typing import Optional

from lib.deeplink.booking_proxy import store_session
from lib.deeplink.engines.cloudbeds import CloudbedsBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

log = logging.getLogger(__name__)

_tier1 = CloudbedsBuilder()


def build_checkout_url(
    request: DeepLinkRequest,
    proxy_host: Optional[str] = None,
) -> DeepLinkResult:
    """Create a proxy session for Cloudbeds with client-side autobook.

    No Playwright needed — the proxy serves the page with injected JS that
    automates room selection in the user's browser. Instant response.
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

    # Build search page path with dates pre-filled
    search_path = (
        f"/en/reservation/{slug}"
        f"?checkin={request.checkin.isoformat()}"
        f"&checkout={request.checkout.isoformat()}"
        f"&adults={request.adults}"
        f"&currency=usd"
    )

    session_id = store_session(
        cookies={},  # No pre-captured cookies — proxy handles cookie flow naturally
        target_host="hotels.cloudbeds.com",
        checkout_path=search_path,
        autobook=True,
    )

    if proxy_host:
        scheme = "https" if "ngrok" in proxy_host else "http"
        proxy_url = f"{scheme}://{proxy_host}/book/{session_id}"
    else:
        proxy_url = f"http://localhost:8000/book/{session_id}"

    log.info(f"Cloudbeds proxy URL: {proxy_url}")
    return DeepLinkResult(
        url=proxy_url,
        engine_name="Cloudbeds",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
        original_url=request.booking_url,
    )
