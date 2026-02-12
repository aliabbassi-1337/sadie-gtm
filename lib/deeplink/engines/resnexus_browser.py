"""ResNexus Tier 2: Reverse proxy with client-side auto-booking.

Flow:
  1. Create proxy session instantly (no browser needed)
  2. User visits /book/{session_id} → cookie set → redirect to proxied search page
  3. Proxy serves ResNexus page with dates pre-filled + autobook JS injected
  4. Autobook JS clicks: Book room → wait for server confirmation → Checkout
  5. User lands on /checkout page with cart populated (server-side ASP.NET session)

Unlike Cloudbeds (client-side React cart), ResNexus has a server-side cart,
so the proxy approach is ideal — session cookies maintain cart state.
"""

import logging
from typing import Optional

from lib.deeplink.booking_proxy import store_session
from lib.deeplink.engines.resnexus import ResNexusBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

log = logging.getLogger(__name__)

_tier1 = ResNexusBuilder()


def build_checkout_url(
    request: DeepLinkRequest,
    proxy_host: Optional[str] = None,
) -> DeepLinkResult:
    """Create a proxy session for ResNexus with client-side autobook."""
    guid = _tier1.extract_slug(request.booking_url)
    if not guid:
        return DeepLinkResult(
            url=request.booking_url,
            engine_name="ResNexus",
            confidence=DeepLinkConfidence.NONE,
            dates_prefilled=False,
            original_url=request.booking_url,
        )

    nights = (request.checkout - request.checkin).days
    startdate = request.checkin.strftime("%m/%d/%Y")

    search_path = (
        f"/resnexus/reservations/book/{guid}"
        f"?startdate={startdate}"
        f"&nights={nights}"
        f"&adults={request.adults}"
    )

    session_id = store_session(
        cookies={},
        target_host="resnexus.com",
        checkout_path=search_path,
        autobook=True,
        autobook_engine="resnexus",
    )

    if proxy_host:
        scheme = "https" if "ngrok" in proxy_host else "http"
        proxy_url = f"{scheme}://{proxy_host}/book/{session_id}"
    else:
        proxy_url = f"http://localhost:8000/book/{session_id}"

    log.info(f"ResNexus proxy URL: {proxy_url}")
    return DeepLinkResult(
        url=proxy_url,
        engine_name="ResNexus",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
        original_url=request.booking_url,
    )
