"""Business logic for deep-link generation.

Tier 1 (URL construction): SiteMinder, Mews — instant, no browser.
Tier 2 (reverse proxy + autobook): Cloudbeds, ResNexus — proxy with client-side JS.
"""

import logging
import secrets
from typing import Optional
from urllib.parse import urlparse

from lib.deeplink.engines import ENGINE_BUILDERS, ENGINE_DOMAIN_PATTERNS
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult
from services.deeplink import repo

log = logging.getLogger(__name__)

# Engines that need reverse proxy + autobook (Tier 2)
TIER2_ENGINES = {"Cloudbeds", "RMS Cloud", "ResNexus"}


def detect_engine(url: str) -> Optional[str]:
    """Detect booking engine name from URL domain.

    Returns engine name (e.g. "SiteMinder") or None.
    """
    parsed = urlparse(url)
    hostname = parsed.hostname or ""

    for engine_name, domains in ENGINE_DOMAIN_PATTERNS.items():
        for domain in domains:
            if domain in hostname:
                return engine_name
    return None


def create_deeplink(request: DeepLinkRequest) -> DeepLinkResult:
    """Generate a deep-link URL with dates/guests pre-filled.

    Pure function — no network, no DB, no async. Tier 1 engines only.
    For Tier 2 engines, use create_proxy_deeplink().
    """
    engine_name = detect_engine(request.booking_url)

    if engine_name and engine_name in ENGINE_BUILDERS:
        return ENGINE_BUILDERS[engine_name].build(request)

    return DeepLinkResult(
        url=request.booking_url,
        engine_name="Unknown",
        confidence=DeepLinkConfidence.NONE,
        dates_prefilled=False,
        original_url=request.booking_url,
    )


def create_proxy_deeplink(
    request: DeepLinkRequest,
    proxy_host: Optional[str] = None,
) -> DeepLinkResult:
    """Generate a proxy URL for Tier 2 engines (instant, no browser needed).

    Tier 1 (SiteMinder, Mews): instant URL construction, no proxy.
    Tier 2 (Cloudbeds, ResNexus): reverse proxy with client-side autobook JS.
    """
    engine_name = detect_engine(request.booking_url)

    if engine_name == "Cloudbeds":
        return _build_cloudbeds_proxy(request, proxy_host)
    if engine_name == "ResNexus":
        return _build_resnexus_proxy(request, proxy_host)

    # Tier 1 or unknown — use sync builder
    return create_deeplink(request)


def create_short_link(url: str) -> str:
    """Create a short link code for a URL. Returns the code."""
    code = secrets.token_urlsafe(6)
    repo.store_short_link(code, url)
    return code


def resolve_short_link(code: str) -> Optional[str]:
    """Resolve a short link code to its URL."""
    return repo.get_short_link(code)


def get_proxy_session(session_id: str) -> Optional[dict]:
    """Retrieve a proxy session by ID."""
    return repo.get_proxy_session(session_id)


async def create_deeplink_for_hotel(
    hotel_id: int,
    checkin,
    checkout,
    adults: int = 2,
    children: int = 0,
    rooms: int = 1,
    promo_code: Optional[str] = None,
    use_proxy: bool = False,
    proxy_host: Optional[str] = None,
) -> DeepLinkResult:
    """Look up a hotel's booking URL from DB, then generate a deep-link."""
    row = await repo.get_hotel_booking_info(hotel_id)

    if not row:
        raise ValueError(f"No booking URL found for hotel_id={hotel_id}")

    request = DeepLinkRequest(
        booking_url=row["booking_url"],
        checkin=checkin,
        checkout=checkout,
        adults=adults,
        children=children,
        rooms=rooms,
        promo_code=promo_code,
    )

    if use_proxy:
        return create_proxy_deeplink(request, proxy_host=proxy_host)
    return create_deeplink(request)


# ---------------------------------------------------------------------------
# Tier 2 proxy session builders (absorbed from engines/*_browser.py)
# ---------------------------------------------------------------------------


def _build_proxy_url(session_id: str, proxy_host: Optional[str]) -> str:
    """Build the user-facing proxy URL from a session ID."""
    if proxy_host:
        scheme = "https" if "ngrok" in proxy_host else "http"
        return f"{scheme}://{proxy_host}/book/{session_id}"
    return f"http://localhost:8000/book/{session_id}"


def _build_cloudbeds_proxy(
    request: DeepLinkRequest,
    proxy_host: Optional[str],
) -> DeepLinkResult:
    """Create a proxy session for Cloudbeds with client-side autobook."""
    builder = ENGINE_BUILDERS["Cloudbeds"]
    slug = builder.extract_slug(request.booking_url)
    if not slug:
        return DeepLinkResult(
            url=request.booking_url,
            engine_name="Cloudbeds",
            confidence=DeepLinkConfidence.NONE,
            dates_prefilled=False,
            original_url=request.booking_url,
        )

    search_path = (
        f"/en/reservation/{slug}"
        f"?checkin={request.checkin.isoformat()}"
        f"&checkout={request.checkout.isoformat()}"
        f"&adults={request.adults}"
        f"&currency=usd"
    )

    session_id = repo.store_proxy_session(
        cookies={},
        target_host="hotels.cloudbeds.com",
        checkout_path=search_path,
        autobook=True,
        autobook_engine="cloudbeds",
    )

    proxy_url = _build_proxy_url(session_id, proxy_host)
    log.info(f"Cloudbeds proxy URL: {proxy_url}")
    return DeepLinkResult(
        url=proxy_url,
        engine_name="Cloudbeds",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
        original_url=request.booking_url,
    )


def _build_resnexus_proxy(
    request: DeepLinkRequest,
    proxy_host: Optional[str],
) -> DeepLinkResult:
    """Create a proxy session for ResNexus with client-side autobook."""
    builder = ENGINE_BUILDERS["ResNexus"]
    guid = builder.extract_slug(request.booking_url)
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

    session_id = repo.store_proxy_session(
        cookies={},
        target_host="resnexus.com",
        checkout_path=search_path,
        autobook=True,
        autobook_engine="resnexus",
    )

    proxy_url = _build_proxy_url(session_id, proxy_host)
    log.info(f"ResNexus proxy URL: {proxy_url}")
    return DeepLinkResult(
        url=proxy_url,
        engine_name="ResNexus",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
        original_url=request.booking_url,
    )
