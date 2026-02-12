"""Deep-link URL generator.

generate_deeplink() — pure function, no network/DB/async. Tier 1 only.
generate_deeplink_async() — async, handles Tier 1 (instant) + Tier 2 (browser).
generate_deeplink_for_hotel() — async, looks up booking URL from DB.
"""

from typing import Optional
from urllib.parse import urlparse

from lib.deeplink.engines import ENGINE_BUILDERS, ENGINE_DOMAIN_PATTERNS
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

# Engines that need browser automation (Tier 2) instead of URL construction
TIER2_ENGINES = {"Cloudbeds", "RMS Cloud"}


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


def generate_deeplink(request: DeepLinkRequest) -> DeepLinkResult:
    """Generate a deep-link URL with dates/guests pre-filled.

    Pure function — no network, no DB, no async. Tier 1 engines only.
    For Tier 2 engines (Cloudbeds, RMS), use generate_deeplink_async().
    """
    engine_name = detect_engine(request.booking_url)

    if engine_name and engine_name in ENGINE_BUILDERS:
        return ENGINE_BUILDERS[engine_name].build(request)

    # Unknown engine: return base URL unchanged
    return DeepLinkResult(
        url=request.booking_url,
        engine_name="Unknown",
        confidence=DeepLinkConfidence.NONE,
        dates_prefilled=False,
        original_url=request.booking_url,
    )


async def generate_deeplink_async(
    request: DeepLinkRequest, headless: bool = True
) -> DeepLinkResult:
    """Generate a deep-link URL, using browser automation for Tier 2 engines.

    Tier 1 (SiteMinder, Mews): instant URL construction, no browser.
    Tier 2 (Cloudbeds, RMS): Playwright automation to get session-based checkout URL.
    """
    engine_name = detect_engine(request.booking_url)

    if engine_name in TIER2_ENGINES:
        if engine_name == "Cloudbeds":
            from lib.deeplink.engines.cloudbeds_browser import build_checkout_url
            return await build_checkout_url(request, headless=headless)
        elif engine_name == "RMS Cloud":
            from lib.deeplink.engines.rms_browser import build_checkout_url
            return await build_checkout_url(request, headless=headless)

    # Tier 1 or unknown — use sync builder
    return generate_deeplink(request)


async def generate_deeplink_for_hotel(
    hotel_id: int,
    checkin,
    checkout,
    adults: int = 2,
    children: int = 0,
    rooms: int = 1,
    promo_code: Optional[str] = None,
    use_browser: bool = False,
) -> DeepLinkResult:
    """Look up a hotel's booking URL from DB, then generate a deep-link.

    Lazy imports to avoid side effects when using the pure function only.
    """
    from db.client import get_conn, queries

    async with get_conn() as conn:
        row = await queries.get_hotel_booking_info(conn, hotel_id=hotel_id)

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

    if use_browser:
        return await generate_deeplink_async(request)
    return generate_deeplink(request)
