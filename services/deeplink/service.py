"""Business logic for deep-link generation.

All data comes from the API request — no URL parsing, no auto-detection.
The caller specifies engine, property_id, dates, and guest counts.

Tier 1 (direct link): SiteMinder, Mews — instant URL construction.
Tier 2 (proxy session): Cloudbeds, ResNexus — reverse proxy with autobook JS.
"""

import logging
import secrets
from datetime import date
from typing import Optional

from lib.deeplink.models import DeepLinkConfidence, DeepLinkResult
from services.deeplink import repo

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Engine config — target hosts and path builders
# ---------------------------------------------------------------------------

ENGINE_HOSTS = {
    "resnexus": "resnexus.com",
    "cloudbeds": "hotels.cloudbeds.com",
}


# ---------------------------------------------------------------------------
# Tier 1: Direct link construction (no proxy)
# ---------------------------------------------------------------------------


def create_direct_link(
    engine: str,
    property_id: str,
    checkin: date,
    checkout: date,
    adults: int = 2,
    children: int = 0,
    rooms: int = 1,
    promo_code: Optional[str] = None,
    rate_id: Optional[str] = None,
    currency: Optional[str] = None,
) -> DeepLinkResult:
    """Build a direct deep-link URL from engine + property_id + dates."""
    builder = DIRECT_LINK_BUILDERS.get(engine)
    if not builder:
        return DeepLinkResult(
            url="",
            engine_name=engine,
            confidence=DeepLinkConfidence.NONE,
            dates_prefilled=False,
        )
    return builder(
        property_id=property_id,
        checkin=checkin,
        checkout=checkout,
        adults=adults,
        children=children,
        rooms=rooms,
        promo_code=promo_code,
        rate_id=rate_id,
        currency=currency,
    )


def _direct_resnexus(property_id: str, checkin: date, checkout: date,
                     adults: int = 2, **kwargs) -> DeepLinkResult:
    nights = (checkout - checkin).days
    startdate = checkin.strftime("%m/%d/%Y")
    url = (
        f"https://resnexus.com/resnexus/reservations/book/{property_id}"
        f"?startdate={startdate}&nights={nights}&adults={adults}"
    )
    return DeepLinkResult(
        url=url,
        engine_name="ResNexus",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
    )


def _direct_cloudbeds(property_id: str, checkin: date, checkout: date,
                      adults: int = 2, rate_id: Optional[str] = None,
                      **kwargs) -> DeepLinkResult:
    parts = [
        f"checkin={checkin.isoformat()}",
        f"checkout={checkout.isoformat()}",
        f"adults={adults}",
    ]
    if rate_id:
        parts.append(f"room_type_id={rate_id}")
    parts.append("submit=1")
    url = f"https://hotels.cloudbeds.com/reservation/{property_id}#{'&'.join(parts)}"
    return DeepLinkResult(
        url=url,
        engine_name="Cloudbeds",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
    )


def _direct_siteminder(property_id: str, checkin: date, checkout: date,
                       adults: int = 2, children: int = 0,
                       **kwargs) -> DeepLinkResult:
    url = (
        f"https://direct-book.com/properties/{property_id}"
        f"?checkInDate={checkin.isoformat()}"
        f"&checkOutDate={checkout.isoformat()}"
        f"&items%5B0%5D%5Badults%5D={adults}"
        f"&items%5B0%5D%5Bchildren%5D={children}"
        f"&items%5B0%5D%5Binfants%5D=0"
    )
    return DeepLinkResult(
        url=url,
        engine_name="SiteMinder",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
    )


def _direct_mews(property_id: str, checkin: date, checkout: date,
                 adults: int = 2, **kwargs) -> DeepLinkResult:
    url = (
        f"https://app.mews.com/distributor/{property_id}"
        f"?mewsStart={checkin.isoformat()}"
        f"&mewsEnd={checkout.isoformat()}"
        f"&mewsAdultCount={adults}"
    )
    return DeepLinkResult(
        url=url,
        engine_name="Mews",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
    )


def _direct_rms(property_id: str, checkin: date, checkout: date,
                adults: int = 2, **kwargs) -> DeepLinkResult:
    url = (
        f"https://{property_id}.rmscloud.com/obookings/Search"
        f"?arrivaldate={checkin.strftime('%d/%m/%Y')}"
        f"&departuredate={checkout.strftime('%d/%m/%Y')}"
        f"&adults={adults}"
    )
    return DeepLinkResult(
        url=url,
        engine_name="RMS Cloud",
        confidence=DeepLinkConfidence.LOW,
        dates_prefilled=True,
    )


DIRECT_LINK_BUILDERS = {
    "resnexus": _direct_resnexus,
    "cloudbeds": _direct_cloudbeds,
    "siteminder": _direct_siteminder,
    "mews": _direct_mews,
    "rms": _direct_rms,
}


# ---------------------------------------------------------------------------
# Tier 2: Proxy session creation
# ---------------------------------------------------------------------------


def create_proxy_session(
    engine: str,
    property_id: str,
    checkin: date,
    checkout: date,
    adults: int = 2,
    children: int = 0,
    rooms: int = 1,
    promo_code: Optional[str] = None,
    rate_id: Optional[str] = None,
    currency: Optional[str] = None,
    autobook: bool = True,
    proxy_host: Optional[str] = None,
) -> DeepLinkResult:
    """Create a reverse-proxy session for Tier 2 engines.

    All data comes from the request — nothing is auto-detected.
    """
    builder = PROXY_BUILDERS.get(engine)
    if not builder:
        return DeepLinkResult(
            url="",
            engine_name=engine,
            confidence=DeepLinkConfidence.NONE,
            dates_prefilled=False,
        )
    return builder(
        property_id=property_id,
        checkin=checkin,
        checkout=checkout,
        adults=adults,
        children=children,
        rooms=rooms,
        promo_code=promo_code,
        rate_id=rate_id,
        currency=currency,
        autobook=autobook,
        proxy_host=proxy_host,
    )


def _proxy_resnexus(
    property_id: str,
    checkin: date,
    checkout: date,
    adults: int = 2,
    autobook: bool = True,
    proxy_host: Optional[str] = None,
    **kwargs,
) -> DeepLinkResult:
    """Create proxy session for ResNexus."""
    nights = (checkout - checkin).days
    startdate = checkin.strftime("%m/%d/%Y")

    checkout_path = (
        f"/resnexus/reservations/book/{property_id}"
        f"?startdate={startdate}&nights={nights}&adults={adults}"
    )

    session_id = repo.store_proxy_session(
        cookies={},
        target_host="resnexus.com",
        checkout_path=checkout_path,
        autobook=autobook,
        autobook_engine="resnexus",
    )

    proxy_url = _build_proxy_url(session_id, proxy_host)
    log.info(f"ResNexus proxy: {proxy_url}")

    return DeepLinkResult(
        url=proxy_url,
        engine_name="ResNexus",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
        session_id=session_id,
    )


def _proxy_cloudbeds(
    property_id: str,
    checkin: date,
    checkout: date,
    adults: int = 2,
    currency: Optional[str] = None,
    autobook: bool = True,
    proxy_host: Optional[str] = None,
    **kwargs,
) -> DeepLinkResult:
    """Create proxy session for Cloudbeds."""
    checkout_path = (
        f"/en/reservation/{property_id}"
        f"?checkin={checkin.isoformat()}"
        f"&checkout={checkout.isoformat()}"
        f"&adults={adults}"
    )
    if currency:
        checkout_path += f"&currency={currency}"

    session_id = repo.store_proxy_session(
        cookies={},
        target_host="hotels.cloudbeds.com",
        checkout_path=checkout_path,
        autobook=autobook,
        autobook_engine="cloudbeds",
    )

    proxy_url = _build_proxy_url(session_id, proxy_host)
    log.info(f"Cloudbeds proxy: {proxy_url}")

    return DeepLinkResult(
        url=proxy_url,
        engine_name="Cloudbeds",
        confidence=DeepLinkConfidence.HIGH,
        dates_prefilled=True,
        session_id=session_id,
    )


PROXY_BUILDERS = {
    "resnexus": _proxy_resnexus,
    "cloudbeds": _proxy_cloudbeds,
}


# ---------------------------------------------------------------------------
# Short links
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_proxy_url(session_id: str, proxy_host: Optional[str]) -> str:
    if proxy_host:
        scheme = "https" if "ngrok" in proxy_host else "http"
        return f"{scheme}://{proxy_host}/book/{session_id}"
    return f"http://localhost:8000/book/{session_id}"
