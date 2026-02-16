"""API routes for deep-link creation and short-link redirects."""

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator

from lib.deeplink.models import DeepLinkRequest
from services.deeplink import service

router = APIRouter()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class DeepLinkBody(BaseModel):
    """Create a deep link for any supported booking engine.

    Tier 1 (SiteMinder, Mews): constructs a direct URL with query params.
    Tier 2 (Cloudbeds, ResNexus): creates a reverse-proxy session.

    Example — ResNexus proxy:
        {
            "engine": "resnexus",
            "property_id": "a1b2c3d4-e5f6-...",
            "checkin": "2026-03-01",
            "checkout": "2026-03-03",
            "adults": 2,
            "proxy": true
        }

    Example — SiteMinder direct link:
        {
            "engine": "siteminder",
            "property_id": "thehindsheaddirect",
            "checkin": "2026-03-01",
            "checkout": "2026-03-03",
            "adults": 2
        }
    """

    engine: str  # "siteminder", "cloudbeds", "mews", "rms", "resnexus"
    property_id: str  # slug, GUID, or property code — engine-specific
    checkin: date
    checkout: date
    adults: int = 2
    children: int = 0
    rooms: int = 1
    promo_code: Optional[str] = None
    rate_id: Optional[str] = None  # room_type_id for Cloudbeds
    currency: Optional[str] = None  # e.g. "usd"
    proxy: bool = False  # True → reverse-proxy session (Tier 2)
    autobook: bool = True  # Auto-click through room selection (Tier 2 only)

    @field_validator("checkout")
    @classmethod
    def checkout_after_checkin(cls, v, info):
        if "checkin" in info.data and v <= info.data["checkin"]:
            raise ValueError("checkout must be after checkin")
        return v

    @field_validator("engine")
    @classmethod
    def normalize_engine(cls, v):
        return v.strip().lower()


class DeepLinkResponse(BaseModel):
    deep_link_url: str
    engine: str
    confidence: str
    dates_prefilled: bool
    short_url: Optional[str] = None
    session_id: Optional[str] = None  # proxy session ID (Tier 2 only)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/api/deeplink", response_model=DeepLinkResponse)
async def create_deeplink(body: DeepLinkBody, request: Request):
    proxy_host = request.headers.get("host", "localhost:8000")

    if body.proxy:
        result = await service.create_proxy_session(
            engine=body.engine,
            property_id=body.property_id,
            checkin=body.checkin,
            checkout=body.checkout,
            adults=body.adults,
            children=body.children,
            rooms=body.rooms,
            promo_code=body.promo_code,
            rate_id=body.rate_id,
            currency=body.currency,
            autobook=body.autobook,
            proxy_host=proxy_host,
        )
    else:
        result = service.create_direct_link(
            engine=body.engine,
            property_id=body.property_id,
            checkin=body.checkin,
            checkout=body.checkout,
            adults=body.adults,
            children=body.children,
            rooms=body.rooms,
            promo_code=body.promo_code,
            rate_id=body.rate_id,
            currency=body.currency,
        )

    resp = DeepLinkResponse(
        deep_link_url=result.url,
        engine=result.engine_name,
        confidence=result.confidence.value,
        dates_prefilled=result.dates_prefilled,
        session_id=result.session_id,
    )

    # For direct links, add a short URL redirect
    if not body.proxy:
        code = await service.create_short_link(result.url)
        base_url = str(request.base_url).rstrip("/")
        resp.short_url = f"{base_url}/r/{code}"

    return resp


@router.get("/r/{code}")
async def redirect(code: str):
    url = await service.resolve_short_link(code)
    if not url:
        raise HTTPException(status_code=404, detail="Link not found")
    # Use JS redirect instead of 302 to bypass ngrok free-tier interstitial
    html = f"""<!DOCTYPE html>
<html><head>
<meta http-equiv="refresh" content="0;url={url}">
<script>window.location.replace("{url}");</script>
</head><body>Redirecting...</body></html>"""
    return HTMLResponse(content=html)
