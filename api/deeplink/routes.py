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
    booking_url: str
    checkin: date
    checkout: date
    adults: int = 2
    children: int = 0
    rooms: int = 1
    promo_code: Optional[str] = None
    rate_id: Optional[str] = None
    use_browser: bool = False  # Set True for Tier 2 engines (Cloudbeds, ResNexus)

    @field_validator("checkout")
    @classmethod
    def checkout_after_checkin(cls, v, info):
        if "checkin" in info.data and v <= info.data["checkin"]:
            raise ValueError("checkout must be after checkin")
        return v


class DeepLinkResponse(BaseModel):
    deep_link_url: str
    engine_name: str
    confidence: str
    dates_prefilled: bool
    short_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/api/deeplink", response_model=DeepLinkResponse)
async def create_deeplink(body: DeepLinkBody, request: Request):
    req = DeepLinkRequest(
        booking_url=body.booking_url,
        checkin=body.checkin,
        checkout=body.checkout,
        adults=body.adults,
        children=body.children,
        rooms=body.rooms,
        promo_code=body.promo_code,
        rate_id=body.rate_id,
    )

    proxy_host = request.headers.get("host", "localhost:8000")

    if body.use_browser:
        result = service.create_proxy_deeplink(req, proxy_host=proxy_host)
    else:
        result = service.create_deeplink(req)

    resp = DeepLinkResponse(
        deep_link_url=result.url,
        engine_name=result.engine_name,
        confidence=result.confidence.value,
        dates_prefilled=result.dates_prefilled,
    )

    # For Tier 1 (non-proxy), add a short URL redirect
    if "/book/" not in result.url:
        code = service.create_short_link(result.url)
        base_url = str(request.base_url).rstrip("/")
        resp.short_url = f"{base_url}/r/{code}"

    return resp


@router.get("/r/{code}")
async def redirect(code: str):
    url = service.resolve_short_link(code)
    if not url:
        raise HTTPException(status_code=404, detail="Link not found")
    # Use JS redirect instead of 302 to bypass ngrok free-tier interstitial
    html = f"""<!DOCTYPE html>
<html><head>
<meta http-equiv="refresh" content="0;url={url}">
<script>window.location.replace("{url}");</script>
</head><body>Redirecting...</body></html>"""
    return HTMLResponse(content=html)
