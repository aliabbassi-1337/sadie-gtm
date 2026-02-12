"""FastAPI redirect service for deep-link short URLs.

Run:
    uv run uvicorn lib.deeplink.redirect_service:app --reload --port 8000
"""

import secrets
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator

from lib.deeplink.generator import generate_deeplink, generate_deeplink_async
from lib.deeplink.models import DeepLinkRequest

app = FastAPI(title="Sadie Deep-Link Service")

# In-memory link store: code → deep_link_url
_links: dict[str, str] = {}


class DeepLinkBody(BaseModel):
    booking_url: str
    checkin: date
    checkout: date
    adults: int = 2
    children: int = 0
    rooms: int = 1
    promo_code: Optional[str] = None
    rate_id: Optional[str] = None
    use_browser: bool = False  # Set True for Tier 2 engines (Cloudbeds, RMS)

    @field_validator("checkout")
    @classmethod
    def checkout_after_checkin(cls, v, info):
        if "checkin" in info.data and v <= info.data["checkin"]:
            raise ValueError("checkout must be after checkin")
        return v


@app.post("/api/deeplink")
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

    if body.use_browser:
        result = await generate_deeplink_async(req)
    else:
        result = generate_deeplink(req)

    code = secrets.token_urlsafe(6)  # 8 chars
    _links[code] = result.url

    base_url = str(request.base_url).rstrip("/")
    short_url = f"{base_url}/r/{code}"

    return {
        "short_url": short_url,
        "deep_link_url": result.url,
        "engine_name": result.engine_name,
        "confidence": result.confidence.value,
        "dates_prefilled": result.dates_prefilled,
    }


@app.get("/r/{code}")
async def redirect(code: str):
    url = _links.get(code)
    if not url:
        raise HTTPException(status_code=404, detail="Link not found")
    # Use JS redirect instead of 302 to bypass ngrok free-tier interstitial
    html = f"""<!DOCTYPE html>
<html><head>
<meta http-equiv="refresh" content="0;url={url}">
<script>window.location.replace("{url}");</script>
</head><body>Redirecting...</body></html>"""
    return HTMLResponse(content=html)
