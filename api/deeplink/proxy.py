"""Reverse proxy HTTP handlers for session-gated booking engines.

Architecture:
  1. Create proxy session with target host + path (no Playwright needed)
  2. User hits /book/{session_id} -> sets proxy cookie -> redirects to proxied page
  3. Proxy serves the booking page with injected JS:
     a. URL interceptors (rewrite fetch/XHR/WebSocket to route through proxy)
     b. Autobook script (clicks Select -> Add -> Book Now automatically)
  4. React cart is populated naturally in the user's browser
  5. User lands on /guests checkout page ready to fill in guest details

Performance:
  - Non-rewritable responses (images, fonts, etc.) are streamed directly
  - JS injection strings are cached per session (same target/proxy host)
  - Regex patterns are pre-compiled at module level
  - Request bodies are streamed to upstream
"""

import json as _json
import logging
import os
from dataclasses import dataclass
from typing import Optional

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse

from lib.deeplink.proxy_utils import (
    CHECKOUT_ADVANCE_JS,
    OVERLAY_HTML,
    OVERLAY_STYLE,
    _RE_BODY_TAG,
    _RE_HEAD_TAG,
    build_injection_js,
    parse_resnexus_room_page,
    rewrite_response_body,
    rewrite_set_cookie_domain,
)
from services.deeplink import service

log = logging.getLogger(__name__)

router = APIRouter()
catchall_router = APIRouter()

PROXY_COOKIE = "_bp_sid"

# Track sessions that already had server-side booking done (prevent duplicates)
_booked_sessions: set[str] = set()


# ---------------------------------------------------------------------------
# Headers to strip
# ---------------------------------------------------------------------------

STRIP_RESPONSE_HEADERS = frozenset({
    "content-encoding",
    "transfer-encoding",
    "content-length",
    "content-security-policy",
    "content-security-policy-report-only",
    "x-frame-options",
    "strict-transport-security",
    "clear-site-data",
})

STRIP_REQUEST_HEADERS = frozenset({
    "host",
    "accept-encoding",
    "x-forwarded-for",
    "x-forwarded-proto",
    "x-real-ip",
    "cf-connecting-ip",
    "cf-ray",
    "cf-visitor",
    "cdn-loop",
})

# Headers injected to look like a real browser navigation (bypasses WAF bot detection)
BROWSER_HEADERS = {
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "upgrade-insecure-requests": "1",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
}

REWRITABLE_CONTENT_TYPES = frozenset({
    "text/html",
    "text/css",
    "application/javascript",
    "text/javascript",
    "application/json",
})


# ---------------------------------------------------------------------------
# Cloudflare Worker fetch proxy (bypasses WAF IP blocking)
# ---------------------------------------------------------------------------

CF_WORKER_URL = os.environ.get("CF_WORKER_URL", "")
CF_PROXY_KEY = os.environ.get("CF_PROXY_KEY", "")

# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

_http_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=30.0,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
    return _http_client


# ---------------------------------------------------------------------------
# Entry point: sets cookie and redirects to the real path
# ---------------------------------------------------------------------------


@router.get("/book/{session_id}")
async def start_proxy_session(session_id: str, request: Request):
    """Set proxy session cookie and redirect to the proxied page."""
    session = await service.get_proxy_session(session_id)
    if not session:
        return HTMLResponse(
            "<h1>Session expired</h1><p>This booking link has expired.</p>",
            status_code=410,
        )

    checkout_path = session["checkout_path"]
    proxy_host = request.headers.get("host", "localhost:8000")
    scheme = "https" if "ngrok" in proxy_host else "http"

    redirect_url = f"{scheme}://{proxy_host}{checkout_path}"

    html = f"""<!DOCTYPE html>
<html><head>
<style>
@keyframes _abspin{{to{{transform:rotate(360deg)}}}}
body{{margin:0;overflow:hidden}}
</style>
<script>
document.cookie = "{PROXY_COOKIE}={session_id}; path=/; SameSite=Lax";
try {{ sessionStorage.removeItem('_ab_resnexus'); }} catch(e) {{}}
window.location.replace("{redirect_url}");
</script>
</head><body>
<div style="position:fixed;top:0;left:0;right:0;bottom:0;
background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);z-index:99999;display:flex;
align-items:center;justify-content:center;flex-direction:column">
<div style="font-size:42px;font-weight:700;color:#fff;margin-bottom:4px;
letter-spacing:-1px;font-family:system-ui,-apple-system,sans-serif">Sadie</div>
<div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;
letter-spacing:2px;margin-bottom:32px">Hotel Concierge</div>
<div style="font-size:18px;font-weight:500;color:#e2e8f0;margin-bottom:8px">
Preparing your booking...</div>
<div style="font-size:14px;color:#94a3b8">This will just take a moment</div>
<div style="margin-top:24px;width:36px;height:36px;border:3px solid #334155;
border-top-color:#3b82f6;border-radius:50%;animation:_abspin 0.8s linear infinite"></div>
</div>
</body></html>"""

    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# Proxy catch-all: proxies any request that has the session cookie
# ---------------------------------------------------------------------------


@router.api_route(
    "/en/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
@router.api_route(
    "/booking/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
@router.api_route(
    "/resnexus/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def proxy_catchall(path: str, request: Request):
    """Proxy requests that have a valid session cookie."""
    full_path = request.url.path

    session_id = request.cookies.get(PROXY_COOKIE)
    if not session_id:
        return Response(content="No proxy session", status_code=404)

    session = await service.get_proxy_session(session_id)
    if not session:
        return Response(content="Session expired", status_code=410)

    return await _do_proxy(request, session, session_id, full_path)


# ---------------------------------------------------------------------------
# Catch-all: handles WAF challenges (Imperva /_Incapsula_Resource), static
# assets, and any other paths the target site might request.
# MUST be registered AFTER app-level routes (see app.py).
# ---------------------------------------------------------------------------


@catchall_router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def proxy_fallback(path: str, request: Request):
    """Fallback proxy for any path with a valid session cookie."""
    session_id = request.cookies.get(PROXY_COOKIE)
    if not session_id:
        return Response(
            content='{"detail":"Not Found"}',
            status_code=404,
            media_type="application/json",
        )

    session = await service.get_proxy_session(session_id)
    if not session:
        return Response(content="Session expired", status_code=410)

    full_path = request.url.path
    return await _do_proxy(request, session, session_id, full_path)


# ---------------------------------------------------------------------------
# Core proxy logic
# ---------------------------------------------------------------------------


def _build_out_headers(
    request: Request,
    target_host: str,
    target_base: str,
    session_cookies: dict,
) -> dict:
    """Build outgoing headers for the upstream request."""
    out_headers = {}
    for key, value in request.headers.items():
        if key.lower() not in STRIP_REQUEST_HEADERS:
            out_headers[key] = value
    out_headers["host"] = target_host
    out_headers["origin"] = target_base
    out_headers["referer"] = target_base + "/"
    out_headers.pop("accept-encoding", None)

    # Inject browser-like headers for WAF bypass
    for key, value in BROWSER_HEADERS.items():
        if key not in out_headers:
            out_headers[key] = value

    if session_cookies:
        injected = "; ".join(f"{k}={v}" for k, v in session_cookies.items())
        existing_cookies = out_headers.get("cookie", "")
        out_headers["cookie"] = f"{existing_cookies}; {injected}" if existing_cookies else injected

    return out_headers


def _build_resp_headers(
    resp: httpx.Response,
    target_host: str,
    target_base: str,
    proxy_host: str,
    proxy_base: str,
) -> tuple[dict, list[str]]:
    """Build response headers and collect rewritten Set-Cookie values."""
    resp_headers = {}
    set_cookies: list[str] = []

    for key, value in resp.headers.multi_items():
        lower = key.lower()
        if lower in STRIP_RESPONSE_HEADERS:
            continue
        if lower == "set-cookie":
            set_cookies.append(rewrite_set_cookie_domain(value, target_host, proxy_host))
            continue
        if lower == "location":
            value = value.replace(target_base, proxy_base)
            value = value.replace(f"//{target_host}", f"//{proxy_host}")
        resp_headers[key] = value

    resp_headers["access-control-allow-origin"] = "*"
    resp_headers["access-control-allow-credentials"] = "true"
    resp_headers["access-control-allow-methods"] = "GET,POST,PUT,DELETE,OPTIONS,PATCH"
    resp_headers["access-control-allow-headers"] = "*"

    return resp_headers, set_cookies


def _build_cf_headers(
    target_url: str,
    cookie_header: str,
    content_type: str = "",
    extra_headers: Optional[dict] = None,
) -> dict:
    """Build headers for a CF Worker request."""
    headers = {
        "X-Target-URL": target_url,
        "X-Proxy-Key": CF_PROXY_KEY,
    }
    if cookie_header:
        headers["X-Target-Cookie"] = cookie_header
    if content_type:
        headers["content-type"] = content_type
    if extra_headers:
        headers["X-Target-Headers"] = _json.dumps(extra_headers)
    return headers


@dataclass
class _BookResult:
    ok: bool
    set_cookies: list  # Set-Cookie headers from BookRoom response


async def _resnexus_server_book(
    cookie_header: str,
    target_base: str,
    target_host: str,
    proxy_host: str,
    guid: str,
    room_id: int,
    csrf_token: str,
) -> _BookResult:
    """POST BookRoom through CF Worker using the user's cookies.

    Returns the Set-Cookie headers from the response so they can be
    forwarded to the user's browser (session may be updated).
    """
    book_url = f"{target_base}/resnexus/reservations/book/{guid}/BookRoom"
    body = f"id={room_id}&rateID="

    extra = {
        "x-csrf-token": csrf_token,
        "x-requested-with": "XMLHttpRequest",
    }

    if CF_WORKER_URL:
        fetch_url = CF_WORKER_URL
        headers = _build_cf_headers(
            book_url, cookie_header,
            content_type="application/x-www-form-urlencoded; charset=UTF-8",
            extra_headers=extra,
        )
    else:
        fetch_url = book_url
        headers = {
            "cookie": cookie_header,
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-csrf-token": csrf_token,
            "x-requested-with": "XMLHttpRequest",
            "origin": target_base,
            "referer": f"{target_base}/resnexus/reservations/book/{guid}",
        }
        for k, v in BROWSER_HEADERS.items():
            headers.setdefault(k, v)

    client = _get_client()
    try:
        resp = await client.post(fetch_url, headers=headers, content=body)
        log.info(
            f"[server_book] BookRoom({room_id}) -> {resp.status_code} "
            f"set-cookie count: {len(resp.headers.get_list('set-cookie'))}"
        )
        # Collect and rewrite Set-Cookie headers for the user's browser
        book_cookies = []
        for sc in resp.headers.get_list("set-cookie"):
            book_cookies.append(
                rewrite_set_cookie_domain(sc, target_host, proxy_host)
            )
        return _BookResult(
            ok=200 <= resp.status_code < 400,
            set_cookies=book_cookies,
        )
    except httpx.RequestError as e:
        log.error(f"[server_book] BookRoom failed: {e}")
        return _BookResult(ok=False, set_cookies=[])


async def _do_proxy(request: Request, session: dict, session_id: str, path: str):
    """Execute the proxy request."""
    target_host = session["target_host"]
    target_base = session["target_base"]
    session_cookies = session["cookies"]

    # Build target URL
    target_url = f"{target_base}{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    # Determine proxy base
    proxy_host = request.headers.get("host", "localhost:8000")
    proxy_base = f"https://{proxy_host}" if "ngrok" in proxy_host else f"http://{proxy_host}"

    out_headers = _build_out_headers(request, target_host, target_base, session_cookies)

    # Route through Cloudflare Worker to bypass WAF IP blocking
    # Worker builds its own clean browser headers — we only send control headers
    if CF_WORKER_URL:
        cookie_header = out_headers.get("cookie", "")
        fetch_url = CF_WORKER_URL
        out_headers = _build_cf_headers(
            target_url, cookie_header,
            content_type=request.headers.get("content-type", ""),
        )
        # Forward AJAX headers (CSRF tokens, XHR flag, etc.)
        extra = {}
        for hdr in ("x-csrf-token", "x-requested-with"):
            val = request.headers.get(hdr)
            if val:
                extra[hdr] = val
        if extra:
            out_headers["X-Target-Headers"] = _json.dumps(extra)
    else:
        fetch_url = target_url
        cookie_header = out_headers.get("cookie", "")

    # Stream request body to upstream instead of buffering
    body = request.stream()

    client = _get_client()

    # Use streaming for the upstream request so we can decide whether to
    # buffer (rewritable) or stream (binary) based on content-type.
    try:
        resp = await client.send(
            client.build_request(
                method=request.method,
                url=fetch_url,
                headers=out_headers,
                content=body,
            ),
            stream=True,
        )
    except httpx.RequestError as e:
        log.error(f"Proxy upstream error: {e}")
        return Response(content=f"Upstream error: {e}", status_code=502)

    content_type = resp.headers.get("content-type", "")
    is_html = "text/html" in content_type
    needs_rewrite = any(ct in content_type for ct in REWRITABLE_CONTENT_TYPES)

    resp_headers, set_cookies = _build_resp_headers(
        resp, target_host, target_base, proxy_host, proxy_base,
    )

    if is_html:
        resp_headers["cache-control"] = "no-store"

    # --- Fast path: stream non-rewritable responses directly ---
    if not needs_rewrite:
        async def _stream_body():
            async for chunk in resp.aiter_bytes(chunk_size=65536):
                yield chunk
            await resp.aclose()

        response = StreamingResponse(
            content=_stream_body(),
            status_code=resp.status_code,
            headers=resp_headers,
        )
        for sc in set_cookies:
            response.headers.append("set-cookie", sc)
        return response

    # --- Rewritable content: must buffer for text substitution ---
    raw = await resp.aread()
    await resp.aclose()

    body_bytes = rewrite_response_body(raw, target_host, target_base, proxy_base)

    if is_html:
        text = body_bytes.decode("utf-8", errors="replace")

        # Skip JS injection on WAF challenge pages — let the browser solve them.
        # NOTE: real pages also contain _Incapsula_Resource (monitoring JS).
        # A true WAF challenge is a short page (<10KB) with ONLY the challenge.
        is_waf_challenge = (
            len(text) < 10000
            and ("_Incapsula_Resource" in text or "cf-challenge" in text)
        ) or "403 Forbidden" in text

        log.info(
            f"[proxy] HTML {path[:80]} len={len(text)} "
            f"waf={is_waf_challenge} engine={session.get('autobook_engine')} "
            f"autobook={session.get('autobook')}"
        )

        if not is_waf_challenge:
            is_checkout_page = "/checkout" in path and len(text) > 10000

            # --- ResNexus server-side booking intercept ---
            # When the proxy gets the room selection page (has BookRoom buttons),
            # make the BookRoom POST server-side using the same cookies/CF Worker
            # path, then redirect the user directly to checkout.
            # Guard: only book once per session to prevent duplicate rooms.
            if (
                session.get("autobook_engine") == "resnexus"
                and session.get("autobook")
                and session_id not in _booked_sessions
                and not is_checkout_page
            ):
                room_page = parse_resnexus_room_page(text, path)
                if room_page.room_id and room_page.csrf_token and room_page.guid:
                    log.info(
                        f"[server_book] Intercepted room page: "
                        f"room={room_page.room_id} guid={room_page.guid}"
                    )
                    book_result = await _resnexus_server_book(
                        cookie_header=cookie_header,
                        target_base=target_base,
                        target_host=target_host,
                        proxy_host=proxy_host,
                        guid=room_page.guid,
                        room_id=room_page.room_id,
                        csrf_token=room_page.csrf_token,
                    )
                    if book_result.ok:
                        _booked_sessions.add(session_id)
                        checkout_path = (
                            f"/resnexus/reservations/book/"
                            f"{room_page.guid}/checkout"
                        )
                        redirect_url = f"{proxy_base}{checkout_path}"
                        log.info(f"[server_book] Success — redirecting to {redirect_url}")
                        response = Response(
                            status_code=302,
                            headers={"location": redirect_url},
                        )
                        # Forward cookies from BOTH the page response and BookRoom POST
                        for sc in set_cookies:
                            response.headers.append("set-cookie", sc)
                        for sc in book_result.set_cookies:
                            response.headers.append("set-cookie", sc)
                        return response
                    else:
                        log.warning("[server_book] BookRoom POST failed, showing room page")

            # Inject URL interceptor JS (rewrite fetch/XHR to go through proxy)
            js = build_injection_js(
                target_host,
                target_base,
                proxy_base,
                proxy_host,
                autobook=False,  # no client-side autobook — server handles it
                autobook_engine=session.get("autobook_engine", "cloudbeds"),
            )

            # On checkout pages, also inject auto-advance JS to skip
            # upsell/cart panels and land on guest info form
            if is_checkout_page:
                js += CHECKOUT_ADVANCE_JS

            # Inject: overlay style + interceptor JS after <head>,
            # overlay HTML div after <body> (renders instantly before JS)
            head_inject = OVERLAY_STYLE + js
            text = _RE_HEAD_TAG.sub(rf"\1{head_inject}", text, count=1)
            text = _RE_BODY_TAG.sub(rf"\1{OVERLAY_HTML}", text, count=1)

        body_bytes = text.encode("utf-8")

    response = Response(
        content=body_bytes,
        status_code=resp.status_code,
        headers=resp_headers,
        media_type=content_type.split(";")[0] if ";" in content_type else content_type,
    )

    for sc in set_cookies:
        response.headers.append("set-cookie", sc)

    return response
