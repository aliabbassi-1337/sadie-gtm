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

import logging
from typing import Optional

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse

from lib.deeplink.proxy_utils import (
    _RE_HEAD_TAG,
    build_injection_js,
    rewrite_response_body,
    rewrite_set_cookie_domain,
)
from services.deeplink import service

log = logging.getLogger(__name__)

router = APIRouter()
catchall_router = APIRouter()

PROXY_COOKIE = "_bp_sid"


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
})

REWRITABLE_CONTENT_TYPES = frozenset({
    "text/html",
    "text/css",
    "application/javascript",
    "text/javascript",
    "application/json",
})


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
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=10),
        )
    return _http_client


# ---------------------------------------------------------------------------
# Entry point: sets cookie and redirects to the real path
# ---------------------------------------------------------------------------


@router.get("/book/{session_id}")
async def start_proxy_session(session_id: str, request: Request):
    """Set proxy session cookie and redirect to the proxied page."""
    session = service.get_proxy_session(session_id)
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
<script>
document.cookie = "{PROXY_COOKIE}={session_id}; path=/; SameSite=Lax";
window.location.replace("{redirect_url}");
</script>
</head><body>Loading checkout...</body></html>"""

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

    session = service.get_proxy_session(session_id)
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

    session = service.get_proxy_session(session_id)
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

    # Stream request body to upstream instead of buffering
    body = request.stream()

    client = _get_client()

    # Use streaming for the upstream request so we can decide whether to
    # buffer (rewritable) or stream (binary) based on content-type.
    try:
        resp = await client.send(
            client.build_request(
                method=request.method,
                url=target_url,
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

        # Cached JS injection — same output for identical session params
        js = build_injection_js(
            target_host,
            target_base,
            proxy_base,
            proxy_host,
            autobook=bool(session.get("autobook")),
            autobook_engine=session.get("autobook_engine", "cloudbeds"),
        )

        text = _RE_HEAD_TAG.sub(rf"\1{js}", text, count=1)
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
