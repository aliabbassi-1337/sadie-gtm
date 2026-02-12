"""Reverse proxy with auto-booking for session-gated booking engines.

Architecture:
  1. Create proxy session with target host + path (no Playwright needed)
  2. User hits /book/{session_id} → sets proxy cookie → redirects to proxied page
  3. Proxy serves the booking page with injected JS:
     a. URL interceptors (rewrite fetch/XHR/WebSocket to route through proxy)
     b. Autobook script (clicks Select → Add → Book Now automatically)
  4. React cart is populated naturally in the user's browser
  5. User lands on /guests checkout page ready to fill in guest details

Works universally for any booking engine (Cloudbeds, RMS, etc.)
"""

import hashlib
import json
import logging
import re
import secrets
from typing import Optional

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

log = logging.getLogger(__name__)

router = APIRouter()

PROXY_COOKIE = "_bp_sid"

# ---------------------------------------------------------------------------
# Session Store (in-memory for POC)
# ---------------------------------------------------------------------------

_sessions: dict[str, dict] = {}


def store_session(
    cookies: dict[str, str],
    target_host: str,
    checkout_path: str,
    autobook: bool = False,
    autobook_engine: str = "cloudbeds",
) -> str:
    """Store proxy session. Returns session_id."""
    # Use random ID so each link is unique (even for same hotel)
    session_id = secrets.token_hex(6)

    _sessions[session_id] = {
        "cookies": cookies,
        "target_host": target_host,
        "target_base": f"https://{target_host}",
        "checkout_path": checkout_path,
        "autobook": autobook,
        "autobook_engine": autobook_engine,
    }
    log.info(f"Stored proxy session {session_id} for {target_host} (autobook={autobook}, engine={autobook_engine})")
    return session_id


def get_session(session_id: str) -> Optional[dict]:
    return _sessions.get(session_id)


# ---------------------------------------------------------------------------
# Headers to strip
# ---------------------------------------------------------------------------

STRIP_RESPONSE_HEADERS = {
    "content-encoding",
    "transfer-encoding",
    "content-length",
    "content-security-policy",
    "content-security-policy-report-only",
    "x-frame-options",
    "strict-transport-security",
    "clear-site-data",
}

STRIP_REQUEST_HEADERS = {
    "host",
    "accept-encoding",
}

REWRITABLE_CONTENT_TYPES = {
    "text/html",
    "text/css",
    "application/javascript",
    "text/javascript",
    "application/json",
}

# ---------------------------------------------------------------------------
# URL rewriting
# ---------------------------------------------------------------------------


def rewrite_response_body(
    body: bytes,
    target_host: str,
    target_base: str,
    proxy_base: str,
) -> bytes:
    """Replace all references to target domain with proxy domain."""
    text = body.decode("utf-8", errors="replace")

    # Full URLs: https://hotels.cloudbeds.com/... → https://proxy/...
    text = text.replace(target_base, proxy_base)
    # Protocol-relative: //hotels.cloudbeds.com → //proxy_host
    proxy_host = proxy_base.split("://", 1)[1] if "://" in proxy_base else proxy_base
    text = text.replace(f"//{target_host}", f"//{proxy_host}")

    return text.encode("utf-8")


def rewrite_set_cookie_domain(cookie_header: str, target_host: str, proxy_host: str) -> str:
    """Rewrite Set-Cookie for proxy: fix domain, strip Secure/HttpOnly/SameSite."""
    cookie_header = re.sub(
        rf"[Dd]omain=\.?{re.escape(target_host)}",
        f"Domain={proxy_host}",
        cookie_header,
    )
    # Strip flags that break cross-domain proxying
    cookie_header = re.sub(r";\s*[Ss]ecure", "", cookie_header)
    cookie_header = re.sub(r";\s*[Hh]ttp[Oo]nly", "", cookie_header)
    cookie_header = re.sub(r";\s*[Ss]ame[Ss]ite=[^;]*", "", cookie_header)
    return cookie_header


# ---------------------------------------------------------------------------
# JS injection — monkey-patches fetch, XHR, WebSocket
# ---------------------------------------------------------------------------

_INTERCEPTOR_JS = """<script>
(function() {
    var TB = '__TARGET_BASE__';
    var PB = '__PROXY_BASE__';
    var TH = '__TARGET_HOST__';
    var PH = '__PROXY_HOST__';

    function rewriteUrl(url) {
        if (!url || typeof url !== 'string') return url;
        url = url.split(TB).join(PB);
        if (url.indexOf('//' + TH) === 0) url = url.split('//' + TH).join('//' + PH);
        return url;
    }

    var _fetch = window.fetch;
    window.fetch = function(input, init) {
        if (typeof input === 'string') {
            input = rewriteUrl(input);
        } else if (input instanceof Request) {
            input = new Request(rewriteUrl(input.url), input);
        }
        return _fetch.call(this, input, init);
    };

    var _open = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function(method, url) {
        if (typeof url === 'string') arguments[1] = rewriteUrl(url);
        return _open.apply(this, arguments);
    };

    var _WS = window.WebSocket;
    window.WebSocket = function(url, protocols) {
        url = url.split(TH).join(PH);
        return new _WS(url, protocols);
    };
    if (_WS) window.WebSocket.prototype = _WS.prototype;

    console.log('[BookingProxy] Interceptors active');
})();
</script>"""

# ---------------------------------------------------------------------------
# Autobook JS — clicks through room selection automatically
# ---------------------------------------------------------------------------

_AUTOBOOK_JS = """<script>
(function() {
    // Only run on the search/reservation page, not on /guests checkout
    if (window.location.pathname.indexOf('/guests') !== -1) {
        console.log('[Autobook] On checkout page, skipping');
        return;
    }

    // Inject spinner keyframes immediately (head exists)
    var style = document.createElement('style');
    style.textContent = '@keyframes _abspin{to{transform:rotate(360deg)}}';
    document.head.appendChild(style);

    var step = 0;
    var maxWait = 30000;
    var startTime = Date.now();

    function showOverlay() {
        if (document.getElementById('_autobook_overlay')) return;
        var overlay = document.createElement('div');
        overlay.id = '_autobook_overlay';
        overlay.innerHTML = '<div style="position:fixed;top:0;left:0;right:0;bottom:0;' +
            'background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);z-index:99999;display:flex;' +
            'align-items:center;justify-content:center;flex-direction:column">' +
            '<div style="font-size:42px;font-weight:700;color:#fff;margin-bottom:4px;' +
            'letter-spacing:-1px;font-family:system-ui,-apple-system,sans-serif">Sadie</div>' +
            '<div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;' +
            'letter-spacing:2px;margin-bottom:32px">Hotel Concierge</div>' +
            '<div style="font-size:18px;font-weight:500;color:#e2e8f0;margin-bottom:8px">' +
            'Preparing your booking...</div>' +
            '<div style="font-size:14px;color:#94a3b8">Selecting the best available room</div>' +
            '<div style="margin-top:24px;width:36px;height:36px;border:3px solid #334155;' +
            'border-top-color:#3b82f6;border-radius:50%;animation:_abspin 0.8s linear infinite"></div>' +
            '</div>';
        document.body.appendChild(overlay);
    }

    function removeOverlay() {
        var o = document.getElementById('_autobook_overlay');
        if (o) o.remove();
    }

    function tryStep() {
        if (Date.now() - startTime > maxWait) {
            console.log('[Autobook] Timeout');
            removeOverlay();
            return;
        }

        if (step === 0) {
            var selectBtn = document.querySelector('[data-testid^="rate-plan-select-individual-button-"]');
            if (selectBtn) {
                console.log('[Autobook] Step 1: Select Accommodations');
                selectBtn.scrollIntoView({block:'center'});
                selectBtn.click();
                step = 1;
                setTimeout(tryStep, 2000);
            } else {
                setTimeout(tryStep, 500);
            }
        } else if (step === 1) {
            var addBtn = document.querySelector('[data-testid^="select-individual-add-button-"]');
            if (addBtn) {
                console.log('[Autobook] Step 2: Add room');
                addBtn.click();
                step = 2;
                setTimeout(tryStep, 2000);
            } else {
                setTimeout(tryStep, 500);
            }
        } else if (step === 2) {
            console.log('[Autobook] Step 3: Close modal');
            // Try multiple ways to close the modal
            var closeBtn = document.querySelector('[data-testid="modal-close-button"]') ||
                           document.querySelector('button[aria-label="Close"]') ||
                           document.querySelector('.modal-close');
            if (closeBtn) {
                closeBtn.click();
            } else {
                // Dispatch Escape to window (React listens there, not document)
                window.dispatchEvent(new KeyboardEvent('keydown', {
                    key: 'Escape', code: 'Escape', keyCode: 27, bubbles: true
                }));
            }
            step = 3;
            setTimeout(tryStep, 1500);
        } else if (step === 3) {
            var bookBtn = document.querySelector('[data-testid="shopping-cart-confirm-button"]');
            if (bookBtn) {
                console.log('[Autobook] Step 4: Book Now');
                removeOverlay();
                bookBtn.click();
            } else {
                setTimeout(tryStep, 500);
            }
        }
    }

    function init() {
        console.log('[Autobook] Starting...');
        showOverlay();
        setTimeout(tryStep, 3000);
    }

    // Must wait for body to exist before appending overlay
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
</script>"""


_AUTOBOOK_RESNEXUS_JS = """<script>
(function() {
    var isCheckout = window.location.pathname.indexOf('/checkout') !== -1;

    // Inject overlay styles immediately
    var style = document.createElement('style');
    style.textContent = '@keyframes _abspin{to{transform:rotate(360deg)}}' +
        '@keyframes _abfade{from{opacity:1}to{opacity:0}}';
    document.head.appendChild(style);

    var OVERLAY_HTML = '<div id="_autobook_overlay" style="position:fixed;top:0;left:0;right:0;bottom:0;' +
        'background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);z-index:99999;display:flex;' +
        'align-items:center;justify-content:center;flex-direction:column">' +
        '<div style="font-size:42px;font-weight:700;color:#fff;margin-bottom:4px;' +
        'letter-spacing:-1px;font-family:system-ui,-apple-system,sans-serif">Sadie</div>' +
        '<div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;' +
        'letter-spacing:2px;margin-bottom:32px">Hotel Concierge</div>' +
        '<div id="_ab_status" style="font-size:18px;font-weight:500;color:#e2e8f0;margin-bottom:8px">' +
        (isCheckout ? 'Almost there...' : 'Preparing your booking...') + '</div>' +
        '<div style="font-size:14px;color:#94a3b8">' +
        (isCheckout ? 'Loading checkout' : 'Selecting the best available room') + '</div>' +
        '<div style="margin-top:24px;width:36px;height:36px;border:3px solid #334155;' +
        'border-top-color:#3b82f6;border-radius:50%;animation:_abspin 0.8s linear infinite"></div>' +
        '</div>';

    // Show overlay immediately (even before body exists, write it into the page)
    document.write(OVERLAY_HTML);

    function removeOverlay() {
        var o = document.getElementById('_autobook_overlay');
        if (o) {
            o.style.animation = '_abfade 0.4s ease forwards';
            setTimeout(function() { o.remove(); }, 400);
        }
    }

    // On checkout page: just show overlay until page is ready, then fade out
    if (isCheckout) {
        console.log('[Autobook] Checkout page — waiting for load');
        window.addEventListener('load', function() {
            setTimeout(removeOverlay, 500);
        });
        return;
    }

    // On booking page: autobook flow
    var maxWait = 30000;
    var startTime = Date.now();
    var step = 0;

    function tryStep() {
        if (Date.now() - startTime > maxWait) {
            console.log('[Autobook] Timeout');
            removeOverlay();
            return;
        }

        if (step === 0) {
            var bookBtn = document.querySelector('button.room-action:not(.booked)');
            if (bookBtn) {
                console.log('[Autobook] Step 1: Clicking Book button');
                bookBtn.click();
                step = 1;
                var s = document.getElementById('_ab_status');
                if (s) s.textContent = 'Booking your room...';
                setTimeout(tryStep, 4000);
            } else {
                var bookedBtn = document.querySelector('button.room-action.booked');
                if (bookedBtn) {
                    console.log('[Autobook] Room already in cart');
                    step = 1;
                    setTimeout(tryStep, 500);
                } else {
                    setTimeout(tryStep, 500);
                }
            }
        } else if (step === 1) {
            console.log('[Autobook] Step 2: Going to checkout');
            // Keep overlay up — checkout page will show its own then fade out
            var checkoutBtn = document.querySelector('a.checkout-button');
            if (checkoutBtn) {
                window.location.href = checkoutBtn.getAttribute('href');
            } else {
                var guidMatch = window.location.pathname.match(/\/book\/([0-9a-fA-F-]+)/i);
                if (guidMatch) {
                    window.location.href = '/resnexus/reservations/book/' + guidMatch[1] + '/checkout';
                }
            }
        }
    }

    function init() {
        console.log('[Autobook] Starting ResNexus autobook...');
        setTimeout(tryStep, 3000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
</script>"""

# Map engine name → autobook JS snippet
_AUTOBOOK_SCRIPTS = {
    "cloudbeds": _AUTOBOOK_JS,
    "resnexus": _AUTOBOOK_RESNEXUS_JS,
}


def _build_interceptor_js(target_host: str, target_base: str, proxy_base: str, proxy_host: str) -> str:
    return (
        _INTERCEPTOR_JS
        .replace("__TARGET_BASE__", target_base)
        .replace("__TARGET_HOST__", target_host)
        .replace("__PROXY_BASE__", proxy_base)
        .replace("__PROXY_HOST__", proxy_host)
    )


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
    session = get_session(session_id)
    if not session:
        return HTMLResponse(
            "<h1>Session expired</h1><p>This booking link has expired.</p>",
            status_code=410,
        )

    checkout_path = session["checkout_path"]
    proxy_host = request.headers.get("host", "localhost:8000")
    scheme = "https" if "ngrok" in proxy_host else "http"

    redirect_url = f"{scheme}://{proxy_host}{checkout_path}"

    # Use JS redirect to set cookie first, then navigate
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

    session = get_session(session_id)
    if not session:
        return Response(content="Session expired", status_code=410)

    return await _do_proxy(request, session, session_id, full_path)


# ---------------------------------------------------------------------------
# Catch-all: handles WAF challenges (Imperva /_Incapsula_Resource), static
# assets, and any other paths the target site might request.
# MUST be registered AFTER app-level routes (see redirect_service.py).
# ---------------------------------------------------------------------------

catchall_router = APIRouter()


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

    session = get_session(session_id)
    if not session:
        return Response(content="Session expired", status_code=410)

    full_path = request.url.path
    return await _do_proxy(request, session, session_id, full_path)


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

    # Build outgoing headers
    out_headers = {}
    for key, value in request.headers.items():
        if key.lower() not in STRIP_REQUEST_HEADERS:
            out_headers[key] = value
    out_headers["host"] = target_host
    out_headers["origin"] = target_base
    out_headers["referer"] = target_base + "/"
    out_headers.pop("accept-encoding", None)

    # Inject session cookies (if any were pre-captured)
    if session_cookies:
        injected = "; ".join(f"{k}={v}" for k, v in session_cookies.items())
        existing_cookies = out_headers.get("cookie", "")
        out_headers["cookie"] = f"{existing_cookies}; {injected}" if existing_cookies else injected

    body = await request.body()

    try:
        client = _get_client()
        resp = await client.request(
            method=request.method,
            url=target_url,
            headers=out_headers,
            content=body,
        )
    except httpx.RequestError as e:
        log.error(f"Proxy upstream error: {e}")
        return Response(content=f"Upstream error: {e}", status_code=502)

    # Build response headers — collect Set-Cookie separately (dict would overwrite dupes)
    resp_headers = {}
    set_cookies: list[str] = []
    for key, value in resp.headers.multi_items():
        if key.lower() in STRIP_RESPONSE_HEADERS:
            continue
        if key.lower() == "set-cookie":
            set_cookies.append(rewrite_set_cookie_domain(value, target_host, proxy_host))
            continue
        if key.lower() == "location":
            value = value.replace(target_base, proxy_base)
            value = value.replace(f"//{target_host}", f"//{proxy_host}")
        resp_headers[key] = value

    # Permissive CORS
    resp_headers["access-control-allow-origin"] = "*"
    resp_headers["access-control-allow-credentials"] = "true"
    resp_headers["access-control-allow-methods"] = "GET,POST,PUT,DELETE,OPTIONS,PATCH"
    resp_headers["access-control-allow-headers"] = "*"

    content_type = resp.headers.get("content-type", "")
    is_html = "text/html" in content_type
    needs_rewrite = any(ct in content_type for ct in REWRITABLE_CONTENT_TYPES)

    # Only block caching for HTML pages (static assets can be cached by browser)
    if is_html:
        resp_headers["cache-control"] = "no-store"

    if needs_rewrite:
        body_bytes = rewrite_response_body(
            resp.content, target_host, target_base, proxy_base,
        )

        # Inject JS into HTML pages
        if is_html:
            text = body_bytes.decode("utf-8", errors="replace")
            js = _build_interceptor_js(target_host, target_base, proxy_base, proxy_host)

            # Add autobook JS if session has autobook enabled
            if session.get("autobook"):
                engine = session.get("autobook_engine", "cloudbeds")
                js += _AUTOBOOK_SCRIPTS.get(engine, _AUTOBOOK_JS)

            text = re.sub(r"(<head[^>]*>)", rf"\1{js}", text, count=1, flags=re.IGNORECASE)
            body_bytes = text.encode("utf-8")

        response = Response(
            content=body_bytes,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=content_type.split(";")[0] if ";" in content_type else content_type,
        )
    else:
        response = Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
        )

    # Append ALL Set-Cookie headers (critical — RES cookie carries cart state)
    for sc in set_cookies:
        response.headers.append("set-cookie", sc)

    return response
