"""Pure utility functions for the booking reverse proxy.

URL/cookie rewriting, JS injection snippets. No state, no I/O.
"""

import re
from functools import lru_cache


# ---------------------------------------------------------------------------
# Pre-compiled regexes (avoid re-compiling on every request)
# ---------------------------------------------------------------------------

_RE_SECURE = re.compile(r";\s*[Ss]ecure")
_RE_HTTPONLY = re.compile(r";\s*[Hh]ttp[Oo]nly")
_RE_SAMESITE = re.compile(r";\s*[Ss]ame[Ss]ite=[^;]*")
_RE_HEAD_TAG = re.compile(r"(<head[^>]*>)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# URL rewriting — single-pass via compiled regex
# ---------------------------------------------------------------------------


@lru_cache(maxsize=32)
def _rewrite_pattern(target_base: str, target_host: str) -> re.Pattern:
    """Compile a single regex that matches both full URLs and protocol-relative refs."""
    # Escape for regex; match full base first (longer), then //host
    return re.compile(
        re.escape(target_base) + "|" + re.escape(f"//{target_host}")
    )


def rewrite_response_body(
    body: bytes,
    target_host: str,
    target_base: str,
    proxy_base: str,
) -> bytes:
    """Replace all references to target domain with proxy domain (single pass)."""
    text = body.decode("utf-8", errors="replace")
    proxy_host = proxy_base.split("://", 1)[1] if "://" in proxy_base else proxy_base

    pattern = _rewrite_pattern(target_base, target_host)
    replacements = {target_base: proxy_base, f"//{target_host}": f"//{proxy_host}"}
    text = pattern.sub(lambda m: replacements[m.group(0)], text)

    return text.encode("utf-8")


# ---------------------------------------------------------------------------
# Cookie rewriting — pre-compiled regexes
# ---------------------------------------------------------------------------


@lru_cache(maxsize=32)
def _domain_pattern(target_host: str) -> re.Pattern:
    return re.compile(rf"[Dd]omain=\.?{re.escape(target_host)}")


def rewrite_set_cookie_domain(cookie_header: str, target_host: str, proxy_host: str) -> str:
    """Rewrite Set-Cookie for proxy: fix domain, strip Secure/HttpOnly/SameSite."""
    cookie_header = _domain_pattern(target_host).sub(f"Domain={proxy_host}", cookie_header)
    cookie_header = _RE_SECURE.sub("", cookie_header)
    cookie_header = _RE_HTTPONLY.sub("", cookie_header)
    cookie_header = _RE_SAMESITE.sub("", cookie_header)
    return cookie_header


# ---------------------------------------------------------------------------
# JS building — cached per (target, proxy) tuple
# ---------------------------------------------------------------------------


@lru_cache(maxsize=32)
def build_interceptor_js(
    target_host: str,
    target_base: str,
    proxy_base: str,
    proxy_host: str,
) -> str:
    """Build the fetch/XHR/WebSocket interceptor JS with substituted values (cached)."""
    return (
        INTERCEPTOR_JS
        .replace("__TARGET_BASE__", target_base)
        .replace("__TARGET_HOST__", target_host)
        .replace("__PROXY_BASE__", proxy_base)
        .replace("__PROXY_HOST__", proxy_host)
    )


@lru_cache(maxsize=32)
def build_injection_js(
    target_host: str,
    target_base: str,
    proxy_base: str,
    proxy_host: str,
    autobook: bool,
    autobook_engine: str,
) -> str:
    """Build the full JS injection string (interceptor + autobook). Cached per session params."""
    js = build_interceptor_js(target_host, target_base, proxy_base, proxy_host)
    if autobook:
        js += AUTOBOOK_SCRIPTS.get(autobook_engine, AUTOBOOK_CLOUDBEDS_JS)
    return js


# ---------------------------------------------------------------------------
# JS constants
# ---------------------------------------------------------------------------

INTERCEPTOR_JS = """<script>
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

AUTOBOOK_CLOUDBEDS_JS = """<script>
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
            var closeBtn = document.querySelector('[data-testid="modal-close-button"]') ||
                           document.querySelector('button[aria-label="Close"]') ||
                           document.querySelector('.modal-close');
            if (closeBtn) {
                closeBtn.click();
            } else {
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

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
</script>"""


AUTOBOOK_RESNEXUS_JS = """<script>
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

    document.write(OVERLAY_HTML);

    function removeOverlay() {
        var o = document.getElementById('_autobook_overlay');
        if (o) {
            o.style.animation = '_abfade 0.4s ease forwards';
            setTimeout(function() { o.remove(); }, 400);
        }
    }

    if (isCheckout) {
        console.log('[Autobook] Checkout page — waiting for load');
        window.addEventListener('load', function() {
            setTimeout(removeOverlay, 500);
        });
        return;
    }

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
            var checkoutBtn = document.querySelector('a.checkout-button');
            if (checkoutBtn) {
                window.location.href = checkoutBtn.getAttribute('href');
            } else {
                var guidMatch = window.location.pathname.match(/\\/book\\/([0-9a-fA-F-]+)/i);
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

# Map engine name -> autobook JS snippet
AUTOBOOK_SCRIPTS = {
    "cloudbeds": AUTOBOOK_CLOUDBEDS_JS,
    "resnexus": AUTOBOOK_RESNEXUS_JS,
}
