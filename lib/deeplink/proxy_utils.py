"""Pure utility functions for the booking reverse proxy.

URL/cookie rewriting, JS injection snippets. No state, no I/O.
"""

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


# ---------------------------------------------------------------------------
# Pre-compiled regexes (avoid re-compiling on every request)
# ---------------------------------------------------------------------------

_RE_SECURE = re.compile(r";\s*[Ss]ecure")
_RE_HTTPONLY = re.compile(r";\s*[Hh]ttp[Oo]nly")
_RE_SAMESITE = re.compile(r";\s*[Ss]ame[Ss]ite=[^;]*")
_RE_HEAD_TAG = re.compile(r"(<head[^>]*>)", re.IGNORECASE)
_RE_BODY_TAG = re.compile(r"(<body[^>]*>)", re.IGNORECASE)

# ResNexus HTML parsing
_RE_BOOK_ROOM = re.compile(r'onclick="BookRoom\((\d+),\s*this\)"')
_RE_MCSRF = re.compile(r'id="MCSRF"\s+value="([^"]+)"')
_RE_RESNEXUS_GUID = re.compile(r"/resnexus/reservations/book/([^/?\s]+)")


# ---------------------------------------------------------------------------
# ResNexus HTML parsing — extract room ID + CSRF for server-side booking
# ---------------------------------------------------------------------------


@dataclass
class ResNexusRoomPage:
    room_id: Optional[int] = None
    csrf_token: Optional[str] = None
    guid: Optional[str] = None


def parse_resnexus_room_page(html: str, path: str = "") -> ResNexusRoomPage:
    """Extract first available BookRoom ID and CSRF token from ResNexus HTML.

    Returns ResNexusRoomPage with room_id, csrf_token, and property GUID.
    All fields are None if the page is not a room selection page.
    """
    result = ResNexusRoomPage()

    # Room ID from first BookRoom button (not BookPackage)
    m = _RE_BOOK_ROOM.search(html)
    if m:
        result.room_id = int(m.group(1))

    # CSRF token from hidden input
    m = _RE_MCSRF.search(html)
    if m:
        result.csrf_token = m.group(1)

    # Property GUID from path or HTML
    m = _RE_RESNEXUS_GUID.search(path or html)
    if m:
        result.guid = m.group(1)

    return result


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
# Inline overlay — renders immediately (no JS needed to show it)
# Injected after <head> as a <style> block and after <body> as a <div>.
# ---------------------------------------------------------------------------

OVERLAY_STYLE = """<style>
@keyframes _abspin{to{transform:rotate(360deg)}}
@keyframes _abfade{from{opacity:1}to{opacity:0}}
#_sadie_overlay{position:fixed;top:0;left:0;right:0;bottom:0;
background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);z-index:99999;display:flex;
align-items:center;justify-content:center;flex-direction:column}
</style>"""

OVERLAY_HTML = """<div id="_sadie_overlay">
<div style="font-size:42px;font-weight:700;color:#fff;margin-bottom:4px;
letter-spacing:-1px;font-family:system-ui,-apple-system,sans-serif">Sadie</div>
<div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;
letter-spacing:2px;margin-bottom:32px">Hotel Concierge</div>
<div id="_sadie_status" style="font-size:18px;font-weight:500;color:#e2e8f0;margin-bottom:8px">
Preparing your booking...</div>
<div style="font-size:14px;color:#94a3b8">This will just take a moment</div>
<div style="margin-top:24px;width:36px;height:36px;border:3px solid #334155;
border-top-color:#3b82f6;border-radius:50%;animation:_abspin 0.8s linear infinite"></div>
</div>
<script>
// Safety: remove overlay after 20s if nothing else does
setTimeout(function(){var o=document.getElementById('_sadie_overlay');if(o)o.remove();},20000);
</script>"""


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
    var SS_KEY = '_ab_resnexus';
    var isCheckout = window.location.pathname.indexOf('/checkout') !== -1;

    // Inject overlay styles
    var style = document.createElement('style');
    style.textContent = '@keyframes _abspin{to{transform:rotate(360deg)}}' +
        '@keyframes _abfade{from{opacity:1}to{opacity:0}}';
    document.head.appendChild(style);

    function showOverlay(statusText, subText) {
        if (document.getElementById('_autobook_overlay')) return;
        var el = document.createElement('div');
        el.id = '_autobook_overlay';
        el.innerHTML = '<div style="position:fixed;top:0;left:0;right:0;bottom:0;' +
            'background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);z-index:99999;display:flex;' +
            'align-items:center;justify-content:center;flex-direction:column">' +
            '<div style="font-size:42px;font-weight:700;color:#fff;margin-bottom:4px;' +
            'letter-spacing:-1px;font-family:system-ui,-apple-system,sans-serif">Sadie</div>' +
            '<div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;' +
            'letter-spacing:2px;margin-bottom:32px">Hotel Concierge</div>' +
            '<div id="_ab_status" style="font-size:18px;font-weight:500;color:#e2e8f0;margin-bottom:8px">' +
            statusText + '</div>' +
            '<div style="font-size:14px;color:#94a3b8">' + subText + '</div>' +
            '<div style="margin-top:24px;width:36px;height:36px;border:3px solid #334155;' +
            'border-top-color:#3b82f6;border-radius:50%;animation:_abspin 0.8s linear infinite"></div>' +
            '</div>';
        document.body.appendChild(el);
    }

    function removeOverlay() {
        var o = document.getElementById('_autobook_overlay');
        if (o) {
            o.style.animation = '_abfade 0.4s ease forwards';
            setTimeout(function() { o.remove(); }, 400);
        }
    }

    function done() {
        try { sessionStorage.removeItem(SS_KEY); } catch(e) {}
        removeOverlay();
    }

    function cartCount() {
        var el = document.querySelector('.view-cart.topCartIcon');
        return el ? parseInt(el.textContent.trim(), 10) || 0 : 0;
    }

    function goToCheckout() {
        var btn = document.querySelector('a.checkout-button');
        if (btn && btn.getAttribute('href')) {
            console.log('[Autobook] Going to checkout');
            window.location.href = btn.getAttribute('href');
            return;
        }
        var base = window.location.pathname.replace(/\\?.*/, '');
        if (base.indexOf('/checkout') === -1) {
            window.location.href = base + '/checkout';
        }
    }

    function init() {
        console.log('[Autobook] Starting ResNexus autobook...');

        if (isCheckout) {
            showOverlay('Almost there...', 'Loading checkout');
            window.addEventListener('load', function() { setTimeout(done, 500); });
            return;
        }

        showOverlay('Preparing your booking...', 'Selecting the best available room');

        var alreadyClicked = false;
        try { alreadyClicked = sessionStorage.getItem(SS_KEY) === 'clicked'; } catch(e) {}
        var maxWait = 30000;
        var startTime = Date.now();

        function poll() {
            if (Date.now() - startTime > maxWait) {
                console.log('[Autobook] Timeout — removing overlay');
                done();
                return;
            }

            var cart = cartCount();
            console.log('[Autobook] Poll: cart=' + cart + ' clicked=' + alreadyClicked);

            // Cart has items → go to checkout
            if (cart > 0) {
                console.log('[Autobook] Cart has ' + cart + ' items — heading to checkout');
                var s = document.getElementById('_ab_status');
                if (s) s.textContent = 'Heading to checkout...';
                setTimeout(goToCheckout, 500);
                return;
            }

            // Haven't clicked yet → find and invoke Add button
            if (!alreadyClicked) {
                var addBtn = document.querySelector('button.room-action.add-room');
                if (addBtn) {
                    var fn = addBtn.getAttribute('onclick') || '';
                    console.log('[Autobook] Found button with onclick: ' + fn);
                    try { sessionStorage.setItem(SS_KEY, 'clicked'); } catch(e) {}
                    alreadyClicked = true;
                    // Invoke the onclick function directly (more reliable than .click())
                    try {
                        new Function(fn).call(addBtn);
                        console.log('[Autobook] Invoked ' + fn + ' successfully');
                    } catch(e) {
                        console.log('[Autobook] Direct invoke failed (' + e.message + '), trying .click()');
                        addBtn.click();
                    }
                    var s = document.getElementById('_ab_status');
                    if (s) s.textContent = 'Adding room to cart...';
                    setTimeout(poll, 3000);
                    return;
                }
                // Buttons not rendered yet — keep polling
                console.log('[Autobook] Waiting for room buttons...');
                setTimeout(poll, 500);
                return;
            }

            // Already clicked, waiting for cart to update
            setTimeout(poll, 500);
        }

        // Wait for page JS to fully load before polling
        setTimeout(poll, 3000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
</script>"""

# ---------------------------------------------------------------------------
# Checkout auto-advance — clicks through upsell/cart panels to guest info
# ---------------------------------------------------------------------------

CHECKOUT_ADVANCE_JS = """<script>
(function() {
    if (window.location.pathname.indexOf('/checkout') === -1) return;

    function removeOverlay() {
        var o = document.getElementById('_sadie_overlay');
        if (o) {
            o.style.animation = '_abfade 0.4s ease forwards';
            setTimeout(function() { o.remove(); }, 400);
        }
    }

    function updateStatus(text) {
        var s = document.getElementById('_sadie_status');
        if (s) s.textContent = text;
    }

    var maxWait = 15000;
    var startTime = Date.now();

    function advancePanel() {
        if (Date.now() - startTime > maxWait) {
            console.log('[AutoCheckout] Timeout — removing overlay');
            removeOverlay();
            return;
        }

        // Done: guest-info-panel is visible
        var guestPanel = document.querySelector('.guest-info-panel.visible');
        if (guestPanel) {
            console.log('[AutoCheckout] Guest info panel visible — done');
            updateStatus('Ready!');
            setTimeout(removeOverlay, 300);
            return;
        }

        // Click any visible .green.checkout-button (advances current panel)
        var btns = document.querySelectorAll('.green.checkout-button');
        for (var i = 0; i < btns.length; i++) {
            var btn = btns[i];
            var rect = btn.getBoundingClientRect();
            if (rect.width > 0 && rect.height > 0 && btn.offsetParent !== null) {
                console.log('[AutoCheckout] Clicking: ' + btn.tagName + ' "' + btn.textContent.trim() + '"');
                updateStatus('Loading checkout...');
                btn.click();
                setTimeout(advancePanel, 1500);
                return;
            }
        }

        // No visible button yet — keep polling
        setTimeout(advancePanel, 500);
    }

    function init() {
        console.log('[AutoCheckout] Starting checkout advance...');
        updateStatus('Loading checkout...');
        setTimeout(advancePanel, 2000);
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
