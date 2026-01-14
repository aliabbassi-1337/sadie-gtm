"""Booking engine detector for hotel websites.

Full detection logic ported from scripts/pipeline/detect.py.
Visits hotel websites using Playwright to detect their booking engine
by analyzing URLs, network requests, and page content.
"""

import re
import asyncio
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, urljoin

from loguru import logger
from pydantic import BaseModel, ConfigDict
from playwright.async_api import async_playwright, Page, BrowserContext, Browser
from playwright.async_api import TimeoutError as PWTimeoutError
import httpx

from services.leadgen.location import LocationExtractor


# =============================================================================
# CONFIGURATION
# =============================================================================

class DetectionConfig(BaseModel):
    """Configuration for the detector."""
    model_config = ConfigDict(frozen=True)

    timeout_page_load: int = 15000      # 15s (was 30s)
    timeout_booking_click: int = 2000   # 2s (was 3s)
    timeout_popup_detect: int = 1000    # 1s (was 1.5s)
    concurrency: int = 5
    pause_between_hotels: float = 0.0   # 0s (was 0.2s) - semaphore handles this
    headless: bool = True
    debug: bool = False  # Enable debug logging
    fast_mode: bool = True  # Reduce waits for speed
    target_location: str = ""  # Filter by location - skip engine detection if mismatch


# =============================================================================
# ENGINE PATTERNS - Injected at runtime from database
# =============================================================================

# Module-level cache for engine patterns (set by caller before detection)
_engine_patterns: Dict[str, List[str]] = {}


def set_engine_patterns(patterns: Dict[str, List[str]]) -> None:
    """Set the engine patterns to use for detection.

    Called by workflow/service after fetching from database.
    """
    global _engine_patterns
    _engine_patterns = patterns
    logger.info(f"Loaded {len(_engine_patterns)} booking engine patterns")


def get_engine_patterns() -> Dict[str, List[str]]:
    """Get the current engine patterns."""
    return _engine_patterns

# Skip big chains and junk domains
SKIP_CHAIN_DOMAINS = [
    "marriott.com", "hilton.com", "ihg.com", "hyatt.com", "wyndham.com",
    "choicehotels.com", "bestwestern.com", "radissonhotels.com", "accor.com",
]

SKIP_JUNK_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com", "youtube.com", "tiktok.com",
    "linkedin.com", "yelp.com", "tripadvisor.com", "google.com",
    "booking.com", "expedia.com", "hotels.com", "airbnb.com", "vrbo.com",
    "dnr.", "parks.", "recreation.", ".gov", ".edu", ".mil",
]


def is_junk_domain(url: str) -> bool:
    """Check if URL is a junk domain that should be skipped."""
    if not url:
        return True
    url_lower = url.lower()
    return any(junk in url_lower for junk in SKIP_JUNK_DOMAINS)


# =============================================================================
# DATA MODELS
# =============================================================================

class DetectionResult(BaseModel):
    """Result of booking engine detection for a hotel."""
    model_config = ConfigDict(from_attributes=True)

    hotel_id: int
    booking_engine: str = ""
    booking_engine_domain: str = ""
    booking_url: str = ""
    detection_method: str = ""
    phone_website: str = ""
    email: str = ""
    room_count: str = ""
    detected_location: str = ""  # Location extracted from website content
    error: str = ""


# =============================================================================
# UTILITIES
# =============================================================================

def extract_domain(url: str) -> str:
    """Extract domain from URL, stripping www. prefix."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def normalize_url(url: str) -> str:
    """Ensure URL has https:// prefix."""
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


async def http_precheck(url: str, timeout: float = 3.0) -> Tuple[bool, str]:
    """Quick HTTP check before launching Playwright."""
    try:
        async with httpx.AsyncClient(
            timeout=timeout, follow_redirects=True, verify=False
        ) as client:
            try:
                resp = await client.head(url)
                # Some servers reject HEAD, fall back to GET
                if resp.status_code == 405:
                    resp = await client.get(url)
            except httpx.HTTPStatusError:
                resp = await client.get(url)
            if resp.status_code >= 400:
                return (False, f"HTTP {resp.status_code}")
            return (True, "")
    except httpx.TimeoutException:
        return (False, "timeout")
    except httpx.ConnectError:
        return (False, "connection_refused")
    except Exception as e:
        return (False, str(e)[:50])


async def batch_precheck(urls: List[Tuple[int, str]], concurrency: int = 20) -> Dict[int, Tuple[bool, str]]:
    """Check multiple URLs in parallel. Returns dict of hotel_id -> (reachable, error)."""
    semaphore = asyncio.Semaphore(concurrency)

    async def check_one(hotel_id: int, url: str) -> Tuple[int, bool, str]:
        async with semaphore:
            reachable, error = await http_precheck(url)
            return (hotel_id, reachable, error)

    tasks = [check_one(hid, url) for hid, url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output = {}
    for r in results:
        if isinstance(r, Exception):
            continue
        hotel_id, reachable, error = r
        output[hotel_id] = (reachable, error)

    return output


# =============================================================================
# ENGINE DETECTION
# =============================================================================

class EngineDetector:
    """Detects booking engines from URLs, HTML, and network requests."""

    @staticmethod
    def from_domain(domain: str) -> Tuple[str, str]:
        """Check if domain matches a known booking engine."""
        if not domain:
            return ("", "")
        for engine_name, patterns in get_engine_patterns().items():
            for pat in patterns:
                if pat in domain:
                    return (engine_name, pat)
        return ("", "")

    @staticmethod
    def from_url(url: str, hotel_domain: str) -> Tuple[str, str, str]:
        """Detect engine from URL. Returns (engine_name, domain, method)."""
        if not url:
            return ("unknown", "", "no_url")

        url_lower = url.lower()
        for engine_name, patterns in get_engine_patterns().items():
            for pat in patterns:
                if pat in url_lower:
                    return (engine_name, pat, "url_pattern_match")

        domain = extract_domain(url)
        if not domain:
            return ("unknown", "", "no_domain")

        engine_name, pat = EngineDetector.from_domain(domain)
        if engine_name:
            return (engine_name, domain, "url_domain_match")

        if hotel_domain and domain != hotel_domain:
            return ("unknown_third_party", domain, "third_party_domain")

        return ("proprietary_or_same_domain", domain, "same_domain")

    @staticmethod
    def from_network(network_urls: Dict[str, str], hotel_domain: str) -> Tuple[str, str, str, str]:
        """Check network requests for engine domains."""
        # First: check for known booking engines
        for host, full_url in network_urls.items():
            engine_name, pat = EngineDetector.from_domain(host)
            if engine_name:
                return (engine_name, host, "network_sniff", full_url)

        # Second: look for booking-related API calls
        booking_keywords = ['book', 'reserv', 'avail', 'pricing', 'checkout', 'payment']
        skip_hosts = [
            'google', 'facebook', 'analytics', 'cdn', 'cloudflare', 'jquery', 'wp-',
            '2o7.net', 'omtrdc.net', 'demdex.net', 'adobedtm', 'omniture',
            'doubleclick', 'adsrvr', 'adnxs', 'criteo', 'taboola', 'outbrain',
            'hotjar', 'mouseflow', 'fullstory', 'heap', 'mixpanel', 'segment',
            'newrelic', 'datadome', 'sentry', 'bugsnag',
            'shopify', 'shop.app', 'myshopify',
            'nowbookit', 'dimmi.com.au', 'sevenrooms', 'opentable', 'resy.com',
        ]

        for host, full_url in network_urls.items():
            if host == hotel_domain:
                continue
            if any(skip in host for skip in skip_hosts):
                continue
            url_lower = full_url.lower()
            for keyword in booking_keywords:
                if keyword in url_lower:
                    return ("unknown_booking_api", host, "network_sniff_keyword", full_url)

        return ("", "", "", "")


# =============================================================================
# CONTACT EXTRACTION
# =============================================================================

class ContactExtractor:
    """Extracts phone numbers, emails, and room count from HTML."""

    PHONE_PATTERNS = [
        r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
    ]
    EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    # Patterns for room count extraction
    ROOM_COUNT_PATTERNS = [
        r'(\d+)\s*(?:guest\s*)?rooms?(?:\s+available)?',
        r'(\d+)\s*(?:boutique\s*)?(?:guest\s*)?rooms?',
        r'(\d+)[\s-]*room\s+(?:hotel|motel|inn|property)',
        r'(?:hotel|property|we)\s+(?:has|have|offers?|features?)\s+(\d+)\s*rooms?',
        r'(?:featuring|with)\s+(\d+)\s*(?:guest\s*)?rooms?',
        r'(\d+)\s*(?:suites?|units?|apartments?|accommodations?)',
    ]

    SKIP_EMAIL_PATTERNS = [
        'example.com', 'domain.com', 'email.com', 'sentry.io',
        'wixpress.com', 'schema.org', '.png', '.jpg', '.gif'
    ]

    @classmethod
    def extract_phones(cls, html: str) -> List[str]:
        """Extract phone numbers from HTML."""
        phones = []
        for pattern in cls.PHONE_PATTERNS:
            phones.extend(re.findall(pattern, html))
        seen = set()
        cleaned = []
        for p in phones:
            p = re.sub(r'[^\d+]', '', p)
            if len(p) >= 10 and p not in seen:
                seen.add(p)
                cleaned.append(p)
        return cleaned[:3]

    @classmethod
    def extract_emails(cls, html: str) -> List[str]:
        """Extract email addresses from HTML."""
        matches = re.findall(cls.EMAIL_PATTERN, html)
        filtered = []
        for email in matches:
            email_lower = email.lower()
            if not any(skip in email_lower for skip in cls.SKIP_EMAIL_PATTERNS):
                if email_lower not in [e.lower() for e in filtered]:
                    filtered.append(email)
        return filtered[:3]

    @classmethod
    def extract_room_count(cls, text: str) -> str:
        """Extract number of rooms from text."""
        text_lower = text.lower()

        for pattern in cls.ROOM_COUNT_PATTERNS:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            for match in matches:
                try:
                    count = int(match)
                    # Sanity check: room count should be reasonable (1-2000)
                    if 1 <= count <= 2000:
                        return str(count)
                except ValueError:
                    continue
        return ""


# =============================================================================
# BOOKING BUTTON FINDER
# =============================================================================

class BookingButtonFinder:
    """Finds and clicks booking buttons on hotel websites."""

    def __init__(self, config: DetectionConfig):
        self.config = config

    def _log(self, msg: str) -> None:
        """Log message if debug is enabled."""
        if self.config.debug:
            logger.debug(msg)

    async def _dismiss_popups(self, page: Page) -> None:
        """Try to dismiss cookie consent and other popups."""
        self._log("    [COOKIES] Trying to dismiss popups...")

        dismiss_selectors = [
            "button:has-text('Accept All')",
            "button:has-text('Accept all')",
            "button:has-text('accept all')",
            "button:has-text('Accept')",
            "button:has-text('accept')",
            "button:has-text('I agree')",
            "button:has-text('Agree')",
            "button:has-text('Got it')",
            "button:has-text('OK')",
            "button:has-text('Allow')",
            "button:has-text('Continue')",
            "a:has-text('Accept')",
            "a:has-text('accept')",
            "[class*='cookie'] button",
            "[class*='Cookie'] button",
            "[id*='cookie'] button",
            "[class*='consent'] button",
            "[class*='gdpr'] button",
            "[class*='privacy'] button:has-text('accept')",
            "[class*='cookie'] [class*='close']",
            "[class*='popup'] [class*='close']",
            "[class*='modal'] [class*='close']",
            "button[aria-label='Close']",
            "button[aria-label='close']",
        ]

        for selector in dismiss_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.count() > 0:
                    visible = await btn.is_visible()
                    if visible:
                        self._log(f"    [COOKIES] Clicking: {selector}")
                        await btn.click(timeout=1000)
                        await asyncio.sleep(0.5)
                        return
            except Exception:
                continue

        self._log("    [COOKIES] No popup found to dismiss")

    async def _debug_page_elements(self, page: Page) -> None:
        """Log all buttons and prominent links on the page for debugging."""
        if not self.config.debug:
            return

        try:
            # Get all buttons
            buttons = await page.locator("button").all()
            button_texts = []
            for b in buttons[:10]:
                try:
                    txt = await b.text_content()
                    if txt and txt.strip():
                        button_texts.append(txt.strip()[:30])
                except Exception:
                    pass
            if button_texts:
                self._log(f"    [DEBUG] Buttons on page: {button_texts}")

            # Get all links with text
            links = await page.locator("a").all()
            link_info = []
            for a in links[:15]:
                try:
                    txt = await a.text_content()
                    href = await a.get_attribute("href") or ""
                    if txt and txt.strip() and len(txt.strip()) < 40:
                        link_info.append(f"'{txt.strip()[:20]}' -> {href[:30] if href else 'no-href'}")
                except Exception:
                    pass
            if link_info:
                self._log(f"    [DEBUG] Links on page: {link_info[:8]}")
        except Exception as e:
            self._log(f"    [DEBUG] Error getting page elements: {e}")

    async def find_candidates(self, page: Page, max_candidates: int = 5) -> List:
        """Find booking button candidates using JavaScript with priority scoring."""
        import time

        self._log("    [FIND] Searching for booking buttons...")
        t0 = time.time()

        # Priority-based JS button finder
        js_result = await page.evaluate("""() => {
            const bookingTerms = ['book', 'reserve', 'availability', 'check rates', 'rooms', 'stay', 'inquire', 'enquire', 'rates', 'pricing', 'get started', 'plan your'];
            const excludeTerms = ['facebook', 'twitter', 'instagram', 'spa ', 'conference', 'wedding', 'restaurant', 'careers', 'terms', 'conditions', 'privacy', 'policy', 'contact', 'about', 'faq', 'gallery', 'reviews', 'gift', 'shop', 'store', 'blog', 'news', 'press'];
            const bookingEngineUrls = ['synxis', 'cloudbeds', 'ipms247', 'windsurfercrs', 'travelclick',
                'webrezpro', 'resnexus', 'thinkreservations', 'asiwebres', 'book-direct', 'bookdirect',
                'reservations', 'booking', 'mews.', 'little-hotelier', 'siteminder', 'thebookingbutton',
                'triptease', 'homhero', 'streamlinevrs', 'freetobook', 'eviivo', 'beds24', 'checkfront',
                'lodgify', 'hostaway', 'guesty', 'staydirectly', 'rentrax', 'bookingmood', 'seekda',
                'profitroom', 'avvio', 'simplotel', 'hotelrunner', 'amenitiz', 'newbook', 'roomraccoon',
                'rezstream', 'fareharbor', 'hirum', 'seekom', 'escapia', 'liverez', 'trackhs'];
            const results = [];
            const currentDomain = window.location.hostname.replace('www.', '');

            const elements = document.querySelectorAll('a, button, input[type="submit"], input[type="button"], [role="button"], [onclick], li[onclick], div[onclick], span[onclick], [class*="book"], [class*="reserve"], [class*="btn"], [class*="button"], [class*="cta"]');

            for (const el of elements) {
                const tag = el.tagName.toLowerCase();
                if (['script', 'style', 'svg', 'path', 'meta', 'link', 'head', 'noscript', 'template'].includes(tag)) continue;

                const text = (el.innerText || el.textContent || el.value || '').toLowerCase().trim();
                const href = (typeof el.href === 'string' ? el.href : el.getAttribute('href') || '').toLowerCase();
                const rect = el.getBoundingClientRect();

                if (rect.width === 0 || rect.height === 0) continue;
                if (rect.width > 600 || rect.height > 150) continue;
                if (rect.width < 20 || rect.height < 15) continue;

                let isExcluded = false;
                for (const term of excludeTerms) {
                    if (href.includes(term) || text.includes(term)) {
                        isExcluded = true;
                        break;
                    }
                }
                if (isExcluded) continue;

                let isExternal = false;
                let linkDomain = '';
                if (href.startsWith('http')) {
                    try {
                        linkDomain = new URL(href).hostname.replace('www.', '');
                        isExternal = linkDomain !== currentDomain;
                    } catch(e) {}
                }

                // Priority scoring
                let priority = 99;
                for (const url of bookingEngineUrls) {
                    if (href.includes(url)) {
                        priority = 0;
                        break;
                    }
                }

                if (priority > 1 && isExternal) {
                    if (text.includes('book') || text.includes('reserve') || text.includes('availability')) {
                        priority = 1;
                    }
                }

                if (priority > 2) {
                    if (text.includes('book now') || text.includes('book a stay') || text.includes('reserve now') || text.includes('book direct')) {
                        priority = isExternal ? 1 : 2;
                    } else if ((text.includes('book') || text.includes('reserve')) && text.length < 30) {
                        priority = isExternal ? 2 : 3;
                    } else if (text.includes('availability') || text.includes('check rates') || text.includes('rooms')) {
                        priority = isExternal ? 2 : 4;
                    }
                }

                if (priority < 99) {
                    const lengthPenalty = Math.floor(text.length / 15);
                    results.push({
                        tag: el.tagName.toLowerCase(),
                        text: text.substring(0, 40),
                        href: href.substring(0, 200),
                        fullHref: el.href || el.getAttribute('href') || '',
                        classes: (el.className || '').substring(0, 100),
                        id: el.id || '',
                        priority: priority + lengthPenalty,
                        isExternal: isExternal,
                        linkDomain: linkDomain,
                        x: rect.x,
                        y: rect.y
                    });
                }

                if (results.length >= 20) break;
            }

            results.sort((a, b) => a.priority - b.priority);
            return results.slice(0, 10);
        }""")

        self._log(f"    [FIND] Found {len(js_result)} candidates in {time.time()-t0:.1f}s")

        candidates = []
        for item in js_result:
            try:
                loc = None

                # Strategy 1: Find by ID (most reliable)
                if item.get('id'):
                    loc = page.locator(f"#{item['id']}").first
                    if await loc.count() > 0:
                        candidates.append(loc)
                        self._log(f"    [FIND] ✓ #{item['id']}: '{item['text'][:25]}'")
                        continue

                # Strategy 2: Find by href
                if item.get('href') and item['href'].startswith('http'):
                    loc = page.locator(f"a[href='{item['href']}']").first
                    if await loc.count() > 0:
                        candidates.append(loc)
                        self._log(f"    [FIND] ✓ href: '{item['text'][:25]}'")
                        continue

                # Strategy 3: Find by text content
                text_clean = item['text'][:25].replace("'", "\\'").replace('"', '\\"')
                if text_clean:
                    loc = page.locator(f"//*[self::a or self::button or self::div or self::span or self::li or self::input or self::label][contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text_clean}')]").first
                    if await loc.count() > 0:
                        candidates.append(loc)
                        self._log(f"    [FIND] ✓ text: '{item['text'][:25]}'")
                        continue

                # Strategy 4: Find by position (last resort)
                if item.get('x') and item.get('y'):
                    loc = page.locator(f"{item['tag']}").first
                    if await loc.count() > 0:
                        candidates.append(loc)
                        self._log(f"    [FIND] ✓ tag: {item['tag']} '{item['text'][:25]}'")

            except Exception as e:
                self._log(f"    [FIND] Error: {e}")
                continue

            if len(candidates) >= max_candidates:
                break

        if not candidates:
            self._log("    [FIND] No booking buttons found")
            await self._debug_page_elements(page)

        return candidates

    async def click_and_navigate(self, context: BrowserContext, page: Page) -> Tuple:
        """Click booking button and return (page, url, method, network_urls)."""
        await self._dismiss_popups(page)
        candidates = await self.find_candidates(page)

        self._log(f"    [CLICK] Found {len(candidates)} candidates")

        if not candidates:
            await self._debug_page_elements(page)
            return (None, None, "no_booking_button_found", {})

        el = candidates[0]

        try:
            el_text = (await asyncio.wait_for(el.text_content(), timeout=2.0) or "").strip()
            el_href = await asyncio.wait_for(el.get_attribute("href"), timeout=2.0) or ""
        except asyncio.TimeoutError:
            el_text = ""
            el_href = ""

        # Check if external
        is_external = ""
        if el_href and el_href.startswith("http"):
            try:
                link_domain = urlparse(el_href).netloc.replace("www.", "")
                page_domain = urlparse(page.url).netloc.replace("www.", "")
                is_external = " [EXTERNAL]" if link_domain != page_domain else ""
            except Exception:
                pass

        self._log(f"    [CLICK] Best candidate: '{el_text[:30]}' -> {el_href[:80] if el_href else 'no-href'}{is_external}")

        # If it has an href, use it directly
        if el_href and not el_href.startswith("#") and not el_href.startswith("javascript:"):
            if not el_href.startswith("http"):
                el_href = urljoin(page.url, el_href)
            self._log(f"    [CLICK] ✓ Booking URL: {el_href[:80]}")
            return (None, el_href, "href_extraction", {})

        # No href - try clicking
        original_url = page.url
        click_network_urls: Dict[str, str] = {}

        def capture_click_request(request):
            try:
                url = request.url
                host = extract_domain(url)
                if host and host not in click_network_urls:
                    click_network_urls[host] = url
            except Exception:
                pass

        page.on("request", capture_click_request)

        try:
            # Try for popup
            try:
                async with context.expect_page(timeout=2000) as p_info:
                    await el.click(force=True, no_wait_after=True)
                new_page = await p_info.value
                page.remove_listener("request", capture_click_request)
                self._log(f"    [CLICK] ✓ Popup: {new_page.url[:60]}")
                return (new_page, new_page.url, "popup_page", click_network_urls)
            except PWTimeoutError:
                pass

            # Check if page URL changed
            await asyncio.sleep(0.5)  # Reduced from 1.5s
            if page.url != original_url:
                page.remove_listener("request", capture_click_request)
                self._log(f"    [CLICK] ✓ Navigated: {page.url[:60]}")
                return (page, page.url, "navigation", click_network_urls)

            # Check network requests made by the click (for widgets)
            page.remove_listener("request", capture_click_request)
            if click_network_urls:
                self._log(f"    [CLICK] Widget detected - captured {len(click_network_urls)} network requests")
                return (page, original_url, "widget_interaction", click_network_urls)

        except Exception as e:
            page.remove_listener("request", capture_click_request)
            self._log(f"    [CLICK] Click failed: {e}")

        return (None, None, "click_failed", click_network_urls)

    async def _try_second_stage_click(self, context: BrowserContext, page: Page) -> Optional[Tuple]:
        """Try to find and click a second booking button (in sidebar/modal)."""
        self._log("    [2ND STAGE] Looking for second button...")

        original_url = page.url

        second_selectors = [
            "button:has-text('check availability')",
            "a:has-text('check availability')",
            "button:has-text('availability')",
            "a:has-text('availability')",
            "button:has-text('book now')",
            "button:has-text('check rates')",
            "button:has-text('search')",
            "button:has-text('view rates')",
            "a:has-text('book now')",
            "a:has-text('check rates')",
            "a[href*='ipms247']",
            "a[href*='synxis']",
            "a[href*='cloudbeds']",
            "input[type='submit']",
            "button[type='submit']",
        ]

        for selector in second_selectors:
            try:
                btn = page.locator(selector).first
                count = await btn.count()
                visible = await btn.is_visible() if count > 0 else False
                self._log(f"    [2ND STAGE] {selector}: count={count}, visible={visible}")

                if count > 0 and visible:
                    href = await btn.get_attribute("href") or ""
                    if href and href.startswith("http"):
                        self._log(f"    [2ND STAGE] Found href: {href[:60]}")
                        return (None, href, "two_stage_href")

                    try:
                        async with context.expect_page(timeout=1500) as p_info:
                            await btn.click(force=True, no_wait_after=True)
                        new_page = await p_info.value
                        self._log(f"    [2ND STAGE] Got popup: {new_page.url[:60]}")
                        return (new_page, new_page.url, "two_stage_popup")
                    except PWTimeoutError:
                        self._log("    [2ND STAGE] No popup from click")

                        await asyncio.sleep(0.5)
                        if page.url != original_url:
                            self._log(f"    [2ND STAGE] URL changed: {page.url[:60]}")
                            return (page, page.url, "two_stage_navigation")
            except Exception as e:
                self._log(f"    [2ND STAGE] Error: {e}")
                continue

        return None


# =============================================================================
# HOTEL PROCESSOR - Main detection logic
# =============================================================================

class HotelProcessor:
    """Processes a single hotel: visits site, detects engine, extracts contacts."""

    def __init__(self, config: DetectionConfig, browser: Browser, semaphore: asyncio.Semaphore, context_queue: asyncio.Queue):
        self.config = config
        self.browser = browser
        self.semaphore = semaphore
        self.button_finder = BookingButtonFinder(config)
        self.context_queue = context_queue

    def _log(self, msg: str) -> None:
        """Log message if debug is enabled."""
        if self.config.debug:
            logger.debug(msg)

    async def process(self, hotel_id: int, name: str, website: str, skip_precheck: bool = False) -> DetectionResult:
        """Process a single hotel and return results."""
        website = normalize_url(website)
        result = DetectionResult(hotel_id=hotel_id)

        logger.info(f"Processing hotel {hotel_id}: {name} | {website}")

        if not website:
            return result

        # Skip junk domains (unless already checked)
        if not skip_precheck:
            website_lower = website.lower()
            if any(junk in website_lower for junk in SKIP_JUNK_DOMAINS):
                result.error = "junk_domain"
                return result

            # HTTP pre-check
            is_reachable, precheck_error = await http_precheck(website)
            if not is_reachable:
                self._log(f"  [PRECHECK] ✗ Skipping (not reachable): {precheck_error}")
                result.error = f"precheck_failed: {precheck_error}"
                return result

        async with self.semaphore:
            result = await self._process_website(website, result)

        return result

    async def _process_website(self, website: str, result: DetectionResult) -> DetectionResult:
        """Visit website and extract all data."""
        import time

        context = await self.context_queue.get()
        page = await context.new_page()

        homepage_network: Dict[str, str] = {}

        def capture_homepage_request(request):
            try:
                url = request.url
                host = extract_domain(url)
                if host and host not in homepage_network:
                    homepage_network[host] = url
            except Exception:
                pass

        page.on("request", capture_homepage_request)

        try:
            # 1. Load homepage
            t0 = time.time()
            try:
                await page.goto(website, timeout=self.config.timeout_page_load, wait_until="domcontentloaded")
            except PWTimeoutError:
                try:
                    await page.goto(website, timeout=15000, wait_until="commit")
                except Exception:
                    pass
            self._log(f"  [TIME] goto: {time.time()-t0:.1f}s")

            await asyncio.sleep(0.5)  # Reduced from 1.5s
            hotel_domain = extract_domain(page.url)
            self._log(f"  Loaded: {hotel_domain}")

            # 2. Extract contacts and location
            t0 = time.time()
            result = await self._extract_contacts(page, result)
            self._log(f"  [TIME] contacts: {time.time()-t0:.1f}s")

            # 3. Check location filter - skip engine detection if mismatch
            if self.config.target_location and result.detected_location:
                if not LocationExtractor.location_matches(result.detected_location, self.config.target_location):
                    self._log(f"  [LOCATION] Mismatch: '{result.detected_location}' != '{self.config.target_location}' - skipping engine detection")
                    result.error = "location_mismatch"
                    await page.close()
                    await self.context_queue.put(context)
                    return result

            engine_name = ""
            engine_domain = ""
            booking_url = ""
            click_method = ""

            # 4. Quick scan homepage HTML for engine patterns
            t0 = time.time()
            html_engine, html_domain = await self._scan_html_for_engines(page)
            self._log(f"  [TIME] homepage_html_scan: {time.time()-t0:.1f}s")

            if html_engine:
                self._log(f"  [STAGE0] ✓ Found engine in homepage HTML: {html_engine}")
                engine_name = html_engine
                engine_domain = html_domain
                click_method = "homepage_html_scan"

                # Try to get booking URL
                booking_url = await self._find_booking_url_from_html(page, hotel_domain)
                if booking_url:
                    self._log(f"  [STAGE0] Sample booking URL: {booking_url[:60]}...")

            # 5. Find booking URL via button click
            if not engine_name or self._needs_fallback(engine_name) or not booking_url:
                self._log(f"  [STAGE1] Looking for booking URL via button click...")
                t0 = time.time()
                button_url, button_method, click_network_urls = await self._find_booking_url(context, page, hotel_domain)
                self._log(f"  [TIME] button_find: {time.time()-t0:.1f}s")

                if button_url:
                    booking_url = button_url
                    click_method = f"{click_method}+{button_method}" if click_method else button_method

                if click_network_urls and self._needs_fallback(engine_name):
                    net_engine, net_domain, _, net_url = EngineDetector.from_network(click_network_urls, hotel_domain)
                    if net_engine:
                        self._log(f"  [WIDGET NET] ✓ Found engine from click network: {net_engine}")
                        engine_name = net_engine
                        engine_domain = net_domain
                        click_method = f"{click_method}+widget_network" if click_method else "widget_network"
                        if net_url and not booking_url:
                            booking_url = net_url

            result.booking_url = booking_url or ""
            result.detection_method = click_method

            # 6. Analyze booking page
            if booking_url and self._needs_fallback(engine_name):
                t0 = time.time()
                engine_name, engine_domain, result = await self._analyze_booking_page(
                    context, booking_url, hotel_domain, click_method, result
                )
                self._log(f"  [TIME] analyze_booking: {time.time()-t0:.1f}s")

            # 7. FALLBACK: Check homepage network
            if self._needs_fallback(engine_name):
                t0 = time.time()
                net_engine, net_domain, _, net_url = EngineDetector.from_network(homepage_network, hotel_domain)
                self._log(f"  [TIME] network_fallback: {time.time()-t0:.1f}s")
                if net_engine and net_engine not in ("unknown_third_party",):
                    engine_name = net_engine
                    engine_domain = net_domain
                    result.detection_method += "+homepage_network"
                    if net_url and not result.booking_url:
                        result.booking_url = net_url

            # 8. FALLBACK: Scan iframes
            if self._needs_fallback(engine_name):
                t0 = time.time()
                frame_engine, frame_domain, frame_url = await self._scan_frames(page)
                self._log(f"  [TIME] frame_scan: {time.time()-t0:.1f}s")
                if frame_engine:
                    engine_name = frame_engine
                    engine_domain = frame_domain
                    result.detection_method += "+frame_scan"
                    if frame_url and not result.booking_url:
                        result.booking_url = frame_url

            # 9. FALLBACK: HTML keyword scan
            if self._needs_fallback(engine_name):
                t0 = time.time()
                html_engine = await self._detect_from_html(page)
                self._log(f"  [TIME] html_detect: {time.time()-t0:.1f}s")
                if html_engine:
                    engine_name = html_engine
                    result.detection_method += "+html_keyword"

            result.booking_engine = engine_name or ""
            result.booking_engine_domain = engine_domain

            # Check for junk booking URLs
            junk_booking_domains = [
                "facebook.com", "instagram.com", "twitter.com", "youtube.com",
                "linkedin.com", "yelp.com", "tripadvisor.com", "google.com",
                "booking.com", "expedia.com", "hotels.com", "airbnb.com", "vrbo.com",
            ]
            if result.booking_url:
                booking_domain = extract_domain(result.booking_url)
                if any(junk in booking_domain for junk in junk_booking_domains):
                    self._log(f"  Junk booking URL detected: {booking_domain}")
                    result.booking_url = ""
                    result.booking_engine = ""
                    result.booking_engine_domain = ""
                    result.error = "junk_booking_url"

            if not result.booking_url and result.booking_engine in ("", "unknown"):
                result.error = "no_booking_found"

            self._log(f"  Engine: {result.booking_engine} ({result.booking_engine_domain or 'n/a'})")

        except PWTimeoutError:
            result.error = "timeout"
            self._log("  ERROR: timeout")
        except Exception as e:
            error_msg = str(e).replace('\n', ' ').replace('\r', '')[:100]
            result.error = f"exception: {error_msg}"
            self._log(f"  ERROR: {e}")
        finally:
            await page.close()
            await self.context_queue.put(context)

        if self.config.pause_between_hotels > 0:
            await asyncio.sleep(self.config.pause_between_hotels)

        return result

    def _needs_fallback(self, engine_name: str) -> bool:
        """Check if we need to try fallback detection."""
        return engine_name in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain")

    async def _extract_contacts(self, page: Page, result: DetectionResult) -> DetectionResult:
        """Extract phone, email, room count, and location from page."""
        try:
            text = await page.evaluate("document.body ? document.body.innerText : ''")
            html = await page.evaluate("document.documentElement.outerHTML")
            phones = ContactExtractor.extract_phones(text)
            emails = ContactExtractor.extract_emails(text)
            room_count = ContactExtractor.extract_room_count(text)
            location = LocationExtractor.extract_location(text, html)

            if phones:
                result.phone_website = phones[0]
            if emails:
                result.email = emails[0]
            if room_count:
                result.room_count = room_count
            if location:
                result.detected_location = location
                self._log(f"  [LOCATION] Detected: {location}")

            # Also extract from tel: and mailto: links
            if not result.phone_website:
                try:
                    tel_links = await page.evaluate("""
                        () => Array.from(document.querySelectorAll('a[href^="tel:"]'))
                            .map(a => a.href.replace('tel:', '').replace(/[^0-9+()-]/g, ''))
                            .filter(p => p.length >= 10)
                    """)
                    if tel_links:
                        result.phone_website = tel_links[0]
                except Exception:
                    pass

            if not result.email:
                try:
                    mailto_links = await page.evaluate("""
                        () => Array.from(document.querySelectorAll('a[href^="mailto:"]'))
                            .map(a => a.href.replace('mailto:', '').split('?')[0])
                            .filter(e => e.includes('@'))
                    """)
                    if mailto_links:
                        result.email = mailto_links[0]
                except Exception:
                    pass

        except Exception:
            pass
        return result

    async def _scan_html_for_engines(self, page: Page) -> Tuple[str, str]:
        """Scan page HTML for booking engine patterns."""
        try:
            html = await page.evaluate("document.documentElement.outerHTML")
            html_lower = html.lower()

            # Extract URLs from HTML
            url_pattern = r'(?:src|href|data-src|action)=["\']?(https?://[^"\'\s>]+)'
            found_urls = re.findall(url_pattern, html, re.IGNORECASE)

            js_url_pattern = r'["\']?(https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^"\'\s]*)["\']?'
            found_urls.extend(re.findall(js_url_pattern, html))

            domains_found = set()
            for url in found_urls:
                domain = extract_domain(url)
                if domain:
                    domains_found.add(domain.lower())

            for domain in domains_found:
                for engine_name, patterns in get_engine_patterns().items():
                    for pat in patterns:
                        if pat.lower() in domain:
                            self._log(f"    [HTML SCAN] Found domain '{domain}' -> {engine_name}")
                            return (engine_name, pat)

            # Full keyword patterns from original script
            keyword_patterns = [
                ("resortpro", "Streamline", "streamlinevrs.com"),
                ("homhero", "HomHero", "homhero.com.au"),
                ("cloudbeds", "Cloudbeds", "cloudbeds.com"),
                ("freetobook", "FreeToBook", "freetobook.com"),
                ("siteminder", "SiteMinder", "siteminder.com"),
                ("thebookingbutton", "SiteMinder", "thebookingbutton.com"),
                ("littlehotelier", "Little Hotelier", "littlehotelier.com"),
                ("webrezpro", "WebRezPro", "webrezpro.com"),
                ("resnexus", "ResNexus", "resnexus.com"),
                ("beds24", "Beds24", "beds24.com"),
                ("checkfront", "Checkfront", "checkfront.com"),
                ("eviivo", "eviivo", "eviivo.com"),
                ("lodgify", "Lodgify", "lodgify.com"),
                ("newbook", "Newbook", "newbook.cloud"),
                ("rmscloud", "RMS Cloud", "rmscloud.com"),
                ("ipms247", "JEHS / iPMS", "ipms247.com"),
                ("synxis", "SynXis / TravelClick", "synxis.com"),
                ("mews.com", "Mews", "mews.com"),
                ("triptease", "Triptease", "triptease.io"),
                ("bookingmood", "BookingMood", "bookingmood.com"),
                ("seekda", "Seekda / KUBE", "seekda.com"),
                ("kube", "Seekda / KUBE", "seekda.com"),
                ("ownerreservations", "OwnerReservations", "ownerreservations.com"),
                ("guestroomgenie", "GuestRoomGenie", "guestroomgenie.com"),
                ("beyondpricing", "Beyond Pricing", "beyondpricing.com"),
                ("hotelkeyapp", "HotelKey", "hotelkeyapp.com"),
                ("prenohq", "Preno", "prenohq.com"),
                ("profitroom", "Profitroom", "profitroom.com"),
                ("avvio", "Avvio", "avvio.com"),
                ("netaffinity", "Net Affinity", "netaffinity.com"),
                ("simplotel", "Simplotel", "simplotel.com"),
                ("cubilis", "Cubilis", "cubilis.com"),
                ("cendyn", "Cendyn", "cendyn.com"),
                ("booklogic", "BookLogic", "booklogic.net"),
                ("ratetiger", "RateTiger", "ratetiger.com"),
                ("d-edge", "D-Edge", "d-edge.com"),
                ("availpro", "D-Edge", "availpro.com"),
                ("bookassist", "BookAssist", "bookassist.com"),
                ("guestcentric", "GuestCentric", "guestcentric.com"),
                ("verticalbooking", "Vertical Booking", "verticalbooking.com"),
                ("busyrooms", "Busy Rooms", "busyrooms.com"),
                ("myhotel.io", "myHotel.io", "myhotel.io"),
                ("hotelspider", "HotelSpider", "hotelspider.com"),
                ("staah", "Staah", "staah.com"),
                ("axisrooms", "AxisRooms", "axisrooms.com"),
                ("e4jconnect", "E4jConnect", "e4jconnect.com"),
                ("vikbooking", "VikBooking", "vikbooking.com"),
                ("apaleo", "Apaleo", "apaleo.com"),
                ("clock-software", "Clock PMS", "clock-software.com"),
                ("clock-pms", "Clock PMS", "clock-pms.com"),
                ("protel", "Protel", "protel.net"),
                ("frontdeskanywhere", "Frontdesk Anywhere", "frontdeskanywhere.com"),
                ("hoteltime", "HotelTime", "hoteltime.com"),
                ("stayntouch", "StayNTouch", "stayntouch.com"),
                ("roomcloud", "RoomCloud", "roomcloud.net"),
                ("oaky", "Oaky", "oaky.com"),
                ("revinate", "Revinate", "revinate.com"),
                ("escapia", "Escapia", "escapia.com"),
                ("liverez", "LiveRez", "liverez.com"),
                ("barefoot", "Barefoot", "barefoot.com"),
                ("trackhs", "Track", "trackhs.com"),
                ("igms", "iGMS", "igms.com"),
                ("smoobu", "Smoobu", "smoobu.com"),
                ("tokeet", "Tokeet", "tokeet.com"),
                ("365villas", "365Villas", "365villas.com"),
                ("rentalsunited", "Rentals United", "rentalsunited.com"),
                ("bookingsync", "BookingSync", "bookingsync.com"),
                ("janiis", "JANIIS", "janiis.com"),
                ("quibblerm", "Quibble", "quibblerm.com"),
                ("hirum", "HiRUM", "hirum.com.au"),
                ("ibooked", "iBooked", "ibooked.net.au"),
                ("seekom", "Seekom", "seekom.com"),
                ("respax", "ResPax", "respax.com"),
                ("bookingcenter", "BookingCenter", "bookingcenter.com"),
                ("rezexpert", "RezExpert", "rezexpert.com"),
                ("supercontrol", "SuperControl", "supercontrol.co.uk"),
                ("anytimebooking", "Anytime Booking", "anytimebooking.eu"),
                ("elinapms", "Elina PMS", "elinapms.com"),
                ("guestline", "Guestline", "guestline.com"),
                ("nonius", "Nonius", "nonius.com"),
                ("visualmatrix", "Visual Matrix", "visualmatrix.com"),
                ("autoclerk", "AutoClerk", "autoclerk.com"),
                ("msisolutions", "MSI", "msisolutions.com"),
                ("skytouch", "SkyTouch", "skytouch.com"),
                ("roomkeypms", "RoomKeyPMS", "roomkeypms.com"),
            ]

            for keyword, engine_name, domain in keyword_patterns:
                pattern = rf'{re.escape(keyword)}[\./\-]'
                if re.search(pattern, html_lower):
                    return (engine_name, domain)

            return ("", "")
        except Exception:
            return ("", "")

    async def _detect_from_html(self, page: Page) -> str:
        """Detect engine from page HTML keywords (fallback)."""
        try:
            html = await page.evaluate("document.documentElement.outerHTML")
            html_lower = html.lower()

            # Simple keyword detection
            simple_patterns = [
                ("cloudbeds", "Cloudbeds"),
                ("synxis", "SynXis / TravelClick"),
                ("mews.com", "Mews"),
                ("siteminder", "SiteMinder"),
                ("littlehotelier", "Little Hotelier"),
                ("webrezpro", "WebRezPro"),
                ("resnexus", "ResNexus"),
                ("freetobook", "FreeToBook"),
                ("beds24", "Beds24"),
                ("checkfront", "Checkfront"),
                ("lodgify", "Lodgify"),
                ("eviivo", "eviivo"),
                ("ipms247", "JEHS / iPMS"),
            ]

            for keyword, engine_name in simple_patterns:
                if keyword in html_lower:
                    return engine_name

            return ""
        except Exception:
            return ""

    async def _find_booking_url_from_html(self, page: Page, hotel_domain: str) -> str:
        """Find booking URL from HTML links."""
        try:
            all_booking_urls = await page.evaluate("""
                (hotelDomain) => {
                    const links = document.querySelectorAll('a[href]');
                    const bookingPatterns = ['/book', '/checkout', '/reserve', '/availability', 'booking=', 'checkin=', '/enquiry', '/inquiry', '/rooms', '/stay', '/accommodation'];
                    const knownEngines = ['synxis', 'cloudbeds', 'lodgify', 'freetobook', 'mews.', 'siteminder', 'thebookingbutton',
                        'webrezpro', 'resnexus', 'beds24', 'checkfront', 'eviivo', 'ipms247', 'asiwebres', 'thinkreservations',
                        'bookdirect', 'rezstream', 'fareharbor', 'newbook', 'roomraccoon', 'hostaway', 'guesty', 'staydirectly',
                        'rentrax', 'bookingmood', 'seekda', 'profitroom', 'avvio', 'simplotel', 'hotelrunner', 'amenitiz'];
                    const junk = ['terms', 'conditions', 'policy', 'privacy', 'faq', 'about', 'appraisal', 'cancellation', 'facebook', 'twitter', 'instagram'];
                    const results = [];

                    for (const a of links) {
                        const href = a.href;
                        const hrefLower = href.toLowerCase();
                        if (!href.startsWith('http')) continue;
                        if (junk.some(j => hrefLower.includes(j))) continue;

                        const matchesPattern = bookingPatterns.some(p => hrefLower.includes(p));
                        const isKnownEngine = knownEngines.some(e => hrefLower.includes(e));
                        if (!matchesPattern && !isKnownEngine) continue;

                        try {
                            const linkDomain = new URL(href).hostname.replace('www.', '');
                            const isExternal = linkDomain !== hotelDomain;
                            results.push({ href, isExternal, domain: linkDomain });
                        } catch(e) {}
                    }

                    // Fallback: property/listing links
                    if (results.length === 0) {
                        for (const a of links) {
                            const href = a.href;
                            const hrefLower = href.toLowerCase();
                            if (hrefLower.includes('/property/') || hrefLower.includes('/listing/') ||
                                hrefLower.includes('/unit/') || hrefLower.includes('/rental/')) {
                                try {
                                    const linkDomain = new URL(href).hostname.replace('www.', '');
                                    const isExternal = linkDomain !== hotelDomain;
                                    results.push({ href, isExternal, domain: linkDomain });
                                } catch(e) {}
                            }
                        }
                    }
                    return results;
                }
            """, hotel_domain)

            if all_booking_urls:
                best_url = None
                best_priority = -1

                for item in all_booking_urls:
                    href = item['href']
                    is_external = item['isExternal']
                    link_domain = item['domain']

                    is_known_engine = False
                    for eng_name, patterns in get_engine_patterns().items():
                        if any(pat in link_domain for pat in patterns):
                            is_known_engine = True
                            break

                    if is_known_engine:
                        priority = 3
                    elif is_external:
                        priority = 2
                    else:
                        priority = 1

                    if priority > best_priority:
                        best_priority = priority
                        best_url = href

                return best_url or ""

            return ""
        except Exception:
            return ""

    async def _find_booking_url(self, context: BrowserContext, page: Page, hotel_domain: str) -> Tuple[str, str, Dict]:
        """Find booking button and get the booking URL."""
        booking_page, booking_url, method, click_network_urls = await self.button_finder.click_and_navigate(context, page)

        if click_network_urls:
            self._log(f"  [WIDGET] Captured {len(click_network_urls)} network requests from click")
            engine_name, engine_domain, net_method, engine_url = EngineDetector.from_network(click_network_urls, hotel_domain)
            if engine_name:
                self._log(f"  [WIDGET] Found engine from click: {engine_name} ({engine_domain})")
                if not booking_url and engine_url:
                    booking_url = engine_url
                    method = "widget_network_sniff"

        if booking_page and booking_page != page:
            try:
                await booking_page.close()
            except Exception:
                pass

        return booking_url, method, click_network_urls

    async def _analyze_booking_page(self, context: BrowserContext, booking_url: str, hotel_domain: str,
                                     click_method: str, result: DetectionResult) -> Tuple[str, str, DetectionResult]:
        """Navigate to booking URL, sniff network, detect engine."""
        self._log(f"  Booking URL: {booking_url[:80]}...")

        page = await context.new_page()
        network_urls: Dict[str, str] = {}
        engine_name = ""
        engine_domain = ""

        def capture_request(request):
            try:
                url = request.url
                host = extract_domain(url)
                if host and host not in network_urls:
                    network_urls[host] = url
            except Exception:
                pass

        page.on("request", capture_request)

        try:
            await page.goto(booking_url, timeout=self.config.timeout_page_load, wait_until="domcontentloaded")
            await asyncio.sleep(1.0)  # Reduced from 3.0s

            # Find external booking URL
            external_booking_url = await self._find_external_booking_url(page, hotel_domain)
            if external_booking_url:
                self._log(f"  [BOOKING PAGE] Found external URL: {external_booking_url[:60]}...")
                result.booking_url = external_booking_url
                engine_name, engine_domain, url_method = EngineDetector.from_url(external_booking_url, hotel_domain)
                if engine_name and engine_name not in ("proprietary_or_same_domain",):
                    result.detection_method = f"{click_method}+external_booking_url"
                    await page.close()
                    return engine_name, engine_domain, result

            # Check network
            engine_name, engine_domain, net_method, engine_url = EngineDetector.from_network(network_urls, hotel_domain)

            if not engine_name:
                engine_name, engine_domain, url_method = EngineDetector.from_url(booking_url, hotel_domain)
                net_method = url_method

            # Scan iframes
            if self._needs_fallback(engine_name):
                frame_engine, frame_domain, frame_url = await self._scan_frames(page)
                if frame_engine:
                    engine_name = frame_engine
                    engine_domain = frame_domain
                    net_method = "iframe_on_booking_page"
                    if frame_url:
                        engine_url = frame_url

            # Scan HTML
            if self._needs_fallback(engine_name):
                html_engine, html_domain = await self._scan_html_for_engines(page)
                if html_engine:
                    engine_name = html_engine
                    engine_domain = html_domain
                    net_method = "html_source_scan"

            # Multi-step: try second button click
            if self._needs_fallback(engine_name):
                try:
                    if not page.is_closed():
                        self._log("  [MULTI-STEP] Trying second button click...")
                        second_page, second_url, second_method, second_network = await self.button_finder.click_and_navigate(context, page)

                        if second_url and second_url != booking_url:
                            self._log(f"  [MULTI-STEP] Found deeper URL: {second_url[:60]}...")
                            result.booking_url = second_url

                            if second_network:
                                net_engine, net_domain, _, net_url = EngineDetector.from_network(second_network, hotel_domain)
                                if net_engine:
                                    engine_name = net_engine
                                    engine_domain = net_domain
                                    net_method = f"{net_method}+second_click_network"
                                    if net_url:
                                        result.booking_url = net_url

                            # Navigate to second URL and scan
                            if self._needs_fallback(engine_name):
                                try:
                                    if not page.is_closed():
                                        await page.goto(second_url, timeout=self.config.timeout_page_load, wait_until="domcontentloaded")
                                        await asyncio.sleep(0.5)  # Reduced from 2.0s

                                        html_engine, html_domain = await self._scan_html_for_engines(page)
                                        if html_engine:
                                            engine_name = html_engine
                                            engine_domain = html_domain
                                            net_method = f"{net_method}+second_page_scan"

                                        if self._needs_fallback(engine_name) and network_urls:
                                            net_engine2, net_domain2, _, net_url2 = EngineDetector.from_network(network_urls, hotel_domain)
                                            if net_engine2:
                                                engine_name = net_engine2
                                                engine_domain = net_domain2
                                                net_method = f"{net_method}+second_page_network"
                                except Exception as e:
                                    self._log(f"  [MULTI-STEP] Error on second page: {e}")

                        if second_page and second_page != page:
                            try:
                                await second_page.close()
                            except Exception:
                                pass
                except Exception as e:
                    self._log(f"  [MULTI-STEP] Error: {e}")

            if engine_url and engine_url != booking_url:
                result.booking_url = engine_url

            result.detection_method = f"{click_method}+{net_method}"

        except Exception as e:
            self._log(f"  Booking page error: {e}")
        finally:
            await page.close()

        return engine_name, engine_domain, result

    async def _find_external_booking_url(self, page: Page, hotel_domain: str) -> str:
        """Find external booking URLs on the current page."""
        try:
            return await page.evaluate("""
                (hotelDomain) => {
                    const links = document.querySelectorAll('a[href]');
                    const bookingText = ['book', 'reserve', 'availability', 'check avail', 'enquire', 'inquire'];
                    const junk = ['terms', 'conditions', 'policy', 'privacy', 'faq', 'facebook', 'instagram', 'twitter', 'sevenrooms', 'opentable', 'resy.com'];

                    for (const a of links) {
                        const href = a.href;
                        if (!href || !href.startsWith('http')) continue;

                        const text = (a.innerText || a.textContent || '').toLowerCase().trim();
                        const ariaLabel = (a.getAttribute('aria-label') || '').toLowerCase();
                        const title = (a.getAttribute('title') || '').toLowerCase();
                        const combinedText = text + ' ' + ariaLabel + ' ' + title;

                        if (!bookingText.some(t => combinedText.includes(t))) continue;
                        if (junk.some(j => href.toLowerCase().includes(j) || combinedText.includes(j))) continue;

                        try {
                            const linkDomain = new URL(href).hostname.replace('www.', '');
                            if (linkDomain !== hotelDomain) {
                                return href;
                            }
                        } catch(e) {}
                    }
                    return '';
                }
            """, hotel_domain)
        except Exception as e:
            self._log(f"  [BOOKING PAGE] Error scanning: {e}")
            return ""

    async def _scan_frames(self, page: Page) -> Tuple[str, str, str]:
        """Scan iframes for booking engine patterns."""
        for frame in page.frames:
            try:
                frame_url = frame.url
            except Exception:
                continue

            if not frame_url or frame_url.startswith("about:"):
                continue

            for engine_name, patterns in get_engine_patterns().items():
                for pat in patterns:
                    if pat in frame_url.lower():
                        return (engine_name, pat, frame_url)

        return ("", "", "")


# =============================================================================
# BATCH DETECTOR - Runs detection on multiple hotels
# =============================================================================

class BatchDetector:
    """Runs detection on multiple hotels concurrently with browser reuse."""

    def __init__(self, config: Optional[DetectionConfig] = None):
        self.config = config or DetectionConfig()

    async def detect_batch(self, hotels: List[Dict]) -> List[DetectionResult]:
        """Detect booking engines for a batch of hotels.

        Args:
            hotels: List of dicts with 'id', 'name', 'website' keys

        Returns:
            List of DetectionResult objects
        """
        if not hotels:
            return []

        results: List[DetectionResult] = []

        # OPTIMIZATION: Batch precheck all URLs first (parallel HTTP checks)
        urls_to_check = []
        for h in hotels:
            website = h.get('website', '')
            if website and not is_junk_domain(website):
                urls_to_check.append((h['id'], normalize_url(website)))

        logger.info(f"Running batch precheck on {len(urls_to_check)} URLs...")
        precheck_results = await batch_precheck(urls_to_check, concurrency=30)

        # Filter to only reachable hotels
        reachable_hotels = []
        for h in hotels:
            hotel_id = h['id']
            website = h.get('website', '')

            # Check for junk domain
            if not website or is_junk_domain(website):
                results.append(DetectionResult(hotel_id=hotel_id, error="junk_domain"))
                continue

            # Check precheck result
            if hotel_id in precheck_results:
                reachable, error = precheck_results[hotel_id]
                if not reachable:
                    results.append(DetectionResult(hotel_id=hotel_id, error=f"precheck_failed: {error}"))
                    continue

            reachable_hotels.append(h)

        logger.info(f"Precheck: {len(reachable_hotels)} reachable, {len(hotels) - len(reachable_hotels)} filtered")

        if not reachable_hotels:
            return results

        # Now process only reachable hotels with Playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            semaphore = asyncio.Semaphore(self.config.concurrency)

            # Create reusable context queue
            context_queue: asyncio.Queue = asyncio.Queue()
            contexts = []
            for _ in range(self.config.concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    ignore_https_errors=True,
                )
                contexts.append(ctx)
                await context_queue.put(ctx)

            processor = HotelProcessor(self.config, browser, semaphore, context_queue)

            # Process only reachable hotels (skip precheck in processor)
            tasks = [
                processor.process(
                    hotel_id=h['id'],
                    name=h['name'],
                    website=h.get('website', ''),
                    skip_precheck=True,  # Already done
                )
                for h in reachable_hotels
            ]

            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Convert exceptions to error results
            for i, result in enumerate(task_results):
                if isinstance(result, Exception):
                    results.append(DetectionResult(
                        hotel_id=reachable_hotels[i]['id'],
                        error=f"exception: {str(result)[:100]}"
                    ))
                else:
                    results.append(result)

            # Clean up
            for ctx in contexts:
                await ctx.close()
            await browser.close()

        return results
