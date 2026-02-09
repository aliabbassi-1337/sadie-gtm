"""Automatic API endpoint discovery for booking engines.

Loads a booking engine URL with Playwright, intercepts all XHR/fetch
requests and responses, and produces a structured report of every API
the frontend calls.

Usage:
    discoverer = ApiDiscoverer(headless=True)
    report = await discoverer.discover("https://direct-book.com/properties/somehotel")
    report.print_report()
"""

import asyncio
import json
import re
import time
from datetime import date, timedelta
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs, unquote

from loguru import logger
from pydantic import BaseModel


# ─────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────

class CapturedApi(BaseModel):
    """A single intercepted API call."""
    url: str
    path: str
    method: str
    status: int
    request_headers: dict = {}
    request_body: Optional[str] = None
    request_body_parsed: Optional[Any] = None
    response_content_type: str = ""
    response_body: Optional[Any] = None
    response_schema: Optional[dict] = None
    timing_ms: Optional[float] = None
    category: str = "api"  # api, graphql, static, tracking, other

    # GraphQL-specific
    graphql_operation: Optional[str] = None
    graphql_variables: Optional[dict] = None
    graphql_persisted_hash: Optional[str] = None


class DiscoveryReport(BaseModel):
    """Full discovery report for a booking engine URL."""
    url: str
    engine: Optional[str] = None
    apis: list[CapturedApi] = []
    cookies: list[dict] = []
    total_requests: int = 0
    discovery_time_s: float = 0.0

    def print_report(self):
        """Pretty-print the discovery report to console."""
        api_calls = [a for a in self.apis if a.category in ("api", "graphql")]
        graphql = [a for a in api_calls if a.category == "graphql"]
        rest = [a for a in api_calls if a.category == "api"]

        print()
        print("=" * 70)
        print(f"  API Discovery Report")
        print(f"  URL: {self.url}")
        if self.engine:
            print(f"  Engine: {self.engine} (detected)")
        print(f"  APIs found: {len(api_calls)}  |  "
              f"GraphQL: {len(graphql)}  |  REST: {len(rest)}")
        print(f"  Total requests intercepted: {self.total_requests}")
        print(f"  Discovery time: {self.discovery_time_s:.1f}s")
        print("=" * 70)

        if graphql:
            print()
            print("── GraphQL Endpoints " + "─" * 49)
            for i, api in enumerate(graphql, 1):
                print()
                print(f"  {i}. {api.method} {api.url[:80]}")
                if api.graphql_operation:
                    print(f"     Operation: {api.graphql_operation}")
                if api.graphql_variables:
                    vars_str = json.dumps(api.graphql_variables, ensure_ascii=False)
                    if len(vars_str) > 100:
                        vars_str = vars_str[:100] + "..."
                    print(f"     Variables: {vars_str}")
                if api.graphql_persisted_hash:
                    print(f"     Persisted Query Hash: {api.graphql_persisted_hash[:40]}...")
                if api.response_schema:
                    schema_str = json.dumps(api.response_schema, indent=2, ensure_ascii=False)
                    for line in schema_str.split("\n")[:15]:
                        print(f"     {line}")
                    if len(schema_str.split("\n")) > 15:
                        print(f"     ... ({len(schema_str.split(chr(10)))} lines total)")
                print(f"     Status: {api.status}  |  {api.timing_ms:.0f}ms" if api.timing_ms else f"     Status: {api.status}")

        if rest:
            print()
            print("── REST Endpoints " + "─" * 52)
            for i, api in enumerate(rest, 1 + len(graphql)):
                print()
                print(f"  {i}. {api.method} {api.url[:80]}")
                if api.request_body_parsed:
                    body_str = json.dumps(api.request_body_parsed, ensure_ascii=False)
                    if len(body_str) > 120:
                        body_str = body_str[:120] + "..."
                    print(f"     Body: {body_str}")
                elif api.request_body:
                    body = api.request_body[:120]
                    if len(api.request_body) > 120:
                        body += "..."
                    print(f"     Body: {body}")
                if api.response_schema:
                    schema_str = json.dumps(api.response_schema, indent=2, ensure_ascii=False)
                    for line in schema_str.split("\n")[:15]:
                        print(f"     {line}")
                    if len(schema_str.split("\n")) > 15:
                        print(f"     ... ({len(schema_str.split(chr(10)))} lines total)")
                print(f"     Status: {api.status}  |  {api.timing_ms:.0f}ms" if api.timing_ms else f"     Status: {api.status}")

        print()
        print("── Cookies " + "─" * 59)
        if self.cookies:
            for c in self.cookies:
                flags = []
                if c.get("httpOnly"):
                    flags.append("httpOnly")
                if c.get("secure"):
                    flags.append("secure")
                flag_str = f"  [{', '.join(flags)}]" if flags else ""
                print(f"  {c['name']} = {str(c.get('value', ''))[:40]}...{flag_str}"
                      if len(str(c.get("value", ""))) > 40
                      else f"  {c['name']} = {c.get('value', '')}{flag_str}")
        else:
            print("  (none)")

        print()
        print("── Summary " + "─" * 59)
        print(f"  Total API calls:    {len(api_calls)}")
        print(f"  JSON responses:     {sum(1 for a in api_calls if a.response_body is not None)}")
        print(f"  GraphQL operations: {len(graphql)}"
              + (f" ({', '.join(a.graphql_operation or '?' for a in graphql)})" if graphql else ""))

        # Detect auth patterns
        auth_found = any(
            "authorization" in (a.request_headers.get("authorization", "") +
                                a.request_headers.get("x-api-key", "") +
                                a.request_headers.get("x-sm-api-key", ""))
            for a in api_calls
        )
        print(f"  Auth required:      {'Yes' if auth_found else 'None detected'}")
        print(f"  Cookies set:        {len(self.cookies)}")
        print()

    def to_json(self) -> str:
        """Serialize the full report to JSON."""
        return self.model_dump_json(indent=2)


# ─────────────────────────────────────────────────────────────
# Tracking / junk URL patterns to skip
# ─────────────────────────────────────────────────────────────

SKIP_DOMAINS = {
    # Analytics
    "google-analytics.com", "googletagmanager.com", "analytics.google.com",
    "www.google-analytics.com", "stats.g.doubleclick.net", "www.googleadservices.com",
    # Social / tracking
    "facebook.com", "facebook.net", "connect.facebook.net",
    "platform.twitter.com", "cdn.syndication.twimg.com",
    "platform.linkedin.com", "snap.licdn.com",
    "www.tiktok.com", "analytics.tiktok.com",
    # Ad networks
    "googlesyndication.com", "adservice.google.com", "pagead2.googlesyndication.com",
    "adsensecustomsearchads.googleapis.com",
    # Session recording / heatmaps
    "hotjar.com", "static.hotjar.com", "script.hotjar.com",
    "fullstory.com", "rs.fullstory.com",
    "clarity.ms", "www.clarity.ms",
    "mouseflow.com", "cdn.mouseflow.com",
    "smartlook.com",
    "cdn.heapanalytics.com",
    # Error tracking
    "sentry.io", "browser.sentry-cdn.com",
    "bugsnag.com",
    # Chat widgets
    "widget.intercom.io", "js.intercomcdn.com",
    "embed.tawk.to",
    "cdn.livechatinc.com",
    "static.zdassets.com",  # Zendesk
    # CDNs (font/css only)
    "fonts.googleapis.com", "fonts.gstatic.com",
    "use.typekit.net", "use.fontawesome.com",
    # Cookie consent
    "cdn.cookielaw.org", "consentcdn.cookiebot.com",
    "cdn.osano.com",
    # Push notifications
    "onesignal.com", "cdn.onesignal.com",
    # Tag managers
    "cdn.segment.com", "api.segment.io",
    "cdn.mxpnl.com",  # Mixpanel
    # Maps (usually not booking API)
    "maps.googleapis.com", "maps.gstatic.com",
    # Feature flags
    "app.launchdarkly.com", "events.launchdarkly.com",
    "clientstream.launchdarkly.com",
    # WAF / bot protection (not booking APIs)
    "edge.sdk.awswaf.com",
    # Logging / monitoring / telemetry
    "rum.browser-intake-datadoghq.com", "logs.browser-intake-datadoghq.com",
    "browser-intake-datadoghq.eu",
    "dc.services.visualstudio.com", "dc.applicationinsights.azure.com",
    "js.monitor.azure.com",
    # Cookie consent
    "consent.cookiefirst.com", "edge.cookiefirst.com",
    "cookiefirst.com",
}

SKIP_PATH_PATTERNS = {
    "/marketing-insights/", "/v2/track",
    "/locales/", "/manifest.json",
}

SKIP_EXTENSIONS = {
    ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
    ".woff", ".woff2", ".ttf", ".eot", ".otf",
    ".mp4", ".webm", ".mp3", ".ogg",
    ".map", ".webp", ".avif",
}


# ─────────────────────────────────────────────────────────────
# Engine detection patterns
# ─────────────────────────────────────────────────────────────

ENGINE_PATTERNS = {
    "SiteMinder": ["direct-book.com"],
    "Cloudbeds": ["hotels.cloudbeds.com", "cloudbeds.com/reservation"],
    "Mews": ["api.mews.com", "app.mews.com", "mews.com/distributor"],
    "RMS Cloud": ["rmscloud.com"],
    "ResNexus": ["resnexus.com", "app.resnexus.com"],
    "innRoad": ["client.innroad.com"],
    "Sirvoy": ["sirvoy.com"],
    "Little Hotelier": ["littlehotelier.com"],
    "WebRezPro": ["webrezpro.com"],
    "Lodgify": ["lodgify.com"],
    "Hospitable": ["hospitable.com"],
    "Guesty": ["guesty.com"],
    "Hostaway": ["hostaway.com"],
    "IPMS247": ["ipms247.com"],
}


# ─────────────────────────────────────────────────────────────
# Core discoverer
# ─────────────────────────────────────────────────────────────

class ApiDiscoverer:
    """Discovers API endpoints used by a booking engine frontend."""

    def __init__(
        self,
        headless: bool = True,
        timeout: int = 30000,
        extra_wait: float = 5.0,
    ):
        self.headless = headless
        self.timeout = timeout
        self.extra_wait = extra_wait

    async def discover(
        self,
        url: str,
        interact: bool = False,
        checkin: Optional[str] = None,
        checkout: Optional[str] = None,
        adults: int = 2,
        har_path: Optional[str] = None,
    ) -> DiscoveryReport:
        """Run API discovery on a booking engine URL.

        Args:
            url: Booking engine URL to discover.
            interact: If True, attempt to fill dates and trigger search.
            checkin: Check-in date (YYYY-MM-DD). Defaults to 2 weeks out.
            checkout: Check-out date (YYYY-MM-DD). Defaults to checkin + 2.
            adults: Number of adults. Defaults to 2.
            har_path: If set, save a HAR file to this path.

        Returns:
            DiscoveryReport with all captured API calls.
        """
        from playwright.async_api import async_playwright

        try:
            from playwright_stealth import stealth_async
            has_stealth = True
        except ImportError:
            has_stealth = False

        from services.leadgen.detector import get_random_user_agent

        captured: list[CapturedApi] = []
        request_times: dict[str, float] = {}
        total_requests = 0
        t0 = time.monotonic()

        pw = None
        browser = None

        try:
            pw = await async_playwright().start()
            browser = await pw.chromium.launch(headless=self.headless)

            context_kwargs = {
                "user_agent": get_random_user_agent(),
                "viewport": {"width": 1280, "height": 800},
            }
            if har_path:
                context_kwargs["record_har_path"] = har_path
                context_kwargs["record_har_url_filter"] = "**/*"

            context = await browser.new_context(**context_kwargs)
            page = await context.new_page()

            if has_stealth:
                await stealth_async(page)

            # ── Request listener (timing + body capture) ─────────
            async def on_request(request):
                nonlocal total_requests
                total_requests += 1
                request_times[request.url + request.method] = time.monotonic()

            page.on("request", on_request)

            # ── Response listener (capture API calls) ────────────
            async def on_response(response):
                try:
                    await self._handle_response(response, captured, request_times)
                except Exception:
                    pass  # Don't crash on individual response failures

            page.on("response", on_response)

            # ── Navigate ─────────────────────────────────────────
            logger.info(f"Loading {url}")
            try:
                await page.goto(url, wait_until="load", timeout=self.timeout)
            except Exception as e:
                logger.warning(f"Page load issue (continuing anyway): {e}")

            # Wait for lazy-loaded API calls
            logger.info(f"Waiting {self.extra_wait}s for API calls...")
            await page.wait_for_timeout(int(self.extra_wait * 1000))

            # ── Optional: interact with the booking flow ─────────
            if interact:
                logger.info("Attempting to interact with booking flow...")
                new_apis = await self._interact_with_booking(
                    page, checkin, checkout, adults
                )
                if new_apis:
                    logger.info(f"Interaction triggered {new_apis} additional API calls")

            # ── Collect cookies ───────────────────────────────────
            cookies = await context.cookies()

            # ── Close page first to kill persistent connections ────
            await page.close()

            # ── Close context (saves HAR) ─────────────────────────
            await context.close()

            elapsed = time.monotonic() - t0

            # ── Detect engine ─────────────────────────────────────
            engine = self._detect_engine(url, captured)

            report = DiscoveryReport(
                url=url,
                engine=engine,
                apis=captured,
                cookies=[
                    {"name": c["name"], "value": c["value"],
                     "domain": c["domain"], "httpOnly": c.get("httpOnly", False),
                     "secure": c.get("secure", False)}
                    for c in cookies
                ],
                total_requests=total_requests,
                discovery_time_s=elapsed,
            )

            if har_path:
                logger.info(f"HAR saved to {har_path}")

            return report

        finally:
            if browser:
                await browser.close()
            if pw:
                await pw.stop()

    # ─────────────────────────────────────────────────────────
    # Response handler
    # ─────────────────────────────────────────────────────────

    async def _handle_response(
        self,
        response,
        captured: list[CapturedApi],
        request_times: dict[str, float],
    ):
        """Process a single response and add to captured list if it's an API call."""
        request = response.request
        url = response.url
        parsed = urlparse(url)

        # Skip tracking/analytics/CDN
        if self._should_skip(parsed):
            return

        # Skip page navigations (document loads)
        if request.resource_type in ("document", "stylesheet", "image", "font", "media"):
            return

        content_type = response.headers.get("content-type", "")

        # Skip HTML responses (not API calls)
        if "text/html" in content_type:
            return

        # Only capture JSON, GraphQL, and form-encoded API responses
        is_json = "application/json" in content_type or "graphql" in content_type
        is_api_url = any(p in parsed.path.lower() for p in [
            "/api/", "/graphql", "/v1/", "/v2/", "/v3/",
            "/booking/", "/reservation", "/availability",
            "/search", "/rooms", "/rates", "/property",
            "/configuration", "/distributor", "/onlineapi/",
        ])

        if not is_json and not is_api_url:
            return

        # Try to read response body
        body = None
        if is_json:
            try:
                body = await response.json()
            except Exception:
                try:
                    text = await response.text()
                    if text and text.strip().startswith(("{", "[")):
                        body = json.loads(text)
                except Exception:
                    pass

        # Skip if we got nothing useful
        if body is None and not is_api_url:
            return

        # Calculate timing
        req_key = request.url + request.method
        timing = None
        if req_key in request_times:
            timing = (time.monotonic() - request_times[req_key]) * 1000

        # Parse request body
        req_body = request.post_data
        req_body_parsed = None
        if req_body:
            try:
                req_body_parsed = json.loads(req_body)
            except (json.JSONDecodeError, TypeError):
                pass

        # Detect GraphQL
        is_graphql = "graphql" in url.lower()
        gql_operation = None
        gql_variables = None
        gql_hash = None

        if is_graphql or (req_body_parsed and isinstance(req_body_parsed, dict) and
                          ("query" in req_body_parsed or "operationName" in req_body_parsed)):
            is_graphql = True

        # Extract GraphQL details from URL params (GET-style persisted queries)
        if is_graphql:
            qs = parse_qs(parsed.query)
            gql_operation = (qs.get("operationName", [None])[0]
                             or (req_body_parsed or {}).get("operationName"))
            # Variables
            vars_raw = qs.get("variables", [None])[0]
            if vars_raw:
                try:
                    gql_variables = json.loads(unquote(vars_raw))
                except (json.JSONDecodeError, TypeError):
                    pass
            elif req_body_parsed and "variables" in req_body_parsed:
                gql_variables = req_body_parsed["variables"]
            # Persisted query hash
            ext_raw = qs.get("extensions", [None])[0]
            if ext_raw:
                try:
                    ext = json.loads(unquote(ext_raw))
                    gql_hash = ext.get("persistedQuery", {}).get("sha256Hash")
                except (json.JSONDecodeError, TypeError):
                    pass
            elif req_body_parsed and "extensions" in req_body_parsed:
                gql_hash = (req_body_parsed.get("extensions", {})
                            .get("persistedQuery", {}).get("sha256Hash"))

        # Extract relevant headers
        req_headers = {}
        for h in ["authorization", "x-api-key", "x-sm-api-key", "x-csrf-token",
                   "x-requested-with", "content-type", "accept"]:
            val = request.headers.get(h)
            if val:
                req_headers[h] = val

        # Build schema from response body
        schema = self._extract_schema(body) if body is not None else None

        captured.append(CapturedApi(
            url=url,
            path=parsed.path,
            method=request.method,
            status=response.status,
            request_headers=req_headers,
            request_body=req_body,
            request_body_parsed=req_body_parsed,
            response_content_type=content_type,
            response_body=body,
            response_schema=schema,
            timing_ms=timing,
            category="graphql" if is_graphql else "api",
            graphql_operation=gql_operation,
            graphql_variables=gql_variables,
            graphql_persisted_hash=gql_hash,
        ))

    # ─────────────────────────────────────────────────────────
    # Filtering
    # ─────────────────────────────────────────────────────────

    def _should_skip(self, parsed) -> bool:
        """Check if a URL should be skipped (tracking, static assets, etc.)."""
        # Skip known tracking domains
        domain = parsed.hostname or ""
        if any(skip in domain for skip in SKIP_DOMAINS):
            return True

        # Skip static asset extensions
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in SKIP_EXTENSIONS):
            return True

        # Skip known non-API paths
        if any(p in path_lower for p in SKIP_PATH_PATTERNS):
            return True

        # Skip data URIs
        if parsed.scheme == "data":
            return True

        return False

    # ─────────────────────────────────────────────────────────
    # Schema extraction
    # ─────────────────────────────────────────────────────────

    def _extract_schema(self, obj: Any, depth: int = 0, max_depth: int = 4) -> Any:
        """Generate a simplified type schema from a JSON value.

        Replaces actual values with type names to show structure without data.
        """
        if depth > max_depth:
            return "..."

        if obj is None:
            return "null"
        elif isinstance(obj, bool):
            return "bool"
        elif isinstance(obj, int):
            return "int"
        elif isinstance(obj, float):
            return "float"
        elif isinstance(obj, str):
            if len(obj) > 100:
                return "str (long)"
            return "str"
        elif isinstance(obj, list):
            if not obj:
                return "[]"
            # Show schema of first element only
            return [self._extract_schema(obj[0], depth + 1, max_depth)]
        elif isinstance(obj, dict):
            result = {}
            for key, val in list(obj.items())[:30]:  # Cap at 30 keys
                result[key] = self._extract_schema(val, depth + 1, max_depth)
            if len(obj) > 30:
                result["..."] = f"({len(obj)} keys total)"
            return result
        else:
            return type(obj).__name__

    # ─────────────────────────────────────────────────────────
    # Engine detection
    # ─────────────────────────────────────────────────────────

    def _detect_engine(self, url: str, captured: list[CapturedApi]) -> Optional[str]:
        """Guess the booking engine from URL patterns."""
        all_urls = [url] + [a.url for a in captured]
        combined = " ".join(all_urls).lower()

        for engine, patterns in ENGINE_PATTERNS.items():
            if any(p in combined for p in patterns):
                return engine

        return None

    # ─────────────────────────────────────────────────────────
    # Booking flow interaction
    # ─────────────────────────────────────────────────────────

    async def _interact_with_booking(
        self,
        page,
        checkin: Optional[str],
        checkout: Optional[str],
        adults: int,
    ) -> int:
        """Attempt to fill dates and trigger a search.

        Returns the number of new API calls triggered.
        """
        from playwright.async_api import TimeoutError as PWTimeoutError

        if not checkin:
            d = date.today() + timedelta(days=14)
            checkin = d.isoformat()
        if not checkout:
            ci = date.fromisoformat(checkin)
            checkout = (ci + timedelta(days=2)).isoformat()

        apis_before = len([1 for _ in page.context.pages])  # rough proxy
        triggered = 0

        # ── Try to fill date inputs ──────────────────────────
        date_selectors = [
            # Standard date inputs
            'input[type="date"]',
            # Name-based
            'input[name*="checkin" i]', 'input[name*="check_in" i]',
            'input[name*="check-in" i]', 'input[name*="arrival" i]',
            'input[name*="start" i]', 'input[name*="from" i]',
            'input[name*="checkout" i]', 'input[name*="check_out" i]',
            'input[name*="check-out" i]', 'input[name*="departure" i]',
            'input[name*="end" i]', 'input[name*="to" i]',
            # Placeholder-based
            'input[placeholder*="Check-in" i]', 'input[placeholder*="Check in" i]',
            'input[placeholder*="Arrival" i]',
            'input[placeholder*="Check-out" i]', 'input[placeholder*="Check out" i]',
            'input[placeholder*="Departure" i]',
            # ID-based
            'input[id*="checkin" i]', 'input[id*="checkout" i]',
            'input[id*="arrival" i]', 'input[id*="departure" i]',
            '#checkin', '#checkout', '#check-in', '#check-out',
            '#startDate', '#endDate',
        ]

        filled_dates = False
        for selector in date_selectors:
            try:
                elements = await page.query_selector_all(selector)
                if elements:
                    el = elements[0]
                    await el.fill(checkin)
                    logger.debug(f"Filled checkin with selector: {selector}")
                    if len(elements) > 1:
                        await elements[1].fill(checkout)
                        logger.debug(f"Filled checkout with selector: {selector}")
                    filled_dates = True
                    break
            except Exception:
                continue

        if not filled_dates:
            logger.debug("Could not find date inputs to fill")

        # ── Try to click search/availability button ──────────
        search_selectors = [
            'button:has-text("Search")',
            'button:has-text("Check Availability")',
            'button:has-text("Check availability")',
            'button:has-text("Find Rooms")',
            'button:has-text("Find rooms")',
            'button:has-text("Search Availability")',
            'button:has-text("Book Now")',
            'button:has-text("Book now")',
            'input[type="submit"]',
            'button[type="submit"]',
            'a:has-text("Search")',
            'a:has-text("Check Availability")',
            '.search-button', '.btn-search', '.book-now',
            '#searchButton', '#btnSearch', '#book-now',
        ]

        clicked = False
        for selector in search_selectors:
            try:
                btn = await page.query_selector(selector)
                if btn and await btn.is_visible():
                    await btn.click()
                    logger.info(f"Clicked search button: {selector}")
                    clicked = True
                    break
            except Exception:
                continue

        if clicked:
            # Wait for new API calls to fire
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except PWTimeoutError:
                pass
            await page.wait_for_timeout(3000)
            triggered += 1
            logger.info("Waited for search results")

        # ── Try to click first room "Select" / "Book" button ─
        room_selectors = [
            'button:has-text("Select")',
            'button:has-text("Book")',
            'button:has-text("Reserve")',
            'button:has-text("Add to Cart")',
            'button:has-text("Choose")',
            'a:has-text("Select")',
            'a:has-text("Book")',
            'a:has-text("Reserve")',
            '.room-select', '.btn-book', '.select-room',
        ]

        for selector in room_selectors:
            try:
                btn = await page.query_selector(selector)
                if btn and await btn.is_visible():
                    await btn.click()
                    logger.info(f"Clicked room button: {selector}")
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10000)
                    except PWTimeoutError:
                        pass
                    await page.wait_for_timeout(3000)
                    triggered += 1
                    break
            except Exception:
                continue

        return triggered
