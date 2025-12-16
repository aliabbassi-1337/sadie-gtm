#!/usr/bin/env python3
"""
Sadie Lead Generation Script
=============================
Unified pipeline that:
1. Scrapes hotels from Google Places API (by geographic area)
2. Visits each hotel website to detect booking engine
3. Extracts phone numbers and emails
4. Takes screenshots of booking pages
5. Filters out bad leads (apartments, OTAs, chains without direct booking, etc.)

Usage:
    python3 sadie_lead_gen.py --center-lat 25.7617 --center-lng -80.1918 --overall-radius-km 35

Requires:
    - GOOGLE_PLACES_API_KEY in .env file
    - playwright install chromium
"""

import csv
import os
import re
import math
import time
import argparse
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

# Default: Miami
DEFAULT_CENTER_LAT = 25.7617
DEFAULT_CENTER_LNG = -80.1918
DEFAULT_OVERALL_RADIUS_KM = 35.0
DEFAULT_GRID_ROWS = 5
DEFAULT_GRID_COLS = 5
DEFAULT_MAX_PAGES_PER_CENTER = 3

OUTPUT_CSV = "sadie_leads.csv"
SCREENSHOTS_DIR = "screenshots"
LOG_FILE = "sadie_lead_gen.log"

# Timeouts (milliseconds)
TIMEOUT_PAGE_LOAD = 20000  # 20s - initial page load
TIMEOUT_BOOKING_CLICK = 10000  # 10s - after clicking booking button
TIMEOUT_POPUP_DETECT = 3000  # 3s - detecting if click opens new tab

# Booking engine patterns - domains that indicate which booking engine is used
ENGINE_PATTERNS = {
    "Cloudbeds": ["cloudbeds.com"],
    "Mews": ["mews.com", "mews.li"],
    "SynXis / TravelClick": ["synxis.com", "travelclick.com"],
    "BookingSuite / Booking.com": ["bookingsuite.com"],
    "Little Hotelier": ["littlehotelier.com"],
    "WebRezPro": ["webrezpro.com"],
    "InnRoad": ["innroad.com"],
    "ResNexus": ["resnexus.com"],
    "Newbook": ["newbook.cloud", "newbooksoftware.com"],
    "RMS Cloud": ["rmscloud.com"],
    "RoomRaccoon": ["roomraccoon.com"],
    "SiteMinder / TheBookingButton": [
        "thebookingbutton.com",
        "siteminder.com",
        "direct-book",
    ],
    "Sabre / CRS": ["sabre.com", "crs.sabre.com"],
    "eZee": ["ezeeabsolute.com", "ezeereservation.com", "ezeetechnosys.com"],
}

# OTA domains we want to skip (these are aggregators, not direct hotel sites)
OTA_DOMAINS_BLACKLIST = [
    "booking.com",
    "expedia.com",
    "hotels.com",
    "airbnb.com",
    "tripadvisor.com",
    "priceline.com",
    "agoda.com",
    "orbitz.com",
    "kayak.com",
    "travelocity.com",
    "hostelworld.com",
    "vrbo.com",
    "ebookers.com",
    "lastminute.com",
    "trivago.com",
    "hotwire.com",
    "travelzoo.com",
]

# Keywords that suggest this isn't a real hotel (bad leads)
BAD_LEAD_KEYWORDS = [
    # Apartments / Condos / Rentals
    "apartment",
    "apartments",
    "condo",
    "condos",
    "condominium",
    "condominiums",
    "vacation rental",
    "vacation rentals",
    "holiday rental",
    "holiday home",
    "townhouse",
    "townhome",
    "villa rental",
    "private home",
    # Hostels (usually not Sadie's target)
    "hostel",
    "hostels",
    "backpacker",
    # Timeshares
    "timeshare",
    "time share",
    "fractional ownership",
    # Extended stay / corporate housing (different market)
    "extended stay",
    "corporate housing",
    "furnished apartment",
    # RV / Camping
    "rv park",
    "rv resort",
    "campground",
    "camping",
    "glamping",
    # Spa / Wellness only
    "day spa",
    "wellness center",
    # Event venues (not accommodation)
    "event venue",
    "wedding venue",
    "banquet hall",
    "conference center",
]

# Search modes for Google Places API
SEARCH_MODES = [
    {"label": "lodging_type", "type": "lodging", "keyword": None},
    {"label": "hotel_keyword", "type": None, "keyword": "hotel"},
    {"label": "motel_keyword", "type": None, "keyword": "motel"},
    {"label": "resort_keyword", "type": None, "keyword": "resort"},
    {"label": "inn_keyword", "type": None, "keyword": "inn"},
    {"label": "boutique_hotel", "type": None, "keyword": "boutique hotel"},
]

# Keywords to find booking buttons
BOOKING_KEYWORDS = [
    "book now",
    "book",
    "reserve",
    "reserve now",
    "reservation",
    "reservations",
    "check availability",
    "check rates",
    "check rate",
    "availability",
    "online booking",
    "book online",
    "book a room",
    "book your stay",
]


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

# Global log file handle
_log_file = None


def init_log_file():
    """Initialize log file (overwrites existing)."""
    global _log_file
    _log_file = open(LOG_FILE, "w", encoding="utf-8")


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if _log_file:
        _log_file.write(line + "\n")
        _log_file.flush()


# ------------------------------------------------------------
# URL / Domain Helpers
# ------------------------------------------------------------


def extract_domain(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    return url


def is_ota_domain(website: str) -> bool:
    if not website:
        return False
    domain = extract_domain(website)
    for ota in OTA_DOMAINS_BLACKLIST:
        if domain.endswith(ota):
            return True
    return False


def is_bad_lead(name: str, website: str = "") -> bool:
    """Check if this looks like a bad lead based on name/website keywords."""
    text = f"{name} {website}".lower()
    for kw in BAD_LEAD_KEYWORDS:
        if kw in text:
            return True
    return False


# ------------------------------------------------------------
# Geo Helpers
# ------------------------------------------------------------


def deg_per_km_lat():
    return 1.0 / 111.0


def deg_per_km_lng(lat_deg: float) -> float:
    return 1.0 / (111.0 * math.cos(math.radians(lat_deg)))


def build_grid_centers(center_lat, center_lng, overall_radius_km, rows, cols):
    lat_span_deg = overall_radius_km * deg_per_km_lat()
    lng_span_deg = overall_radius_km * deg_per_km_lng(center_lat)

    min_lat = center_lat - lat_span_deg
    max_lat = center_lat + lat_span_deg
    min_lng = center_lng - lng_span_deg
    max_lng = center_lng + lng_span_deg

    centers = []
    for i in range(rows):
        row_frac = 0.0 if rows == 1 else i / (rows - 1)
        lat = min_lat + (max_lat - min_lat) * row_frac
        for j in range(cols):
            col_frac = 0.0 if cols == 1 else j / (cols - 1)
            lng = min_lng + (max_lng - min_lng) * col_frac
            centers.append((lat, lng))
    return centers


# ------------------------------------------------------------
# Google Places API
# ------------------------------------------------------------


def places_nearby(
    api_key, lat, lng, radius_m, place_type=None, keyword=None, page_token=None
):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "key": api_key,
        "location": f"{lat},{lng}",
        "radius": radius_m,
    }
    if place_type:
        params["type"] = place_type
    if keyword:
        params["keyword"] = keyword
    if page_token:
        params["pagetoken"] = page_token

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def place_details(api_key, place_id):
    """Fetch detailed info including phone number."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": api_key,
        "place_id": place_id,
        "fields": "name,geometry,website,business_status,formatted_phone_number,international_phone_number,rating,user_ratings_total,formatted_address,types",
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


# ------------------------------------------------------------
# Booking Engine Detection
# ------------------------------------------------------------


def detect_engine_from_domain(domain: str):
    """Check if domain matches any known booking engine."""
    if not domain:
        return ("", "")
    for engine_name, patterns in ENGINE_PATTERNS.items():
        for pat in patterns:
            if pat in domain:
                return (engine_name, pat)
    return ("", "")


def detect_engine_from_url(url: str, hotel_domain: str):
    """Try to detect engine from a full URL."""
    if not url:
        return ("unknown", "", "no_url")

    # Check URL path for patterns like "direct-book"
    url_lower = url.lower()
    for engine_name, patterns in ENGINE_PATTERNS.items():
        for pat in patterns:
            if pat in url_lower:
                return (engine_name, pat, "url_pattern_match")

    domain = extract_domain(url)
    if not domain:
        return ("unknown", "", "no_domain")

    engine_name, pat = detect_engine_from_domain(domain)
    if engine_name:
        return (engine_name, domain, "url_domain_match")

    if hotel_domain and domain != hotel_domain:
        return ("unknown_third_party", domain, "third_party_domain")

    return ("proprietary_or_same_domain", domain, "same_domain")


def detect_engine_from_html(html: str):
    """Keyword-based detection from HTML content."""
    if not html:
        return ("", "")

    low = html.lower()

    checks = [
        ("cloudbeds", "Cloudbeds"),
        ("mews.com", "Mews"),
        ("mews.li", "Mews"),
        ("synxis", "SynXis / TravelClick"),
        ("travelclick", "SynXis / TravelClick"),
        ("littlehotelier", "Little Hotelier"),
        ("little hotelier", "Little Hotelier"),
        ("webrezpro", "WebRezPro"),
        ("innroad", "InnRoad"),
        ("resnexus", "ResNexus"),
        ("newbook", "Newbook"),
        ("roomraccoon", "RoomRaccoon"),
        ("siteminder", "SiteMinder / TheBookingButton"),
        ("thebookingbutton", "SiteMinder / TheBookingButton"),
        ("direct-book", "SiteMinder / TheBookingButton"),
        ("ezee", "eZee"),
        ("rmscloud", "RMS Cloud"),
        ("rms cloud", "RMS Cloud"),
    ]

    for keyword, engine_name in checks:
        if keyword in low:
            return (engine_name, "html_keyword")

    return ("", "")


# ------------------------------------------------------------
# Contact Extraction (Phone & Email)
# ------------------------------------------------------------

PHONE_PATTERNS = [
    r"\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",  # US format
    r"\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}",  # International
]

EMAIL_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"


def extract_phones_from_html(html: str) -> list:
    """Extract phone numbers from HTML."""
    phones = []
    for pattern in PHONE_PATTERNS:
        matches = re.findall(pattern, html)
        phones.extend(matches)
    # Clean and dedupe
    cleaned = []
    seen = set()
    for p in phones:
        p = re.sub(r"[^\d+]", "", p)
        if len(p) >= 10 and p not in seen:
            seen.add(p)
            cleaned.append(p)
    return cleaned[:3]  # Return top 3


def extract_emails_from_html(html: str) -> list:
    """Extract email addresses from HTML."""
    matches = re.findall(EMAIL_PATTERN, html)
    # Filter out common non-contact emails
    filtered = []
    skip_patterns = [
        "example.com",
        "domain.com",
        "email.com",
        "sentry.io",
        "wixpress.com",
        "schema.org",
        ".png",
        ".jpg",
        ".gif",
    ]
    for email in matches:
        email_lower = email.lower()
        if not any(skip in email_lower for skip in skip_patterns):
            if email_lower not in [e.lower() for e in filtered]:
                filtered.append(email)
    return filtered[:3]


# ------------------------------------------------------------
# Playwright Browser Automation
# ------------------------------------------------------------


async def find_booking_button_candidates(page, max_candidates: int = 5):
    """Find booking CTA buttons/links on the page."""
    candidates = []
    loc = page.locator("a, button")
    count = await loc.count()

    for i in range(count):
        el = loc.nth(i)
        try:
            text = (await el.inner_text() or "").strip()
        except Exception:
            continue
        if not text:
            continue
        lower = text.lower()
        for kw in BOOKING_KEYWORDS:
            if kw in lower:
                candidates.append(el)
                break
        if len(candidates) >= max_candidates:
            break
    return candidates


async def click_and_get_booking_page(
    context, page, timeout_ms: int = TIMEOUT_BOOKING_CLICK
):
    """Click booking button and return the booking page/URL."""
    candidates = await find_booking_button_candidates(page)
    if not candidates:
        return (None, None, "no_booking_button_found")

    original_url = page.url
    last_booking_url = None
    last_booking_page = None

    for el in candidates:
        # Try popup / new tab
        try:
            async with context.expect_page(timeout=TIMEOUT_POPUP_DETECT) as p_info:
                await el.click()
            new_page = await p_info.value
            try:
                await new_page.wait_for_load_state(
                    "domcontentloaded", timeout=timeout_ms
                )
            except PWTimeoutError:
                pass
            return (new_page, new_page.url, "popup_page")
        except PWTimeoutError:
            # Maybe same tab navigation
            try:
                await el.click()
            except Exception:
                continue
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            except PWTimeoutError:
                pass

            if page.url != original_url:
                last_booking_page = page
                last_booking_url = page.url

    if last_booking_url:
        return (last_booking_page, last_booking_url, "same_page_navigation")

    return (None, None, "no_booking_button_effective")


async def detect_engine_from_frames(page):
    """Check iframes for booking engine signatures. Returns (engine, domain, method, frame_url)."""
    for frame in page.frames:
        try:
            frame_url = frame.url
        except Exception:
            continue

        # Skip about:blank and same-origin frames
        if not frame_url or frame_url.startswith("about:"):
            continue

        # Check URL for engine patterns
        for engine_name, patterns in ENGINE_PATTERNS.items():
            for pat in patterns:
                if pat in frame_url.lower():
                    return engine_name, pat, "frame_url_match", frame_url

        # Check frame HTML
        try:
            html = await frame.content()
        except Exception:
            html = ""
        engine, method = detect_engine_from_html(html)
        if engine:
            return engine, "", f"frame_{method}", frame_url

    return "", "", "", ""


def sniff_network_for_engine(network_urls: dict, hotel_domain: str):
    """Check captured network requests for booking engine domains. Returns (engine, domain, method, full_url)."""
    for host, full_url in network_urls.items():
        engine_name, pat = detect_engine_from_domain(host)
        if engine_name:
            return (engine_name, host, "network_sniff", full_url)
    return ("", "", "", "")


# ------------------------------------------------------------
# Main Hotel Processing
# ------------------------------------------------------------


async def process_hotel(
    idx, total, hotel, browser, semaphore, screenshots_dir, pause_sec
):
    """Process a single hotel: visit site, detect engine, extract contacts, screenshot."""

    name = hotel.get("name", "")
    website = normalize_url(hotel.get("website", ""))
    phone_from_google = hotel.get("phone", "")

    log(f"[{idx}/{total}] {name} | {website}")

    result = {
        "name": name,
        "website": website,
        "address": hotel.get("address", ""),
        "latitude": hotel.get("lat", ""),
        "longitude": hotel.get("lng", ""),
        "phone_google": phone_from_google,
        "phone_website": "",
        "email": "",
        "rating": hotel.get("rating", ""),
        "review_count": hotel.get("review_count", ""),
        "booking_url": "",
        "booking_engine": "",
        "booking_engine_domain": "",
        "detection_method": "",
        "screenshot_path": "",
        "place_id": hotel.get("place_id", ""),
        "error": "",
    }

    if not website:
        result["error"] = "no_website"
        return result

    async with semaphore:
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Capture network requests - store full URLs for engine domains
        network_urls = {}  # host -> full_url (keeps first URL seen for each host)

        def handle_request(request):
            try:
                url = request.url
                host = extract_domain(url)
                if host and host not in network_urls:
                    network_urls[host] = url
            except Exception:
                pass

        page.on("request", handle_request)

        try:
            await page.goto(
                website, timeout=TIMEOUT_PAGE_LOAD, wait_until="domcontentloaded"
            )
            # Give page a moment to render JS
            await asyncio.sleep(2)
            hotel_domain = extract_domain(page.url)
            log(f"  Loaded: {hotel_domain}")

            # Extract contacts from main page
            try:
                main_html = await page.content()
                phones = extract_phones_from_html(main_html)
                emails = extract_emails_from_html(main_html)
                if phones:
                    result["phone_website"] = phones[0]
                if emails:
                    result["email"] = emails[0]
            except Exception:
                pass

            # Try to find and click booking button
            booking_page, booking_url, method = await click_and_get_booking_page(
                context, page
            )
            result["booking_url"] = booking_url or ""
            result["detection_method"] = method

            engine_name = ""
            engine_domain = ""
            detection_method = method

            # Detect engine from booking URL
            if booking_url:
                log(f"  Booking URL: {booking_url}")
                engine_name, engine_domain, url_method = detect_engine_from_url(
                    booking_url, hotel_domain
                )
                detection_method = f"{method}+{url_method}"

            # If unclear, check booking page HTML
            if booking_page and engine_name in (
                "",
                "unknown",
                "unknown_third_party",
                "proprietary_or_same_domain",
            ):
                try:
                    html = await booking_page.content()
                except Exception:
                    html = ""
                html_engine, html_method = detect_engine_from_html(html)
                if html_engine:
                    engine_name = html_engine
                    detection_method = f"{detection_method}+{html_method}"

            # Check network requests
            if engine_name in (
                "",
                "unknown",
                "unknown_third_party",
                "proprietary_or_same_domain",
            ):
                net_engine, net_domain, net_method, net_url = sniff_network_for_engine(
                    network_urls, hotel_domain
                )
                if net_engine:
                    engine_name = net_engine
                    if not engine_domain:
                        engine_domain = net_domain
                    detection_method = f"{detection_method}+{net_method}"
                    # Use network URL as booking URL if we don't have one
                    if not result["booking_url"] and net_url:
                        result["booking_url"] = net_url

            # Check frames/iframes
            if engine_name in (
                "",
                "unknown",
                "unknown_third_party",
                "proprietary_or_same_domain",
            ):
                (
                    frame_engine,
                    frame_domain,
                    frame_method,
                    frame_url,
                ) = await detect_engine_from_frames(booking_page or page)
                if frame_engine:
                    engine_name = frame_engine
                    if not engine_domain:
                        engine_domain = frame_domain
                    detection_method = f"{detection_method}+{frame_method}"
                    # Capture frame URL as booking URL if we don't have one
                    if not result["booking_url"] and frame_url:
                        result["booking_url"] = frame_url

            # Take screenshot of booking page
            if booking_page or booking_url:
                try:
                    screenshot_page = booking_page or page
                    safe_name = re.sub(r"[^\w\-]", "_", name)[:50]
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    screenshot_filename = f"{safe_name}_{timestamp}.png"
                    screenshot_path = os.path.join(screenshots_dir, screenshot_filename)
                    await screenshot_page.screenshot(
                        path=screenshot_path, full_page=False
                    )
                    result["screenshot_path"] = screenshot_filename
                    log(f"  Screenshot: {screenshot_filename}")
                except Exception as e:
                    log(f"  Screenshot failed: {e}")

            result["booking_engine"] = engine_name or "unknown"
            result["booking_engine_domain"] = engine_domain
            result["detection_method"] = detection_method

            log(
                f"  Engine: {result['booking_engine']} ({result['booking_engine_domain'] or 'n/a'})"
            )

        except PWTimeoutError:
            result["error"] = "timeout"
            log("  ERROR: timeout")
        except Exception as e:
            result["error"] = f"exception: {str(e)[:100]}"
            log(f"  ERROR: {e}")

        await context.close()

        if pause_sec > 0:
            await asyncio.sleep(pause_sec)

    return result


# ------------------------------------------------------------
# Main Pipeline
# ------------------------------------------------------------


async def run_pipeline(
    api_key: str,
    center_lat: float,
    center_lng: float,
    overall_radius_km: float,
    grid_rows: int,
    grid_cols: int,
    max_pages_per_center: int,
    max_results: int,
    output_csv: str,
    screenshots_dir: str,
    concurrency: int,
    headless: bool,
    pause_sec: float,
    skip_scrape: bool,
    input_csv: str,
):
    """Main pipeline: scrape hotels -> detect engines -> save results."""

    # Ensure screenshots directory exists
    Path(screenshots_dir).mkdir(parents=True, exist_ok=True)

    hotels_to_process = []

    # --------------------------------------------------------
    # PHASE 1: Scrape hotels from Google Places (or load from CSV)
    # --------------------------------------------------------

    if skip_scrape and input_csv and os.path.exists(input_csv):
        log(f"Loading hotels from {input_csv} (skipping Google Places scrape)")
        with open(input_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                hotels_to_process.append(
                    {
                        "name": row.get("name", ""),
                        "lat": row.get("latitude", row.get("lat", "")),
                        "lng": row.get("longitude", row.get("lng", "")),
                        "website": row.get("website", ""),
                        "phone": row.get(
                            "phone", row.get("formatted_phone_number", "")
                        ),
                        "address": row.get("address", row.get("formatted_address", "")),
                        "rating": row.get("rating", ""),
                        "review_count": row.get(
                            "review_count", row.get("user_ratings_total", "")
                        ),
                        "place_id": row.get("place_id", ""),
                    }
                )
    else:
        log("PHASE 1: Scraping hotels from Google Places API")

        # Calculate search radius
        spacing_km = (overall_radius_km * 2) / max(grid_rows - 1, grid_cols - 1, 1)
        search_radius_km = max(3.0, spacing_km * 0.75)
        radius_m = int(search_radius_km * 1000)

        centers = build_grid_centers(
            center_lat, center_lng, overall_radius_km, grid_rows, grid_cols
        )

        log(
            f"Center: {center_lat:.4f}, {center_lng:.4f} | Radius: {overall_radius_km}km"
        )
        log(
            f"Grid: {grid_rows}x{grid_cols} ({len(centers)} centers) | Search radius: {search_radius_km:.1f}km"
        )

        seen_place_ids = set()
        stats = {
            "candidates": 0,
            "kept": 0,
            "no_website": 0,
            "ota": 0,
            "bad_lead": 0,
            "duplicates": 0,
        }

        for center_idx, (lat, lng) in enumerate(centers, 1):
            log(f"Center {center_idx}/{len(centers)}: {lat:.4f}, {lng:.4f}")

            for mode in SEARCH_MODES:
                page_token = None
                page_count = 0

                while page_count < max_pages_per_center:
                    page_count += 1
                    try:
                        nearby = places_nearby(
                            api_key,
                            lat,
                            lng,
                            radius_m,
                            place_type=mode["type"],
                            keyword=mode["keyword"],
                            page_token=page_token,
                        )
                    except Exception as e:
                        log(f"  API error: {e}")
                        break

                    status = nearby.get("status")
                    if status not in ("OK", "ZERO_RESULTS"):
                        if status == "OVER_QUERY_LIMIT":
                            log("  OVER_QUERY_LIMIT - stopping")
                            break
                        break

                    results = nearby.get("results", [])
                    if not results:
                        break

                    for r in results:
                        place_id = r.get("place_id")
                        name = r.get("name", "").strip()

                        if not place_id or not name:
                            continue

                        if place_id in seen_place_ids:
                            stats["duplicates"] += 1
                            continue
                        seen_place_ids.add(place_id)

                        # Fetch details
                        try:
                            details = place_details(api_key, place_id)
                        except Exception as e:
                            continue

                        if details.get("status") != "OK":
                            continue

                        result = details.get("result", {})
                        website = (result.get("website") or "").strip()

                        stats["candidates"] += 1

                        # Filter: must have website
                        if not website:
                            stats["no_website"] += 1
                            continue

                        # Filter: skip OTAs
                        if is_ota_domain(website):
                            stats["ota"] += 1
                            continue

                        # Filter: skip bad leads
                        if is_bad_lead(name, website):
                            stats["bad_lead"] += 1
                            continue

                        # Keep this hotel
                        geometry = result.get("geometry", {}).get("location", {})
                        hotels_to_process.append(
                            {
                                "name": name,
                                "lat": geometry.get("lat"),
                                "lng": geometry.get("lng"),
                                "website": website,
                                "phone": result.get("international_phone_number")
                                or result.get("formatted_phone_number", ""),
                                "address": result.get("formatted_address", ""),
                                "rating": result.get("rating", ""),
                                "review_count": result.get("user_ratings_total", ""),
                                "place_id": place_id,
                            }
                        )
                        stats["kept"] += 1
                        log(f"  + {name}")

                        if max_results > 0 and stats["kept"] >= max_results:
                            break

                    if max_results > 0 and stats["kept"] >= max_results:
                        break

                    page_token = nearby.get("next_page_token")
                    if not page_token:
                        break
                    time.sleep(2.0)

                if max_results > 0 and stats["kept"] >= max_results:
                    break

            if max_results > 0 and stats["kept"] >= max_results:
                break

        log(f"\nPhase 1 complete: {stats['kept']} hotels found")
        log(
            f"  Skipped: {stats['no_website']} no website, {stats['ota']} OTA, {stats['bad_lead']} bad leads, {stats['duplicates']} duplicates"
        )

    if not hotels_to_process:
        log("No hotels to process. Exiting.")
        return

    # --------------------------------------------------------
    # PHASE 2: Visit sites, detect engines, extract contacts
    # --------------------------------------------------------

    log(f"\nPHASE 2: Processing {len(hotels_to_process)} hotels with Playwright")

    # Check for existing output to enable resume
    processed_keys = set()
    append_mode = False
    if os.path.exists(output_csv):
        with open(output_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get("name", ""), normalize_url(row.get("website", "")))
                processed_keys.add(key)
        if processed_keys:
            append_mode = True
            log(f"Found {len(processed_keys)} already processed, will skip them")

    # Filter to only unprocessed hotels
    hotels_remaining = []
    for h in hotels_to_process:
        key = (h["name"], normalize_url(h["website"]))
        if key not in processed_keys:
            hotels_remaining.append(h)

    if not hotels_remaining:
        log("All hotels already processed. Nothing to do.")
        return

    log(f"{len(hotels_remaining)} hotels remaining to process")

    fieldnames = [
        "name",
        "website",
        "booking_url",
        "booking_engine",
        "booking_engine_domain",
        "detection_method",
        "error",
        "phone_google",
        "phone_website",
        "email",
        "address",
        "latitude",
        "longitude",
        "rating",
        "review_count",
        "screenshot_path",
        "place_id",
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        semaphore = asyncio.Semaphore(concurrency)

        tasks = [
            process_hotel(
                idx,
                len(hotels_remaining),
                hotel,
                browser,
                semaphore,
                screenshots_dir,
                pause_sec,
            )
            for idx, hotel in enumerate(hotels_remaining, 1)
        ]

        mode = "a" if append_mode else "w"
        with open(output_csv, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not append_mode:
                writer.writeheader()

            stats = {"processed": 0, "known_engine": 0, "errors": 0}
            flush_every = 10
            since_flush = 0

            for coro in asyncio.as_completed(tasks):
                result = await coro
                stats["processed"] += 1

                if result.get("error"):
                    stats["errors"] += 1

                engine = result.get("booking_engine", "")
                if engine and engine not in (
                    "unknown",
                    "unknown_third_party",
                    "proprietary_or_same_domain",
                ):
                    stats["known_engine"] += 1

                writer.writerow(result)
                since_flush += 1

                if since_flush >= flush_every:
                    f.flush()
                    since_flush = 0
                    log(
                        f"[Progress] {stats['processed']}/{len(hotels_remaining)} processed, {stats['known_engine']} known engines, {stats['errors']} errors"
                    )

            f.flush()

        await browser.close()

    log(f"\n{'=' * 60}")
    log(f"COMPLETE!")
    log(f"Processed: {stats['processed']} hotels")
    log(f"Known booking engines: {stats['known_engine']}")
    log(f"Errors: {stats['errors']}")
    log(f"Output: {output_csv}")
    log(f"Screenshots: {screenshots_dir}/")
    log(f"{'=' * 60}")


def main():
    load_dotenv()
    init_log_file()

    parser = argparse.ArgumentParser(
        description="Sadie Lead Generation - Find hotels with booking engines"
    )

    # Location args
    parser.add_argument(
        "--center-lat",
        type=float,
        default=DEFAULT_CENTER_LAT,
        help="Center latitude (default: Miami)",
    )
    parser.add_argument(
        "--center-lng",
        type=float,
        default=DEFAULT_CENTER_LNG,
        help="Center longitude (default: Miami)",
    )
    parser.add_argument(
        "--overall-radius-km",
        type=float,
        default=DEFAULT_OVERALL_RADIUS_KM,
        help="Overall radius in km (default: 35)",
    )
    parser.add_argument(
        "--grid-rows",
        type=int,
        default=DEFAULT_GRID_ROWS,
        help="Grid rows (default: 5)",
    )
    parser.add_argument(
        "--grid-cols",
        type=int,
        default=DEFAULT_GRID_COLS,
        help="Grid cols (default: 5)",
    )
    parser.add_argument(
        "--max-pages-per-center",
        type=int,
        default=DEFAULT_MAX_PAGES_PER_CENTER,
        help="Max API pages per center per mode (default: 3)",
    )

    # Limits
    parser.add_argument(
        "--max-results", type=int, default=0, help="Max hotels to scrape (0 = no limit)"
    )

    # Output
    parser.add_argument("--output", default=OUTPUT_CSV, help="Output CSV file")
    parser.add_argument(
        "--screenshots-dir", default=SCREENSHOTS_DIR, help="Screenshots directory"
    )

    # Processing options
    parser.add_argument(
        "--concurrency", type=int, default=5, help="Parallel browser tabs (default: 5)"
    )
    parser.add_argument("--headed", action="store_true", help="Show browser UI")
    parser.add_argument(
        "--pause", type=float, default=0.5, help="Pause between hotels (default: 0.5s)"
    )

    # Skip scrape option (use existing CSV)
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip Google Places scrape, use --input CSV instead",
    )
    parser.add_argument(
        "--input", default="", help="Input CSV file (used with --skip-scrape)"
    )

    args = parser.parse_args()

    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key and not args.skip_scrape:
        raise SystemExit(
            "Missing GOOGLE_PLACES_API_KEY in .env (or use --skip-scrape with --input)"
        )

    asyncio.run(
        run_pipeline(
            api_key=api_key or "",
            center_lat=args.center_lat,
            center_lng=args.center_lng,
            overall_radius_km=args.overall_radius_km,
            grid_rows=args.grid_rows,
            grid_cols=args.grid_cols,
            max_pages_per_center=args.max_pages_per_center,
            max_results=args.max_results,
            output_csv=args.output,
            screenshots_dir=args.screenshots_dir,
            concurrency=args.concurrency,
            headless=not args.headed,
            pause_sec=args.pause,
            skip_scrape=args.skip_scrape,
            input_csv=args.input,
        )
    )


if __name__ == "__main__":
    main()
