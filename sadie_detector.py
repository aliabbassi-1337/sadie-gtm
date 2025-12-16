#!/usr/bin/env python3
"""
Sadie Detector - Booking Engine Detection
==========================================
Visits hotel websites to detect booking engines, extract contacts, and take screenshots.

Usage:
    python3 sadie_detector.py --input hotels_scraped.csv

Requires:
    - playwright install chromium
"""

import csv
import os
import re
import argparse
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

OUTPUT_CSV = "sadie_leads.csv"
SCREENSHOTS_DIR = "screenshots"
LOG_FILE = "sadie_detector.log"

# Timeouts (milliseconds)
TIMEOUT_PAGE_LOAD = 20000       # 20s
TIMEOUT_BOOKING_CLICK = 10000   # 10s
TIMEOUT_POPUP_DETECT = 3000     # 3s

# Booking engine patterns
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
    "SiteMinder / TheBookingButton": ["thebookingbutton.com", "siteminder.com", "direct-book"],
    "Sabre / CRS": ["sabre.com", "crs.sabre.com"],
    "eZee": ["ezeeabsolute.com", "ezeereservation.com", "ezeetechnosys.com"],
}

BOOKING_KEYWORDS = [
    "book now", "book", "reserve", "reserve now", "reservation", "reservations",
    "check availability", "check rates", "check rate", "availability",
    "online booking", "book online", "book a room", "book your stay",
]


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

_log_file = None

def init_log_file():
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
# Helpers
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


# ------------------------------------------------------------
# Engine Detection
# ------------------------------------------------------------

def detect_engine_from_domain(domain: str):
    if not domain:
        return ("", "")
    for engine_name, patterns in ENGINE_PATTERNS.items():
        for pat in patterns:
            if pat in domain:
                return (engine_name, pat)
    return ("", "")


def detect_engine_from_url(url: str, hotel_domain: str):
    if not url:
        return ("unknown", "", "no_url")
    
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
    ]

    for keyword, engine_name in checks:
        if keyword in low:
            return (engine_name, "html_keyword")

    return ("", "")


# ------------------------------------------------------------
# Contact Extraction
# ------------------------------------------------------------

PHONE_PATTERNS = [
    r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
    r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
]

EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'


def extract_phones_from_html(html: str) -> list:
    phones = []
    for pattern in PHONE_PATTERNS:
        matches = re.findall(pattern, html)
        phones.extend(matches)
    cleaned = []
    seen = set()
    for p in phones:
        p = re.sub(r'[^\d+]', '', p)
        if len(p) >= 10 and p not in seen:
            seen.add(p)
            cleaned.append(p)
    return cleaned[:3]


def extract_emails_from_html(html: str) -> list:
    matches = re.findall(EMAIL_PATTERN, html)
    filtered = []
    skip_patterns = ['example.com', 'domain.com', 'email.com', 'sentry.io', 
                     'wixpress.com', 'schema.org', '.png', '.jpg', '.gif']
    for email in matches:
        email_lower = email.lower()
        if not any(skip in email_lower for skip in skip_patterns):
            if email_lower not in [e.lower() for e in filtered]:
                filtered.append(email)
    return filtered[:3]


# ------------------------------------------------------------
# Browser Automation
# ------------------------------------------------------------

async def find_booking_button_candidates(page, max_candidates: int = 5):
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


async def click_and_get_booking_page(context, page, timeout_ms: int = TIMEOUT_BOOKING_CLICK):
    candidates = await find_booking_button_candidates(page)
    if not candidates:
        return (None, None, "no_booking_button_found")

    original_url = page.url
    last_booking_url = None
    last_booking_page = None

    for el in candidates:
        try:
            async with context.expect_page(timeout=TIMEOUT_POPUP_DETECT) as p_info:
                await el.click()
            new_page = await p_info.value
            try:
                await new_page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            except PWTimeoutError:
                pass
            return (new_page, new_page.url, "popup_page")
        except PWTimeoutError:
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
    for frame in page.frames:
        try:
            frame_url = frame.url
        except Exception:
            continue
        
        if not frame_url or frame_url.startswith("about:"):
            continue
        
        for engine_name, patterns in ENGINE_PATTERNS.items():
            for pat in patterns:
                if pat in frame_url.lower():
                    return engine_name, pat, "frame_url_match", frame_url

        try:
            html = await frame.content()
        except Exception:
            html = ""
        engine, method = detect_engine_from_html(html)
        if engine:
            return engine, "", f"frame_{method}", frame_url

    return "", "", "", ""


def sniff_network_for_engine(network_urls: dict, hotel_domain: str):
    for host, full_url in network_urls.items():
        engine_name, pat = detect_engine_from_domain(host)
        if engine_name:
            return (engine_name, host, "network_sniff", full_url)
    return ("", "", "", "")


# ------------------------------------------------------------
# Hotel Processing
# ------------------------------------------------------------

async def process_hotel(idx, total, hotel, browser, semaphore, screenshots_dir, pause_sec):
    name = hotel.get("name", "")
    website = normalize_url(hotel.get("website", ""))
    phone_from_google = hotel.get("phone", "")
    
    log(f"[{idx}/{total}] {name} | {website}")
    
    result = {
        "name": name,
        "website": website,
        "address": hotel.get("address", ""),
        "latitude": hotel.get("lat", hotel.get("latitude", "")),
        "longitude": hotel.get("lng", hotel.get("longitude", "")),
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
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()
        
        network_urls = {}
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
            await page.goto(website, timeout=TIMEOUT_PAGE_LOAD, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            hotel_domain = extract_domain(page.url)
            log(f"  Loaded: {hotel_domain}")
            
            # Extract contacts
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
            
            # Click booking button
            booking_page, booking_url, method = await click_and_get_booking_page(context, page)
            result["booking_url"] = booking_url or ""
            result["detection_method"] = method
            
            engine_name = ""
            engine_domain = ""
            detection_method = method
            
            # Detect from URL
            if booking_url:
                log(f"  Booking URL: {booking_url}")
                engine_name, engine_domain, url_method = detect_engine_from_url(booking_url, hotel_domain)
                detection_method = f"{method}+{url_method}"
            
            # Check booking page HTML
            if booking_page and engine_name in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain"):
                try:
                    html = await booking_page.content()
                except Exception:
                    html = ""
                html_engine, html_method = detect_engine_from_html(html)
                if html_engine:
                    engine_name = html_engine
                    detection_method = f"{detection_method}+{html_method}"
            
            # Check network
            if engine_name in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain"):
                net_engine, net_domain, net_method, net_url = sniff_network_for_engine(network_urls, hotel_domain)
                if net_engine:
                    engine_name = net_engine
                    if not engine_domain:
                        engine_domain = net_domain
                    detection_method = f"{detection_method}+{net_method}"
                    if not result["booking_url"] and net_url:
                        result["booking_url"] = net_url
            
            # Check frames
            if engine_name in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain"):
                frame_engine, frame_domain, frame_method, frame_url = await detect_engine_from_frames(booking_page or page)
                if frame_engine:
                    engine_name = frame_engine
                    if not engine_domain:
                        engine_domain = frame_domain
                    detection_method = f"{detection_method}+{frame_method}"
                    if not result["booking_url"] and frame_url:
                        result["booking_url"] = frame_url
            
            # Screenshot
            if booking_page or booking_url:
                try:
                    screenshot_page = booking_page or page
                    safe_name = re.sub(r'[^\w\-]', '_', name)[:50]
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    screenshot_filename = f"{safe_name}_{timestamp}.png"
                    screenshot_path = os.path.join(screenshots_dir, screenshot_filename)
                    await screenshot_page.screenshot(path=screenshot_path, full_page=False)
                    result["screenshot_path"] = screenshot_filename
                    log(f"  Screenshot: {screenshot_filename}")
                except Exception as e:
                    log(f"  Screenshot failed: {e}")
            
            result["booking_engine"] = engine_name or "unknown"
            result["booking_engine_domain"] = engine_domain
            result["detection_method"] = detection_method
            
            log(f"  Engine: {result['booking_engine']} ({result['booking_engine_domain'] or 'n/a'})")
            
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
# Main
# ------------------------------------------------------------

async def run_detector(
    input_csv: str,
    output_csv: str,
    screenshots_dir: str,
    concurrency: int,
    headless: bool,
    pause_sec: float,
):
    log("Sadie Detector - Booking Engine Detection")
    
    Path(screenshots_dir).mkdir(parents=True, exist_ok=True)
    
    # Load hotels
    hotels = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            hotels.append(row)
    
    log(f"Loaded {len(hotels)} hotels from {input_csv}")
    
    # Resume support
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
    
    hotels_remaining = []
    for h in hotels:
        key = (h.get("name", ""), normalize_url(h.get("website", "")))
        if key not in processed_keys:
            hotels_remaining.append(h)
    
    if not hotels_remaining:
        log("All hotels already processed. Nothing to do.")
        return
    
    log(f"{len(hotels_remaining)} hotels remaining to process")
    
    fieldnames = [
        "name", "website", "booking_url", "booking_engine", "booking_engine_domain",
        "detection_method", "error",
        "phone_google", "phone_website", "email", "address",
        "latitude", "longitude", "rating", "review_count",
        "screenshot_path", "place_id"
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        semaphore = asyncio.Semaphore(concurrency)
        
        tasks = [
            process_hotel(idx, len(hotels_remaining), hotel, browser, semaphore, screenshots_dir, pause_sec)
            for idx, hotel in enumerate(hotels_remaining, 1)
        ]
        
        mode = "a" if append_mode else "w"
        with open(output_csv, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not append_mode:
                writer.writeheader()
            
            stats = {"processed": 0, "known_engine": 0, "errors": 0}
            
            for coro in asyncio.as_completed(tasks):
                result = await coro
                stats["processed"] += 1
                
                if result.get("error"):
                    stats["errors"] += 1
                
                engine = result.get("booking_engine", "")
                if engine and engine not in ("unknown", "unknown_third_party", "proprietary_or_same_domain"):
                    stats["known_engine"] += 1
                
                writer.writerow(result)
                f.flush()
        
        await browser.close()
    
    log(f"\n{'='*60}")
    log(f"COMPLETE!")
    log(f"Processed: {stats['processed']} hotels")
    log(f"Known booking engines: {stats['known_engine']}")
    log(f"Errors: {stats['errors']}")
    log(f"Output: {output_csv}")
    log(f"Screenshots: {screenshots_dir}/")
    log(f"{'='*60}")


def main():
    init_log_file()
    
    parser = argparse.ArgumentParser(description="Sadie Detector - Booking Engine Detection")
    
    parser.add_argument("--input", required=True, help="Input CSV with hotels")
    parser.add_argument("--output", default=OUTPUT_CSV, help="Output CSV file")
    parser.add_argument("--screenshots-dir", default=SCREENSHOTS_DIR)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--pause", type=float, default=0.5)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        raise SystemExit(f"Input file not found: {args.input}")
    
    asyncio.run(run_detector(
        input_csv=args.input,
        output_csv=args.output,
        screenshots_dir=args.screenshots_dir,
        concurrency=args.concurrency,
        headless=not args.headed,
        pause_sec=args.pause,
    ))


if __name__ == "__main__":
    main()

