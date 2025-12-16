#!/usr/bin/env python3
"""
Sadie Detector - Booking Engine Detection
==========================================
Visits hotel websites to detect booking engines, extract contacts, and take screenshots.

Usage:
    python3 sadie_detector.py --input hotels.csv
"""

import csv
import os
import re
import argparse
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Central configuration for the detector."""
    # Timeouts (milliseconds)
    timeout_page_load: int = 15000      # 15s
    timeout_booking_click: int = 3000   # 3s (was 10s!)
    timeout_popup_detect: int = 1500    # 1.5s
    
    # Output
    output_csv: str = "sadie_leads.csv"
    screenshots_dir: str = "screenshots"
    log_file: str = "sadie_detector.log"
    
    # Processing
    concurrency: int = 5
    pause_between_hotels: float = 0.2
    headless: bool = True


# Booking engine URL patterns
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
    "SiteMinder": ["thebookingbutton.com", "siteminder.com", "direct-book"],
    "Sabre / CRS": ["sabre.com", "crs.sabre.com"],
    "eZee": ["ezeeabsolute.com", "ezeereservation.com", "ezeetechnosys.com"],
    "RezTrip": ["reztrip.com"],
    "IHG": ["ihg.com"],
    "Marriott": ["marriott.com"],
    "Hilton": ["hilton.com"],
}

# Keywords to identify booking buttons
BOOKING_BUTTON_KEYWORDS = [
    "book now", "book", "reserve", "reserve now", 
    "reservation", "reservations", "check availability", 
    "check rates", "availability", "book online", "book a room",
]

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class HotelInput:
    """Input data for a hotel."""
    name: str
    website: str
    phone: str = ""
    address: str = ""
    latitude: str = ""
    longitude: str = ""
    rating: str = ""
    review_count: str = ""
    place_id: str = ""


@dataclass
class HotelResult:
    """Output data for a processed hotel."""
    name: str = ""
    website: str = ""
    booking_url: str = ""
    booking_engine: str = ""
    booking_engine_domain: str = ""
    detection_method: str = ""
    error: str = ""
    phone_google: str = ""
    phone_website: str = ""
    email: str = ""
    address: str = ""
    latitude: str = ""
    longitude: str = ""
    rating: str = ""
    review_count: str = ""
    screenshot_path: str = ""
    place_id: str = ""


# ============================================================================
# LOGGING
# ============================================================================

class Logger:
    """Simple logger that writes to both console and file."""
    
    def __init__(self, log_file: str):
        self.file = open(log_file, "w", encoding="utf-8")
    
    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line)
        self.file.write(line + "\n")
        self.file.flush()
    
    def close(self):
        self.file.close()


# Global logger instance
logger: Optional[Logger] = None

def log(msg: str) -> None:
    if logger:
        logger.log(msg)
    else:
        print(msg)


# ============================================================================
# URL UTILITIES
# ============================================================================

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


# ============================================================================
# ENGINE DETECTION
# ============================================================================

class EngineDetector:
    """Detects booking engines from URLs, HTML, and network requests."""
    
    @staticmethod
    def from_domain(domain: str) -> tuple[str, str]:
        """Check if domain matches a known booking engine. Returns (engine_name, pattern)."""
        if not domain:
            return ("", "")
        for engine_name, patterns in ENGINE_PATTERNS.items():
            for pat in patterns:
                if pat in domain:
                    return (engine_name, pat)
        return ("", "")
    
    @staticmethod
    def from_url(url: str, hotel_domain: str) -> tuple[str, str, str]:
        """Detect engine from URL. Returns (engine_name, domain, method)."""
        if not url:
            return ("unknown", "", "no_url")
        
        url_lower = url.lower()
        
        # Check URL for engine patterns
        for engine_name, patterns in ENGINE_PATTERNS.items():
            for pat in patterns:
                if pat in url_lower:
                    return (engine_name, pat, "url_pattern_match")
        
        domain = extract_domain(url)
        if not domain:
            return ("unknown", "", "no_domain")
        
        # Check domain
        engine_name, pat = EngineDetector.from_domain(domain)
        if engine_name:
            return (engine_name, domain, "url_domain_match")
        
        # Third-party domain (not hotel's own)
        if hotel_domain and domain != hotel_domain:
            return ("unknown_third_party", domain, "third_party_domain")
        
        return ("proprietary_or_same_domain", domain, "same_domain")
    
    @staticmethod
    def from_network(network_urls: dict, hotel_domain: str) -> tuple[str, str, str, str]:
        """Check network requests for engine domains. Returns (engine, domain, method, full_url)."""
        for host, full_url in network_urls.items():
            engine_name, pat = EngineDetector.from_domain(host)
            if engine_name:
                return (engine_name, host, "network_sniff", full_url)
        return ("", "", "", "")


# ============================================================================
# CONTACT EXTRACTION
# ============================================================================

class ContactExtractor:
    """Extracts phone numbers and emails from HTML."""
    
    PHONE_PATTERNS = [
        r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US format
        r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}',  # International
    ]
    
    EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    
    SKIP_EMAIL_PATTERNS = [
        'example.com', 'domain.com', 'email.com', 'sentry.io',
        'wixpress.com', 'schema.org', '.png', '.jpg', '.gif'
    ]
    
    @classmethod
    def extract_phones(cls, html: str) -> list[str]:
        """Extract phone numbers from HTML."""
        phones = []
        for pattern in cls.PHONE_PATTERNS:
            phones.extend(re.findall(pattern, html))
        
        # Clean and dedupe
        seen = set()
        cleaned = []
        for p in phones:
            p = re.sub(r'[^\d+]', '', p)
            if len(p) >= 10 and p not in seen:
                seen.add(p)
                cleaned.append(p)
        return cleaned[:3]
    
    @classmethod
    def extract_emails(cls, html: str) -> list[str]:
        """Extract email addresses from HTML."""
        matches = re.findall(cls.EMAIL_PATTERN, html)
        
        filtered = []
        for email in matches:
            email_lower = email.lower()
            if not any(skip in email_lower for skip in cls.SKIP_EMAIL_PATTERNS):
                if email_lower not in [e.lower() for e in filtered]:
                    filtered.append(email)
        return filtered[:3]


# ============================================================================
# BROWSER AUTOMATION
# ============================================================================

class BookingButtonFinder:
    """Finds and clicks booking buttons on hotel websites."""
    
    # Domains to skip (social media, etc.)
    SKIP_DOMAINS = [
        "facebook.com", "twitter.com", "instagram.com", "linkedin.com",
        "youtube.com", "tiktok.com", "pinterest.com", "yelp.com",
        "tripadvisor.com", "google.com", "maps.google.com",
        "mailto:", "tel:", "javascript:"
    ]
    
    def __init__(self, config: Config):
        self.config = config
    
    async def _dismiss_popups(self, page):
        """Try to dismiss cookie consent and other popups."""
        log("    [COOKIES] Trying to dismiss popups...")
        
        dismiss_selectors = [
            # Common cookie consent buttons
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
            # By class/id
            "[class*='cookie'] button",
            "[class*='Cookie'] button",
            "[id*='cookie'] button",
            "[class*='consent'] button",
            "[class*='gdpr'] button",
            "[class*='privacy'] button:has-text('accept')",
            # Close buttons
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
                        log(f"    [COOKIES] Clicking: {selector}")
                        await btn.click(timeout=1000)
                        await asyncio.sleep(0.5)
                        return
            except Exception:
                continue
        
        log("    [COOKIES] No popup found to dismiss")
    
    async def find_candidates(self, page, max_candidates: int = 3) -> list:
        """Find elements that look like booking buttons."""
        import time
        candidates = []
        
        # Use Playwright's text selector - try common booking button texts
        # Filter out social media links with :not()
        selectors = [
            "button:has-text('book')",
            "a:has-text('book'):not([href*='facebook']):not([href*='twitter']):not([href*='instagram']):not([href*='spa']):not([href*='conference'])",
            "a:has-text('reserve'):not([href*='facebook']):not([href*='spa']):not([href*='conference'])",
            "a:has-text('availability'):not([href*='facebook'])",
            "button:has-text('reserve')",
            "button:has-text('availability')",
            # Additional patterns for hotels that use different text
            "a:has-text('check rates')",
            "a:has-text('find rooms')",
            "a:has-text('rooms')",
            "button:has-text('find')",
            "a[href*='reservations']",
            "a[href*='booking']",
        ]
        
        for selector in selectors:
            try:
                t0 = time.time()
                loc = page.locator(selector).first
                count = await loc.count()
                log(f"    [FIND] {selector}: {time.time()-t0:.1f}s (count={count})")
                if count > 0:
                    candidates.append(loc)
                    if len(candidates) >= max_candidates:
                        break
            except Exception as e:
                log(f"    [FIND] {selector}: error {e}")
                continue
        
        return candidates
    
    async def click_and_navigate(self, context, page) -> tuple:
        """Click booking button and return (page, url, method)."""
        # Try to dismiss cookie consent banners first
        await self._dismiss_popups(page)
        
        candidates = await self.find_candidates(page)
        
        # Sort candidates: prioritize those with real hrefs over hash/no-href
        async def get_href_score(el):
            try:
                href = await el.get_attribute("href") or ""
                if href.startswith("http"):
                    return 0  # Best: full URL
                elif href.startswith("/") and len(href) > 1:
                    return 1  # OK: relative path
                elif href == "#" or href.startswith("#") or not href:
                    return 2  # Worst: hash or no href
                return 1
            except:
                return 2
        
        # Sort by href quality
        scored = [(await get_href_score(c), i, c) for i, c in enumerate(candidates)]
        scored.sort(key=lambda x: (x[0], x[1]))
        candidates = [c for _, _, c in scored]
        
        log(f"    [CLICK] Found {len(candidates)} candidates")
        
        if not candidates:
            # Debug: show what buttons/links ARE on the page
            await self._debug_page_elements(page)
            return (None, None, "no_booking_button_found")
        
        original_url = page.url
        
        import time
        
        for i, el in enumerate(candidates):
            try:
                # Get element info
                el_text = await el.text_content() or ""
                el_href = await el.get_attribute("href") or ""
                log(f"    [CLICK] Candidate {i}: '{el_text[:30].strip()}' -> {el_href[:60] if el_href else 'no-href'}")
                
                # If it's a link with an external URL, just grab it!
                # Skip: internal paths, hash links, social media, clearly non-booking pages, mailto/tel
                if el_href:
                    href_lower = el_href.lower()
                    is_internal = el_href.startswith("/") or el_href.startswith("#")
                    is_protocol_link = el_href.startswith("mailto:") or el_href.startswith("tel:")
                    is_social = any(s in href_lower for s in ["facebook", "twitter", "instagram", "youtube", "linkedin"])
                    is_bad_page = any(s in href_lower for s in ["/spa", "/conference", "/restaurant", "/dining", "/event", "/wedding", "/careers", "/contact", "/getaway", "/offer"])
                    is_external_booking = not is_internal and not is_protocol_link and not is_social and not is_bad_page
                
                if el_href and is_external_booking:
                    log(f"    [CLICK] Found booking URL in href, skipping click")
                    return (None, el_href, "href_extraction")
                
                # Otherwise try clicking
                click_start = time.time()
                try:
                    async with context.expect_page(timeout=1000) as p_info:
                        await el.click(force=True, no_wait_after=True)
                    new_page = await p_info.value
                    log(f"    [CLICK] Got popup in {time.time() - click_start:.1f}s")
                    return (new_page, new_page.url, "popup_page")
                except PWTimeoutError:
                    log(f"    [CLICK] No popup after {time.time() - click_start:.1f}s, checking URL...")
                except Exception as click_err:
                    log(f"    [CLICK] Click failed: {str(click_err)[:80]}")
                    continue  # Try next candidate
                
                # No popup - wait for sidebar/modal to appear
                await asyncio.sleep(0.5)
                
                # Check if URL changed
                if page.url != original_url:
                    log(f"    [CLICK] URL changed to: {page.url[:60]}")
                    return (page, page.url, "same_page_navigation")
                
                log("    [CLICK] URL unchanged, trying 2nd stage...")
                # Try second-stage click (sidebar might have appeared)
                second_stage = await self._try_second_stage_click(context, page)
                if second_stage:
                    return second_stage
                
                # Return page for iframe scanning
                return (page, None, "clicked_no_navigation")
                    
            except Exception as e:
                log(f"    [CLICK] Error getting element info: {e}")
                continue
        
        return (None, None, "no_booking_button_found")
    
    async def _debug_page_elements(self, page):
        """Log all buttons and prominent links on the page for debugging."""
        try:
            # Get all buttons
            buttons = await page.locator("button").all()
            button_texts = []
            for b in buttons[:10]:  # Limit to first 10
                try:
                    txt = await b.text_content()
                    if txt and txt.strip():
                        button_texts.append(txt.strip()[:30])
                except Exception:
                    pass
            if button_texts:
                log(f"    [DEBUG] Buttons on page: {button_texts}")
            
            # Get all links with text
            links = await page.locator("a").all()
            link_info = []
            for a in links[:15]:  # Limit to first 15
                try:
                    txt = await a.text_content()
                    href = await a.get_attribute("href") or ""
                    if txt and txt.strip() and len(txt.strip()) < 40:
                        link_info.append(f"'{txt.strip()[:20]}' -> {href[:30] if href else 'no-href'}")
                except Exception:
                    pass
            if link_info:
                log(f"    [DEBUG] Links on page: {link_info[:8]}")
        except Exception as e:
            log(f"    [DEBUG] Error getting page elements: {e}")
    
    async def _try_second_stage_click(self, context, page) -> tuple:
        """Try to find and click a second booking button (in sidebar/modal)."""
        log("    [2ND STAGE] Looking for second button...")
        
        # Look for booking buttons that might have appeared
        second_selectors = [
            "button:has-text('book now')",
            "button:has-text('check rates')",
            "button:has-text('check availability')",
            "button:has-text('search')",
            "button:has-text('view rates')",
            "a:has-text('book now')",
            "a:has-text('check rates')",
            "input[type='submit']",
            "button[type='submit']",
        ]
        
        for selector in second_selectors:
            try:
                btn = page.locator(selector).first
                count = await btn.count()
                visible = await btn.is_visible() if count > 0 else False
                log(f"    [2ND STAGE] {selector}: count={count}, visible={visible}")
                
                if count > 0 and visible:
                    try:
                        async with context.expect_page(timeout=1500) as p_info:
                            await btn.click(force=True, no_wait_after=True)
                        new_page = await p_info.value
                        log(f"    [2ND STAGE] Got popup: {new_page.url[:60]}")
                        return (new_page, new_page.url, "two_stage_popup")
                    except PWTimeoutError:
                        log("    [2ND STAGE] No popup from click")
            except Exception as e:
                log(f"    [2ND STAGE] Error: {e}")
                continue
        
        return None


# ============================================================================
# HOTEL PROCESSOR
# ============================================================================

class HotelProcessor:
    """Processes a single hotel: visits site, detects engine, extracts contacts."""
    
    def __init__(self, config: Config, browser, semaphore, screenshots_dir: str):
        self.config = config
        self.browser = browser
        self.semaphore = semaphore
        self.screenshots_dir = screenshots_dir
        self.button_finder = BookingButtonFinder(config)
    
    async def process(self, idx: int, total: int, hotel: dict) -> HotelResult:
        """Process a single hotel and return results."""
        name = hotel.get("name", "")
        website = normalize_url(hotel.get("website", ""))
        
        log(f"[{idx}/{total}] {name} | {website}")
        
        result = HotelResult(
            name=name,
            website=website,
            phone_google=hotel.get("phone", ""),
            address=hotel.get("address", ""),
            latitude=hotel.get("latitude", hotel.get("lat", "")),
            longitude=hotel.get("longitude", hotel.get("lng", "")),
            rating=hotel.get("rating", ""),
            review_count=hotel.get("review_count", ""),
            place_id=hotel.get("place_id", ""),
        )
        
        if not website:
            result.error = "no_website"
            return result
        
        async with self.semaphore:
            result = await self._process_website(result)
        
        return result
    
    async def _process_website(self, result: HotelResult) -> HotelResult:
        """Visit website and extract all data."""
        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()
        
        # Capture homepage network requests (for fallback detection)
        homepage_network = {}
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
            import time
            
            # 1. Load homepage
            t0 = time.time()
            await page.goto(result.website, timeout=self.config.timeout_page_load, wait_until="domcontentloaded")
            log(f"  [TIME] goto: {time.time()-t0:.1f}s")
            
            t0 = time.time()
            await asyncio.sleep(1)
            log(f"  [TIME] sleep: {time.time()-t0:.1f}s")
            
            hotel_domain = extract_domain(page.url)
            log(f"  Loaded: {hotel_domain}")
            
            # 2. Extract contacts
            t0 = time.time()
            result = await self._extract_contacts_fast(page, result)
            log(f"  [TIME] contacts: {time.time()-t0:.1f}s")
            
            # 3. Find and click booking button
            t0 = time.time()
            booking_url, click_method = await self._find_booking_url(context, page)
            log(f"  [TIME] button_find: {time.time()-t0:.1f}s")
            result.booking_url = booking_url or ""
            result.detection_method = click_method
            
            engine_name = ""
            engine_domain = ""
            
            # 4. PRIMARY: Navigate to booking URL
            if booking_url:
                t0 = time.time()
                engine_name, engine_domain, result = await self._analyze_booking_page(
                    context, booking_url, hotel_domain, click_method, result
                )
                log(f"  [TIME] analyze_booking: {time.time()-t0:.1f}s")
            
            # 5. FALLBACK: Check homepage network
            if self._needs_fallback(engine_name):
                t0 = time.time()
                net_engine, net_domain, _, net_url = EngineDetector.from_network(homepage_network, hotel_domain)
                log(f"  [TIME] network_fallback: {time.time()-t0:.1f}s")
                if net_engine and net_engine not in ("unknown_third_party",):
                    engine_name = net_engine
                    engine_domain = net_domain
                    result.detection_method += "+homepage_network"
                    if net_url and not result.booking_url:
                        result.booking_url = net_url
            
            # 6. FALLBACK: Scan iframes
            if self._needs_fallback(engine_name):
                t0 = time.time()
                frame_engine, frame_domain, frame_url = await self._scan_frames(page)
                log(f"  [TIME] frame_scan: {time.time()-t0:.1f}s")
                if frame_engine:
                    engine_name = frame_engine
                    engine_domain = frame_domain
                    result.detection_method += "+frame_scan"
                    if frame_url and not result.booking_url:
                        result.booking_url = frame_url
            
            # 7. FALLBACK: HTML keyword
            if self._needs_fallback(engine_name):
                t0 = time.time()
                html_engine = await self._detect_from_html(page)
                log(f"  [TIME] html_detect: {time.time()-t0:.1f}s")
                if html_engine:
                    engine_name = html_engine
                    result.detection_method += "+html_keyword"
            
            result.booking_engine = engine_name or "unknown"
            result.booking_engine_domain = engine_domain
            
            # Take homepage screenshot if no booking screenshot was taken
            if not result.screenshot_path:
                result = await self._take_screenshot(page, result, suffix="_homepage")
            
            log(f"  Engine: {result.booking_engine} ({result.booking_engine_domain or 'n/a'})")
            
        except PWTimeoutError:
            result.error = "timeout"
            log("  ERROR: timeout")
        except Exception as e:
            result.error = f"exception: {str(e)[:100]}"
            log(f"  ERROR: {e}")
        
        await context.close()
        
        if self.config.pause_between_hotels > 0:
            await asyncio.sleep(self.config.pause_between_hotels)
        
        return result
    
    def _needs_fallback(self, engine_name: str) -> bool:
        """Check if we need to try fallback detection."""
        return engine_name in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain")
    
    async def _extract_contacts_fast(self, page, result: HotelResult) -> HotelResult:
        """Extract phone and email using JS evaluate (non-blocking)."""
        try:
            # Get body text via JS - doesn't wait for page stability
            text = await page.evaluate("document.body ? document.body.innerText : ''")
            phones = ContactExtractor.extract_phones(text)
            emails = ContactExtractor.extract_emails(text)
            
            if phones:
                result.phone_website = phones[0]
            if emails:
                result.email = emails[0]
        except Exception:
            pass
        return result
    
    async def _find_booking_url(self, context, page) -> tuple[str, str]:
        """Find booking button and get the booking URL."""
        booking_page, booking_url, method = await self.button_finder.click_and_navigate(context, page)
        
        # Close the booking page if it opened (we'll open a fresh one for sniffing)
        if booking_page and booking_page != page:
            try:
                await booking_page.close()
            except Exception:
                pass
        
        return booking_url, method
    
    async def _analyze_booking_page(self, context, booking_url: str, hotel_domain: str, 
                                     click_method: str, result: HotelResult) -> tuple[str, str, HotelResult]:
        """Navigate to booking URL, sniff network, detect engine, take screenshot.
        Returns (engine_name, engine_domain, result)."""
        log(f"  Booking URL: {booking_url[:80]}...")
        
        page = await context.new_page()
        network_urls = {}
        engine_name = ""
        engine_domain = ""
        
        # Capture all network requests
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
            # Navigate to booking URL
            await page.goto(booking_url, timeout=self.config.timeout_page_load, wait_until="domcontentloaded")
            await asyncio.sleep(1.5)  # Let booking engine load
            
            # Detect engine from network requests (most reliable)
            engine_name, engine_domain, net_method, engine_url = EngineDetector.from_network(network_urls, hotel_domain)
            
            # Fallback: check the URL itself
            if not engine_name:
                engine_name, engine_domain, url_method = EngineDetector.from_url(booking_url, hotel_domain)
                net_method = url_method
            
            # Update booking URL if we found a better one from network
            if engine_url and engine_url != booking_url:
                result.booking_url = engine_url
            
            result.detection_method = f"{click_method}+{net_method}"
            
            # Take screenshot as proof
            result = await self._take_screenshot(page, result)
            
        except Exception as e:
            log(f"  Booking page error: {e}")
        finally:
            await page.close()
        
        return engine_name, engine_domain, result
    
    async def _scan_frames(self, page) -> tuple[str, str, str]:
        """Scan iframes for booking engine patterns. Returns (engine, domain, url)."""
        for frame in page.frames:
            try:
                frame_url = frame.url
            except Exception:
                continue
            
            if not frame_url or frame_url.startswith("about:"):
                continue
            
            # Check frame URL for engine patterns (fast, no waiting)
            for engine_name, patterns in ENGINE_PATTERNS.items():
                for pat in patterns:
                    if pat in frame_url.lower():
                        return (engine_name, pat, frame_url)
        
        # Skip frame HTML scanning - too slow and unreliable
        return ("", "", "")
    
    async def _detect_from_html(self, page) -> str:
        """Detect engine from page HTML keywords (non-blocking)."""
        try:
            # Use evaluate instead of content() - doesn't wait
            html = await page.evaluate("document.documentElement.outerHTML")
            return self._detect_engine_from_html_content(html)
        except Exception:
            return ""
    
    def _detect_engine_from_html_content(self, html: str) -> str:
        """Check HTML for booking engine keywords."""
        if not html:
            return ""
        
        low = html.lower()
        
        keywords = [
            ("cloudbeds", "Cloudbeds"),
            ("synxis", "SynXis / TravelClick"),
            ("travelclick", "SynXis / TravelClick"),
            ("mews.com", "Mews"),
            ("littlehotelier", "Little Hotelier"),
            ("siteminder", "SiteMinder"),
            ("thebookingbutton", "SiteMinder"),
            ("direct-book", "SiteMinder"),
            ("webrezpro", "WebRezPro"),
            ("innroad", "InnRoad"),
            ("resnexus", "ResNexus"),
            ("newbook", "Newbook"),
            ("roomraccoon", "RoomRaccoon"),
            ("ezee", "eZee"),
            ("rmscloud", "RMS Cloud"),
            ("reztrip", "RezTrip"),
        ]
        
        for keyword, engine in keywords:
            if keyword in low:
                return engine
        
        return ""
    
    async def _take_screenshot(self, page, result: HotelResult, suffix: str = "") -> HotelResult:
        """Take screenshot of page."""
        try:
            safe_name = re.sub(r'[^\w\-]', '_', result.name)[:50]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_name}{suffix}_{timestamp}.png"
            path = os.path.join(self.screenshots_dir, filename)
            
            await page.screenshot(path=path, full_page=False)
            result.screenshot_path = filename
            log(f"  Screenshot: {filename}")
        except Exception as e:
            log(f"  Screenshot failed: {e}")
        
        return result


# ============================================================================
# MAIN PIPELINE
# ============================================================================

class DetectorPipeline:
    """Main pipeline that orchestrates the detection process."""
    
    def __init__(self, config: Config):
        self.config = config
    
    async def run(self, input_csv: str):
        """Run the full detection pipeline."""
        log("Sadie Detector - Booking Engine Detection")
        
        # Setup
        Path(self.config.screenshots_dir).mkdir(parents=True, exist_ok=True)
        
        # Load hotels
        hotels = self._load_hotels(input_csv)
        log(f"Loaded {len(hotels)} hotels from {input_csv}")
        
        # Resume support
        hotels, append_mode = self._filter_processed(hotels)
        
        if not hotels:
            log("All hotels already processed. Nothing to do.")
            return
        
        log(f"{len(hotels)} hotels remaining to process")
        
        # Process hotels
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            semaphore = asyncio.Semaphore(self.config.concurrency)
            
            processor = HotelProcessor(self.config, browser, semaphore, self.config.screenshots_dir)
            
            tasks = [
                processor.process(idx, len(hotels), hotel)
                for idx, hotel in enumerate(hotels, 1)
            ]
            
            # Write results
            await self._write_results(tasks, append_mode)
            
            await browser.close()
    
    def _load_hotels(self, input_csv: str) -> list[dict]:
        """Load hotels from CSV."""
        hotels = []
        with open(input_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                hotels.append(row)
        return hotels
    
    def _filter_processed(self, hotels: list[dict]) -> tuple[list[dict], bool]:
        """Filter out already-processed hotels. Returns (remaining, append_mode)."""
        if not os.path.exists(self.config.output_csv):
            return hotels, False
        
        processed = set()
        with open(self.config.output_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (row.get("name", ""), normalize_url(row.get("website", "")))
                processed.add(key)
        
        if not processed:
            return hotels, False
        
        log(f"Found {len(processed)} already processed, will skip them")
        
        remaining = [
            h for h in hotels
            if (h.get("name", ""), normalize_url(h.get("website", ""))) not in processed
        ]
        return remaining, True
    
    async def _write_results(self, tasks: list, append_mode: bool):
        """Write results to CSV as they complete."""
        fieldnames = list(HotelResult.__dataclass_fields__.keys())
        
        mode = "a" if append_mode else "w"
        with open(self.config.output_csv, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not append_mode:
                writer.writeheader()
            
            stats = {"processed": 0, "known_engine": 0, "errors": 0}
            
            for coro in asyncio.as_completed(tasks):
                result = await coro
                stats["processed"] += 1
                
                if result.error:
                    stats["errors"] += 1
                
                if result.booking_engine not in ("unknown", "unknown_third_party", "proprietary_or_same_domain"):
                    stats["known_engine"] += 1
                
                writer.writerow(asdict(result))
                f.flush()
        
        self._print_summary(stats)
    
    def _print_summary(self, stats: dict):
        """Print final summary."""
        log(f"\n{'='*60}")
        log("COMPLETE!")
        log(f"Processed: {stats['processed']} hotels")
        log(f"Known booking engines: {stats['known_engine']}")
        log(f"Errors: {stats['errors']}")
        log(f"Output: {self.config.output_csv}")
        log(f"Screenshots: {self.config.screenshots_dir}/")
        log(f"{'='*60}")


# ============================================================================
# CLI
# ============================================================================

async def main_async(args):
    config = Config(
        output_csv=args.output,
        screenshots_dir=args.screenshots_dir,
        concurrency=args.concurrency,
        pause_between_hotels=args.pause,
        headless=not args.headed,
    )
    
    pipeline = DetectorPipeline(config)
    await pipeline.run(args.input)


def main():
    global logger
    
    parser = argparse.ArgumentParser(description="Sadie Detector - Booking Engine Detection")
    parser.add_argument("--input", required=True, help="Input CSV with hotels")
    parser.add_argument("--output", default="sadie_leads.csv", help="Output CSV file")
    parser.add_argument("--screenshots-dir", default="screenshots")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--pause", type=float, default=0.5)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        raise SystemExit(f"Input file not found: {args.input}")
    
    logger = Logger("sadie_detector.log")
    
    try:
        asyncio.run(main_async(args))
    finally:
        logger.close()


if __name__ == "__main__":
    main()
