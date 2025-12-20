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
import sys
import argparse
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from dataclasses import dataclass, asdict

from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Central configuration for the detector."""
    # Timeouts (milliseconds)
    timeout_page_load: int = 30000      # 30s (we use fallback if slow)
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
    "Vacatia": ["vacatia.com"],
    "JEHS / iPMS": ["ipms247.com", "live.ipms247.com"],
    "Windsurfer CRS": ["windsurfercrs.com", "res.windsurfercrs.com"],
    "ThinkReservations": ["thinkreservations.com", "secure.thinkreservations.com"],
    "ASI Web Reservations": ["asiwebres.com", "reservation.asiwebres.com"],
    "IQWebBook": ["iqwebbook.com", "us01.iqwebbook.com"],
    "BookDirect": ["bookdirect.net", "ococean.bookdirect.net"],
    "RezStream": ["rezstream.com", "guest.rezstream.com"],
    "Reseze": ["reseze.net"],
    "WebRez": ["webrez.com", "secure.webrez.com"],
    "IB Strategies": ["ibstrategies.com", "secure.ibstrategies.com"],
    "Morey's Piers": ["moreyspiers.com", "irm.moreyspiers.com"],
    "ReservationKey": ["reservationkey.com", "v2.reservationkey.com"],
    "FareHarbor": ["fareharbor.com"],
    "Firefly Reservations": ["fireflyreservations.com", "app.fireflyreservations.com"],
    "Lodgify": ["lodgify.com", "checkout.lodgify.com"],
    "eviivo": ["eviivo.com", "via.eviivo.com"],
    "LuxuryRes": ["luxuryres.com"],
}

# Keywords to identify booking buttons
BOOKING_BUTTON_KEYWORDS = [
    "book now", "book", "reserve", "reserve now", 
    "reservation", "reservations", "check availability", 
    "check rates", "availability", "book online", "book a room",
]

# Big chains to skip - they have their own booking systems, not good leads
SKIP_CHAIN_DOMAINS = [
    "marriott.com", "hilton.com", "ihg.com", "hyatt.com", "wyndham.com",
    "choicehotels.com", "bestwestern.com", "radissonhotels.com", "accor.com",
]

# Skip social media and junk websites (not real hotel sites)
SKIP_JUNK_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com", "youtube.com", "tiktok.com",
    "linkedin.com", "yelp.com", "tripadvisor.com", "google.com",
    "booking.com", "expedia.com", "hotels.com", "airbnb.com", "vrbo.com",
    "dnr.", "parks.", "recreation.", ".gov", ".edu", ".mil",
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


# ============================================================================
# LOGGING
# ============================================================================

def setup_logging(debug: bool = False):
    """Configure loguru logging."""
    logger.remove()
    
    # Console: INFO by default, DEBUG if flag set
    log_level = "DEBUG" if debug else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level,
        colorize=True,
    )
    
    # File: Always DEBUG
    logger.add(
        "sadie_detector.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="DEBUG",
        rotation="10 MB",
    )


def log(msg: str) -> None:
    """Log wrapper for backwards compatibility."""
    logger.info(msg)


def log_debug(msg: str) -> None:
    """Debug log."""
    logger.debug(msg)


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
        # Order matters: more specific/reliable first, generic fallbacks last
        selectors = [
            # HIGHEST PRIORITY: Direct links to known booking engines
            "a[href*='ipms247']",
            "a[href*='synxis']",
            "a[href*='cloudbeds']",
            "a[href*='direct-book']",
            "a[href*='bookingsuite']",
            "a[href*='travelclick']",
            "a[href*='webrezpro']",
            "a[href*='resnexus']",
            "a[href*='windsurfercrs']",
            "a[href*='thinkreservations']",
            "a[href*='asiwebres']",
            # INPUT SUBMIT buttons (often the actual booking button!)
            "input[type='submit'][value*='Availability' i]",
            "input[type='submit'][value*='Book' i]",
            "input[type='submit'][value*='Reserve' i]",
            "input[type='submit'][value*='Check' i]",
            # Availability buttons (often link to booking engines)
            "button:has-text('check availability')",
            "a:has-text('check availability')",
            "button:has-text('availability')",
            "a:has-text('availability')",
            "[role='button']:has-text('availability')",
            # Book now buttons
            "button:has-text('book now')",
            "a:has-text('book now'):not([href*='facebook'])",
            "[role='button']:has-text('book now')",
            "div:has-text('book now'):not(:has(div:has-text('book now')))",  # leaf div only
            "span:has-text('book now')",
            # Brizy and other page builders use custom link elements
            "[data-brz-link-type] :has-text('book')",
            ".brz-btn:has-text('book')",
            "[class*='btn']:has-text('book now')",
            "[class*='button']:has-text('book now')",
            # General book buttons
            "button:has-text('book')",
            "a:has-text('book'):not([href*='facebook']):not([href*='twitter']):not([href*='instagram']):not([href*='spa']):not([href*='conference'])",
            "[role='button']:has-text('book')",
            # Header/navbar specific booking links
            "header a:has-text('book')",
            "nav a:has-text('book')",
            "[class*='header'] a:has-text('book')",
            "[class*='nav'] a:has-text('book')",
            # Reserve buttons
            "button:has-text('reserve')",
            "a:has-text('reserve'):not([href*='facebook']):not([href*='spa']):not([href*='conference'])",
            "[role='button']:has-text('reserve')",
            # Check rates
            "button:has-text('check rates')",
            "a:has-text('check rates')",
            # URL-based selectors (other booking engine links)
            "a[href*='reservations']",
            "a[href*='booking']",
            # FALLBACK: Generic room links (lower priority)
            "a:has-text('find rooms')",
            "a:has-text('rooms'):not([href*='facebook'])",
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
        
        # JavaScript fallback: find clickable elements with booking text
        if not candidates:
            log("    [FIND] No candidates from selectors, trying JS fallback...")
            try:
                js_result = await page.evaluate("""() => {
                    const bookingTerms = ['book now', 'book online', 'reserve now', 'reserve', 'check availability', 'book'];
                    const bookingUrls = ['synxis', 'cloudbeds', 'ipms247', 'windsurfercrs', 'travelclick', 'webrezpro', 'resnexus', 'thinkreservations', 'booking', 'reservations'];
                    const results = [];
                    
                    // Check ALL elements that could be clickable (not just standard buttons)
                    const elements = document.querySelectorAll('a, button, input[type="submit"], input[type="button"], li, div, span, [role="button"], [onclick], [class*="book"], [class*="reserve"], [class*="cta"]');
                    for (const el of elements) {
                        const text = (el.innerText || el.textContent || el.value || '').toLowerCase().trim();
                        const href = el.href || el.getAttribute('href') || '';
                        const rect = el.getBoundingClientRect();
                        
                        // Skip invisible elements
                        if (rect.width === 0 || rect.height === 0) continue;
                        // Skip very large elements (likely containers)
                        if (rect.width > 500 || rect.height > 200) continue;
                        
                        // Check if href contains booking engine URL
                        let isBookingUrl = false;
                        for (const url of bookingUrls) {
                            if (href.toLowerCase().includes(url)) {
                                isBookingUrl = true;
                                break;
                            }
                        }
                        
                        // Check if text matches booking terms
                        let isBookingText = false;
                        for (const term of bookingTerms) {
                            if (text === term || (text.includes(term) && text.length < 50)) {
                                isBookingText = true;
                                break;
                            }
                        }
                        
                        if (isBookingUrl || isBookingText) {
                            results.push({
                                tag: el.tagName.toLowerCase(),
                                text: text.substring(0, 30),
                                href: href,
                                classes: el.className || '',
                                id: el.id || '',
                                priority: isBookingUrl ? 0 : 1  // Prioritize URL matches
                            });
                        }
                        if (results.length >= 10) break;
                    }
                    // Sort by priority (URL matches first)
                    results.sort((a, b) => a.priority - b.priority);
                    return results.slice(0, 5);
                }""")
                
                log(f"    [FIND] JS fallback found: {js_result}")
                
                # Convert JS results back to locators
                for item in js_result:
                    try:
                        loc = None
                        # Try multiple strategies to find the element
                        if item.get('id'):
                            loc = page.locator(f"#{item['id']}").first
                        elif item.get('href') and item['href'].startswith('http'):
                            # Try finding by href (could be a, or li/div containing a)
                            loc = page.locator(f"[href='{item['href']}']").first
                            if await loc.count() == 0:
                                loc = page.locator(f"*:has([href='{item['href']}'])").first
                        elif item.get('classes') and 'book' in item['classes'].lower():
                            # Try finding by class
                            loc = page.locator(f".{item['classes'].split()[0]}").first
                        
                        # Fallback to text content
                        if not loc or await loc.count() == 0:
                            text_escaped = item['text'][:20].replace("'", "\\'")
                            loc = page.locator(f"{item['tag']}:has-text('{text_escaped}')").first
                        
                        if loc and await loc.count() > 0:
                            candidates.append(loc)
                            log(f"    [FIND] Added JS candidate: {item['tag']} '{item['text'][:25]}' href={item.get('href', 'none')[:30]}")
                    except Exception as e:
                        log(f"    [FIND] Error converting JS result: {e}")
            except Exception as e:
                log(f"    [FIND] JS fallback error: {e}")
        
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
            except Exception:
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
                # Get element info with short timeout (element might be stale)
                try:
                    el_text = (await asyncio.wait_for(el.text_content(), timeout=2.0) or "").strip().lower()
                    el_href = await asyncio.wait_for(el.get_attribute("href"), timeout=2.0) or ""
                except asyncio.TimeoutError:
                    log(f"    [CLICK] Candidate {i}: STALE, skipping...")
                    continue
                
                log(f"    [CLICK] Candidate {i}: '{el_text[:30]}' -> {el_href[:60] if el_href else 'no-href'}")
                
                # If it's a link with an external URL, just grab it!
                # Skip: internal paths, hash links, social media, clearly non-booking pages, mailto/tel, images, CDNs
                if el_href:
                    href_lower = el_href.lower()
                    is_internal = el_href.startswith("/") or el_href.startswith("#")
                    is_protocol_link = el_href.startswith("mailto:") or el_href.startswith("tel:")
                    is_social = any(s in href_lower for s in ["facebook", "twitter", "instagram", "youtube", "linkedin"])
                    # Skip navigation pages that aren't booking engines
                    is_bad_page = any(s in href_lower for s in [
                        "/spa", "/conference", "/restaurant", "/dining", "/event", "/wedding", 
                        "/careers", "/contact", "/getaway", "/offer", "/group-booking",
                        "/rooms", "/suites", "/accommodations", "/amenities", "/gallery", "/about",
                        "/room-", "rooms-and-", "rooms_and_", "/packages", "/specials"
                    ])
                    is_image = any(href_lower.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf"])
                    is_cdn = any(cdn in href_lower for cdn in ["cdn.", "cloudfront", "cloudinary", "crowdriff", "imgix", "fastly"])
                    is_external_booking = not is_internal and not is_protocol_link and not is_social and not is_bad_page and not is_image and not is_cdn
                    
                    # Check if it's a known external booking engine domain
                    is_known_booking_engine = False
                    if is_external_booking:
                        href_domain = extract_domain(el_href)
                        for engine_patterns in ENGINE_PATTERNS.values():
                            for pattern in engine_patterns:
                                if pattern in href_domain.lower():
                                    is_known_booking_engine = True
                                    break
                            if is_known_booking_engine:
                                break
                
                # Only skip clicking if it's a known external booking engine
                # Otherwise, click it to capture any JavaScript-triggered navigation
                if el_href and is_external_booking and is_known_booking_engine:
                    log("    [CLICK] Found known booking engine URL in href, using directly")
                    return (None, el_href, "href_extraction")
                
                # Scroll element into view first
                try:
                    await el.scroll_into_view_if_needed(timeout=2000)
                except Exception:
                    pass
                
                # Try clicking
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
                    # If click failed, try JS click as fallback
                    try:
                        await el.evaluate("el => el.click()")
                        await asyncio.sleep(0.5)
                        if page.url != original_url:
                            log(f"    [CLICK] JS click worked, URL: {page.url[:60]}")
                            return (page, page.url, "js_click")
                    except Exception:
                        pass
                    log(f"    [CLICK] Click failed: {str(click_err)[:60]}")
                    continue
                
                # No popup - wait for JS navigation (Brizy and similar builders need more time)
                await asyncio.sleep(1.0)
                
                # Check if URL changed to an external booking engine
                if page.url != original_url:
                    new_url = page.url
                    new_domain = extract_domain(new_url)
                    original_domain = extract_domain(original_url)
                    
                    # Check if we navigated to an external booking engine
                    is_external = new_domain != original_domain
                    is_booking_engine = False
                    for engine_patterns in ENGINE_PATTERNS.values():
                        for pattern in engine_patterns:
                            if pattern in new_domain.lower():
                                is_booking_engine = True
                                break
                        if is_booking_engine:
                            break
                    
                    if is_external and is_booking_engine:
                        log(f"    [CLICK] ✓ Found booking engine: {new_url[:60]}")
                        return (page, new_url, "same_page_navigation")
                    elif is_external:
                        log(f"    [CLICK] ✓ External URL: {new_url[:60]}")
                        return (page, new_url, "same_page_navigation")
                    else:
                        log("    [CLICK] Internal navigation, going back...")
                        try:
                            await page.go_back(timeout=5000)
                            await asyncio.sleep(0.3)
                        except Exception:
                            pass
                        continue
                
                log("    [CLICK] URL unchanged, trying 2nd stage...")
                # Try second-stage click (sidebar might have appeared)
                second_stage = await self._try_second_stage_click(context, page)
                if second_stage:
                    return second_stage
                
                # This candidate didn't work - go back to homepage to try next
                log(f"    [CLICK] Candidate {i} failed, going back...")
                try:
                    await page.goto(original_url, timeout=15000, wait_until="domcontentloaded")
                    await asyncio.sleep(0.3)
                except Exception:
                    pass
                continue
                    
            except Exception as e:
                log(f"    [CLICK] Error: {e}")
                continue
        
        # No candidates worked - return page for iframe scanning as last resort
        return (page if page else None, None, "no_booking_button_found")
    
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
        
        original_url = page.url
        
        # Look for booking buttons that might have appeared
        second_selectors = [
            # Availability first
            "button:has-text('check availability')",
            "a:has-text('check availability')",
            "button:has-text('availability')",
            "a:has-text('availability')",
            # Then book/rates
            "button:has-text('book now')",
            "button:has-text('check rates')",
            "button:has-text('search')",
            "button:has-text('view rates')",
            "a:has-text('book now')",
            "a:has-text('check rates')",
            # Direct booking engine links
            "a[href*='ipms247']",
            "a[href*='synxis']",
            "a[href*='cloudbeds']",
            # Submit buttons
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
                    # Try to get href first (if it's a link)
                    href = await btn.get_attribute("href") or ""
                    if href and href.startswith("http"):
                        log(f"    [2ND STAGE] Found href: {href[:60]}")
                        return (None, href, "two_stage_href")
                    
                    try:
                        async with context.expect_page(timeout=1500) as p_info:
                            await btn.click(force=True, no_wait_after=True)
                        new_page = await p_info.value
                        log(f"    [2ND STAGE] Got popup: {new_page.url[:60]}")
                        return (new_page, new_page.url, "two_stage_popup")
                    except PWTimeoutError:
                        log("    [2ND STAGE] No popup from click")
                        
                        # Check if URL changed (form submission)
                        await asyncio.sleep(0.5)
                        if page.url != original_url:
                            log(f"    [2ND STAGE] URL changed: {page.url[:60]}")
                            return (page, page.url, "two_stage_navigation")
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
        # Support both 'name' and 'hotel' column names
        name = hotel.get("name") or hotel.get("hotel", "")
        website = normalize_url(hotel.get("website", ""))
        
        # Fallback: use domain as name if no name provided
        if not name and website:
            name = extract_domain(website).replace("www.", "").split(".")[0].title()
        
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
        )
        
        if not website:
            # Not an error - just skip silently (no website to check)
            return result
        
        async with self.semaphore:
            result = await self._process_website(result)
        
        return result
    
    async def _process_website(self, result: HotelResult) -> HotelResult:
        """Visit website and extract all data."""
        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            ignore_https_errors=True,  # Some hotel sites have bad SSL certs
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
            
            # 1. Load homepage (don't wait for full load - find buttons ASAP)
            t0 = time.time()
            try:
                # Use shorter timeout and don't wait for network idle
                await page.goto(result.website, timeout=30000, wait_until="domcontentloaded")
            except PWTimeoutError:
                # If domcontentloaded times out, try with just commit
                try:
                    await page.goto(result.website, timeout=15000, wait_until="commit")
                except Exception:
                    pass
            log(f"  [TIME] goto: {time.time()-t0:.1f}s")
            
            # Brief pause to let JS render (but not too long)
            await asyncio.sleep(0.5)
            
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
            
            # Check if booking URL is actually a junk domain (facebook, etc) - mark for retry
            junk_booking_domains = [
                "facebook.com", "instagram.com", "twitter.com", "youtube.com",
                "linkedin.com", "yelp.com", "tripadvisor.com", "google.com",
                "booking.com", "expedia.com", "hotels.com", "airbnb.com", "vrbo.com",
            ]
            if result.booking_url:
                booking_domain = extract_domain(result.booking_url)
                if any(junk in booking_domain for junk in junk_booking_domains):
                    log(f"  Junk booking URL detected: {booking_domain} - marking for retry")
                    result.booking_url = ""
                    result.booking_engine = ""
                    result.booking_engine_domain = ""
                    result.error = "junk_booking_url_retry"
            
            # Mark as error ONLY if we found no booking URL, no known engine, AND no contact info
            has_contact_info = bool(result.phone_website or result.phone_google or result.email)
            if not result.booking_url and result.booking_engine == "unknown" and not has_contact_info:
                result.error = "no_booking_found"
            elif not result.booking_url and result.booking_engine == "unknown" and has_contact_info:
                result.booking_engine = "contact_only"  # Mark as contact-only lead
            
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
        
        # Resume support - get existing successful results to preserve
        hotels, existing_results = self._filter_processed(hotels)
        
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
            
            # Write results (merging with existing successful results)
            await self._write_results(tasks, existing_results)
            
            await browser.close()
    
    def _load_hotels(self, input_csv: str) -> list[dict]:
        """Load hotels from CSV, filtering out big chains and junk sites."""
        hotels = []
        skipped_chains = 0
        skipped_junk = 0
        with open(input_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                website = row.get("website", "").lower()
                # Skip big hotel chains - they have their own booking systems
                if any(chain in website for chain in SKIP_CHAIN_DOMAINS):
                    skipped_chains += 1
                    continue
                # Skip social media and junk domains
                if any(junk in website for junk in SKIP_JUNK_DOMAINS):
                    skipped_junk += 1
                    continue
                hotels.append(row)
        if skipped_chains:
            log(f"Skipped {skipped_chains} big chain hotels (Marriott, Hilton, etc.)")
        if skipped_junk:
            log(f"Skipped {skipped_junk} junk URLs (Facebook, gov sites, etc.)")
        return hotels
    
    def _filter_processed(self, hotels: list[dict]) -> tuple[list[dict], dict]:
        """Filter out already-processed hotels. Returns (remaining, existing_results).
        
        Hotels with errors are NOT filtered out - they can be retried.
        existing_results is a dict of {(name, website): row_dict} for successful results only.
        """
        if not os.path.exists(self.config.output_csv):
            return hotels, {}
        
        # Read existing results, separating successful from failed
        existing_results = {}  # Will contain successful results to preserve
        error_count = 0
        
        with open(self.config.output_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row_name = row.get("name") or row.get("hotel", "")
                key = (row_name, normalize_url(row.get("website", "")))
                
                # Check if this row had an error
                error = row.get("error", "").strip()
                if error:
                    error_count += 1
                    # Don't keep error rows - they'll be retried and updated
                else:
                    existing_results[key] = row
        
        if not existing_results and error_count == 0:
            return hotels, {}
        
        log(f"Found {len(existing_results)} successful, {error_count} with errors (will retry)")
        
        # Only skip successfully processed hotels - errors can be retried
        remaining = [
            h for h in hotels
            if ((h.get("name") or h.get("hotel", "")), normalize_url(h.get("website", ""))) not in existing_results
        ]
        return remaining, existing_results
    
    async def _write_results(self, tasks: list, existing_results: dict):
        """Write results to CSV as they complete, merging with existing successful results."""
        fieldnames = list(HotelResult.__dataclass_fields__.keys())
        
        # Create output directory if needed
        output_dir = os.path.dirname(self.config.output_csv)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # Collect all new results first
        stats = {"processed": 0, "known_engine": 0, "booking_url_found": 0, "contact_only": 0, "errors": 0, "skipped_no_result": 0, "saved": 0}
        new_results = {}  # {(name, website): result_dict}
        
        for coro in asyncio.as_completed(tasks):
            result = await coro
            stats["processed"] += 1
            
            if result.error:
                stats["errors"] += 1
            
            if result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain"):
                stats["known_engine"] += 1
            
            # Count contact_only leads separately
            if result.booking_engine == "contact_only":
                stats["contact_only"] += 1
            
            # Count as hit if we found a booking URL (regardless of engine recognition)
            has_booking_url = result.booking_url and result.booking_url.strip()
            if has_booking_url:
                stats["booking_url_found"] += 1
            
            # Only save if we found a booking URL or a known engine
            has_known_engine = result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "proprietary_or_same_domain")
            
            # Also save errors so we can track/retry them
            has_error = bool(result.error)
            
            if has_booking_url or has_known_engine or has_error:
                key = (result.name, normalize_url(result.website))
                new_results[key] = asdict(result)
                stats["saved"] += 1
            else:
                stats["skipped_no_result"] += 1
                log(f"  Skipped {result.name}: no booking URL or known engine found")
        
        # Merge: existing successful results + new results (new overwrites old errors)
        merged_results = {**existing_results, **new_results}
        
        # Write the complete merged file
        with open(self.config.output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in merged_results.values():
                writer.writerow(row)
        
        self._print_summary(stats)
    
    def _print_summary(self, stats: dict):
        """Print final summary with hit rate."""
        total = stats['processed']
        known = stats['known_engine']
        booking_urls_found = stats['booking_url_found']
        contact_only_found = stats.get('contact_only', 0)
        
        # Hit rate = percentage of hotels where we found a booking URL OR contact info
        hits = booking_urls_found + contact_only_found
        hit_rate = (hits / total * 100) if total > 0 else 0
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("DETECTION COMPLETE!")
        logger.info("=" * 60)
        logger.info(f"Hotels processed:    {total}")
        logger.info(f"Saved to output:      {stats.get('saved', 0)}")
        logger.info(f"Booking URLs found:   {booking_urls_found}")
        logger.info(f"Contact-only leads:   {contact_only_found}")
        logger.info(f"Known engines:        {known}")
        logger.info(f"Skipped (no result):  {stats.get('skipped_no_result', 0)}")
        logger.info(f"Errors:               {stats['errors']}")
        logger.info("-" * 60)
        logger.info(f"HIT RATE:             {hit_rate:.1f}%")
        logger.info("=" * 60)
        logger.info(f"Output: {self.config.output_csv}")
        logger.info(f"Screenshots: {self.config.screenshots_dir}/")


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
    parser = argparse.ArgumentParser(description="Sadie Detector - Booking Engine Detection")
    parser.add_argument("--input", required=True, help="Input CSV with hotels")
    parser.add_argument("--output", default="sadie_leads.csv", help="Output CSV file")
    parser.add_argument("--screenshots-dir", default="screenshots")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--pause", type=float, default=0.5)
    parser.add_argument("--debug", action="store_true", help="Run browser in headed mode + verbose logging")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        raise SystemExit(f"Input file not found: {args.input}")
    
    # --debug implies headed mode
    if args.debug:
        args.headed = True
    
    setup_logging(args.debug)
    
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
