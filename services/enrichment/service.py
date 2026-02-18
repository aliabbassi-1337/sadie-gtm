from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List, Dict, Callable, Any
import asyncio
import html
import json
import os
import re
import time

import httpx
from loguru import logger
from dotenv import load_dotenv
from pydantic import BaseModel
from playwright.async_api import async_playwright

from services.enrichment import repo
from services.enrichment import state_utils
from services.enrichment.room_count_enricher import (
    enrich_hotel_room_count,
    get_llm_api_key,
    log,
)
from services.enrichment.customer_proximity import (
    log as proximity_log,
)
from services.enrichment.website_enricher import WebsiteEnricher
from services.enrichment.archive_scraper import ArchiveScraper, ExtractedBookingData
from services.enrichment.rms_repo import RMSRepo
from services.enrichment.rms_queue import RMSQueue, MockQueue
from lib.rms import RMSHotelRecord, QueueStats

load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
MAX_QUEUE_DEPTH = 1000


# =============================================================================
# RMS Result Models
# =============================================================================

class EnrichResult(BaseModel):
    """Result of RMS enrichment."""
    processed: int
    enriched: int
    failed: int


class EnqueueResult(BaseModel):
    """Result of enqueueing hotels."""
    total_found: int
    enqueued: int
    skipped: bool
    reason: Optional[str] = None


class ConsumeResult(BaseModel):
    """Result of consuming from queue."""
    messages_processed: int
    hotels_processed: int
    hotels_enriched: int
    hotels_failed: int


class SiteMinderEnrichmentResult(BaseModel):
    """Result of enriching a hotel via SiteMinder API."""
    hotel_id: int
    success: bool
    name: Optional[str] = None
    website: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    error: Optional[str] = None

    def to_update_dict(self) -> Dict[str, Any]:
        return {
            "hotel_id": self.hotel_id,
            "name": self.name,
            "website": self.website,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "email": self.email,
            "phone": self.phone,
            "lat": self.lat,
            "lon": self.lon,
        }


class MewsEnrichmentResult(BaseModel):
    """Result of enriching a hotel via Mews API."""
    hotel_id: int
    success: bool
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    error: Optional[str] = None

    def to_update_dict(self) -> Dict[str, Any]:
        return {
            "hotel_id": self.hotel_id,
            "name": self.name,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "email": self.email,
            "phone": self.phone,
            "lat": self.lat,
            "lon": self.lon,
        }


# ============================================================================
# BOOKING PAGE ENRICHMENT (name + address extraction)
# ============================================================================
# ExtractedBookingData is imported from archive_scraper


class BookingPageEnrichmentResult(BaseModel):
    """Result of enriching a hotel from its booking page."""
    success: bool
    name_updated: bool = False
    address_updated: bool = False
    skipped: bool = False
    is_dead: bool = False  # True if URL is 404 (don't retry)


class HotelEnrichmentCandidate(BaseModel):
    """Hotel needing enrichment from booking page."""
    id: int
    name: Optional[str] = None
    booking_url: str
    slug: Optional[str] = None
    engine_name: Optional[str] = None
    needs_name: bool = False
    needs_address: bool = False


class BookingPageEnricher:
    """Extracts hotel name and address from booking engine pages."""
    
    @staticmethod
    def extract_json_ld(html: str) -> Optional[Dict[str, Any]]:
        """Extract JSON-LD structured data from HTML."""
        try:
            pattern = r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
            matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
            
            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                                return item
                    elif data.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                        return data
                    elif "@graph" in data:
                        for item in data["@graph"]:
                            if item.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                                return item
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass
        return None

    @staticmethod
    def parse_address_from_json_ld(json_ld: Dict[str, Any]) -> ExtractedBookingData:
        """Parse address from JSON-LD structured data."""
        name = None
        address = None
        city = None
        state = None
        country = None
        
        if "name" in json_ld:
            name = html.unescape(json_ld["name"].strip())
        
        addr_data = json_ld.get("address", {})
        if isinstance(addr_data, str):
            address = addr_data
        elif isinstance(addr_data, dict):
            address = addr_data.get("streetAddress")
            city = addr_data.get("addressLocality")
            state = addr_data.get("addressRegion")
            country = addr_data.get("addressCountry")
            
            if isinstance(country, dict):
                country = country.get("name") or country.get("@id")
        
        return ExtractedBookingData(
            name=name, address=address, city=city, state=state, country=country
        )

    @staticmethod
    def extract_from_meta_tags(html: str) -> ExtractedBookingData:
        """Extract data from meta tags."""
        name = None
        city = None
        state = None
        country = None
        
        # og:title for name
        match = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not match:
            match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']', html, re.IGNORECASE)
        if match:
            raw = html.unescape(match.group(1).strip())
            parts = re.split(r'\s*[-|–]\s*', raw)
            parsed_name = parts[0].strip()
            if parsed_name.lower() not in ['book now', 'reservation', 'booking', 'home', 'unknown']:
                name = parsed_name
        
        # Fallback to <title>
        if not name:
            match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
            if match:
                raw = html.unescape(match.group(1).strip())
                parts = re.split(r'\s*[-|–]\s*', raw)
                parsed_name = parts[0].strip()
                if parsed_name.lower() not in ['book now', 'reservation', 'booking', 'home', 'unknown']:
                    name = parsed_name
        
        # og:locality / og:region for city/state
        city_match = re.search(r'<meta[^>]+property=["\'](?:og:locality|business:contact_data:locality)["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if city_match:
            city = city_match.group(1).strip()
        
        state_match = re.search(r'<meta[^>]+property=["\'](?:og:region|business:contact_data:region)["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if state_match:
            state = state_match.group(1).strip()
        
        country_match = re.search(r'<meta[^>]+property=["\'](?:og:country-name|business:contact_data:country_name)["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if country_match:
            country = country_match.group(1).strip()
        
        return ExtractedBookingData(name=name, city=city, state=state, country=country)

    async def extract_from_url(
        self,
        client: httpx.AsyncClient,
        booking_url: str,
    ) -> Optional[ExtractedBookingData]:
        """Scrape hotel name and address from booking page."""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            
            resp = await client.get(booking_url, headers=headers, follow_redirects=True, timeout=30.0)
            
            if resp.status_code != 200:
                return None
            
            html = resp.text
            
            # Try Cloudbeds-specific extraction first (richest data with email/phone)
            cloudbeds_data = self.extract_from_cloudbeds(html)
            if cloudbeds_data and (cloudbeds_data.city or cloudbeds_data.address):
                # Also try to get name from other sources since Cloudbeds doesn't include it
                name = None
                json_ld = self.extract_json_ld(html)
                if json_ld and json_ld.get("name"):
                    name = json_ld["name"].strip()
                if not name:
                    meta_data = self.extract_from_meta_tags(html)
                    name = meta_data.name
                
                cloudbeds_data.name = name
                return cloudbeds_data
            
            # Try JSON-LD (most structured)
            json_ld = self.extract_json_ld(html)
            if json_ld:
                data = self.parse_address_from_json_ld(json_ld)
                if data.name or data.city:
                    return data
            
            # Fall back to meta tags
            data = self.extract_from_meta_tags(html)
            if data.name or data.city:
                return data
            
            return None
            
        except Exception:
            return None

    async def extract_from_url_with_archive_fallback(
        self,
        client: httpx.AsyncClient,
        booking_url: str,
    ) -> Optional[ExtractedBookingData]:
        """Extract data from booking page with archive fallback for 404s.
        
        Uses ArchiveScraper to try: live page -> Common Crawl -> Wayback Machine
        """
        scraper = ArchiveScraper(client)
        return await scraper.extract(booking_url, use_archives=True)
    
    async def extract_from_url_with_status(
        self,
        client: httpx.AsyncClient,
        booking_url: str,
    ) -> "ExtractionResult":
        """Extract data from booking page with status (for 404 detection).
        
        Returns ExtractionResult with status:
        - 'success': extracted data successfully
        - 'no_data': page exists but couldn't extract data
        - 'dead': 404 or permanently unavailable (don't retry)
        """
        from services.enrichment.archive_scraper import ExtractionResult
        scraper = ArchiveScraper(client)
        return await scraper.extract_with_status(booking_url, use_archives=True)

    @staticmethod
    def extract_from_cloudbeds(html: str) -> Optional[ExtractedBookingData]:
        """
        Extract address and contact info from Cloudbeds booking pages.
        
        Cloudbeds uses a specific structure with:
        - div[data-testid="property-address-and-contact"]
        - p[data-be-text="true"] for each line
        - mailto: links for email
        
        Typical structure (in order):
        1. Street address
        2. City
        3. "State Country" (space-separated)
        4. Zip code
        5. Contact name (optional)
        6. Phone (optional)
        7. Email in mailto anchor (optional)
        """
        # Check if this is a Cloudbeds page
        if 'data-testid="property-address-and-contact"' not in html and 'cb-address-and-contact' not in html:
            return None
        
        # Extract all text lines from the address container
        # Pattern: <p ... data-be-text="true">content</p>
        text_pattern = r'<p[^>]*data-be-text="true"[^>]*>([^<]*(?:<a[^>]*>([^<]*)</a>[^<]*)?)</p>'
        matches = re.findall(text_pattern, html, re.IGNORECASE | re.DOTALL)
        
        if not matches:
            return None
        
        # Extract clean text values
        lines = []
        for match in matches:
            # match[0] is full content, match[1] is anchor text if present
            text = match[1].strip() if match[1] else match[0].strip()
            if text:
                lines.append(text)
        
        if len(lines) < 3:
            return None
        
        # Parse the lines
        address = None
        city = None
        state = None
        country = None
        zip_code = None
        phone = None
        email = None
        contact_name = None
        
        # Line 0: Street address
        if len(lines) > 0:
            address = lines[0]
        
        # Line 1: City
        if len(lines) > 1:
            city = lines[1]
        
        # Line 2+: Look for "State Country" pattern e.g. "Texas US", "California USA"
        # Enhanced regex to properly extract state and country codes
        state_country_pattern = re.compile(
            r'^([A-Za-z\s]+)\s+(US|USA|AU|UK|CA|NZ|GB|IE|MX|AR|PR|CO|IT|ES|FR|DE|PT|BR|CL|PE|CR|PA)$',
            re.IGNORECASE
        )
        
        # Search lines 2-5 for the "State Country" pattern
        for line in lines[2:6] if len(lines) > 2 else []:
            match = state_country_pattern.match(line.strip())
            if match:
                state = match.group(1).strip()
                country_code = match.group(2).strip().upper()
                country = 'USA' if country_code in ['US', 'USA'] else country_code
                break
        
        # Fallback: original rsplit approach for unrecognized patterns
        if not state and len(lines) > 2:
            state_country = lines[2].strip()
            parts = state_country.rsplit(' ', 1)
            if len(parts) == 2 and len(parts[1]) <= 4:
                state = parts[0].strip()
                country_raw = parts[1].strip().upper()
                if country_raw in ['US', 'USA']:
                    country = 'USA'
                elif len(country_raw) <= 3:
                    country = country_raw
            elif len(parts) == 1:
                state = state_country
        
        # Line 3: Zip code (numeric or alphanumeric like "78006")
        if len(lines) > 3:
            potential_zip = lines[3].strip()
            # Check if it looks like a zip code (mostly digits, 3-10 chars)
            if re.match(r'^[\d\w\-\s]{3,10}$', potential_zip) and any(c.isdigit() for c in potential_zip):
                zip_code = potential_zip
                start_idx = 4
            else:
                # Not a zip, might be contact name
                start_idx = 3
        else:
            start_idx = len(lines)
        
        # Remaining lines: contact info
        for i in range(start_idx, len(lines)):
            line = lines[i].strip()
            
            # Check for email (contains @)
            if '@' in line:
                email = line
            # Check for phone (has digits and common phone chars)
            elif re.match(r'^[\d\-\(\)\s\+\.]{7,20}$', line):
                phone = line
            # Otherwise it might be a contact name
            elif not contact_name and not any(c.isdigit() for c in line):
                contact_name = line
        
        # Also try to extract email from mailto: link if not found
        if not email:
            email_match = re.search(r'href=["\']mailto:([^"\']+)["\']', html)
            if email_match:
                email = email_match.group(1).strip()
        
        return ExtractedBookingData(
            address=address,
            city=city,
            state=state,
            country=country,
            zip_code=zip_code,
            phone=phone,
            email=email,
            contact_name=contact_name,
        )

    @staticmethod
    def needs_name_enrichment(hotel) -> bool:
        """Check if hotel needs name enrichment."""
        name = hotel.name if hasattr(hotel, 'name') else hotel.get("name", "")
        return not name or (isinstance(name, str) and name.startswith("Unknown"))

    @staticmethod
    def needs_address_enrichment(hotel) -> bool:
        """Check if hotel needs address/location enrichment (city, state, or country)."""
        city = hotel.city if hasattr(hotel, 'city') else hotel.get("city", "")
        state = hotel.state if hasattr(hotel, 'state') else hotel.get("state", "")
        country = hotel.country if hasattr(hotel, 'country') else hotel.get("country", "")
        return not city or not state or not country


class IService(ABC):
    """Enrichment Service - Enrich hotel data with room counts and proximity."""

    @abstractmethod
    async def enrich_room_counts(self, limit: int = 100) -> int:
        """
        Get room counts for hotels with websites.
        Uses regex extraction first, then falls back to Groq LLM.
        Tracks status in hotel_room_count table (0=failed, 1=success).

        Args:
            limit: Max hotels to process

        Returns number of hotels successfully enriched.
        """
        pass

    @abstractmethod
    async def calculate_customer_proximity(
        self,
        limit: int = 100,
        max_distance_km: float = 100.0,
    ) -> int:
        """
        Calculate distance to nearest Sadie customer for hotels.
        Updates hotel_customer_proximity table.
        Returns number of hotels processed.
        """
        pass

    @abstractmethod
    async def get_pending_enrichment_count(self) -> int:
        """
        Count hotels waiting for enrichment (has website, not yet processed).
        """
        pass

    @abstractmethod
    async def get_pending_proximity_count(self) -> int:
        """
        Count hotels waiting for proximity calculation.
        """
        pass

    @abstractmethod
    async def enrich_by_coordinates(
        self,
        limit: int = 100,
        sources: list = None,
        concurrency: int = 10,
    ) -> dict:
        """
        Enrich parcel data hotels using Serper Places API.

        For hotels with coordinates but no real names (SF, Maryland parcel data),
        search Places API at those coordinates to find the actual hotel.
        Updates name, website, phone, rating.

        Args:
            limit: Max hotels to process
            sources: Optional list of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
            concurrency: Max concurrent API requests

        Returns dict with enriched/not_found/errors counts.
        """
        pass

    @abstractmethod
    async def get_pending_coordinate_enrichment_count(
        self,
        sources: list = None,
    ) -> int:
        """
        Count hotels waiting for coordinate-based enrichment.

        Args:
            sources: Optional list of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
        """
        pass

    @abstractmethod
    async def geocode_hotels_by_name(
        self,
        limit: int = 100,
        source: str = None,
        concurrency: int = 10,
    ) -> dict:
        """
        Geocode hotels using Serper Places API by hotel name.

        For crawl data hotels with names but no location, search Google Places
        to find their address, city, state, country, coordinates, and phone.

        Args:
            limit: Max hotels to process
            source: Optional source filter (e.g., 'cloudbeds_crawl')
            concurrency: Max concurrent API requests

        Returns dict with enriched/not_found/errors/api_calls counts.
        """
        pass

    @abstractmethod
    async def get_hotels_needing_geocoding_count(self, source: str = None) -> int:
        """Count hotels needing geocoding (have name, missing city)."""
        pass

    # RMS Enrichment
    @abstractmethod
    async def enrich_rms_hotels(self, hotels: List[RMSHotelRecord], concurrency: int = 6, force_overwrite: bool = False) -> EnrichResult:
        """Enrich RMS hotels by scraping their booking pages."""
        pass

    @abstractmethod
    async def enqueue_rms_for_enrichment(self, limit: int = 5000, batch_size: int = 10, force: bool = False) -> EnqueueResult:
        """Find and enqueue RMS hotels needing enrichment."""
        pass

    @abstractmethod
    async def consume_rms_enrichment_queue(self, concurrency: int = 6, max_messages: int = 0, should_stop: Optional[Callable[[], bool]] = None, force_overwrite: bool = False) -> ConsumeResult:
        """Consume and process RMS enrichment queue."""
        pass

    # SiteMinder Enrichment
    @abstractmethod
    async def process_siteminder_hotel(self, hotel_id: int, booking_url: str) -> "SiteMinderEnrichmentResult":
        """Process a single hotel via SiteMinder property API."""
        pass

    @abstractmethod
    async def enqueue_siteminder_for_enrichment(
        self, limit: int = 1000, missing_location: bool = False, country: str = None, dry_run: bool = False,
    ) -> int:
        """Enqueue SiteMinder hotels for SQS-based enrichment."""
        pass

    # Mews Enrichment
    @abstractmethod
    async def process_mews_hotel(self, hotel_id: int, booking_url: str) -> "MewsEnrichmentResult":
        """Process a single hotel via Mews API."""
        pass

    @abstractmethod
    async def enqueue_mews_for_enrichment(
        self, limit: int = 1000, missing_location: bool = False, country: str = None, dry_run: bool = False,
    ) -> int:
        """Enqueue Mews hotels for SQS-based enrichment."""
        pass

    # Cloudbeds Enrichment
    @abstractmethod
    async def enrich_cloudbeds_hotels(self, limit: int = 100, concurrency: int = 6, delay: float = 1.0) -> EnrichResult:
        """Enrich Cloudbeds hotels by scraping their booking pages.
        
        Args:
            limit: Max hotels to process
            concurrency: Concurrent browser contexts
            delay: Seconds between batches (rate limiting, default 1.0)
        """
        pass

    @abstractmethod
    async def get_cloudbeds_enrichment_status(self) -> Dict[str, int]:
        """Get Cloudbeds enrichment status counts."""
        pass

    @abstractmethod
    async def get_cloudbeds_hotels_needing_enrichment(self, limit: int = 100) -> List:
        """Get Cloudbeds hotels needing enrichment for dry-run."""
        pass

    @abstractmethod
    async def batch_update_cloudbeds_enrichment(self, results: List[Dict]) -> int:
        """Batch update Cloudbeds enrichment results."""
        pass

    @abstractmethod
    async def batch_mark_cloudbeds_failed(self, hotel_ids: List[int]) -> int:
        """Mark Cloudbeds hotels as failed (for retry later)."""
        pass

    @abstractmethod
    async def consume_cloudbeds_queue(
        self,
        queue_url: str,
        concurrency: int = 5,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> ConsumeResult:
        """Consume and process Cloudbeds enrichment queue."""
        pass

    # RMS Availability Check
    @abstractmethod
    async def check_rms_availability(
        self,
        limit: int = 100,
        concurrency: int = 30,
        force: bool = False,
        dry_run: bool = False,
        proxy_mode: str = "auto",
    ) -> Dict[str, Any]:
        """Check RMS Australia hotel availability via ibe12 API.

        Args:
            limit: Max hotels to process
            concurrency: Concurrent requests
            force: Re-check all (ignore previous checks)
            dry_run: Don't update database
            proxy_mode: auto, direct, brightdata, free, proxy

        Returns dict with counts: available, no_avail, inconclusive, errors, db_written.
        """
        pass

    @abstractmethod
    async def reset_rms_availability(self) -> int:
        """Reset all Australia RMS availability results to NULL."""
        pass

    @abstractmethod
    async def get_rms_availability_status(self) -> Dict[str, int]:
        """Get availability check stats: total, pending, has_availability, no_availability."""
        pass

    @abstractmethod
    async def verify_rms_availability(
        self,
        sample_size: int = 10,
        proxy_mode: str = "direct",
    ) -> Dict[str, int]:
        """Re-check a random sample of results to verify correctness.

        Returns dict with matches, mismatches counts.
        """
        pass

    # BIG4 Australia
    @abstractmethod
    async def scrape_big4_parks(
        self,
        concurrency: int = 10,
        delay: float = 0.5,
    ) -> Dict[str, Any]:
        """Scrape all BIG4 holiday parks from big4.com.au.

        Discovers parks from state listing pages, scrapes each park page
        for structured data (JSON-LD), and upserts into hotels table.

        Returns dict with discovered/total_big4/with_email/with_phone/with_address counts.
        """
        pass

    @abstractmethod
    async def enrich_big4_websites(
        self,
        delay: float = 1.0,
        limit: int = 0,
    ) -> Dict[str, Any]:
        """Enrich BIG4 parks with real websites via DuckDuckGo search.

        Returns dict with total/found/updated counts.
        """
        pass

    # Owner/GM Enrichment
    @abstractmethod
    async def run_owner_enrichment(
        self,
        hotels: Optional[List[Dict]] = None,
        limit: int = 20,
        concurrency: int = 5,
        layer: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run owner/GM enrichment waterfall.

        Args:
            hotels: Optional pre-fetched hotel list (e.g. from SQS).
                    If None, fetches pending hotels from DB.
            limit: Max hotels to process (ignored if hotels provided)
            concurrency: Max concurrent enrichments
            layer: Optional specific layer to run (e.g. 'rdap', 'website')

        Returns dict with processed/found/contacts/verified counts.
        """
        pass

    @abstractmethod
    async def get_owner_enrichment_stats(self) -> Dict[str, Any]:
        """Get owner enrichment pipeline statistics."""
        pass

    @abstractmethod
    async def get_hotels_pending_owner_enrichment(
        self, limit: int = 100, layer: Optional[int] = None,
    ) -> List[Dict]:
        """Get hotels that need owner enrichment."""
        pass

    @abstractmethod
    async def get_decision_makers_for_hotel(self, hotel_id: int) -> List[Dict]:
        """Get all discovered decision makers for a hotel."""
        pass

class Service(IService):
    def __init__(self, rms_repo: Optional[RMSRepo] = None, rms_queue = None) -> None:
        self._rms_repo = rms_repo or RMSRepo()
        self._rms_queue = rms_queue or RMSQueue()
        self._shutdown_requested = False

    def request_shutdown(self):
        """Request graceful shutdown."""
        self._shutdown_requested = True
        logger.info("Shutdown requested")

    # Garbage values that should not overwrite existing data
    GARBAGE_VALUES = {
        # Empty/whitespace
        '', ' ',
        # Punctuation
        '-', '--', '---', '.', '..', '...',
        # Boolean-like
        'false', 'true',
        # Null-like
        'null', 'none', 'undefined',
        # N/A variants
        'n/a', 'na',
        # Status words
        'unknown', 'error', 'loading', 'test',
        # JavaScript garbage
        '[object object]', 'nan', 'infinity',
        # HTML entities
        '&nbsp;', '&#65279;', '<br>', '<br/>',
        # Placeholder text
        'tbd', 'todo', 'fixme', 'placeholder', 'example', 'sample', 'demo',
        # System/UI names (booking engine garbage)
        'online bookings', 'book now', 'booking engine', 'hotel booking engine',
        'reservation', 'reservations', 'search', 'home',
        # Numeric garbage
        '0', '00', '000', '0.0',
    }
    
    def _is_garbage(self, value: Optional[str], field: str = None) -> bool:
        """Check if a value is garbage and should not be written to DB.
        
        Returns True if the value should be discarded.
        """
        if value is None:
            return True
        if not isinstance(value, str):
            return True
        
        cleaned = value.strip().lower()
        
        # Empty or in garbage list
        if cleaned in self.GARBAGE_VALUES:
            return True
        
        # Too short (except for state which can be 2 chars)
        if field != 'state' and len(cleaned) < 2:
            return True
        
        # State-specific: must be at least 2 chars
        if field == 'state' and len(cleaned) < 2:
            return True
        
        return False
    
    def _sanitize_enrichment_data(self, updates: List[Dict]) -> List[Dict]:
        """Sanitize enrichment data by setting garbage values to None.
        
        Values set to None will be skipped by COALESCE in SQL (keeps existing).
        This is called in Service layer before passing to repo for persistence.
        """
        fields = ['name', 'address', 'city', 'state', 'country', 'phone', 'email', 'website']
        
        for u in updates:
            for field in fields:
                value = u.get(field)
                if self._is_garbage(value, field):
                    u[field] = None
        
        return updates
    
    def _normalize_states_in_batch(self, updates: List[Dict]) -> List[Dict]:
        """Normalize state abbreviations to full names in a batch of updates.
        
        Modifies the 'state' field in each dict using state_utils.normalize_state.
        This is called in Service layer before passing to repo for persistence.
        """
        for u in updates:
            raw_state = u.get("state")
            if raw_state:
                normalized = state_utils.normalize_state(raw_state, u.get("country"))
                if normalized != raw_state:
                    logger.debug(f"State normalized: '{raw_state}' -> '{normalized}'")
                u["state"] = normalized
        return updates

    async def enrich_room_counts(
        self,
        limit: int = 100,
        free_tier: bool = False,
        concurrency: int = 50,
        state: str = None,
        country: str = None,
    ) -> int:
        """
        Get room counts for hotels with websites.
        Uses regex extraction first, then falls back to Groq LLM estimation.
        Tracks status in hotel_room_count table (0=failed, 1=success).

        Args:
            limit: Max hotels to process
            free_tier: If True, use slow sequential mode (30 RPM). Default False (1000 RPM).
            concurrency: Max concurrent requests when not in free_tier mode. Default 50.
            state: Optional state filter (e.g., "California")
            country: Optional country filter (e.g., "United States")

        Returns number of hotels successfully enriched.
        """
        # Check for API key
        if not get_llm_api_key():
            log("Error: AZURE_OPENAI_API_KEY not found in .env")
            return 0

        # Claim hotels for enrichment (multi-worker safe)
        hotels = await repo.claim_hotels_for_enrichment(limit=limit, state=state, country=country)

        if not hotels:
            log("No hotels pending enrichment")
            return 0

        mode = "free tier (sequential)" if free_tier else f"paid tier ({concurrency} concurrent)"
        log(f"Claimed {len(hotels)} hotels for enrichment ({mode})")

        async def process_hotel(client: httpx.AsyncClient, hotel, semaphore: asyncio.Semaphore = None):
            """Process a single hotel, optionally with semaphore."""
            if semaphore:
                async with semaphore:
                    return await self._enrich_single_hotel(client, hotel)
            else:
                result = await self._enrich_single_hotel(client, hotel)
                # Free tier: add delay between requests
                await asyncio.sleep(2.5)
                return result

        enriched_count = 0

        async with httpx.AsyncClient(verify=False, limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)) as client:
            if free_tier:
                # Sequential processing with delays (30 RPM)
                for hotel in hotels:
                    success = await process_hotel(client, hotel)
                    if success:
                        enriched_count += 1
            else:
                # Concurrent processing (1000 RPM)
                semaphore = asyncio.Semaphore(concurrency)
                tasks = [process_hotel(client, hotel, semaphore) for hotel in hotels]
                results = await asyncio.gather(*tasks)
                enriched_count = sum(1 for r in results if r)

        log(f"Enrichment complete: {enriched_count}/{len(hotels)} hotels enriched")
        return enriched_count

    async def _enrich_single_hotel(self, client: httpx.AsyncClient, hotel) -> bool:
        """Enrich a single hotel with room count and contact info. Returns True if successful."""
        room_count, source, discovered_website, phone, email = await enrich_hotel_room_count(
            client=client,
            hotel_id=hotel.id,
            hotel_name=hotel.name,
            website=hotel.website,
            city=hotel.city,
            state=hotel.state,
        )

        # Save discovered website to hotel record (even if room count extraction failed)
        if discovered_website:
            await repo.update_hotel_website_only(hotel.id, discovered_website)

        # Save contact info if discovered (only fills in missing values via COALESCE)
        if phone or email:
            await repo.update_hotel_contact_info(hotel.id, phone=phone, email=email)

        if room_count:
            # Confidence mapping by source
            confidence_map = {
                "regex": Decimal("1.0"),       # Regex from known website
                "llm": Decimal("0.7"),         # LLM from known website content
                "llm_search": Decimal("0.5"),  # LLM estimated website + room count
            }
            confidence = confidence_map.get(source, Decimal("0.5"))
            await repo.insert_room_count(
                hotel_id=hotel.id,
                room_count=room_count,
                source=source,
                confidence=confidence,
                status=1,  # Success
            )
            return True
        else:
            # Insert failure record so we don't retry
            await repo.insert_room_count(
                hotel_id=hotel.id,
                room_count=None,
                source=None,
                confidence=None,
                status=0,  # Failed
            )
            return False

    async def calculate_customer_proximity(
        self,
        limit: int = 100,
        max_distance_km: float = 100.0,
        concurrency: int = 20,
    ) -> int:
        """
        Calculate distance to nearest Sadie customer for hotels.
        Uses PostGIS for efficient spatial queries.
        Runs in parallel with semaphore-controlled concurrency.
        Returns number of hotels with nearby customers found.
        """
        import asyncio

        # Get hotels needing proximity calculation
        hotels = await repo.get_hotels_pending_proximity(limit=limit)

        if not hotels:
            proximity_log("No hotels pending proximity calculation")
            return 0

        proximity_log(f"Processing {len(hotels)} hotels for proximity calculation (concurrency={concurrency})")

        semaphore = asyncio.Semaphore(concurrency)
        processed_count = 0

        async def process_hotel(hotel):
            nonlocal processed_count

            # Skip if hotel has no location
            if hotel.latitude is None or hotel.longitude is None:
                return

            async with semaphore:
                # Find nearest customer using PostGIS
                nearest = await repo.find_nearest_customer(
                    hotel_id=hotel.id,
                    max_distance_km=max_distance_km,
                )

                if nearest:
                    # Insert proximity record with customer
                    await repo.insert_customer_proximity(
                        hotel_id=hotel.id,
                        existing_customer_id=nearest["existing_customer_id"],
                        distance_km=Decimal(str(round(nearest["distance_km"], 1))),
                    )
                    proximity_log(
                        f"  {hotel.name}: nearest customer is {nearest['customer_name']} "
                        f"({round(nearest['distance_km'], 1)}km)"
                    )
                    processed_count += 1
                else:
                    # Insert with NULL to mark as processed (no nearby customer)
                    await repo.insert_customer_proximity_none(hotel_id=hotel.id)
                    proximity_log(f"  {hotel.name}: no customer within {max_distance_km}km")

        # Run all hotels in parallel
        await asyncio.gather(*[process_hotel(h) for h in hotels])

        proximity_log(
            f"Proximity calculation complete: {processed_count}/{len(hotels)} "
            f"hotels have nearby customers"
        )
        return processed_count

    async def get_pending_enrichment_count(
        self,
        state: str = None,
        country: str = None,
    ) -> int:
        """Count hotels waiting for enrichment (has website, not yet in hotel_room_count).
        
        Args:
            state: Optional state filter (e.g., "California")
            country: Optional country filter (e.g., "United States")
        """
        return await repo.get_pending_enrichment_count(state=state, country=country)

    async def get_pending_proximity_count(self) -> int:
        """Count hotels waiting for proximity calculation."""
        return await repo.get_pending_proximity_count()

    async def enrich_websites(
        self,
        limit: int = 100,
        source_filter: str = None,
        state_filter: str = None,
        concurrency: int = 10,
    ) -> dict:
        """
        Find websites for hotels that don't have them via Serper search.

        Runs concurrently with semaphore-controlled parallelism.
        Requires SERPER_API_KEY environment variable.

        Args:
            limit: Max hotels to process
            source_filter: Filter by source (e.g., 'dbpr')
            state_filter: Filter by state (e.g., 'FL')
            concurrency: Max concurrent API requests (default 10)

        Returns:
            Stats dict with found/not_found/errors counts
        """
        if not SERPER_API_KEY:
            log("Error: SERPER_API_KEY not found in environment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0}

        # Claim hotels atomically (multi-worker safe)
        hotels = await repo.claim_hotels_for_website_enrichment(
            limit=limit,
            source_filter=source_filter,
            state_filter=state_filter,
        )

        if not hotels:
            log("No hotels found needing website enrichment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        log(f"Claimed {len(hotels)} hotels for website enrichment (concurrency={concurrency})")

        enricher = WebsiteEnricher(api_key=SERPER_API_KEY, delay_between_requests=0)
        semaphore = asyncio.Semaphore(concurrency)

        found = 0
        not_found = 0
        errors = 0
        skipped_chains = 0
        api_calls = 0
        completed = 0

        async def process_hotel(hotel: dict) -> tuple[str, bool]:
            """Process a single hotel, returns (status, has_website)."""
            nonlocal found, not_found, errors, skipped_chains, api_calls, completed

            async with semaphore:
                result = await enricher.find_website(
                    name=hotel["name"],
                    city=hotel["city"],
                    state=hotel.get("state", "FL"),
                    address=hotel.get("address"),
                )

                if result.website:
                    found += 1
                    api_calls += 1
                    await repo.update_hotel_website(hotel["id"], result.website)
                    # Save location if returned from Serper Places (only if hotel doesn't have one)
                    if result.lat and result.lng:
                        await repo.update_hotel_location_point_if_null(hotel["id"], result.lat, result.lng)
                    await repo.update_website_enrichment_status(
                        hotel["id"], status=1, source="serper"
                    )
                elif result.error == "chain_hotel":
                    skipped_chains += 1
                    # Mark as processed but no API call made
                    await repo.update_website_enrichment_status(
                        hotel["id"], status=0, source="chain_skip"
                    )
                elif result.error == "no_match":
                    not_found += 1
                    api_calls += 1
                    await repo.update_website_enrichment_status(
                        hotel["id"], status=0, source="serper"
                    )
                else:
                    errors += 1
                    api_calls += 1
                    await repo.update_website_enrichment_status(
                        hotel["id"], status=0, source="serper"
                    )

                completed += 1
                if completed % 50 == 0:
                    log(f"  Progress: {completed}/{len(hotels)} ({found} found, {skipped_chains} chains skipped)")

        # Run all hotel enrichments concurrently
        await asyncio.gather(*[process_hotel(h) for h in hotels])

        log(f"Website enrichment complete: {found} found, {not_found} not found, {skipped_chains} chains skipped, {errors} errors")

        return {
            "total": len(hotels),
            "found": found,
            "not_found": not_found,
            "skipped_chains": skipped_chains,
            "errors": errors,
            "api_calls": api_calls,
        }

    async def enrich_locations_only(
        self,
        limit: int = 100,
        source_filter: str = None,
        state_filter: str = None,
        concurrency: int = 10,
    ) -> dict:
        """
        Find locations for hotels that have websites but no coordinates.

        Uses Serper Places API to look up hotel by name/address and get lat/lng.
        Only updates location, never touches the website field.

        Args:
            limit: Max hotels to process
            source_filter: Filter by source (e.g., 'texas_hot')
            state_filter: Filter by state (e.g., 'TX')
            concurrency: Max concurrent API requests (default 10)

        Returns:
            Stats dict with found/not_found/errors counts
        """
        if not SERPER_API_KEY:
            log("Error: SERPER_API_KEY not found in environment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        # Get hotels with website but no location
        hotels = await repo.get_hotels_pending_location_from_places(
            limit=limit,
            source_filter=source_filter,
            state_filter=state_filter,
        )

        if not hotels:
            log("No hotels found needing location enrichment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        log(f"Found {len(hotels)} hotels needing location enrichment (concurrency={concurrency})")

        enricher = WebsiteEnricher(api_key=SERPER_API_KEY, delay_between_requests=0)
        semaphore = asyncio.Semaphore(concurrency)

        found = 0
        not_found = 0
        errors = 0
        api_calls = 0
        completed = 0

        async def process_hotel(hotel: dict) -> None:
            """Process a single hotel for location lookup."""
            nonlocal found, not_found, errors, api_calls, completed

            async with semaphore:
                # Use Serper Places to find location
                # Returns tuple: (website, lat, lng, confidence)
                website, lat, lng, confidence = await enricher.find_website_places(
                    name=hotel["name"],
                    city=hotel["city"],
                    state=hotel.get("state") or "TX",
                    address=hotel.get("address"),
                )
                api_calls += 1

                if lat and lng:
                    found += 1
                    # Only update location, not website
                    await repo.update_hotel_location_point_if_null(
                        hotel["id"], lat, lng
                    )
                    log(f"  {hotel['name']}: found location ({lat}, {lng})")
                else:
                    not_found += 1
                    log(f"  {hotel['name']}: location not found")

                completed += 1
                if completed % 50 == 0:
                    log(f"  Progress: {completed}/{len(hotels)} ({found} found)")

        # Run all hotel lookups concurrently
        await asyncio.gather(*[process_hotel(h) for h in hotels])

        log(f"Location enrichment complete: {found} found, {not_found} not found, {errors} errors")

        return {
            "total": len(hotels),
            "found": found,
            "not_found": not_found,
            "errors": errors,
            "api_calls": api_calls,
        }

    async def enrich_by_coordinates(
        self,
        limit: int = 100,
        sources: list = None,
        concurrency: int = 10,
    ) -> dict:
        """
        Enrich parcel data hotels using Serper Places API.

        For hotels with coordinates but no real names (SF, Maryland parcel data),
        search Places API at those coordinates to find the actual hotel.

        Args:
            limit: Max hotels to process
            sources: Optional list of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
            concurrency: Max concurrent API requests

        Returns dict with enriched/not_found/errors/api_calls counts.
        """
        if not SERPER_API_KEY:
            log("Error: SERPER_API_KEY not found in environment")
            return {"total": 0, "enriched": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        # Get hotels pending enrichment
        hotels = await repo.get_hotels_pending_coordinate_enrichment(
            limit=limit,
            sources=sources,
        )

        if not hotels:
            log("No hotels pending coordinate enrichment")
            return {"total": 0, "enriched": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        log(f"Processing {len(hotels)} hotels for coordinate enrichment (concurrency={concurrency})")

        enricher = WebsiteEnricher(api_key=SERPER_API_KEY, validate_urls=False)
        semaphore = asyncio.Semaphore(concurrency)

        enriched = 0
        not_found = 0
        errors = 0
        api_calls = 0
        completed = 0

        async def process_hotel(hotel: dict) -> None:
            nonlocal enriched, not_found, errors, api_calls, completed

            async with semaphore:
                hotel_id = hotel["id"]
                lat = hotel["latitude"]
                lon = hotel["longitude"]
                category = hotel.get("category", "hotel")
                original_name = hotel["name"]

                try:
                    result = await enricher.find_by_coordinates(lat, lon, category)
                    api_calls += 1

                    if result and result.get("name"):
                        new_name = result["name"]
                        website = result.get("website")
                        phone = result.get("phone")
                        rating = result.get("rating")
                        address = result.get("address")

                        await repo.update_hotel_from_places(
                            hotel_id=hotel_id,
                            name=new_name,
                            website=website,
                            phone=phone,
                            rating=rating,
                            address=address,
                        )
                        enriched += 1
                        log(f"  {original_name[:40]:<40} -> {new_name}{' [website]' if website else ''}")
                    else:
                        not_found += 1
                except Exception as e:
                    errors += 1
                    log(f"  Error processing {original_name}: {e}")

                completed += 1
                if completed % 50 == 0:
                    log(f"  Progress: {completed}/{len(hotels)} ({enriched} enriched)")

        # Run all hotel enrichments concurrently
        await asyncio.gather(*[process_hotel(h) for h in hotels])

        log(f"Coordinate enrichment complete: {enriched} enriched, {not_found} not found, {errors} errors")

        return {
            "total": len(hotels),
            "enriched": enriched,
            "not_found": not_found,
            "errors": errors,
            "api_calls": api_calls,
        }

    async def get_pending_coordinate_enrichment_count(
        self,
        sources: list = None,
    ) -> int:
        """Count hotels waiting for coordinate-based enrichment.

        Args:
            sources: Optional list of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
        """
        return await repo.get_pending_coordinate_enrichment_count(sources=sources)

    # ========================================================================
    # BOOKING PAGE ENRICHMENT (name + address from booking URLs)
    # ========================================================================

    async def get_hotels_needing_booking_page_enrichment(
        self,
        limit: int = 1000,
        engine: Optional[str] = None,
    ) -> List[HotelEnrichmentCandidate]:
        """Get hotels needing name or address enrichment from booking pages.
        
        Args:
            limit: Max hotels to return
            engine: Optional filter by booking engine name
            
        Returns list of HotelEnrichmentCandidate models.
        """
        hotels = await repo.get_hotels_needing_booking_page_enrichment(limit=limit)
        
        if engine:
            hotels = [h for h in hotels if (h.engine_name or "").lower() == engine.lower()]
        
        return hotels

    async def enrich_hotel_from_booking_page(
        self,
        client: httpx.AsyncClient,
        hotel_id: int,
        booking_url: str,
        delay: float = 0.5,
        use_archive_fallback: bool = False,
    ) -> BookingPageEnrichmentResult:
        """Enrich a single hotel from its booking page.
        
        Auto-detects what needs enrichment (name, address, or both).
        Only updates missing fields, preserves existing data.
        
        Args:
            client: httpx AsyncClient for making requests
            hotel_id: Hotel ID to enrich
            booking_url: Booking page URL to scrape
            delay: Delay before request (rate limiting)
            use_archive_fallback: Try Common Crawl/Wayback if live page fails
            
        Returns BookingPageEnrichmentResult.
        """
        # Get current hotel state
        hotel = await repo.get_hotel_by_id(hotel_id)
        if not hotel:
            return BookingPageEnrichmentResult(success=False, skipped=True)
        
        enricher = BookingPageEnricher()
        needs_name = enricher.needs_name_enrichment(hotel)
        needs_address = enricher.needs_address_enrichment(hotel)
        
        if not needs_name and not needs_address:
            return BookingPageEnrichmentResult(success=True, skipped=True)
        
        # Rate limiting (skip for archive since they don't rate limit)
        if not use_archive_fallback:
            await asyncio.sleep(delay)
        
        # Extract data from booking page (with optional archive fallback and 404 detection)
        if use_archive_fallback:
            extraction = await enricher.extract_from_url_with_status(client, booking_url)
            if extraction.status == 'dead':
                # URL is 404 and not in archives - mark as permanently failed
                return BookingPageEnrichmentResult(success=False, is_dead=True)
            data = extraction.data
        else:
            data = await enricher.extract_from_url(client, booking_url)
        
        if not data:
            return BookingPageEnrichmentResult(success=True)
        
        # Update database
        name_to_update = data.name if needs_name and data.name else None
        has_location = data.city or data.state
        
        try:
            if needs_address and has_location:
                await repo.update_hotel_name_and_location(
                    hotel_id=hotel_id,
                    name=name_to_update,
                    address=data.address,
                    city=data.city,
                    state=data.state,
                    country=data.country,
                    phone=data.phone,
                    email=data.email,
                )
                return BookingPageEnrichmentResult(
                    success=True, 
                    name_updated=bool(name_to_update), 
                    address_updated=True
                )
            elif needs_name and data.name:
                await repo.update_hotel_name(hotel_id=hotel_id, name=data.name)
                return BookingPageEnrichmentResult(success=True, name_updated=True)
            else:
                return BookingPageEnrichmentResult(success=True)
        except Exception as e:
            log(f"Failed to update hotel {hotel_id}: {e}")
            return BookingPageEnrichmentResult(success=False)

    async def enrich_from_booking_pages_batch(
        self,
        hotels: List[HotelEnrichmentCandidate],
        delay: float = 0.5,
        concurrency: int = 10,
    ) -> dict:
        """Enrich multiple hotels from their booking pages.
        
        Args:
            hotels: List of HotelEnrichmentCandidate models
            delay: Delay between requests (rate limiting)
            concurrency: Max concurrent requests
            
        Returns stats dict.
        """
        if not hotels:
            return {"total": 0, "names_updated": 0, "addresses_updated": 0, "skipped": 0, "errors": 0}
        
        semaphore = asyncio.Semaphore(concurrency)
        stats = {"total": len(hotels), "names_updated": 0, "addresses_updated": 0, "skipped": 0, "errors": 0}
        completed = 0
        
        async def process(client: httpx.AsyncClient, hotel: HotelEnrichmentCandidate):
            nonlocal completed
            async with semaphore:
                result = await self.enrich_hotel_from_booking_page(
                    client=client,
                    hotel_id=hotel.id,
                    booking_url=hotel.booking_url,
                    delay=delay,
                )
                
                if result.skipped:
                    stats["skipped"] += 1
                elif result.success:
                    if result.name_updated:
                        stats["names_updated"] += 1
                    if result.address_updated:
                        stats["addresses_updated"] += 1
                else:
                    stats["errors"] += 1
                
                completed += 1
                if completed % 100 == 0:
                    log(f"  Progress: {completed}/{len(hotels)} ({stats['names_updated']} names, {stats['addresses_updated']} addresses)")
        
        async with httpx.AsyncClient() as client:
            await asyncio.gather(*[process(client, h) for h in hotels])
        
        return stats

    # =========================================================================
    # GEOCODING BY NAME (Serper Places API)
    # =========================================================================

    async def get_hotels_needing_geocoding_count(
        self, source: str = None, engine: str = None, country: str = None
    ) -> int:
        """Count hotels needing geocoding (have name, missing city/state)."""
        return await repo.get_hotels_needing_geocoding_count(
            source=source, engine=engine, country=country
        )

    async def get_hotels_needing_geocoding(
        self, limit: int = 1000, source: str = None, engine: str = None, country: str = None
    ) -> List[repo.HotelGeocodingCandidate]:
        """Get hotels needing geocoding (have name, missing city/state)."""
        return await repo.get_hotels_needing_geocoding(
            limit=limit, source=source, engine=engine, country=country
        )

    async def geocode_hotels_by_name(
        self,
        limit: int = 100,
        source: str = None,
        engine: str = None,
        country: str = None,
        concurrency: int = 10,
    ) -> dict:
        """
        Geocode hotels using Serper Places API by hotel name.

        For crawl data hotels with names but no location, search Google Places
        to find their address, city, state, country, coordinates, and phone.

        Args:
            limit: Max hotels to process
            source: Optional source filter (e.g., 'cloudbeds_crawl')
            engine: Optional booking engine filter (e.g., 'Cloudbeds', 'RMS Cloud')
            country: Optional country filter (e.g., 'United States')
            concurrency: Max concurrent API requests

        Returns dict with enriched/not_found/errors/api_calls counts.
        """
        from services.enrichment.website_enricher import WebsiteEnricher

        if not SERPER_API_KEY:
            log("Error: SERPER_API_KEY not found in environment")
            return {"total": 0, "enriched": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        # Get hotels needing geocoding
        hotels = await repo.get_hotels_needing_geocoding(
            limit=limit, source=source, engine=engine, country=country
        )

        if not hotels:
            log("No hotels found needing geocoding")
            return {"total": 0, "enriched": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        log(f"Found {len(hotels)} hotels needing geocoding")
        if source:
            log(f"  Filtered by source: {source}")

        stats = {
            "total": len(hotels),
            "enriched": 0,
            "not_found": 0,
            "errors": 0,
            "api_calls": 0,
        }

        semaphore = asyncio.Semaphore(concurrency)
        enricher = WebsiteEnricher(SERPER_API_KEY)
        
        # Collect updates for batch processing
        pending_updates = []

        async def geocode_hotel(hotel: repo.HotelGeocodingCandidate) -> Optional[dict]:
            async with semaphore:
                try:
                    stats["api_calls"] += 1

                    # Search Serper Places by hotel name
                    result = await enricher.find_by_name(hotel.name)

                    if not result:
                        stats["not_found"] += 1
                        return None

                    # Parse location from address if available
                    city = None
                    state = None
                    country = None
                    
                    address = result.get("address")
                    if address:
                        city, state, country = self._parse_address_components(address)

                    return {
                        "hotel_id": hotel.id,
                        "address": address,
                        "city": city,
                        "state": state,
                        "country": country,
                        "latitude": result.get("latitude"),
                        "longitude": result.get("longitude"),
                        "phone": result.get("phone"),
                        "email": result.get("email"),
                    }

                except Exception as e:
                    log(f"Error geocoding hotel {hotel.id}: {e}")
                    stats["errors"] += 1
                    return None

        # Process in batches with transaction-wrapped updates
        batch_size = 100
        for i in range(0, len(hotels), batch_size):
            batch = hotels[i:i + batch_size]
            
            # Geocode batch concurrently
            results = await asyncio.gather(*[geocode_hotel(h) for h in batch])
            
            # Collect successful results
            batch_updates = [r for r in results if r is not None]
            
            # Bulk UPDATE (single atomic query using unnest)
            if batch_updates:
                updated = await repo.batch_update_hotel_geocoding(batch_updates)
                stats["enriched"] += updated
            
            log(f"  Progress: {min(i + batch_size, len(hotels))}/{len(hotels)} "
                f"(enriched: {stats['enriched']}, not_found: {stats['not_found']}, errors: {stats['errors']})")

        log(f"Geocoding complete: {stats['enriched']} enriched, {stats['not_found']} not found, "
            f"{stats['errors']} errors, {stats['api_calls']} API calls")

        return stats

    def _parse_address_components(self, address: str) -> tuple:
        """Parse city, state, country from an address string.
        
        Examples:
            "123 Main St, Miami, FL 33101, USA" -> ("Miami", "FL", "USA")
            "456 Beach Rd, Sydney NSW 2000, Australia" -> ("Sydney", "NSW", "Australia")
        """
        if not address:
            return None, None, None

        # Split by comma
        parts = [p.strip() for p in address.split(",")]
        
        if len(parts) < 2:
            return None, None, None

        city = None
        state = None
        country = None

        # Last part is usually country or state+zip
        last = parts[-1].strip()
        
        # Check if last part is a known country
        if last.upper() in ["USA", "US", "UNITED STATES", "CANADA", "AUSTRALIA", "UK", "UNITED KINGDOM"]:
            country = last
            # Second to last should be state/province + zip
            if len(parts) >= 3:
                state_zip = parts[-2].strip()
                # Extract state code (usually 2 letters before zip)
                state_match = re.match(r"([A-Z]{2,3})\s*\d*", state_zip.upper())
                if state_match:
                    state = state_match.group(1)
                # City is the part before state
                if len(parts) >= 4:
                    city = parts[-3].strip()
                else:
                    city = parts[-2].strip()
        else:
            # Last part might be state+zip with no country
            state_match = re.match(r"([A-Z]{2,3})\s*\d*", last.upper())
            if state_match:
                state = state_match.group(1)
                if len(parts) >= 3:
                    city = parts[-2].strip()
            else:
                # Just try to get city from second to last
                if len(parts) >= 2:
                    city = parts[-2].strip()

        return city, state, country

    # =========================================================================
    # RMS Enrichment
    # =========================================================================

    async def enrich_rms_hotels(self, hotels: List[RMSHotelRecord], concurrency: int = 6, force_overwrite: bool = False) -> EnrichResult:
        """Enrich RMS hotels using fast API with adaptive Brightdata fallback.
        
        Strategy:
        1. Try API first (fast, ~100ms per hotel)
        2. Auto-switch to Brightdata after 3 consecutive failures
        3. Switch back to direct after success
        4. Fall back to Playwright only if API completely fails
        
        Args:
            hotels: List of hotels to enrich
            concurrency: Max concurrent requests
            force_overwrite: If True, overwrite existing data. If False, only fill empty fields.
        
        Uses batch UPDATE at the end for efficiency.
        """
        from lib.rms.api_client import AdaptiveRMSApiClient
        
        processed = enriched = failed = 0
        batch_updates: List[dict] = []
        failed_urls: List[str] = []
        
        semaphore = asyncio.Semaphore(concurrency)
        
        async with AdaptiveRMSApiClient() as api_client:
            async def enrich_one(hotel: RMSHotelRecord) -> tuple[bool, bool, Optional[dict]]:
                """Returns (processed, enriched, update_dict or None)."""
                async with semaphore:
                    url = hotel.booking_url if hotel.booking_url.startswith("http") else f"https://{hotel.booking_url}"
                    
                    # Parse clientId and server from URL
                    # Format 1: /Search/Index/{id}/90/ (bookings.rmscloud.com)
                    # Format 2: /rates/index/{id}/90/ (bookings.rmscloud.com)
                    # Format 3: /search/index/{id} (no trailing number)
                    # Format 4: bookings.rmscloud.com/{slug} (direct slug)
                    # Format 5: ibe12.rmscloud.com/{id} (IBE servers)
                    import re
                    slug = None
                    server = "bookings.rmscloud.com"
                    
                    # Extract server from URL
                    server_match = re.search(r'(bookings\d*\.rmscloud\.com)', url)
                    if server_match:
                        server = server_match.group(1)
                    
                    # Try /Search/Index/ or /rates/index/ format (with or without trailing number)
                    match = re.search(r'/(?:Search|rates)/[Ii]ndex/([^/]+)(?:/\d+)?/?$', url, re.IGNORECASE)
                    if match:
                        slug = match.group(1)
                    else:
                        # Try IBE format: ibe12.rmscloud.com/{numeric_id}
                        ibe_match = re.search(r'(ibe\d+\.rmscloud\.com)/(\d+)', url)
                        if ibe_match:
                            server = ibe_match.group(1)
                            slug = ibe_match.group(2)
                        else:
                            # Try direct slug format: bookings.rmscloud.com/{hex_slug}
                            direct_match = re.search(r'bookings\d*\.rmscloud\.com/([a-f0-9]{16,})$', url)
                            if direct_match:
                                slug = direct_match.group(1)
                    
                    # Skip if we couldn't parse the URL
                    if not slug:
                        logger.debug(f"Could not parse RMS URL: {url}")
                        return (True, False, None)
                    
                    # Try API first (fast)
                    data = await api_client.extract(slug, server)
                    method = "api"
                    
                    # Fall back to HTML if API didn't get enough data
                    if not data or not data.has_data():
                        data = await api_client.extract_from_html(slug, server)
                        method = "html"
                    
                    if data and data.has_data():
                        loc_str = f" @ ({data.latitude}, {data.longitude})" if data.latitude else ""
                        logger.debug(f"Enriched [{method}] {hotel.hotel_id}: {data.name} | {data.city}, {data.state}{loc_str}")
                        return (True, True, {
                            "hotel_id": hotel.hotel_id,
                            "booking_url": hotel.booking_url,
                            "name": data.name,
                            "address": data.address,
                            "city": data.city,
                            "state": data.state,
                            "country": data.country,
                            "phone": data.phone,
                            "email": data.email,
                            "website": data.website,
                            "latitude": data.latitude,
                            "longitude": data.longitude,
                        })
                    else:
                        return (True, False, None)
            
            tasks = [enrich_one(h) for h in hotels]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.debug(f"Enrichment error for {hotels[i].hotel_id}: {result}")
                    failed += 1
                    failed_urls.append(hotels[i].booking_url)
                else:
                    pr, en, update_dict = result
                    processed += 1 if pr else 0
                    if en and update_dict:
                        enriched += 1
                        batch_updates.append(update_dict)
                    elif pr:
                        failed += 1
                        failed_urls.append(hotels[i].booking_url)
        
        # Batch update all results (sanitize garbage, then normalize states)
        if batch_updates or failed_urls:
            self._sanitize_enrichment_data(batch_updates)
            self._normalize_states_in_batch(batch_updates)
            updated = await self._rms_repo.batch_update_enrichment(batch_updates, failed_urls, force_overwrite=force_overwrite)
            logger.info(f"Batch update: {updated} hotels updated, {len(failed_urls)} marked failed")
        
        return EnrichResult(processed=processed, enriched=enriched, failed=failed)

    async def enqueue_rms_for_enrichment(self, limit: int = 5000, batch_size: int = 10, force: bool = False) -> EnqueueResult:
        """Find and enqueue RMS hotels needing enrichment.
        
        Args:
            limit: Max hotels to enqueue
            batch_size: Hotels per SQS message
            force: If True, enqueue ALL hotels regardless of current data state
        """
        stats = self._rms_queue.get_stats()
        logger.info(f"Queue: {stats.pending} pending, {stats.in_flight} in flight")
        if stats.pending > MAX_QUEUE_DEPTH:
            return EnqueueResult(total_found=0, enqueued=0, skipped=True, reason=f"Queue depth exceeds {MAX_QUEUE_DEPTH}")
        hotels = await self._rms_repo.get_hotels_needing_enrichment(limit, force=force)
        logger.info(f"Found {len(hotels)} hotels needing enrichment")
        if not hotels:
            return EnqueueResult(total_found=0, enqueued=0, skipped=False)
        enqueued = self._rms_queue.enqueue_hotels(hotels, batch_size)
        logger.success(f"Enqueued {enqueued} hotels")
        return EnqueueResult(total_found=len(hotels), enqueued=enqueued, skipped=False)

    async def consume_rms_enrichment_queue(self, concurrency: int = 6, max_messages: int = 0, should_stop: Optional[Callable[[], bool]] = None, force_overwrite: bool = False) -> ConsumeResult:
        """Consume and process RMS enrichment queue.
        
        Args:
            concurrency: Max concurrent requests
            max_messages: Max messages to process (0=infinite)
            should_stop: Callback to check if we should stop
            force_overwrite: If True, overwrite existing data. If False, only fill empty fields.
        """
        messages_processed = hotels_processed = hotels_enriched = hotels_failed = 0
        should_stop = should_stop or (lambda: self._shutdown_requested)
        logger.info(f"Starting consumer (concurrency={concurrency}, force_overwrite={force_overwrite})")
        empty_receives = 0
        while not should_stop():
            if max_messages > 0 and messages_processed >= max_messages:
                break
            # Use long-poll receive instead of checking inaccurate queue stats
            # (wait_time_seconds=20 is already set in rms_queue.receive_messages)
            messages = self._rms_queue.receive_messages(min(concurrency, 10))
            if not messages:
                empty_receives += 1
                if empty_receives >= 3:  # 3 * 20s = 60s of empty receives
                    if max_messages > 0:
                        break  # Exit if we have a message limit and queue seems empty
                    if empty_receives % 3 == 0:
                        logger.info("Queue empty, waiting for messages...")
                continue
            empty_receives = 0
            logger.info(f"Processing {len(messages)} messages")
            receipt_handles_to_delete = []
            for msg in messages:
                if should_stop():
                    break
                receipt_handles_to_delete.append(msg.receipt_handle)
                if not msg.hotels:
                    continue
                try:
                    result = await self.enrich_rms_hotels(msg.hotels, concurrency, force_overwrite=force_overwrite)
                    hotels_processed += result.processed
                    hotels_enriched += result.enriched
                    hotels_failed += result.failed
                    messages_processed += 1
                except Exception as e:
                    logger.error(f"Error: {e}")
                    hotels_failed += len(msg.hotels)
            # Batch delete all processed messages
            if receipt_handles_to_delete:
                await asyncio.to_thread(self._rms_queue.delete_messages_batch, receipt_handles_to_delete)
            logger.info(f"Progress: {hotels_processed} processed, {hotels_enriched} enriched")
        return ConsumeResult(messages_processed=messages_processed, hotels_processed=hotels_processed, hotels_enriched=hotels_enriched, hotels_failed=hotels_failed)

    # =========================================================================
    # RMS Query Methods
    # =========================================================================

    async def get_rms_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        return await self._rms_repo.get_hotels_needing_enrichment(limit)

    async def get_rms_stats(self) -> Dict[str, int]:
        return await self._rms_repo.get_stats()

    async def count_rms_needing_enrichment(self) -> int:
        return await self._rms_repo.count_needing_enrichment()

    def get_rms_queue_stats(self) -> QueueStats:
        return self._rms_queue.get_stats()

    # =========================================================================
    # Cloudbeds Enrichment
    # =========================================================================

    async def enrich_cloudbeds_hotels(self, limit: int = 100, concurrency: int = 6, delay: float = 1.0) -> EnrichResult:
        """Enrich Cloudbeds hotels by scraping their booking pages.
        
        Args:
            limit: Max hotels to process
            concurrency: Concurrent browser contexts
            delay: Seconds between batches (rate limiting, default 1.0)
        """
        from lib.browser import BrowserPool
        from lib.cloudbeds import CloudbedsScraper

        candidates = await repo.get_cloudbeds_hotels_needing_enrichment(limit=limit)
        hotels = [{"id": c.id, "booking_url": c.booking_url} for c in candidates]

        if not hotels:
            logger.info("No Cloudbeds hotels need enrichment")
            return EnrichResult(processed=0, enriched=0, failed=0)

        logger.info(f"Found {len(hotels)} Cloudbeds hotels to enrich")

        total_enriched = 0
        total_errors = 0
        results_buffer = []

        async with BrowserPool(concurrency=concurrency) as pool:
            scrapers = [CloudbedsScraper(page) for page in pool.pages]
            logger.info(f"Created {concurrency} browser contexts (delay={delay}s between batches)")

            async def process_hotel(page, hotel):
                scraper = scrapers[pool.pages.index(page)]
                try:
                    data = await scraper.extract(hotel["booking_url"])
                    return (hotel["id"], bool(data), data, None if data else "no_data")
                except Exception as e:
                    return (hotel["id"], False, None, str(e)[:100])

            results = await pool.process_batch(hotels, process_hotel, delay_between_batches=delay)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    total_errors += 1
                    logger.warning(f"  Hotel {hotels[i]['id']}: error - {result}")
                    continue

                hotel_id, success, data, error = result
                if success and data and (data.city or data.name):
                    results_buffer.append({
                        "hotel_id": hotel_id,
                        "name": data.name,
                        "address": data.address,
                        "city": data.city,
                        "state": data.state,
                        "country": data.country,
                        "phone": data.phone,
                        "email": data.email,
                        "zip_code": data.zip_code,
                        "contact_name": data.contact_name,
                    })
                    logger.info(f"  Hotel {hotel_id}: {data.name[:25] if data.name else ''}, {data.city}")
                elif error:
                    total_errors += 1
                    logger.warning(f"  Hotel {hotel_id}: {error}")

        if results_buffer:
            self._sanitize_enrichment_data(results_buffer)
            self._normalize_states_in_batch(results_buffer)
            total_enriched = await repo.batch_update_cloudbeds_enrichment(results_buffer)
            logger.info(f"Updated {total_enriched} hotels")

        return EnrichResult(processed=len(hotels), enriched=total_enriched, failed=total_errors)

    async def enrich_cloudbeds_hotels_api(self, limit: int = 100, concurrency: int = 20, use_brightdata: bool = True) -> EnrichResult:
        """Enrich Cloudbeds hotels using property_info API (no Playwright needed).
        
        This is MUCH faster than browser scraping. Uses simple HTTP POST to get:
        - name, address, city, state, country
        - lat/lng coordinates
        - phone, email
        
        Args:
            limit: Max hotels to process
            concurrency: Concurrent HTTP requests (can be much higher than browser, default 20)
            use_brightdata: Use Brightdata proxy (recommended to avoid blocks)
        """
        from lib.cloudbeds.api_client import CloudbedsApiClient, extract_property_code

        candidates = await repo.get_cloudbeds_hotels_needing_enrichment(limit=limit)
        hotels = [{"id": c.id, "booking_url": c.booking_url} for c in candidates]

        if not hotels:
            logger.info("No Cloudbeds hotels need enrichment")
            return EnrichResult(processed=0, enriched=0, failed=0)

        logger.info(f"Found {len(hotels)} Cloudbeds hotels to enrich via API (concurrency={concurrency})")

        client = CloudbedsApiClient(use_brightdata=use_brightdata)
        semaphore = asyncio.Semaphore(concurrency)
        
        results_buffer = []
        failed_ids = []
        processed = 0

        async def process_hotel(hotel: dict) -> None:
            nonlocal processed
            async with semaphore:
                hotel_id = hotel["id"]
                url = hotel["booking_url"]
                
                property_code = extract_property_code(url)
                if not property_code:
                    logger.debug(f"Hotel {hotel_id}: could not extract property code from {url}")
                    failed_ids.append(hotel_id)
                    processed += 1
                    return
                
                try:
                    data = await client.extract(property_code)
                    processed += 1
                    
                    if data and data.has_data():
                        results_buffer.append({
                            "hotel_id": hotel_id,
                            "name": data.name,
                            "address": data.address,
                            "city": data.city,
                            "state": data.state,
                            "country": data.country,
                            "phone": data.phone,
                            "email": data.email,
                            "lat": data.latitude,
                            "lon": data.longitude,
                            "zip_code": data.zip_code,
                            "contact_name": data.contact_name,
                        })
                        loc = f" @ ({data.latitude:.4f}, {data.longitude:.4f})" if data.has_location() else ""
                        logger.debug(f"Hotel {hotel_id}: {data.name[:30] if data.name else ''} | {data.city}, {data.country}{loc}")
                    else:
                        failed_ids.append(hotel_id)
                        logger.debug(f"Hotel {hotel_id}: no data from API")
                except Exception as e:
                    failed_ids.append(hotel_id)
                    logger.debug(f"Hotel {hotel_id}: API error - {e}")
                    processed += 1

        # Process all hotels concurrently
        await asyncio.gather(*[process_hotel(h) for h in hotels])
        
        # Batch update results (sanitize + normalize in Service layer)
        enriched = 0
        if results_buffer:
            self._sanitize_enrichment_data(results_buffer)
            self._normalize_states_in_batch(results_buffer)
            enriched = await repo.batch_update_cloudbeds_enrichment(results_buffer)
            logger.info(f"Updated {enriched} hotels with Cloudbeds API data")
        
        if failed_ids:
            await repo.batch_set_last_enrichment_attempt(failed_ids)
            logger.info(f"Marked {len(failed_ids)} hotels as failed")

        return EnrichResult(processed=processed, enriched=enriched, failed=len(failed_ids))

    async def get_cloudbeds_enrichment_status(self) -> Dict[str, int]:
        """Get Cloudbeds enrichment status counts."""
        total = await repo.get_cloudbeds_hotels_total_count()
        needing = await repo.get_cloudbeds_hotels_needing_enrichment_count()
        return {
            "total": total,
            "needing_enrichment": needing,
            "already_enriched": total - needing,
        }

    async def get_cloudbeds_hotels_needing_enrichment(self, limit: int = 100) -> List:
        """Get Cloudbeds hotels needing enrichment for dry-run."""
        return await repo.get_cloudbeds_hotels_needing_enrichment(limit=limit)

    async def batch_update_cloudbeds_enrichment(self, results: List[Dict]) -> int:
        """Batch update Cloudbeds enrichment results.
        
        Sanitizes garbage values and normalizes states before persisting.
        """
        self._sanitize_enrichment_data(results)
        self._normalize_states_in_batch(results)
        return await repo.batch_update_cloudbeds_enrichment(results)

    async def batch_mark_cloudbeds_failed(self, hotel_ids: List[int]) -> int:
        """Mark Cloudbeds hotels as failed (for retry later)."""
        return await repo.batch_set_last_enrichment_attempt(hotel_ids)

    async def consume_cloudbeds_queue_api(
        self,
        queue_url: str,
        concurrency: int = 100,
        use_brightdata: bool = True,
        should_stop: Optional[Callable[[], bool]] = None,
        force_overwrite: bool = False,
    ) -> ConsumeResult:
        """Consume Cloudbeds queue using fast API (no Playwright).
        
        This is MUCH faster than the browser-based consumer:
        - No Playwright overhead
        - High concurrency (100+ concurrent API requests)
        - Gets lat/lng directly from API
        - HTTP/2 connection pooling
        - Non-blocking DB writes
        
        Args:
            queue_url: SQS queue URL
            concurrency: Concurrent API requests (default 100)
            use_brightdata: Use Brightdata proxy (recommended)
            should_stop: Callback to check if we should stop
            force_overwrite: Always overwrite existing data (default False)
        """
        from lib.cloudbeds.api_client import CloudbedsApiClient, extract_property_code
        from infra.sqs import receive_messages, delete_messages_batch, get_queue_attributes

        should_stop = should_stop or (lambda: self._shutdown_requested)
        
        messages_processed = 0
        hotels_processed = 0
        hotels_enriched = 0
        hotels_failed = 0
        
        batch_results = []
        batch_failed_ids = []
        BATCH_SIZE = 200  # Larger batches = fewer DB round trips
        VISIBILITY_TIMEOUT = 120  # 2 min to handle large batches
        SQS_FETCH_PARALLEL = min(concurrency // 5, 50)  # Scale with concurrency
        
        # Background task for DB writes (don't block main loop)
        pending_db_tasks = []

        # Use context manager for connection pooling
        async with CloudbedsApiClient(use_brightdata=use_brightdata) as client:
            semaphore = asyncio.Semaphore(concurrency)

            logger.info(f"Starting Cloudbeds API consumer (concurrency={concurrency}, brightdata={use_brightdata}, sqs_parallel={SQS_FETCH_PARALLEL})")

            async def process_message(msg: dict) -> tuple:
                """Process a single SQS message."""
                async with semaphore:
                    body = msg["body"]
                    hotel_id = body.get("hotel_id")
                    booking_url = body.get("booking_url")

                    if not hotel_id or not booking_url:
                        return (msg, None, False, None, "invalid_message")

                    property_code = extract_property_code(booking_url)
                    if not property_code:
                        return (msg, hotel_id, False, None, "no_property_code")

                    try:
                        data = await client.extract(property_code)
                        if data and data.has_data():
                            return (msg, hotel_id, True, data, None)
                        else:
                            return (msg, hotel_id, False, None, "no_data")
                    except Exception as e:
                        return (msg, hotel_id, False, None, str(e)[:50])

            empty_receives = 0
            last_log_time = time.time()
            
            while not should_stop():
                # Receive multiple batches concurrently to maximize throughput
                # SQS limits to 10 messages per request, so fetch multiple batches
                batch_tasks = [
                    asyncio.to_thread(
                        receive_messages,
                        queue_url,
                        max_messages=10,
                        visibility_timeout=VISIBILITY_TIMEOUT,
                        wait_time_seconds=0,  # No wait - poll aggressively when queue has messages
                    )
                    for _ in range(SQS_FETCH_PARALLEL)
                ]
                batch_results_raw = await asyncio.gather(*batch_tasks)
                messages = [m for batch in batch_results_raw if batch for m in batch]

                if not messages:
                    empty_receives += 1
                    if empty_receives >= 3:
                        # Queue might be empty, switch to long polling
                        await asyncio.sleep(2)
                    if empty_receives % 10 == 0:
                        logger.info("Queue empty, waiting for messages...")
                    continue
                
                empty_receives = 0

                # Process all messages concurrently (semaphore limits actual concurrency)
                results = await asyncio.gather(*[process_message(m) for m in messages])

                # Handle results - collect receipt handles for batch delete
                receipt_handles_to_delete = []
                
                for msg, hotel_id, success, data, error in results:
                    receipt_handles_to_delete.append(msg["receipt_handle"])
                    
                    if hotel_id is None:
                        # Invalid message
                        continue

                    hotels_processed += 1
                    messages_processed += 1

                    if success and data:
                        batch_results.append({
                            "hotel_id": hotel_id,
                            "name": data.name,
                            "address": data.address,
                            "city": data.city,
                            "state": data.state,
                            "country": data.country,
                            "phone": data.phone,
                            "email": data.email,
                            "lat": data.latitude,
                            "lon": data.longitude,
                            "zip_code": data.zip_code,
                            "contact_name": data.contact_name,
                        })
                        hotels_enriched += 1
                        loc = f" @ ({data.latitude:.2f}, {data.longitude:.2f})" if data.has_location() else ""
                        logger.debug(f"Hotel {hotel_id}: {data.name[:30] if data.name else ''} | {data.city}{loc}")
                    else:
                        batch_failed_ids.append(hotel_id)
                        hotels_failed += 1
                        logger.debug(f"Hotel {hotel_id}: {error}")

                # Batch delete all messages from queue (fire and forget)
                delete_task = asyncio.create_task(
                    asyncio.to_thread(delete_messages_batch, queue_url, receipt_handles_to_delete)
                )
                pending_db_tasks.append(delete_task)

                # Batch update to database (sanitize + normalize in Service layer)
                if len(batch_results) >= BATCH_SIZE:
                    self._sanitize_enrichment_data(batch_results)
                    self._normalize_states_in_batch(batch_results)
                    updated = await repo.batch_update_cloudbeds_enrichment(batch_results)
                    logger.info(f"Batch update: {updated} hotels | Total: {hotels_processed} processed, {hotels_enriched} enriched")
                    batch_results = []

                if len(batch_failed_ids) >= BATCH_SIZE:
                    failed_to_save = batch_failed_ids[:]
                    batch_failed_ids = []
                    pending_db_tasks.append(
                        asyncio.create_task(repo.batch_set_last_enrichment_attempt(failed_to_save))
                    )

                # Clean up completed tasks periodically
                pending_db_tasks = [t for t in pending_db_tasks if not t.done()]

                # Log throughput every 10 seconds
                now = time.time()
                if now - last_log_time >= 10:
                    elapsed = now - last_log_time
                    rate = hotels_processed / max(1, (now - last_log_time + hotels_processed / 100))
                    logger.info(f"Progress: {hotels_processed} processed, {hotels_enriched} enriched ({hotels_enriched*100//max(1,hotels_processed)}% success), ~{len(messages)} msg/batch")
                    last_log_time = now

            # Wait for all pending DB tasks to complete
            if pending_db_tasks:
                await asyncio.gather(*pending_db_tasks, return_exceptions=True)

            # Final batch update (sanitize + normalize in Service layer)
            if batch_results:
                self._sanitize_enrichment_data(batch_results)
                self._normalize_states_in_batch(batch_results)
                await repo.batch_update_cloudbeds_enrichment(batch_results)
            if batch_failed_ids:
                await repo.batch_set_last_enrichment_attempt(batch_failed_ids)

        return ConsumeResult(
            messages_processed=messages_processed,
            hotels_processed=hotels_processed,
            hotels_enriched=hotels_enriched,
            hotels_failed=hotels_failed,
        )

    async def consume_cloudbeds_queue(
        self,
        queue_url: str,
        concurrency: int = 5,
        should_stop: Optional[Callable[[], bool]] = None,
        force_overwrite: bool = False,
    ) -> ConsumeResult:
        """Consume and process Cloudbeds enrichment queue.
        
        This is the main entry point for the SQS consumer workflow.
        Handles browser lifecycle, message processing, and batch updates.
        
        Args:
            force_overwrite: Always overwrite existing data with API data
        """
        from lib.browser import BrowserPool
        from lib.cloudbeds import CloudbedsScraper
        from infra.sqs import receive_messages, delete_message, get_queue_attributes

        should_stop = should_stop or (lambda: self._shutdown_requested)
        
        messages_processed = 0
        hotels_processed = 0
        hotels_enriched = 0
        hotels_failed = 0
        
        batch_results = []
        batch_failed_ids = []
        BATCH_SIZE = 50
        VISIBILITY_TIMEOUT = 300

        logger.info(f"Starting Cloudbeds consumer (concurrency={concurrency})")

        async with BrowserPool(concurrency=concurrency) as pool:
            scrapers = [CloudbedsScraper(page) for page in pool.pages]
            logger.info(f"Created {concurrency} browser contexts")

            while not should_stop():
                # Receive messages from SQS
                messages = receive_messages(
                    queue_url,
                    max_messages=min(concurrency, 10),
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    wait_time_seconds=10,
                )

                if not messages:
                    logger.debug("No messages, waiting...")
                    continue

                # Parse and validate messages
                valid_messages = []
                for msg in messages:
                    body = msg["body"]
                    hotel_id = body.get("hotel_id")
                    booking_url = body.get("booking_url")

                    if not hotel_id or not booking_url:
                        delete_message(queue_url, msg["receipt_handle"])
                        continue

                    valid_messages.append((msg, hotel_id, booking_url))

                if not valid_messages:
                    continue

                # Process hotels in parallel
                tasks = []
                message_map = {}

                for i, (msg, hotel_id, booking_url) in enumerate(valid_messages[:concurrency]):
                    message_map[hotel_id] = msg
                    tasks.append(self._process_cloudbeds_hotel(scrapers[i], hotel_id, booking_url))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Rate limit: 1 second delay between batches
                await asyncio.sleep(1.0)

                # Handle results
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Task error: {result}")
                        continue

                    hotel_id, success, data, error = result
                    msg = message_map.get(hotel_id)
                    if not msg:
                        continue

                    if success and data:
                        batch_results.append({
                            "hotel_id": hotel_id,
                            "name": data.name,
                            "address": data.address,
                            "city": data.city,
                            "state": data.state,
                            "country": data.country,
                            "phone": data.phone,
                            "email": data.email,
                            "zip_code": getattr(data, "zip_code", None),
                            "contact_name": getattr(data, "contact_name", None),
                        })
                        hotels_enriched += 1

                        parts = []
                        if data.name:
                            parts.append(f"name={data.name[:20]}")
                        if data.city:
                            parts.append(f"city={data.city}")
                        logger.info(f"  Hotel {hotel_id}: {', '.join(parts)}")
                    elif error == "404_not_found":
                        batch_failed_ids.append(hotel_id)
                        hotels_failed += 1
                        logger.warning(f"  Hotel {hotel_id}: 404 - will retry")
                    elif error:
                        logger.warning(f"  Hotel {hotel_id}: {error}")

                    delete_message(queue_url, msg["receipt_handle"])
                    hotels_processed += 1
                    messages_processed += 1

                # Batch update periodically (sanitize + normalize in Service layer)
                if len(batch_results) >= BATCH_SIZE:
                    self._sanitize_enrichment_data(batch_results)
                    self._normalize_states_in_batch(batch_results)
                    updated = await repo.batch_update_cloudbeds_enrichment(batch_results)
                    logger.info(f"Batch update: {updated} hotels")
                    batch_results = []

                if len(batch_failed_ids) >= BATCH_SIZE:
                    marked = await repo.batch_set_last_enrichment_attempt(batch_failed_ids)
                    logger.info(f"Marked {marked} hotels for retry in 7 days")
                    batch_failed_ids = []

                # Log progress
                attrs = get_queue_attributes(queue_url)
                remaining = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Progress: {hotels_processed} processed, {hotels_enriched} enriched, {hotels_failed} failed, ~{remaining} remaining")

            # Final batch flush (sanitize + normalize in Service layer)
            if batch_results:
                self._sanitize_enrichment_data(batch_results)
                self._normalize_states_in_batch(batch_results)
                updated = await repo.batch_update_cloudbeds_enrichment(batch_results)
                logger.info(f"Final batch update: {updated} hotels")

            if batch_failed_ids:
                marked = await repo.batch_set_last_enrichment_attempt(batch_failed_ids)
                logger.info(f"Final batch: {marked} hotels marked for retry in 7 days")

        logger.info(f"Consumer stopped. Total: {hotels_processed} processed, {hotels_enriched} enriched")
        return ConsumeResult(
            messages_processed=messages_processed,
            hotels_processed=hotels_processed,
            hotels_enriched=hotels_enriched,
            hotels_failed=hotels_failed,
        )

    async def _process_cloudbeds_hotel(self, scraper, hotel_id: int, booking_url: str):
        """Process a single Cloudbeds hotel. Returns (hotel_id, success, data, error)."""
        try:
            data = await scraper.extract(booking_url)

            if not data:
                return (hotel_id, False, None, "no_data")

            # Check for garbage data (Cloudbeds homepage or error pages)
            if data.name and data.name.lower() in ['cloudbeds.com', 'cloudbeds', 'book now', 'reservation']:
                return (hotel_id, False, None, "404_not_found")
            if data.city and 'soluções online' in data.city.lower():
                return (hotel_id, False, None, "404_not_found")

            return (hotel_id, True, data, None)

        except Exception as e:
            return (hotel_id, False, None, str(e)[:100])


    # =========================================================================
    # LOCATION NORMALIZATION
    # =========================================================================

    # Country code to full name mapping
    COUNTRY_NAMES = {
        "USA": "United States", "US": "United States", "AU": "Australia",
        "UK": "United Kingdom", "GB": "United Kingdom", "NZ": "New Zealand",
        "DE": "Germany", "FR": "France", "ES": "Spain", "IT": "Italy",
        "MX": "Mexico", "JP": "Japan", "CN": "China", "IN": "India",
        "BR": "Brazil", "AR": "Argentina", "CL": "Chile", "CO": "Colombia",
        "PE": "Peru", "ZA": "South Africa", "EG": "Egypt", "MA": "Morocco",
        "KE": "Kenya", "TH": "Thailand", "VN": "Vietnam", "ID": "Indonesia",
        "MY": "Malaysia", "SG": "Singapore", "PH": "Philippines", "KR": "South Korea",
        "TW": "Taiwan", "HK": "Hong Kong", "AE": "United Arab Emirates",
        "IL": "Israel", "TR": "Turkey", "GR": "Greece", "PT": "Portugal",
        "NL": "Netherlands", "BE": "Belgium", "CH": "Switzerland", "AT": "Austria",
        "SE": "Sweden", "NO": "Norway", "DK": "Denmark", "FI": "Finland",
        "PL": "Poland", "CZ": "Czech Republic", "HU": "Hungary", "RO": "Romania",
        "IE": "Ireland", "PR": "Puerto Rico",
    }

    # US state codes to full names
    US_STATE_NAMES = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
        "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
    }

    # Australian state codes to full names
    AU_STATE_NAMES = {
        "NSW": "New South Wales", "VIC": "Victoria", "QLD": "Queensland",
        "WA": "Western Australia", "SA": "South Australia", "TAS": "Tasmania",
        "ACT": "Australian Capital Territory", "NT": "Northern Territory",
    }

    # =========================================================================
    # SITEMINDER ENRICHMENT
    # =========================================================================

    async def batch_update_siteminder_enrichment(self, updates: List[Dict]) -> int:
        """Batch update hotels with SiteMinder enrichment data."""
        return await repo.batch_update_siteminder_enrichment(updates)

    async def batch_set_siteminder_enrichment_failed(self, hotel_ids: List[int]) -> int:
        """Mark SiteMinder hotels as enrichment failed."""
        return await repo.batch_set_siteminder_enrichment_failed(hotel_ids)

    async def get_siteminder_hotels_needing_enrichment(self, limit: int = 100):
        """Get SiteMinder hotels needing enrichment."""
        return await repo.get_siteminder_hotels_needing_enrichment(limit=limit)

    # =========================================================================
    # MEWS ENRICHMENT
    # =========================================================================

    async def batch_update_mews_enrichment(self, updates: List[Dict]) -> int:
        """Batch update hotels with Mews enrichment data."""
        return await repo.batch_update_mews_enrichment(updates)

    async def get_mews_hotels_needing_enrichment(self, limit: int = 100):
        """Get Mews hotels needing enrichment."""
        return await repo.get_mews_hotels_needing_enrichment(limit=limit)

    async def process_siteminder_hotel(
        self, hotel_id: int, booking_url: str, client=None,
    ) -> "SiteMinderEnrichmentResult":
        """Process a single hotel using the SiteMinder property API."""
        import httpx
        from lib.siteminder.api_client import SiteMinderClient, extract_channel_code

        try:
            if client is None:
                async with SiteMinderClient() as c:
                    data = await c.get_property_data_from_url(booking_url)
            else:
                data = await client.get_property_data_from_url(booking_url)

            if not data or not data.name:
                return SiteMinderEnrichmentResult(hotel_id=hotel_id, success=False, error="no_data")

            return SiteMinderEnrichmentResult(
                hotel_id=hotel_id,
                success=True,
                name=data.name,
                website=data.website,
                address=data.address,
                city=data.city,
                state=data.state,
                country=data.country,
                email=data.email,
                phone=data.phone,
                lat=data.lat,
                lon=data.lon,
            )
        except httpx.TimeoutException:
            return SiteMinderEnrichmentResult(hotel_id=hotel_id, success=False, error="timeout")
        except Exception as e:
            return SiteMinderEnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])

    async def enqueue_siteminder_for_enrichment(
        self,
        limit: int = 1000,
        missing_location: bool = False,
        country: str = None,
        dry_run: bool = False,
    ) -> int:
        """Enqueue SiteMinder hotels for SQS-based enrichment."""
        from infra.sqs import send_messages_batch, get_queue_attributes

        queue_url = os.getenv("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL", "")
        if not queue_url and not dry_run:
            logger.error("SQS_SITEMINDER_ENRICHMENT_QUEUE_URL not set")
            return 0

        # Check queue backlog
        if queue_url and not dry_run:
            attrs = get_queue_attributes(queue_url)
            waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
            if waiting > 100:
                logger.info(f"Skipping enqueue - queue has {waiting} messages")
                return 0

        # Fetch candidates
        if missing_location:
            candidates = await repo.get_siteminder_hotels_missing_location(limit=limit, country=country)
        else:
            candidates = await repo.get_siteminder_hotels_needing_enrichment(limit=limit)

        if not candidates:
            logger.info("No SiteMinder hotels to enqueue")
            return 0

        logger.info(f"Found {len(candidates)} SiteMinder hotels to enqueue")

        if dry_run:
            for h in candidates[:10]:
                logger.info(f"  Would enqueue: {h.id} - {h.booking_url[:50]}...")
            return len(candidates)

        messages = [{"hotel_id": h.id, "booking_url": h.booking_url} for h in candidates]
        sent = send_messages_batch(queue_url, messages)
        logger.info(f"Enqueued {sent}/{len(candidates)} hotels")
        return sent

    # =========================================================================
    # MEWS ENRICHMENT
    # =========================================================================

    async def process_mews_hotel(
        self, hotel_id: int, booking_url: str, client=None,
    ) -> "MewsEnrichmentResult":
        """Process a single hotel using the Mews API client."""
        import httpx
        from lib.mews.api_client import MewsApiClient

        try:
            slug = booking_url.rstrip("/").split("/")[-1]
        except Exception:
            return MewsEnrichmentResult(hotel_id=hotel_id, success=False, error="invalid_url")

        try:
            if client is None:
                c = MewsApiClient(timeout=30.0, use_brightdata=True)
                await c.initialize()
                try:
                    data = await c.extract(slug)
                finally:
                    await c.close()
            else:
                data = await client.extract(slug)

            if not data or not data.is_valid:
                return MewsEnrichmentResult(hotel_id=hotel_id, success=False, error="no_data")

            return MewsEnrichmentResult(
                hotel_id=hotel_id,
                success=True,
                name=data.name,
                address=data.address,
                city=data.city,
                state=data.state,
                country=data.country,
                email=data.email,
                phone=data.phone,
                lat=data.lat,
                lon=data.lon,
            )
        except httpx.TimeoutException:
            # Timeout is NOT "no data" -- return special error so consumer can retry
            return MewsEnrichmentResult(hotel_id=hotel_id, success=False, error="timeout")
        except Exception as e:
            return MewsEnrichmentResult(hotel_id=hotel_id, success=False, error=str(e)[:100])

    async def enqueue_mews_for_enrichment(
        self,
        limit: int = 1000,
        missing_location: bool = False,
        country: str = None,
        dry_run: bool = False,
    ) -> int:
        """Enqueue Mews hotels for SQS-based enrichment."""
        from infra.sqs import send_messages_batch, get_queue_attributes

        queue_url = os.getenv("SQS_MEWS_ENRICHMENT_QUEUE_URL", "")
        if not queue_url and not dry_run:
            logger.error("SQS_MEWS_ENRICHMENT_QUEUE_URL not set")
            return 0

        # Check queue backlog
        if queue_url and not dry_run:
            attrs = get_queue_attributes(queue_url)
            waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
            if waiting > 100:
                logger.info(f"Skipping enqueue - queue has {waiting} messages")
                return 0

        # Fetch candidates
        if missing_location:
            candidates = await repo.get_mews_hotels_missing_location(limit=limit, country=country)
        else:
            candidates = await repo.get_mews_hotels_needing_enrichment(limit=limit)

        if not candidates:
            logger.info("No Mews hotels to enqueue")
            return 0

        logger.info(f"Found {len(candidates)} Mews hotels to enqueue")

        if dry_run:
            for h in candidates[:10]:
                logger.info(f"  Would enqueue: {h.id} - {h.booking_url[:50]}...")
            return len(candidates)

        messages = [{"hotel_id": h.id, "booking_url": h.booking_url} for h in candidates]
        sent = send_messages_batch(queue_url, messages)
        logger.info(f"Enqueued {sent}/{len(candidates)} hotels")
        return sent

    # =========================================================================
    # STATE NORMALIZATION
    # =========================================================================

    def normalize_state(self, state: Optional[str], country: Optional[str] = None) -> Optional[str]:
        """Normalize state abbreviation to full name.
        
        Delegates to state_utils.normalize_state for consistent logic across codebase.
        
        Args:
            state: State value (could be abbreviation or full name)
            country: Country hint (unused currently, but available for future logic)
        
        Returns:
            Full state name if abbreviation found, otherwise original value
        """
        return state_utils.normalize_state(state, country)

    async def normalize_countries_bulk(self, dry_run: bool = False, conn=None) -> dict:
        """Normalize country codes and variations to standard English names.

        Three passes (each a single batch UPDATE):
        1. ISO 2-letter codes (AU -> Australia, etc.)
        2. Native language names and common variations (USA -> United States, etc.)
        3. Garbage values -> NULL

        Returns:
            dict with 'total_fixed' count
        """
        from services.enrichment.country_utils import COUNTRY_CODES, COUNTRY_VARIATIONS, GARBAGE_COUNTRIES

        total = 0
        garbage_list = [g for g in GARBAGE_COUNTRIES if g != "NA"]

        if dry_run:
            code_counts = await repo.count_hotels_by_country_values(list(COUNTRY_CODES.keys()), conn=conn)
            for code, name in COUNTRY_CODES.items():
                count = code_counts.get(code, 0)
                if count > 0:
                    logger.info(f"  [DRY-RUN] {code} -> {name}: {count}")
                    total += count

            var_counts = await repo.count_hotels_by_country_values(list(COUNTRY_VARIATIONS.keys()), conn=conn)
            for old, new in COUNTRY_VARIATIONS.items():
                count = var_counts.get(old, 0)
                if count > 0:
                    logger.info(f"  [DRY-RUN] {old[:40]} -> {new}: {count}")
                    total += count

            garb_counts = await repo.count_hotels_by_country_values(garbage_list, conn=conn)
            for g in garbage_list:
                count = garb_counts.get(g, 0)
                if count > 0:
                    logger.info(f"  [DRY-RUN] '{g}' -> NULL: {count}")
                    total += count

            logger.info(f"[DRY-RUN] Would fix {total} hotels")
        else:
            count = await repo.batch_update_country_values(
                list(COUNTRY_CODES.keys()), list(COUNTRY_CODES.values()), conn=conn,
            )
            if count > 0:
                logger.info(f"  ISO country codes: {count} hotels")
            total += count

            count = await repo.batch_update_country_values(
                list(COUNTRY_VARIATIONS.keys()), list(COUNTRY_VARIATIONS.values()), conn=conn,
            )
            if count > 0:
                logger.info(f"  Country variations: {count} hotels")
            total += count

            count = await repo.batch_null_country_values(garbage_list, conn=conn)
            if count > 0:
                logger.info(f"  Garbage -> NULL: {count} hotels")
            total += count

            logger.success(f"Normalized {total} country values")

        return {"total_fixed": total}

    async def normalize_states_bulk(self, dry_run: bool = False, conn=None) -> dict:
        """Normalize state abbreviations to full names in database.

        Processes each supported country separately with its own state map.
        Uses batch UPDATEs — one per country for abbreviations, one per country for junk.

        Returns:
            dict with 'fixes' (list of tuples) and 'total_fixed' count
        """
        # Global junk values that are never valid states in any country
        GLOBAL_JUNK = [
            '*', '-', '--', '---', '.', '...', '/', 'N/A', 'n/a', 'NA', 'na',
            'none', 'None', 'null', 'NULL', 'TBD', 'tbd',
            'unknown', 'Unknown', 'UNKNOWN', 'test', 'Test', 'TEST',
            '-- Select --', '--Select--', '- Select -', 'Select',
        ]

        COUNTRY_STATE_MAPS = {
            "United States": state_utils.US_STATES,
            "Australia": state_utils.AU_STATES,
            "Canada": state_utils.CA_PROVINCES,
        }

        all_fixes = []
        fixes_by_country = {}   # country -> {old_states: [], new_states: []}
        nulls_by_country = {}   # country -> [junk_states]

        # Phase 1: Read
        for country_name, state_map in COUNTRY_STATE_MAPS.items():
            rows = await repo.get_state_counts_for_country(country_name, conn=conn)
            if not rows:
                continue

            valid_full_names = set(state_map.values())
            logger.info(f"{country_name}: {len(rows)} unique state values")

            old_states = []
            new_states = []
            junk_states = []

            for row in rows:
                state = row['state']
                count = row['cnt']

                if state in valid_full_names:
                    continue

                state_upper = state.strip().upper()
                if state_upper in state_map:
                    new_state = state_map[state_upper]
                    old_states.append(state)
                    new_states.append(new_state)
                    all_fixes.append((state, new_state, country_name, count))
                    logger.info(f"  \"{state}\" -> \"{new_state}\" ({count} hotels)")
                elif not state_utils.is_valid_state(state, country_name):
                    junk_states.append(state)
                    all_fixes.append((state, None, country_name, count))
                    logger.info(f"  Junk \"{state}\" in {country_name} ({count} hotels) -> NULL")

            if old_states:
                fixes_by_country[country_name] = {"old_states": old_states, "new_states": new_states}
            if junk_states:
                nulls_by_country[country_name] = junk_states

        if not all_fixes:
            logger.info("No state normalization needed")
            return {"fixes": [], "total_fixed": 0}

        if dry_run:
            would_fix = sum(c for _, _, _, c in all_fixes)
            logger.info(f"Dry run complete. Would fix {would_fix} hotels.")
            return {"fixes": all_fixes, "total_fixed": 0, "would_fix": would_fix}

        # Phase 2: Write
        total_fixed = 0
        junk_nulled = 0

        # Global junk cleanup first (all countries)
        global_junk_count = await repo.batch_null_global_junk_states(GLOBAL_JUNK, conn=conn)
        if global_junk_count:
            junk_nulled += global_junk_count
            total_fixed += global_junk_count
            logger.info(f"  Global junk states: {global_junk_count} -> NULL")

        # Per-country abbreviation expansion
        for country_name, fix_data in fixes_by_country.items():
            count = await repo.batch_update_state_values(
                country_name, fix_data["old_states"], fix_data["new_states"], conn=conn,
            )
            total_fixed += count
            logger.info(f"  {country_name} abbreviations: {count} hotels")

        # Per-country junk cleanup (country-specific invalid states)
        for country_name, junk_list in nulls_by_country.items():
            count = await repo.batch_null_state_values(country_name, junk_list, conn=conn)
            junk_nulled += count
            total_fixed += count
            logger.info(f"  {country_name} junk states: {count} -> NULL")

        logger.success(f"Normalized {total_fixed} hotels ({junk_nulled} junk states NULLed).")
        return {"fixes": all_fixes, "total_fixed": total_fixed}

    async def infer_locations_bulk(self, dry_run: bool = False, conn=None) -> dict:
        """Infer and fix country/state for misclassified or missing hotels.

        Uses TLD, phone prefix, and address patterns to infer the correct
        country and state. Fetches ALL hotels with signals in a single query,
        then processes in memory. When the inferred country differs from current,
        also extracts the correct state for the new country (or NULLs the old one).

        All fixes are applied in a single batch UPDATE.

        Returns:
            dict with 'country_fixes' count and 'state_fixes' count
        """
        from services.enrichment.location_inference import infer_location

        all_fixes = []

        # Single query — fetch all hotels with at least one signal
        rows = await repo.get_all_hotels_for_location_inference(conn=conn)
        logger.info(f"Scanning {len(rows)} hotels with signals...")

        for row in rows:
            inferred_country, inferred_state, confidence = infer_location(
                website=row["website"],
                phone_google=row["phone_google"],
                phone_website=row["phone_website"],
                address=row["address"],
                current_country=row["country"],
                current_state=row["state"],
            )

            if not inferred_country:
                continue
            if inferred_country == row["country"]:
                continue
            if inferred_country == "US/CA":
                continue
            if confidence < 0.5:
                continue

            all_fixes.append({
                "id": row["id"],
                "new_country": inferred_country,
                "new_state": inferred_state,
                "confidence": confidence,
            })

        if not all_fixes:
            logger.info("No location fixes needed")
            return {"country_fixes": 0, "state_fixes": 0, "details": {}}

        # Log summary by inferred country
        details = {}
        by_country = {}
        for fix in all_fixes:
            c = fix["new_country"]
            by_country[c] = by_country.get(c, 0) + 1

        for c, count in sorted(by_country.items(), key=lambda x: -x[1]):
            prefix = "[DRY-RUN] " if dry_run else ""
            logger.info(f"  {prefix}-> {c}: {count} hotels")
            details[c] = count

        country_fixes = len(all_fixes)
        state_fixes = sum(1 for f in all_fixes if f["new_state"])

        if not dry_run:
            ids = [f["id"] for f in all_fixes]
            countries = [f["new_country"] for f in all_fixes]
            states = [f["new_state"] for f in all_fixes]  # None keeps existing state

            count = await repo.batch_fix_hotel_locations(ids, countries, states, conn=conn)
            logger.info(f"  Batch updated {count} hotels")
            logger.success(f"Inferred {country_fixes} countries, {state_fixes} states")
        else:
            logger.info(f"[DRY-RUN] Would fix {country_fixes} countries, {state_fixes} states")

        return {"country_fixes": country_fixes, "state_fixes": state_fixes, "details": details}

    async def enrich_state_city_from_address_bulk(self, dry_run: bool = False, conn=None) -> dict:
        """Enrich missing state/city by parsing address text.

        Extracts county/state and city from structured address fields
        for hotels that have an address but missing state or city.
        Currently supports: United Kingdom, Australia.

        All fixes applied in a single batch UPDATE within one transaction.

        Returns:
            dict with 'state_fixes' and 'city_fixes' counts
        """
        from services.enrichment.location_inference import extract_state_city_from_address

        SUPPORTED_COUNTRIES = ['United Kingdom', 'Australia', 'United States']

        all_ids = []
        all_states = []
        all_cities = []
        state_count = 0
        city_count = 0

        for country in SUPPORTED_COUNTRIES:
            rows = await repo.get_hotels_for_address_enrichment(country, conn=conn)

            if not rows:
                continue

            logger.info(f"Parsing {len(rows)} {country} addresses for state/city...")

            for row in rows:
                state, city = extract_state_city_from_address(row['address'], country)

                # Reject garbage cities (LLM hallucinations, booking instructions, site names)
                if city and (
                    len(city) > 40
                    or '(' in city
                    or re.search(r'\b(the|a|an|our|is|has|are|was|and|club|site|resort|hotel|motel|park|camping)\b', city, re.I)
                ):
                    city = None

                need_state = state and not row['state']
                need_city = city and not row['city']

                if need_state or need_city:
                    all_ids.append(row['id'])
                    all_states.append(state if need_state else None)
                    all_cities.append(city if need_city else None)
                    if need_state:
                        state_count += 1
                    if need_city:
                        city_count += 1

            # Log summary for this country
            country_state = sum(1 for i, s in enumerate(all_states) if s and i >= len(all_ids) - len(rows))
            country_city = sum(1 for i, c in enumerate(all_cities) if c and i >= len(all_ids) - len(rows))
            if country_state or country_city:
                logger.info(f"  {country}: {country_state} states, {country_city} cities to enrich")

        if not all_ids:
            logger.info("No address enrichment needed")
            return {"state_fixes": 0, "city_fixes": 0}

        if dry_run:
            logger.info(f"[DRY-RUN] Would enrich {state_count} states, {city_count} cities")
        else:
            count = await repo.batch_enrich_hotel_state_city(all_ids, all_states, all_cities, conn=conn)
            logger.info(f"  Batch enriched {count} hotels")
            logger.success(f"Enriched {state_count} states, {city_count} cities from addresses")

        return {"state_fixes": state_count, "city_fixes": city_count}

    def extract_state_from_text(self, text: str) -> Optional[str]:
        """Extract US state from a text string (address, city, etc).
        
        Delegates to state_utils.extract_state_from_text for consistent logic.
        
        Looks for:
        1. Full state names (case insensitive)
        2. State abbreviations with context clues
        
        Returns the full state name if found, None otherwise.
        """
        return state_utils.extract_state_from_text(text)

    def extract_state(self, address: Optional[str], city: Optional[str]) -> Optional[str]:
        """Extract state from address or city field.
        
        Delegates to state_utils.extract_state for consistent logic.
        
        Tries address first (more likely to contain state), then city.
        
        Args:
            address: Hotel address field
            city: Hotel city field
            
        Returns:
            Full state name if found, None otherwise
        """
        return state_utils.extract_state(address, city)

    async def get_normalization_status(self) -> dict:
        """Get counts of data needing location normalization."""
        return await repo.get_normalization_status()

    async def normalize_locations(self, dry_run: bool = False) -> dict:
        """
        Normalize all location data (countries, states).
        
        Returns dict with counts of fixed records.
        """
        stats = {"australian_fixed": 0, "zips_fixed": 0, "countries_fixed": 0, "states_fixed": 0}
        
        # Fix Australian hotels incorrectly in USA
        for code, name in self.AU_STATE_NAMES.items():
            if dry_run:
                continue
            fixed = await repo.fix_australian_state(code, name)
            if fixed > 0:
                logger.info(f"Fixed {fixed} hotels: state={code} -> country=Australia, state={name}")
                stats["australian_fixed"] += fixed
        
        # Fix states with zip codes
        states_with_zips = await repo.get_states_with_zips()
        for old_state in states_with_zips:
            code = old_state.split()[0]
            new_state = self.US_STATE_NAMES.get(code, code)
            if dry_run:
                continue
            fixed = await repo.fix_state_with_zip(old_state, new_state)
            if fixed > 0:
                logger.info(f"Fixed {fixed} hotels: '{old_state}' -> '{new_state}'")
                stats["zips_fixed"] += fixed
        
        # Normalize country codes
        for code, name in self.COUNTRY_NAMES.items():
            if code in ("CA", "SA"):  # Skip ambiguous codes
                continue
            if dry_run:
                continue
            fixed = await repo.normalize_country(code, name)
            if fixed > 0:
                logger.info(f"Normalized {fixed} hotels: country='{code}' -> '{name}'")
                stats["countries_fixed"] += fixed
        
        # Normalize US state codes
        for code, name in self.US_STATE_NAMES.items():
            if dry_run:
                continue
            fixed = await repo.normalize_us_state(code, name)
            if fixed > 0:
                logger.debug(f"Normalized {fixed} hotels: state='{code}' -> '{name}'")
                stats["states_fixed"] += fixed
        
        stats["total"] = sum(stats.values())
        return stats

    # =========================================================================
    # LOCATION ENRICHMENT (reverse geocoding)
    # =========================================================================

    async def get_pending_location_enrichment_count(self) -> int:
        """Count hotels needing location enrichment (have coords, missing city)."""
        return await repo.get_pending_location_enrichment_count()

    async def enrich_locations_reverse_geocode(
        self,
        limit: int = 100,
        concurrency: int = 50,
        use_nominatim: bool = False,
    ) -> dict:
        """
        Reverse geocode hotels that have coordinates but missing city/state.
        
        Uses Serper Places API by default (fast, supports concurrency).
        Falls back to Nominatim if requested (free, 1 req/sec rate limit).
        Normalizes state abbreviations to full names.
        
        Args:
            limit: Max hotels to process
            concurrency: Number of concurrent requests (only for Serper)
            use_nominatim: Use free Nominatim API (slow) instead of Serper
            
        Returns dict with total/enriched/failed counts.
        """
        hotels = await repo.get_hotels_pending_location_enrichment(limit=limit)
        
        if not hotels:
            logger.info("No hotels pending location enrichment")
            return {"total": 0, "enriched": 0, "failed": 0}
        
        logger.info(f"Reverse geocoding {len(hotels)} hotels (concurrency={concurrency})...")
        
        if use_nominatim:
            return await self._enrich_locations_nominatim(hotels)
        else:
            return await self._enrich_locations_serper(hotels, concurrency)

    async def _enrich_locations_nominatim(self, hotels: list) -> dict:
        """Reverse geocode using Nominatim (slow, 1 req/sec)."""
        from services.leadgen.geocoding import reverse_geocode
        
        enriched = 0
        failed = 0
        
        for hotel in hotels:
            hotel_id = hotel['id']
            name = hotel['name']
            lat = hotel['latitude']
            lng = hotel['longitude']
            
            logger.info(f"  {name[:50]} ({lat}, {lng})...")
            
            result = await reverse_geocode(lat, lng)
            
            if result and result.city:
                state = result.state
                if state and state.upper() in self.US_STATE_NAMES:
                    state = self.US_STATE_NAMES[state.upper()]
                
                await repo.update_hotel_location_fields(
                    hotel_id=hotel_id,
                    address=result.address,
                    city=result.city,
                    state=state,
                    country=result.country,
                )
                
                logger.info(f"    -> {result.city}, {state}")
                enriched += 1
            else:
                logger.warning(f"    -> No city found")
                failed += 1
            
            await asyncio.sleep(1.1)  # Nominatim rate limit
        
        logger.info(f"Location enrichment complete: {enriched} enriched, {failed} failed")
        return {"total": len(hotels), "enriched": enriched, "failed": failed}

    async def _enrich_locations_serper(self, hotels: list, concurrency: int = 50) -> dict:
        """Reverse geocode using Serper Places API (fast, concurrent)."""
        from services.enrichment.website_enricher import WebsiteEnricher
        
        api_key = os.getenv("SERPER_API_KEY")
        if not api_key:
            logger.error("SERPER_API_KEY not set, falling back to Nominatim")
            return await self._enrich_locations_nominatim(hotels)
        
        enriched = 0
        failed = 0
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_hotel(enricher: WebsiteEnricher, hotel: dict) -> bool:
            """Process a single hotel with rate limiting."""
            async with semaphore:
                hotel_id = hotel['id']
                name = hotel['name']
                lat = hotel['latitude']
                lng = hotel['longitude']
                country = hotel.get('country', 'United States')
                
                result = await enricher.reverse_geocode(lat, lng)
                
                if result and result.get('city'):
                    # Normalize state using country-aware logic
                    state = result.get('state')
                    result_country = result.get('country') or country
                    
                    if state:
                        # Validate and normalize - rejects garbage like "MEASURE", "XX", etc.
                        state = state_utils.validate_and_normalize_state(state, result_country)
                    
                    await repo.update_hotel_location_fields(
                        hotel_id=hotel_id,
                        address=result.get('address'),
                        city=result.get('city'),
                        state=state,
                        country=result_country,
                    )
                    
                    logger.debug(f"  {name[:40]}: {result.get('city')}, {state}")
                    return True
                else:
                    logger.debug(f"  {name[:40]}: no location found")
                    return False
        
        async with WebsiteEnricher(api_key, max_concurrent=concurrency) as enricher:
            tasks = [process_hotel(enricher, h) for h in hotels]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for r in results:
                if isinstance(r, Exception):
                    logger.error(f"Error: {r}")
                    failed += 1
                elif r:
                    enriched += 1
                else:
                    failed += 1
        
        logger.info(f"Location enrichment complete: {enriched} enriched, {failed} failed")
        return {"total": len(hotels), "enriched": enriched, "failed": failed}

    # =========================================================================
    # STATE EXTRACTION FROM ADDRESS
    # =========================================================================

    async def extract_states_from_address(self, limit: int = 1000, dry_run: bool = False) -> dict:
        """Extract US state from address field for hotels missing state.
        
        Uses regex patterns to find state names and abbreviations in address/city fields.
        Only updates hotels where state could be extracted.
        
        Args:
            limit: Max hotels to process
            dry_run: If True, return matches without updating database
            
        Returns:
            dict with total/matched/updated counts and sample matches for dry_run
        """
        # Get US hotels missing state
        hotels = await repo.get_us_hotels_missing_state(limit=limit)
        
        if not hotels:
            logger.info("No US hotels found missing state")
            return {"total": 0, "matched": 0, "updated": 0}
        
        logger.info(f"Found {len(hotels)} US hotels without state")
        
        # Extract states from address or city
        updates = []
        for hotel in hotels:
            state = self.extract_state(hotel.address, hotel.city)
            if state:
                updates.append({
                    "id": hotel.id,
                    "name": hotel.name,
                    "address": hotel.address,
                    "city": hotel.city,
                    "state": state,
                })
        
        logger.info(f"Found state in address for {len(updates)} hotels")
        
        if dry_run:
            logger.info("Dry run - showing first 20 matches:")
            for u in updates[:20]:
                name = u['name'][:40] if u['name'] else 'N/A'
                addr = u['address'][:60] if u['address'] else 'N/A'
                logger.info(f"  {u['id']}: {name}")
                logger.info(f"      Address: {addr}...")
                logger.info(f"      -> State: {u['state']}")
            return {
                "total": len(hotels),
                "matched": len(updates),
                "updated": 0,
                "samples": updates[:20],
            }
        
        # Batch update
        updated = 0
        if updates:
            updated = await repo.batch_update_extracted_states(updates)
            logger.success(f"Updated {updated} hotels with extracted state")
        
        return {"total": len(hotels), "matched": len(updates), "updated": updated}

    async def infer_state_from_city_bulk(self, dry_run: bool = False, conn=None) -> dict:
        """Infer missing state from city using self-referencing data.

        Builds a lookup of (city, country) -> state from hotels that already
        have both city and state. Only uses unambiguous mappings (one state
        per city+country combo). Then applies to hotels missing state.

        Returns:
            dict with 'total_missing', 'matched', 'updated' counts
        """
        # Step 1: Build reference lookup
        ref_rows = await repo.get_city_state_reference_pairs(conn=conn)

        if not ref_rows:
            logger.info("No city-state reference data found")
            return {"total_missing": 0, "matched": 0, "updated": 0}

        # Build {(city_lower, country): set(states)}
        city_state_map: dict[tuple[str, str], set[str]] = {}
        for row in ref_rows:
            key = (row['city_lower'], row['country'])
            city_state_map.setdefault(key, set()).add(row['state'])

        # Filter to unambiguous only (skip Portland, Springfield, etc.)
        unambiguous = {k: next(iter(v)) for k, v in city_state_map.items() if len(v) == 1}
        ambiguous_count = sum(1 for v in city_state_map.values() if len(v) > 1)
        logger.info(f"City-state lookup: {len(unambiguous)} unambiguous, {ambiguous_count} ambiguous (skipped)")

        # Step 2: Get hotels missing state with city
        missing_rows = await repo.get_hotels_missing_state_with_city(conn=conn)

        if not missing_rows:
            logger.info("No hotels with city but missing state")
            return {"total_missing": 0, "matched": 0, "updated": 0}

        logger.info(f"Found {len(missing_rows)} hotels with city but no state")

        # Step 3: Match against lookup
        ids = []
        states = []
        for row in missing_rows:
            key = (row['city'].lower(), row['country'])
            state = unambiguous.get(key)
            if state:
                ids.append(row['id'])
                states.append(state)

        if not ids:
            logger.info("No city-state matches found")
            return {"total_missing": len(missing_rows), "matched": 0, "updated": 0}

        logger.info(f"Matched {len(ids)} hotels to infer state from city")

        if dry_run:
            logger.info(f"[DRY-RUN] Would set state for {len(ids)} hotels from city lookup")
            # Show sample
            for i in range(min(10, len(ids))):
                row = missing_rows[[r['id'] for r in missing_rows].index(ids[i])]
                logger.info(f"  {ids[i]}: {row['city']} ({row['country']}) -> {states[i]}")
        else:
            count = await repo.batch_set_state_from_city(ids, states, conn=conn)
            logger.info(f"  Batch set state for {count} hotels")
            logger.success(f"Inferred state from city for {len(ids)} hotels")

        return {"total_missing": len(missing_rows), "matched": len(ids), "updated": len(ids) if not dry_run else 0}

    async def cleanup_garbage_cities(self, dry_run: bool = False, conn=None) -> dict:
        """NULL out city values that are LLM hallucinations (long sentences).

        Detects cities >30 chars containing common English articles/prepositions,
        which are telltale signs of hallucinated text from LLM extraction.

        Returns:
            dict with 'cleaned' count
        """
        count = await repo.cleanup_garbage_cities(dry_run=dry_run, conn=conn)
        if dry_run:
            logger.info(f"[DRY-RUN] Would clean {count} garbage city values")
        elif count > 0:
            logger.success(f"Cleaned {count} garbage city values")
        else:
            logger.info("No garbage cities found")
        return {"cleaned": count}

    def send_normalize_trigger(self):
        """Send a normalization trigger message to SQS.

        No-op if SQS_NORMALIZATION_QUEUE_URL is not set.
        Safe to call multiple times — normalization is idempotent.
        """
        queue_url = os.getenv("SQS_NORMALIZATION_QUEUE_URL", "")
        if not queue_url:
            return

        try:
            from infra.sqs import send_message
            send_message(queue_url, {"action": "normalize"})
            logger.info("Sent normalization trigger to SQS")
        except Exception as e:
            logger.warning(f"Failed to send normalization trigger: {e}")

    # ================================================================
    # RMS AVAILABILITY CHECK
    # ================================================================

    async def _process_hotel_availability(
        self,
        hotel: dict,
        semaphore: asyncio.Semaphore,
        proxy_pool,
    ) -> dict:
        """Check availability for a single hotel via ibe12 API."""
        from lib.rms.ibe12 import extract_client_id, get_jwt_cookie, check_availability
        from datetime import datetime, timedelta

        hotel_id = hotel["hotel_id"]
        name = hotel.get("name", "Unknown")
        booking_url = hotel.get("booking_url")

        if not booking_url:
            return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": False, "reason": "no_url"}

        client_id = extract_client_id(booking_url)
        if not client_id:
            return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": False, "reason": "bad_url"}

        async with semaphore:
            async with proxy_pool.create_client() as client:
                jwt_status = await get_jwt_cookie(client, client_id)
                if jwt_status == "not_found":
                    logger.info(f"  - {name}: not found (JWT 404)")
                    return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": False, "reason": "not_found"}
                if jwt_status != "ok":
                    logger.debug(f"  ? {name}: JWT failed")
                    return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": None, "reason": "jwt_failed"}

                arrive = (datetime.now() + timedelta(days=14)).strftime("%m/%d/%Y")
                depart = (datetime.now() + timedelta(days=16)).strftime("%m/%d/%Y")
                has_rooms, n_cats = await check_availability(client, client_id, arrive, depart)

                if has_rooms is None:
                    logger.debug(f"  ? {name}: inconclusive")
                    return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": None, "reason": "inconclusive"}

                if n_cats > 0:
                    if has_rooms:
                        logger.info(f"  + {name}: AVAILABLE ({n_cats} categories)")
                    else:
                        logger.info(f"  + {name}: has categories ({n_cats}), no live rates")
                    return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": True, "reason": "available" if has_rooms else "has_categories"}

                logger.info(f"  - {name}: no categories")
                return {"hotel_id": hotel_id, "hotel_name": name, "has_availability": False, "reason": "no_categories"}

    async def check_rms_availability(
        self,
        limit: int = 100,
        concurrency: int = 30,
        force: bool = False,
        dry_run: bool = False,
        proxy_mode: str = "auto",
    ) -> Dict[str, Any]:
        """Check RMS Australia hotel availability via ibe12 API."""
        from lib.proxy import ProxyPool
        from collections import defaultdict

        proxy_pool = ProxyPool(proxy_mode)
        if proxy_mode in ("free", "auto"):
            await proxy_pool.init_free_proxies()

        hotels = await repo.get_rms_hotels_pending_availability(limit, force)
        if not hotels:
            logger.info("No Australia RMS leads pending availability check")
            return {"available": 0, "no_avail": 0, "inconclusive": 0, "errors": 0, "db_written": 0}

        total = len(hotels)
        mode = "FORCE RECHECK" if force else "pending only"
        logger.info(f"Processing {total} Australia RMS leads ({mode}, concurrency={concurrency})")

        semaphore = asyncio.Semaphore(concurrency)
        processed = 0
        db_written = 0
        errors = 0
        reasons = defaultdict(int)
        counts = {"available": 0, "no_avail": 0, "inconclusive": 0}
        pending_flush = []
        flush_batch_size = 50
        start_time = time.monotonic()

        async def flush_batch(batch):
            nonlocal db_written
            valid = [r for r in batch if r["has_availability"] is not None]
            if not valid:
                return
            hotel_ids = [r["hotel_id"] for r in valid]
            statuses = [r["has_availability"] for r in valid]
            written = await repo.batch_update_rms_availability(hotel_ids, statuses)
            db_written += written

        tasks = {asyncio.ensure_future(self._process_hotel_availability(h, semaphore, proxy_pool)): h for h in hotels}
        for coro in asyncio.as_completed(tasks.keys()):
            result = await coro
            if isinstance(result, Exception):
                errors += 1
                logger.error(f"Hotel check error: {result}")
                continue

            processed += 1
            reasons[result.get("reason", "unknown")] += 1
            ha = result.get("has_availability")
            if ha is True:
                counts["available"] += 1
            elif ha is False:
                counts["no_avail"] += 1
            else:
                counts["inconclusive"] += 1

            if ha is not None and not dry_run:
                pending_flush.append(result)

            if len(pending_flush) >= flush_batch_size:
                batch = pending_flush.copy()
                pending_flush.clear()
                await flush_batch(batch)

            if processed % 100 == 0:
                elapsed = time.monotonic() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (total - processed) / rate if rate > 0 else 0
                logger.info(
                    f"PROGRESS: {processed}/{total} ({processed*100//total}%) | "
                    f"avail={counts['available']} no={counts['no_avail']} skip={counts['inconclusive']} err={errors} | "
                    f"DB written={db_written} | {rate:.0f}/s ETA {eta:.0f}s"
                )

        if pending_flush and not dry_run:
            await flush_batch(pending_flush)
            pending_flush.clear()

        elapsed = time.monotonic() - start_time
        logger.info("=" * 60)
        logger.info("RMS AVAILABILITY ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Leads processed: {processed} in {elapsed:.1f}s ({processed/elapsed:.0f}/s)" if elapsed > 0 else f"Leads processed: {processed}")
        logger.info(f"  Has availability: {counts['available']}")
        logger.info(f"  No availability:  {counts['no_avail']}")
        logger.info(f"  Inconclusive:     {counts['inconclusive']}")
        logger.info(f"  Errors:           {errors}")
        logger.info(f"Breakdown:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            logger.info(f"  {reason:15s}: {count}")
        if not dry_run:
            logger.info(f"Database written: {db_written} leads (incremental)")
        else:
            logger.info("[DRY RUN] No database updates made")
        logger.info("=" * 60)

        result = {**counts, "errors": errors, "db_written": db_written, "reasons": dict(reasons)}

        if not dry_run and db_written > 0:
            verify = await self.verify_rms_availability(sample_size=10, proxy_mode="direct")
            result["verification"] = verify

        return result

    async def reset_rms_availability(self) -> int:
        """Reset all Australia RMS availability results to NULL."""
        count = await repo.reset_rms_availability()
        logger.info(f"Reset {count} availability results to NULL")
        return count

    async def get_rms_availability_status(self) -> Dict[str, int]:
        """Get availability check stats."""
        stats = await repo.get_rms_availability_stats()
        logger.info("=" * 60)
        logger.info("RMS AUSTRALIA AVAILABILITY STATUS")
        logger.info("=" * 60)
        logger.info(f"Total Australia RMS leads: {stats['total']}")
        logger.info(f"Pending checks: {stats['pending']}")
        logger.info(f"Has availability: {stats['has_availability']}")
        logger.info(f"No availability: {stats['no_availability']}")
        logger.info("=" * 60)
        return stats

    async def verify_rms_availability(
        self,
        sample_size: int = 10,
        proxy_mode: str = "direct",
    ) -> Dict[str, int]:
        """Re-check a random sample of results to verify correctness."""
        from lib.proxy import ProxyPool

        samples = await repo.get_rms_verification_samples(sample_size)
        if not samples:
            logger.warning("VERIFY: No results to verify")
            return {"matches": 0, "mismatches": 0}

        proxy_pool = ProxyPool(proxy_mode)
        semaphore = asyncio.Semaphore(10)

        logger.info(f"VERIFY: Re-checking {len(samples)} random hotels...")
        tasks = [self._process_hotel_availability(h, semaphore, proxy_pool) for h in samples]
        recheck = await asyncio.gather(*tasks, return_exceptions=True)

        matches = 0
        mismatches = 0
        for hotel, result in zip(samples, recheck):
            if isinstance(result, Exception):
                logger.warning(f"VERIFY: {hotel['name']}: recheck error: {result}")
                continue
            actual = result.get("has_availability")
            name = hotel.get("name", "Unknown")
            if actual is None:
                logger.warning(f"VERIFY: {name}: recheck inconclusive")
            elif actual == hotel.get("has_availability"):
                matches += 1
                logger.info(f"VERIFY OK: {name}: {'avail' if actual else 'no_avail'} confirmed")
            else:
                mismatches += 1
                logger.error(f"VERIFY MISMATCH: {name}: recheck={'avail' if actual else 'no_avail'}")

        logger.info(f"VERIFY: {matches}/{matches + mismatches} consistent ({mismatches} mismatches)")
        return {"matches": matches, "mismatches": mismatches}

    # =========================================================================
    # BIG4 Australia
    # =========================================================================

    async def scrape_big4_parks(
        self,
        concurrency: int = 10,
        delay: float = 0.5,
    ) -> Dict[str, Any]:
        """Scrape all BIG4 holiday parks and upsert into hotels table."""
        from lib.big4 import Big4Scraper

        async with Big4Scraper(concurrency=concurrency, delay=delay) as scraper:
            parks = await scraper.scrape_all()

        if not parks:
            return {"discovered": 0, "total_big4": 0, "with_email": 0, "with_phone": 0, "with_address": 0}

        logger.info(f"Deduplicating and importing {len(parks)} parks to DB...")

        await repo.upsert_big4_parks(
            names=[p.name for p in parks],
            slugs=[p.slug for p in parks],
            phones=[p.phone for p in parks],
            emails=[p.email for p in parks],
            websites=[p.full_url for p in parks],
            addresses=[p.address for p in parks],
            cities=[p.city for p in parks],
            states=[p.state for p in parks],
            postcodes=[p.postcode for p in parks],
            lats=[p.latitude for p in parks],
            lons=[p.longitude for p in parks],
        )
        total_big4 = await repo.get_big4_count()

        result = {
            "discovered": len(parks),
            "total_big4": total_big4,
            "with_email": sum(1 for p in parks if p.email),
            "with_phone": sum(1 for p in parks if p.phone),
            "with_address": sum(1 for p in parks if p.address),
        }

        logger.info(
            f"BIG4 complete: {result['discovered']} discovered, "
            f"{result['total_big4']} total in DB, "
            f"{result['with_email']} with email, "
            f"{result['with_phone']} with phone"
        )

        return result

    async def enrich_big4_websites(
        self,
        delay: float = 1.0,
        limit: int = 0,
    ) -> Dict[str, Any]:
        """Enrich BIG4 parks with real websites via DuckDuckGo search."""
        from lib.big4.scraper import lookup_websites
        from db.client import get_conn

        async with get_conn() as conn:
            rows = await conn.fetch("""
                SELECT id, name FROM sadie_gtm.hotels
                WHERE (external_id_type = 'big4' OR source LIKE '%::big4')
                ORDER BY name
            """)

        parks = [dict(r) for r in rows]
        if limit:
            parks = parks[:limit]

        logger.info(f"Looking up websites for {len(parks)} BIG4 parks...")

        results = await lookup_websites(parks, delay=delay)

        if results:
            hotel_ids = [r[0] for r in results]
            websites = [r[1] for r in results]
            await repo.update_big4_websites(hotel_ids, websites)

        return {
            "total": len(parks),
            "found": len(results),
        }

    # =========================================================================
    # Owner/GM Enrichment
    # =========================================================================

    async def run_owner_enrichment(
        self,
        hotels: Optional[List[Dict]] = None,
        limit: int = 20,
        concurrency: int = 5,
        layer: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run owner/GM enrichment waterfall."""
        from services.enrichment.owner_enricher import enrich_batch
        from services.enrichment.owner_models import (
            LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
            LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
            LAYER_GOV_DATA, LAYER_CT_CERTS, LAYER_ABN_ASIC,
        )

        layer_map = {
            "ct-certs": LAYER_CT_CERTS,
            "rdap": LAYER_RDAP,
            "whois-history": LAYER_WHOIS_HISTORY,
            "dns": LAYER_DNS,
            "website": LAYER_WEBSITE,
            "reviews": LAYER_REVIEWS,
            "gov-data": LAYER_GOV_DATA,
            "email-verify": LAYER_EMAIL_VERIFY,
            "abn-asic": LAYER_ABN_ASIC,
        }

        layer_mask = layer_map.get(layer, 0x1FF) if layer else 0x1FF

        if hotels is None:
            layer_filter = layer_mask if layer and layer != "all" else None
            hotels = await repo.get_hotels_pending_owner_enrichment(
                limit=limit, layer=layer_filter,
            )

        if not hotels:
            logger.info("No hotels pending owner enrichment")
            return {"processed": 0, "found": 0, "contacts": 0, "verified": 0}

        logger.info(f"Owner enrichment: {len(hotels)} hotels, concurrency={concurrency}")

        results = await enrich_batch(
            hotels=hotels, concurrency=concurrency, layers=layer_mask,
        )

        # Persist results (service layer handles all DB writes)
        saved_count = await self._persist_owner_results(results)

        found = sum(1 for r in results if r.found_any)
        total_contacts = sum(len(r.decision_makers) for r in results)
        verified = sum(
            sum(1 for dm in r.decision_makers if dm.email_verified)
            for r in results
        )

        logger.info(
            f"Owner enrichment done: {len(results)} processed, "
            f"{found} with contacts, {saved_count} saved"
        )

        return {
            "processed": len(results),
            "found": found,
            "contacts": total_contacts,
            "verified": verified,
            "saved": saved_count,
            "results": results,
        }


    async def _persist_owner_results(self, results: list) -> int:
        """Internal: persist owner enrichment results to DB."""
        saved_count = 0
        for result in results:
            # Cache domain intel
            if result.domain_intel:
                if result.domain_intel.registrant_name or result.domain_intel.registrant_org:
                    await repo.cache_domain_intel(result.domain_intel)
                if result.domain_intel.email_provider or result.domain_intel.mx_records:
                    await repo.cache_dns_intel(result.domain_intel)
                if result.domain_intel.ct_org_name or result.domain_intel.ct_cert_count:
                    await repo.cache_cert_intel(result.domain_intel)

            # Insert decision makers
            if result.decision_makers:
                count = await repo.batch_insert_decision_makers(
                    result.hotel_id, result.decision_makers,
                )
                saved_count += count
                status = 1  # complete
            else:
                status = 2  # no_results

            await repo.update_owner_enrichment_status(
                result.hotel_id, status, result.layers_completed,
            )

        return saved_count

    async def get_owner_enrichment_stats(self) -> Dict[str, Any]:
        """Get owner enrichment pipeline statistics."""
        return await repo.get_owner_enrichment_stats()

    async def get_hotels_pending_owner_enrichment(
        self, limit: int = 100, layer: Optional[int] = None,
    ) -> List[Dict]:
        """Get hotels that need owner enrichment."""
        return await repo.get_hotels_pending_owner_enrichment(limit=limit, layer=layer)

    async def get_decision_makers_for_hotel(self, hotel_id: int) -> List[Dict]:
        """Get all discovered decision makers for a hotel."""
        return await repo.get_decision_makers_for_hotel(hotel_id)
