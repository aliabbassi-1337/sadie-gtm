from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List, Dict, Callable, Any
import asyncio
import json
import os
import re

import httpx
from loguru import logger
from dotenv import load_dotenv
from pydantic import BaseModel
from playwright.async_api import async_playwright, BrowserContext
from playwright_stealth import Stealth

from services.enrichment import repo
from services.enrichment.room_count_enricher import (
    enrich_hotel_room_count,
    get_groq_api_key,
    log,
)
from services.enrichment.customer_proximity import (
    log as proximity_log,
)
from services.enrichment.website_enricher import WebsiteEnricher
from services.enrichment.archive_scraper import ArchiveScraper, ExtractedBookingData
from services.enrichment.rms_repo import RMSRepo
from services.enrichment.rms_queue import RMSQueue, MockQueue
from lib.rms import RMSScraper, RMSHotelRecord, QueueStats

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
            name = json_ld["name"].strip()
        
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
            raw = match.group(1).strip()
            parts = re.split(r'\s*[-|–]\s*', raw)
            parsed_name = parts[0].strip()
            if parsed_name.lower() not in ['book now', 'reservation', 'booking', 'home', 'unknown']:
                name = parsed_name
        
        # Fallback to <title>
        if not name:
            match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
            if match:
                raw = match.group(1).strip()
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
    async def enrich_rms_hotels(self, hotels: List[RMSHotelRecord], concurrency: int = 6) -> EnrichResult:
        """Enrich RMS hotels by scraping their booking pages."""
        pass

    @abstractmethod
    async def enqueue_rms_for_enrichment(self, limit: int = 5000, batch_size: int = 10) -> EnqueueResult:
        """Find and enqueue RMS hotels needing enrichment."""
        pass

    @abstractmethod
    async def consume_rms_enrichment_queue(self, concurrency: int = 6, max_messages: int = 0, should_stop: Optional[Callable[[], bool]] = None) -> ConsumeResult:
        """Consume and process RMS enrichment queue."""
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


class Service(IService):
    def __init__(self, rms_repo: Optional[RMSRepo] = None, rms_queue = None) -> None:
        self._rms_repo = rms_repo or RMSRepo()
        self._rms_queue = rms_queue or RMSQueue()
        self._shutdown_requested = False

    def request_shutdown(self):
        """Request graceful shutdown."""
        self._shutdown_requested = True
        logger.info("Shutdown requested")

    async def enrich_room_counts(
        self,
        limit: int = 100,
        free_tier: bool = False,
        concurrency: int = 15,
    ) -> int:
        """
        Get room counts for hotels with websites.
        Uses regex extraction first, then falls back to Groq LLM estimation.
        Tracks status in hotel_room_count table (0=failed, 1=success).

        Args:
            limit: Max hotels to process
            free_tier: If True, use slow sequential mode (30 RPM). Default False (1000 RPM).
            concurrency: Max concurrent requests when not in free_tier mode. Default 15.

        Returns number of hotels successfully enriched.
        """
        # Check for API key
        if not get_groq_api_key():
            log("Error: ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY not found in .env")
            return 0

        # Claim hotels for enrichment (multi-worker safe)
        hotels = await repo.claim_hotels_for_enrichment(limit=limit)

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

        async with httpx.AsyncClient(verify=False) as client:
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
        """Enrich a single hotel with room count. Returns True if successful."""
        room_count, source = await enrich_hotel_room_count(
            client=client,
            hotel_id=hotel.id,
            hotel_name=hotel.name,
            website=hotel.website,
        )

        if room_count:
            confidence = Decimal("1.0") if source == "regex" else Decimal("0.7")
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

    async def get_pending_enrichment_count(self) -> int:
        """Count hotels waiting for enrichment (has website, not yet in hotel_room_count)."""
        return await repo.get_pending_enrichment_count()

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

    async def get_hotels_needing_geocoding_count(self, source: str = None) -> int:
        """Count hotels needing geocoding (have name, missing city)."""
        return await repo.get_hotels_needing_geocoding_count(source=source)

    async def get_hotels_needing_geocoding(
        self, limit: int = 1000, source: str = None
    ) -> List[repo.HotelGeocodingCandidate]:
        """Get hotels needing geocoding (have name, missing city)."""
        return await repo.get_hotels_needing_geocoding(limit=limit, source=source)

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
        from services.enrichment.website_enricher import WebsiteEnricher

        if not SERPER_API_KEY:
            log("Error: SERPER_API_KEY not found in environment")
            return {"total": 0, "enriched": 0, "not_found": 0, "errors": 0, "api_calls": 0}

        # Get hotels needing geocoding
        hotels = await repo.get_hotels_needing_geocoding(limit=limit, source=source)

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

    async def enrich_rms_hotels(self, hotels: List[RMSHotelRecord], concurrency: int = 6) -> EnrichResult:
        """Enrich RMS hotels by scraping their booking pages."""
        processed = enriched = failed = 0
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            contexts: List[BrowserContext] = []
            scrapers: List[RMSScraper] = []
            for _ in range(concurrency):
                ctx = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                page = await ctx.new_page()
                stealth = Stealth()
                await stealth.apply_stealth_async(page)
                contexts.append(ctx)
                scrapers.append(RMSScraper(page))
            semaphore = asyncio.Semaphore(concurrency)

            async def enrich_one(hotel: RMSHotelRecord, idx: int) -> tuple[bool, bool]:
                async with semaphore:
                    # Rate limit: stagger requests to avoid hammering RMS servers
                    # With 6 concurrent + 1s delay, we get ~6 requests per 6-7 seconds
                    await asyncio.sleep(idx % concurrency)  # Stagger initial requests
                    
                    scraper = scrapers[idx % len(scrapers)]
                    url = hotel.booking_url if hotel.booking_url.startswith("http") else f"https://{hotel.booking_url}"
                    slug = url.split("/")[-1]
                    data = await scraper.extract(url, slug)
                    if data and data.has_data():
                        await self._rms_repo.update_hotel(hotel_id=hotel.hotel_id, name=data.name, address=data.address, city=data.city, state=data.state, country=data.country, phone=data.phone, email=data.email, website=data.website)
                        await self._rms_repo.update_enrichment_status(hotel.booking_url, 1)
                        logger.info(f"Enriched {hotel.hotel_id}: {data.name}")
                        return (True, True)
                    else:
                        await self._rms_repo.update_enrichment_status(hotel.booking_url, -1)
                        return (True, False)

            tasks = [enrich_one(h, i) for i, h in enumerate(hotels)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Enrichment error: {result}")
                    failed += 1
                else:
                    pr, en = result
                    processed += 1 if pr else 0
                    enriched += 1 if en else 0
                    if pr and not en:
                        failed += 1
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        return EnrichResult(processed=processed, enriched=enriched, failed=failed)

    async def enqueue_rms_for_enrichment(self, limit: int = 5000, batch_size: int = 10) -> EnqueueResult:
        """Find and enqueue RMS hotels needing enrichment."""
        stats = self._rms_queue.get_stats()
        logger.info(f"Queue: {stats.pending} pending, {stats.in_flight} in flight")
        if stats.pending > MAX_QUEUE_DEPTH:
            return EnqueueResult(total_found=0, enqueued=0, skipped=True, reason=f"Queue depth exceeds {MAX_QUEUE_DEPTH}")
        hotels = await self._rms_repo.get_hotels_needing_enrichment(limit)
        logger.info(f"Found {len(hotels)} hotels needing enrichment")
        if not hotels:
            return EnqueueResult(total_found=0, enqueued=0, skipped=False)
        enqueued = self._rms_queue.enqueue_hotels(hotels, batch_size)
        logger.success(f"Enqueued {enqueued} hotels")
        return EnqueueResult(total_found=len(hotels), enqueued=enqueued, skipped=False)

    async def consume_rms_enrichment_queue(self, concurrency: int = 6, max_messages: int = 0, should_stop: Optional[Callable[[], bool]] = None) -> ConsumeResult:
        """Consume and process RMS enrichment queue."""
        messages_processed = hotels_processed = hotels_enriched = hotels_failed = 0
        should_stop = should_stop or (lambda: self._shutdown_requested)
        logger.info(f"Starting consumer (concurrency={concurrency})")
        while not should_stop():
            if max_messages > 0 and messages_processed >= max_messages:
                break
            stats = self._rms_queue.get_stats()
            if stats.pending == 0 and stats.in_flight == 0:
                if max_messages > 0:
                    break
                logger.info("Queue empty, waiting...")
                await asyncio.sleep(30)
                continue
            messages = self._rms_queue.receive_messages(min(concurrency, 10))
            if not messages:
                continue
            logger.info(f"Processing {len(messages)} messages")
            for msg in messages:
                if should_stop():
                    break
                if not msg.hotels:
                    self._rms_queue.delete_message(msg.receipt_handle)
                    continue
                try:
                    result = await self.enrich_rms_hotels(msg.hotels, concurrency)
                    hotels_processed += result.processed
                    hotels_enriched += result.enriched
                    hotels_failed += result.failed
                    self._rms_queue.delete_message(msg.receipt_handle)
                    messages_processed += 1
                except Exception as e:
                    logger.error(f"Error: {e}")
                    hotels_failed += len(msg.hotels)
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
                    })
                    logger.info(f"  Hotel {hotel_id}: {data.name[:25] if data.name else ''}, {data.city}")
                elif error:
                    total_errors += 1
                    logger.warning(f"  Hotel {hotel_id}: {error}")

        if results_buffer:
            total_enriched = await repo.batch_update_cloudbeds_enrichment(results_buffer)
            logger.info(f"Updated {total_enriched} hotels")

        return EnrichResult(processed=len(hotels), enriched=total_enriched, failed=total_errors)

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
        """Batch update Cloudbeds enrichment results."""
        return await repo.batch_update_cloudbeds_enrichment(results)

    async def batch_mark_cloudbeds_failed(self, hotel_ids: List[int]) -> int:
        """Mark Cloudbeds hotels as failed (for retry later)."""
        return await repo.batch_set_last_enrichment_attempt(hotel_ids)

    async def consume_cloudbeds_queue(
        self,
        queue_url: str,
        concurrency: int = 5,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> ConsumeResult:
        """Consume and process Cloudbeds enrichment queue.
        
        This is the main entry point for the SQS consumer workflow.
        Handles browser lifecycle, message processing, and batch updates.
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

                # Batch update periodically
                if len(batch_results) >= BATCH_SIZE:
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

            # Final batch flush
            if batch_results:
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
