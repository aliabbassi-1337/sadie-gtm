"""LeadGen Service - Scraping and detection pipeline."""

import math
import re
from abc import ABC, abstractmethod
from optparse import Option
from typing import Dict, List, Tuple, Optional

import httpx
from loguru import logger
from pydantic import BaseModel
import json

from services.leadgen import repo
from services.leadgen.constants import HotelStatus
from services.leadgen.detector import BatchDetector, DetectionConfig, DetectionResult
from services.leadgen.geocoding import CityLocation, geocode_city, fetch_city_boundary
from services.leadgen.reverse_lookup import (
    BOOKING_ENGINE_DORKS,
    ReverseLookupResult,
    ReverseLookupStats,
)
from services.leadgen.booking_engines import (
    GuestbookScraper,
    GuestbookProperty,
    CommonCrawlEnumerator,
    CloudbedsPropertyExtractor,
)
from db.models.hotel import Hotel
from db.client import init_db, get_conn, queries
from services.leadgen.grid_scraper import GridScraper, ScrapedHotel, ScrapeEstimate, DEFAULT_CELL_SIZE_KM

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Re-export for public API
__all__ = ["IService", "Service", "ScrapeEstimate", "CityLocation", "ScrapeRegion"]


class ScrapeRegion(BaseModel):
    """A polygon region for targeted scraping."""
    id: Optional[int] = None
    name: str
    state: str
    region_type: str = "city"  # city, corridor, custom
    polygon_geojson: Optional[str] = None  # GeoJSON string
    center_lat: float
    center_lng: float
    radius_km: Optional[float] = None
    cell_size_km: float = 2.0
    priority: int = 0
    
    @property
    def bounds(self) -> Optional[dict]:
        """Parse polygon GeoJSON and return bounding box."""
        if not self.polygon_geojson:
            return None
        try:
            geojson = json.loads(self.polygon_geojson)
            geom_type = geojson.get("type")
            
            # Extract all coordinate points based on geometry type
            all_coords = []
            if geom_type == "Polygon":
                # Polygon: coordinates = [ring1, ring2, ...] where ring = [[lng, lat], ...]
                for ring in geojson.get("coordinates", []):
                    all_coords.extend(ring)
            elif geom_type == "MultiPolygon":
                # MultiPolygon: coordinates = [polygon1, polygon2, ...] where polygon = [ring1, ...]
                for polygon in geojson.get("coordinates", []):
                    for ring in polygon:
                        all_coords.extend(ring)
            else:
                return None
            
            if not all_coords:
                return None
            
            lngs = [c[0] for c in all_coords]
            lats = [c[1] for c in all_coords]
            return {
                "lat_min": min(lats),
                "lat_max": max(lats),
                "lng_min": min(lngs),
                "lng_max": max(lngs),
            }
        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            return None


class IService(ABC):
    """LeadGen Service - Scraping pipeline."""

    @abstractmethod
    async def scrape_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float,
        cell_size_km: float = DEFAULT_CELL_SIZE_KM,
    ) -> int:
        """
        Scrape hotels in a circular region using adaptive grid.
        Returns number of hotels found.
        """
        pass

    @abstractmethod
    async def detect_booking_engines(self, limit: int = 100) -> List[DetectionResult]:
        """
        Detect booking engines for hotels with status=0 (scraped).
        Updates status to 1 (detected) or 99 (no_booking_engine).
        Returns list of detection results.
        """
        pass

    @abstractmethod
    async def get_hotels_pending_detection(self, limit: int = 100) -> List[Hotel]:
        """
        Get hotels that need booking engine detection.
        Returns list of Hotel models.
        """
        pass

    @abstractmethod
    def estimate_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float,
        cell_size_km: float = DEFAULT_CELL_SIZE_KM,
    ) -> ScrapeEstimate:
        """Estimate cost for scraping a circular region."""
        pass

    @abstractmethod
    async def get_engine_patterns(self) -> Dict[str, List[str]]:
        """
        Get booking engine patterns for detection.
        Returns dict mapping engine name to list of domain patterns.
        """
        pass

    @abstractmethod
    async def save_detection_results(self, results: List[DetectionResult]) -> Tuple[int, int, int]:
        """
        Save detection results to database.
        Returns (detected_count, error_count, retriable_count) tuple.
        Retriable errors (timeout, browser) don't create HBE records - SQS will retry.
        """
        pass

    @abstractmethod
    async def get_hotels_by_ids(self, hotel_ids: List[int]) -> List[Hotel]:
        """
        Get hotels by list of IDs.
        Used by worker to fetch batch from SQS message.
        """
        pass

    @abstractmethod
    async def enqueue_hotels_for_detection(
        self,
        limit: int = 1000,
        batch_size: int = 20,
        categories: Optional[List[str]] = None,
    ) -> int:
        """
        Enqueue hotels for detection via SQS.
        Queries hotels with status=0 and no hotel_booking_engines record.
        Detection tracked by hotel_booking_engines presence, not status.
        Optionally filter by categories (e.g., ['hotel', 'motel']).
        Returns count of hotels enqueued.
        """
        pass

    @abstractmethod
    async def save_reverse_lookup_results(
        self,
        results: List[dict],
        source: str = "reverse_lookup",
    ) -> dict:
        """
        Save reverse lookup results to database.
        Hotels from reverse lookup already have known booking engines.
        Creates hotel record AND hotel_booking_engines record.
        Returns dict with insert/error counts.
        """
        pass

    @abstractmethod
    async def reverse_lookup(
        self,
        locations: List[str],
        engines: Optional[List[str]] = None,
        max_results_per_dork: int = 100,
    ) -> Tuple[List[ReverseLookupResult], ReverseLookupStats]:
        """
        Search for hotels by their booking engine URLs via Google dorks.
        Returns pre-qualified leads with known booking engines.
        """
        pass

    @abstractmethod
    def get_reverse_lookup_dorks(
        self,
        engines: Optional[List[str]] = None,
    ) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of dorks for reverse lookup (for dry-run display)."""
        pass

    @abstractmethod
    async def get_hotels_for_retry(
        self,
        state: str,
        limit: int = 100,
        source_pattern: Optional[str] = None,
    ) -> List[dict]:
        """Get hotels with retryable errors (timeout, 5xx, browser exceptions)."""
        pass

    @abstractmethod
    async def reset_hotels_for_retry(self, hotel_ids: List[int]) -> int:
        """Reset hotel status and delete HBE records to allow retry.

        Returns number of hotels reset.
        """
        pass

    @abstractmethod
    async def enumerate_guestbook(
        self,
        bbox: Optional[Dict] = None,
        max_pages: Optional[int] = None,
        cloudbeds_only: bool = True,
    ) -> List[Dict]:
        """
        Enumerate hotels from TheGuestbook API (Cloudbeds partner directory).
        
        Args:
            bbox: Bounding box polygon (default: continental US)
            max_pages: Limit number of pages (for testing)
            cloudbeds_only: Only return properties with beiStatus='automated'
            
        Returns list of hotel dicts with name, lat, lng, website, bei_status.
        """
        pass

    @abstractmethod
    async def enumerate_cloudbeds_sitemap(
        self,
        max_subdomains: Optional[int] = None,
        concurrency: int = 5,
        delay: float = 0.5,
    ) -> List[Dict]:
        """
        Enumerate hotels from Cloudbeds sitemap (hotels.cloudbeds.com/sitemap.xml).
        
        Args:
            max_subdomains: Limit number of subdomains to scrape (for testing)
            concurrency: Number of concurrent requests
            delay: Delay between requests in seconds
            
        Returns list of hotel dicts with subdomain, name, url.
        """
        pass

    @abstractmethod
    async def enumerate_commoncrawl(
        self,
        max_indices: Optional[int] = None,
        year: Optional[int] = None,
        concurrency: int = 10,
        fetch_details: bool = True,
        use_archives: bool = True,
    ) -> List[Dict]:
        """
        Enumerate Cloudbeds hotels from Common Crawl CDX API.
        
        Common Crawl indexes billions of web pages. We query their CDX API
        to find all indexed Cloudbeds reservation URLs, then fetch hotel
        details from archived HTML (fast, no rate limits) or by scraping
        Cloudbeds directly (slow, rate limited).
        
        Args:
            max_indices: Limit number of CC indices to query (default: all ~350)
            year: Only query indices from specific year
            concurrency: Concurrent requests
            fetch_details: If True, fetch hotel name/details
            use_archives: If True (default), fetch from CC archives (fast).
                         If False, scrape Cloudbeds (slow, rate limited).
            
        Returns list of hotel dicts with slug, name, city, booking_url.
        """
        pass

    @abstractmethod
    async def save_booking_engine_leads(
        self,
        leads: List[Dict],
        source: str,
        booking_engine: str,
    ) -> Dict:
        """
        Save leads with known booking engine to database.
        
        Creates hotel record AND hotel_booking_engines record.
        
        Args:
            leads: List of hotel dicts with name, website, lat, lng, etc.
            source: Source identifier (e.g., 'guestbook_florida', 'cloudbeds_sitemap')
            booking_engine: Booking engine name (e.g., 'Cloudbeds', 'RMS Cloud')
            
        Returns dict with insert/skip/error counts.
        """
        pass

    @abstractmethod
    async def ingest_commoncrawl_hotels(
        self,
        hotels: List[Dict],
        source_tag: str = "commoncrawl",
    ) -> Dict:
        """
        Ingest Common Crawl hotels with smart deduplication.
        
        Strategy:
        1. Match by name (case-insensitive) - if found, append source
        2. If no match, insert new hotel
        3. Link to Cloudbeds booking engine
        
        Source encoding: existing_source::new_source (e.g., 'dbpr::commoncrawl')
        
        Returns dict with counts: inserted, updated, engines_linked, errors
        """
        pass

    @abstractmethod
    async def ingest_crawled_urls(
        self,
        file_path: str,
        booking_engine: str,
        source_tag: str = "commoncrawl",
        scrape_names: bool = True,
        concurrency: int = 50,
        use_common_crawl: bool = True,
        fuzzy_match: bool = True,
        fuzzy_threshold: float = 0.7,
        checkpoint_file: Optional[str] = None,
    ) -> Dict:
        """
        Ingest crawled booking engine URLs/slugs from a file.
        
        Improvements:
        1. Uses Common Crawl S3 archives directly (50+ hotels/sec, no rate limits)
        2. Better name extraction (og:title, h1, meta tags, schema.org)
        3. Extracts actual hotel website when available
        4. Fuzzy deduplication with pg_trgm similarity
        5. Checkpoint/resume for large imports
        
        Args:
            file_path: Path to text file with slugs/URLs
            booking_engine: Engine name (cloudbeds, mews, rms, siteminder)
            source_tag: Source identifier for tracking
            scrape_names: If True, scrape hotel names (recommended)
            concurrency: Number of concurrent requests
            use_common_crawl: Use CC archives (fast) vs Wayback (slow)
            fuzzy_match: Enable fuzzy name matching for deduplication
            fuzzy_threshold: Similarity threshold (0.0-1.0, default 0.7)
            checkpoint_file: Path to save progress for resume
            
        Returns dict with counts: total, inserted, updated, fuzzy_matched, etc.
        """
        pass


class Service(IService):
    def __init__(self, detection_config: DetectionConfig = None, api_key: Optional[str] = None) -> None:
        self.detection_config = detection_config or DetectionConfig()
        self._api_key = api_key

    async def scrape_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float,
        cell_size_km: float = DEFAULT_CELL_SIZE_KM,
    ) -> int:
        """
        Scrape hotels in a circular region using adaptive grid.
        Saves incrementally after each batch so progress isn't lost.

        Args:
            center_lat: Center latitude of the region
            center_lng: Center longitude of the region
            radius_km: Radius in kilometers
            cell_size_km: Cell size in km (smaller = more thorough, default 2km)

        Returns:
            Number of hotels found and saved to database
        """
        logger.info(f"Starting region scrape: center=({center_lat}, {center_lng}), radius={radius_km}km")

        # Initialize scraper
        scraper = GridScraper(api_key=self._api_key, cell_size_km=cell_size_km)

        # Track total saved
        saved_count = 0

        # Incremental save callback
        async def save_batch(batch_hotels):
            nonlocal saved_count
            count = await self._save_hotels(batch_hotels, source="grid_region")
            saved_count += count

        # Run the scrape with incremental saving
        hotels, stats = await scraper.scrape_region(
            center_lat, center_lng, radius_km,
            on_batch_complete=save_batch
        )

        logger.info(
            f"Region scrape complete: {stats.hotels_found} found, "
            f"{saved_count} saved, {stats.api_calls} API calls, "
            f"{stats.cells_searched} cells ({stats.cells_skipped} skipped, {stats.cells_reduced} reduced)"
        )

        return saved_count

    async def _save_hotels(self, hotels: List[ScrapedHotel], source: str) -> int:
        """Convert scraped hotels to dicts and save to database."""
        if not hotels:
            return 0

        hotel_dicts = []
        for h in hotels:
            hotel_dicts.append({
                "name": h.name,
                "external_id": h.external_id,  # Primary dedup key
                "external_id_type": h.external_id_type,
                "website": h.website,
                "phone_google": h.phone,
                "phone_website": None,
                "email": None,
                "latitude": h.latitude,
                "longitude": h.longitude,
                "address": h.address,
                "city": h.city,
                "state": h.state,
                "country": "USA",
                "rating": h.rating,
                "review_count": h.review_count,
                "status": 0,  # scraped status
                "source": source,
            })

        return await repo.insert_hotels_bulk(hotel_dicts)

    async def detect_booking_engines(self, limit: int = 100) -> List[DetectionResult]:
        """
        Detect booking engines for hotels with status=0 (scraped).

        1. Query hotels pending detection
        2. Handle chain hotels (create HBE record, set status=-1)
        3. Run batch detection (visits websites, detects engines)
        4. Update database with results
        5. Return detection results
        """
        from services.leadgen.detector import get_chain_name

        # Get hotels to process
        hotels = await repo.get_hotels_pending_detection(limit=limit)
        if not hotels:
            logger.info("No hotels pending detection")
            return []

        logger.info(f"Processing {len(hotels)} hotels for detection")

        # Separate chain hotels from non-chain hotels
        chain_hotels = []
        non_chain_hotels = []
        for h in hotels:
            chain_name = get_chain_name(h.website) if h.website else None
            if chain_name:
                chain_hotels.append((h, chain_name))
            else:
                non_chain_hotels.append(h)

        # Handle chain hotels - create HBE record and set status=-1
        results = []
        if chain_hotels:
            logger.info(f"Found {len(chain_hotels)} chain hotels, marking as detected")
            for hotel, chain_name in chain_hotels:
                # Get or create booking engine for this chain
                engine = await repo.get_booking_engine_by_name(chain_name)
                if engine:
                    engine_id = engine.id
                else:
                    engine_id = await repo.insert_booking_engine(
                        name=chain_name,
                        domains=None,
                        tier=0,  # Chains are tier 0 (not our target)
                    )

                # Create HBE record
                await repo.insert_hotel_booking_engine(
                    hotel_id=hotel.id,
                    booking_engine_id=engine_id,
                    booking_url=hotel.website,
                    detection_method=f"chain:{chain_name}",
                    status=1,  # Success - detected
                )

                # Set hotel status to -1 (rejected - chain hotel)
                await repo.update_hotel_status(
                    hotel_id=hotel.id,
                    status=-1,
                )

                results.append(DetectionResult(
                    hotel_id=hotel.id,
                    booking_engine=chain_name,
                    detection_method=f"chain:{chain_name}",
                ))

        if not non_chain_hotels:
            logger.info("All hotels were chains, no detection needed")
            return results

        logger.info(f"Running detection for {len(non_chain_hotels)} non-chain hotels")

        # Convert to dicts for detector
        hotel_dicts = [
            {"id": h.id, "name": h.name, "website": h.website, "city": h.city or ""}
            for h in non_chain_hotels
        ]

        # Run detection
        detector = BatchDetector(self.detection_config)
        detection_results = await detector.detect_batch(hotel_dicts)

        # Update database with results
        for result in detection_results:
            await self._save_detection_result(result)

        # Combine chain results with detection results
        results.extend(detection_results)

        # Log summary
        chains = sum(1 for r in results if r.detection_method and r.detection_method.startswith("chain:"))
        detected = sum(1 for r in results if r.booking_engine and r.booking_engine not in ("", "unknown", "unknown_third_party"))
        failed = sum(1 for r in results if r.error)
        logger.info(f"Detection complete: {chains} chains, {detected - chains} engines detected, {failed} errors, {len(results) - detected - failed} no engine")

        return results

    async def _save_detection_result(self, result: DetectionResult) -> None:
        """Save detection result to database."""
        try:
            # Log error to detection_errors table if there was one
            if result.error:
                # Parse error type from error string (e.g., "precheck_failed: timeout" -> "precheck_failed")
                error_type = result.error.split(":")[0].strip()
                await repo.insert_detection_error(
                    hotel_id=result.hotel_id,
                    error_type=error_type,
                    error_message=result.error,
                    detected_location=result.detected_location or None,
                )

            # Handle location mismatch (detected in detector, skipped engine detection)
            if result.error == "location_mismatch":
                await repo.update_hotel_status(
                    hotel_id=result.hotel_id,
                    status=HotelStatus.LOCATION_MISMATCH,
                    phone_website=result.phone_website or None,
                    email=result.email or None,
                )
                return

            # Check if error is retriable (timeout, browser crash, etc.)
            # For retriable errors, DON'T create HBE record - let SQS retry
            retriable_errors = ("timeout", "precheck_failed: timeout", "browser", "context")
            is_retriable = result.error and any(e in result.error.lower() for e in retriable_errors)
            
            if result.error and result.error not in ("location_mismatch",):
                if is_retriable:
                    # Skip HBE creation - hotel will be picked up on SQS retry
                    # Just save contact info if we got any
                    if result.phone_website or result.email:
                        await repo.update_hotel_contact_info(
                            hotel_id=result.hotel_id,
                            phone_website=result.phone_website or None,
                            email=result.email or None,
                        )
                    return "retriable"  # Signal that this was a retriable error
                else:
                    # Non-retriable error - create HBE record to prevent infinite retry
                    await repo.insert_hotel_booking_engine(
                        hotel_id=result.hotel_id,
                        booking_engine_id=None,
                        detection_method=f"error:{result.error}",
                        status=-1,  # Failed, non-retriable
                    )
                    # Save contact info if we got any
                    if result.phone_website or result.email:
                        await repo.update_hotel_contact_info(
                            hotel_id=result.hotel_id,
                            phone_website=result.phone_website or None,
                            email=result.email or None,
                        )
                    return

            if result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "unknown_booking_api"):
                # Found a booking engine
                # Get or create booking engine record
                engine = await repo.get_booking_engine_by_name(result.booking_engine)
                if engine:
                    engine_id = engine.id
                else:
                    # Insert new engine (tier=2 for discovered)
                    engine_id = await repo.insert_booking_engine(
                        name=result.booking_engine,
                        domains=[result.booking_engine_domain] if result.booking_engine_domain else None,
                        tier=2,
                    )

                # Link hotel to booking engine (this marks detection as complete)
                await repo.insert_hotel_booking_engine(
                    hotel_id=result.hotel_id,
                    booking_engine_id=engine_id,
                    booking_url=result.booking_url or None,
                    detection_method=result.detection_method or None,
                    status=1,  # Success
                )

                # Save phone/email but don't change status - hotel stays at PENDING (0)
                # Detection completion is tracked by hotel_booking_engines record
                # Launcher will set status=1 when all enrichments are complete
                if result.phone_website or result.email:
                    await repo.update_hotel_contact_info(
                        hotel_id=result.hotel_id,
                        phone_website=result.phone_website or None,
                        email=result.email or None,
                    )
            else:
                # No booking engine found
                await repo.update_hotel_status(
                    hotel_id=result.hotel_id,
                    status=HotelStatus.NO_BOOKING_ENGINE,
                    phone_website=result.phone_website or None,
                    email=result.email or None,
                )

        except Exception as e:
            logger.error(f"Error saving detection result for hotel {result.hotel_id}: {e}")

    async def get_hotels_pending_detection(self, limit: int = 100) -> List[Hotel]:
        """Get hotels that need booking engine detection."""
        return await repo.get_hotels_pending_detection(limit=limit)

    async def get_pending_detection_count(self) -> int:
        """Count hotels waiting for detection (status=0)."""
        hotels = await repo.get_hotels_pending_detection(limit=10000)
        return len(hotels)

    async def get_engine_patterns(self) -> Dict[str, List[str]]:
        """Get booking engine patterns for detection.

        Returns dict mapping engine name to list of domain patterns.
        e.g. {"Cloudbeds": ["cloudbeds.com"], "Mews": ["mews.com", "mews.li"]}
        """
        engines = await repo.get_all_booking_engines()
        return {engine.name: engine.domains for engine in engines if engine.domains}

    async def save_detection_results(self, results: List[DetectionResult]) -> Tuple[int, int, int]:
        """Save detection results to database.

        Returns (detected_count, error_count, retriable_count) tuple.
        Note: Location mismatches are not counted as errors or detections.
        Retriable errors (timeout, browser) don't create HBE records and should trigger SQS retry.
        """
        detected = 0
        errors = 0
        retriable = 0

        for result in results:
            try:
                save_result = await self._save_detection_result(result)

                if save_result == "retriable":
                    retriable += 1
                elif result.error == "location_mismatch":
                    # Don't count as detected or error
                    pass
                elif result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "unknown_booking_api"):
                    detected += 1
                elif result.error:
                    errors += 1
            except Exception as e:
                logger.error(f"Error saving result for hotel {result.hotel_id}: {e}")
                errors += 1

        return (detected, errors, retriable)

    async def get_hotels_by_ids(self, hotel_ids: List[int]) -> List[Hotel]:
        """Get hotels by list of IDs."""
        return await repo.get_hotels_by_ids(hotel_ids=hotel_ids)

    async def enqueue_hotels_for_detection(
        self,
        limit: int = 1000,
        batch_size: int = 20,
        categories: Optional[List[str]] = None,
    ) -> int:
        """Enqueue hotels for detection via SQS.

        Queries hotels with status=0 and no hotel_booking_engines record.
        Sends to SQS in batches. Does NOT update status - detection is tracked
        by presence of hotel_booking_engines record.
        Optionally filter by categories (e.g., ['hotel', 'motel']).

        Returns count of hotels enqueued.
        """
        from infra.sqs import send_messages_batch, get_queue_url

        # Get hotels pending detection (status=0, no booking engine record)
        hotels = await repo.get_hotels_pending_detection(limit=limit, categories=categories)
        if not hotels:
            return 0

        hotel_ids = [h.id for h in hotels]

        # Create messages in batches of batch_size
        messages = []
        for i in range(0, len(hotel_ids), batch_size):
            batch_ids = hotel_ids[i:i + batch_size]
            messages.append({"hotel_ids": batch_ids})

        # Send to SQS
        queue_url = get_queue_url()
        sent = send_messages_batch(queue_url, messages)
        logger.info(f"Sent {sent} messages to SQS ({len(hotel_ids)} hotels)")

        # No status update needed - detection is tracked by hotel_booking_engines record
        return len(hotel_ids)

    async def save_reverse_lookup_results(
        self,
        results: List[dict],
        source: str = "reverse_lookup",
    ) -> dict:
        """
        Save reverse lookup results to database.
        Hotels from reverse lookup already have known booking engines.
        Creates hotel record AND hotel_booking_engines record.
        """
        stats = {
            "total": len(results),
            "inserted": 0,
            "engines_linked": 0,
            "skipped_no_website": 0,
            "errors": 0,
        }

        # Cache booking engine IDs to avoid repeated lookups
        engine_cache: Dict[str, int] = {}

        for i, r in enumerate(results):
            if (i + 1) % 50 == 0:
                logger.info(f"  Saved {i + 1}/{len(results)}...")

            try:
                website = r.get("website") or r.get("booking_url")
                if not website:
                    stats["skipped_no_website"] += 1
                    continue

                # Insert hotel
                hotel_id = await repo.insert_hotel(
                    name=r["name"],
                    website=website,
                    source=source,
                    status=0,
                )
                stats["inserted"] += 1

                # Get or create booking engine (cached)
                booking_engine = r["booking_engine"]
                if booking_engine not in engine_cache:
                    engine = await repo.get_booking_engine_by_name(booking_engine)
                    if engine:
                        engine_cache[booking_engine] = engine.id
                    else:
                        engine_id = await repo.insert_booking_engine(
                            name=booking_engine,
                            tier=2,
                        )
                        engine_cache[booking_engine] = engine_id

                # Link hotel to booking engine
                await repo.insert_hotel_booking_engine(
                    hotel_id=hotel_id,
                    booking_engine_id=engine_cache[booking_engine],
                    booking_url=r.get("booking_url"),
                    detection_method="reverse_lookup",
                    status=1,
                )
                stats["engines_linked"] += 1

            except Exception as e:
                logger.warning(f"Error saving {r.get('name', 'unknown')}: {e}")
                stats["errors"] += 1

        return stats

    def get_reverse_lookup_dorks(
        self,
        engines: Optional[List[str]] = None,
    ) -> List[Tuple[str, str, Optional[str]]]:
        """Get list of dorks for reverse lookup (for dry-run display)."""
        dorks = BOOKING_ENGINE_DORKS
        if engines:
            engines_lower = [e.lower() for e in engines]
            dorks = [d for d in BOOKING_ENGINE_DORKS if d[0] in engines_lower]
        return dorks

    async def reverse_lookup(
        self,
        locations: List[str],
        engines: Optional[List[str]] = None,
        max_results_per_dork: int = 100,
    ) -> Tuple[List[ReverseLookupResult], ReverseLookupStats]:
        """
        Search for hotels by their booking engine URLs via Google dorks.
        Returns pre-qualified leads with known booking engines.
        """
        seen_urls: set = set()
        all_results: List[ReverseLookupResult] = []
        stats = ReverseLookupStats()

        dorks_to_run = self.get_reverse_lookup_dorks(engines)

        async with httpx.AsyncClient(timeout=30.0) as client:
            for location in locations:
                logger.info(f"Running {len(dorks_to_run)} dorks for location: {location}")

                for engine_name, dork_template, url_pattern in dorks_to_run:
                    dork = dork_template.format(location=location)
                    stats.dorks_run += 1
                    stats.api_calls += 1

                    try:
                        resp = await client.post(
                            SERPER_SEARCH_URL,
                            headers={"X-API-KEY": self._api_key, "Content-Type": "application/json"},
                            json={"q": dork, "num": max_results_per_dork},
                        )

                        if resp.status_code != 200:
                            logger.error(f"Serper error {resp.status_code}: {resp.text[:100]}")
                            continue

                        search_results = resp.json().get("organic", [])
                        stats.results_found += len(search_results)

                        for r in search_results:
                            url = r.get("link", "")
                            title = r.get("title", "")
                            snippet = r.get("snippet", "")

                            if url in seen_urls:
                                continue
                            seen_urls.add(url)

                            # Validate URL matches expected pattern
                            if url_pattern and not re.search(url_pattern, url, re.I):
                                continue

                            # Extract hotel name
                            name = self._extract_hotel_name_from_title(title, url)
                            if not name or len(name) < 3:
                                continue

                            result = ReverseLookupResult(
                                name=name,
                                booking_url=url,
                                booking_engine=engine_name,
                                snippet=snippet,
                                source_dork=dork,
                            )
                            all_results.append(result)

                            if engine_name not in stats.by_engine:
                                stats.by_engine[engine_name] = 0
                            stats.by_engine[engine_name] += 1

                    except Exception as e:
                        logger.error(f"Error running dork '{dork}': {e}")

        stats.unique_results = len(all_results)
        logger.info(f"Reverse lookup complete: {stats.unique_results} unique results from {stats.api_calls} API calls")
        for engine, count in sorted(stats.by_engine.items(), key=lambda x: -x[1]):
            logger.info(f"  {engine}: {count}")

        return all_results, stats

    def _extract_hotel_name_from_title(self, title: str, url: str) -> str:
        """Extract hotel name from search result title."""
        name = title
        suffixes = [
            " - Book Direct", " | Book Now", " - Official Site",
            " - Reservations", " | Reservations", " - Hotels.com",
            " - Booking.com", " | Booking", " - Cloudbeds",
            ", United States of America", ", USA", ", Florida",
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
            if name.lower().endswith(suffix.lower()):
                name = name[:-len(suffix)]

        if "cloudbeds.com" in name.lower() or len(name) > 100:
            match = re.search(r'/reservation/(\w+)', url)
            if match:
                slug = match.group(1)
                name = re.sub(r'([a-z])([A-Z])', r'\1 \2', slug)

        return name.strip()

    def estimate_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float,
        cell_size_km: float = DEFAULT_CELL_SIZE_KM,
    ) -> ScrapeEstimate:
        """Estimate cost for scraping a circular region."""
        scraper = GridScraper.__new__(GridScraper)  # Skip __init__ validation
        scraper.cell_size_km = cell_size_km
        scraper.hybrid = False
        scraper.aggressive = False
        return scraper.estimate_region(center_lat, center_lng, radius_km)

    # =========================================================================
    # TARGET CITIES
    # =========================================================================

    async def get_target_cities(self, state: str, limit: int = 100) -> List[CityLocation]:
        """
        Get target cities for a state from the database.
        
        Returns list of CityLocation objects with coordinates and radius.
        Cities must be added via add_target_city() first.
        """
        rows = await repo.get_target_cities_by_state(state, limit=limit)
        return [
            CityLocation(
                name=row["name"],
                state=row["state"],
                lat=row["lat"],
                lng=row["lng"],
                radius_km=row["radius_km"] or 12.0,
                display_name=row["display_name"],
            )
            for row in rows
        ]

    async def add_target_city(
        self,
        name: str,
        state: str,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        radius_km: Optional[float] = None,
    ) -> CityLocation:
        """
        Add a city to the scrape target list.
        
        If lat/lng not provided, geocodes the city using Nominatim API.
        Stores result in database for future use.
        
        Args:
            name: City name (e.g., "Miami")
            state: State code (e.g., "FL")
            lat: Latitude (optional, will geocode if not provided)
            lng: Longitude (optional, will geocode if not provided)
            radius_km: Scrape radius (optional, uses default based on city size)
            
        Returns:
            CityLocation with coordinates
        """
        # Check if already exists
        existing = await repo.get_target_city(name, state)
        if existing:
            return CityLocation(
                name=existing["name"],
                state=existing["state"],
                lat=existing["lat"],
                lng=existing["lng"],
                radius_km=existing["radius_km"] or 12.0,
                display_name=existing["display_name"],
            )
        
        # Geocode if coordinates not provided
        if lat is None or lng is None:
            logger.info(f"Geocoding {name}, {state}...")
            geocoded = await geocode_city(name, state)
            lat = geocoded.lat
            lng = geocoded.lng
            display_name = geocoded.display_name
            if radius_km is None:
                radius_km = geocoded.radius_km
        else:
            display_name = f"{name}, {state}, USA"
        
        if radius_km is None:
            radius_km = 12.0
        
        # Save to database
        await repo.insert_target_city(
            name=name,
            state=state,
            lat=lat,
            lng=lng,
            radius_km=radius_km,
            display_name=display_name,
            source="nominatim" if display_name else "manual",
        )
        
        return CityLocation(
            name=name,
            state=state,
            lat=lat,
            lng=lng,
            radius_km=radius_km,
            display_name=display_name,
        )

    async def remove_target_city(self, name: str, state: str) -> None:
        """Remove a city from the scrape target list."""
        await repo.delete_target_city(name, state)

    async def count_target_cities(self, state: str) -> int:
        """Count how many target cities are configured for a state."""
        return await repo.count_target_cities_by_state(state)

    # =========================================================================
    # SCRAPE REGIONS (Polygon-based scraping)
    # =========================================================================

    async def get_regions(self, state: str) -> List[ScrapeRegion]:
        """Get all scrape regions for a state."""
        rows = await repo.get_regions_by_state(state)
        return [
            ScrapeRegion(
                id=row["id"],
                name=row["name"],
                state=row["state"],
                region_type=row["region_type"],
                polygon_geojson=row.get("polygon_geojson"),
                center_lat=row["center_lat"],
                center_lng=row["center_lng"],
                radius_km=row.get("radius_km"),
                cell_size_km=row.get("cell_size_km", 2.0),
                priority=row.get("priority", 0),
            )
            for row in rows
        ]

    async def get_region(self, name: str, state: str) -> Optional[ScrapeRegion]:
        """Get a specific region by name and state."""
        row = await repo.get_region_by_name(name, state)
        if not row:
            return None
        return ScrapeRegion(
            id=row["id"],
            name=row["name"],
            state=row["state"],
            region_type=row["region_type"],
            polygon_geojson=row.get("polygon_geojson"),
            center_lat=row["center_lat"],
            center_lng=row["center_lng"],
            radius_km=row.get("radius_km"),
            cell_size_km=row.get("cell_size_km", 2.0),
            priority=row.get("priority", 0),
        )

    async def add_region(
        self,
        name: str,
        state: str,
        center_lat: float,
        center_lng: float,
        radius_km: float,
        region_type: str = "city",
        cell_size_km: float = 2.0,
        priority: int = 0,
    ) -> ScrapeRegion:
        """Add a circular region from center point and radius."""
        region_id = await repo.insert_region(
            name=name,
            state=state,
            center_lat=center_lat,
            center_lng=center_lng,
            radius_km=radius_km,
            region_type=region_type,
            cell_size_km=cell_size_km,
            priority=priority,
        )
        # Fetch the region to get the generated polygon
        region = await self.get_region(name, state)
        return region

    async def add_region_geojson(
        self,
        name: str,
        state: str,
        polygon_geojson: str,
        center_lat: float,
        center_lng: float,
        region_type: str = "custom",
        cell_size_km: float = 2.0,
        priority: int = 0,
    ) -> ScrapeRegion:
        """Add a region from a GeoJSON polygon."""
        await repo.insert_region_geojson(
            name=name,
            state=state,
            polygon_geojson=polygon_geojson,
            center_lat=center_lat,
            center_lng=center_lng,
            region_type=region_type,
            cell_size_km=cell_size_km,
            priority=priority,
        )
        region = await self.get_region(name, state)
        return region

    async def remove_region(self, name: str, state: str) -> None:
        """Remove a region."""
        await repo.delete_region(name, state)

    async def clear_regions(self, state: str) -> None:
        """Remove all regions for a state."""
        await repo.delete_regions_by_state(state)

    async def count_regions(self, state: str) -> int:
        """Count regions for a state."""
        return await repo.count_regions_by_state(state)

    async def get_total_region_area(self, state: str) -> float:
        """Get total area of all regions for a state in km²."""
        return await repo.get_total_region_area_km2(state)

    async def generate_regions_from_cities(
        self,
        state: str,
        clear_existing: bool = True,
    ) -> List[ScrapeRegion]:
        """
        Generate circular regions from all target cities in a state.
        Uses each city's radius_km as the region radius.
        """
        if clear_existing:
            await self.clear_regions(state)
        
        cities = await self.get_target_cities(state)
        regions = []
        
        for city in cities:
            radius = city.radius_km or 12.0
            # Determine cell size based on radius (smaller cells for denser areas)
            if radius >= 20:
                cell_size = 2.0  # Large metros
            elif radius >= 12:
                cell_size = 1.5  # Medium cities
            else:
                cell_size = 1.0  # Small dense areas
            
            region = await self.add_region(
                name=city.name,
                state=state,
                center_lat=city.lat,
                center_lng=city.lng,
                radius_km=radius,
                region_type="city",
                cell_size_km=cell_size,
                priority=1 if radius >= 20 else 0,  # Major metros first
            )
            regions.append(region)
            logger.info(f"Created region: {city.name} ({radius}km radius, {cell_size}km cells)")
        
        return regions

    async def generate_regions_from_boundaries(
        self,
        state: str,
        clear_existing: bool = True,
    ) -> List[ScrapeRegion]:
        """
        Generate optimized regions using REAL city boundaries from OpenStreetMap.
        
        Much more efficient than circles for:
        - Coastal cities (no ocean coverage)
        - Islands (exact land area only)
        - Irregular city shapes
        
        Falls back to circular regions for cities without OSM boundary data.
        """
        import asyncio
        
        if clear_existing:
            await self.clear_regions(state)
        
        cities = await self.get_target_cities(state)
        regions = []
        
        for city in cities:
            # Rate limit: Nominatim allows 1 req/sec
            await asyncio.sleep(1.1)
            
            # Try to fetch real boundary
            boundary = await fetch_city_boundary(city.name, state)
            
            radius = city.radius_km or 12.0
            if radius >= 20:
                cell_size = 2.0
            elif radius >= 12:
                cell_size = 1.5
            else:
                cell_size = 1.0
            
            if boundary:
                # Use real OSM boundary
                region = await self.add_region_geojson(
                    name=city.name,
                    state=state,
                    polygon_geojson=boundary.polygon_geojson,
                    center_lat=boundary.lat,
                    center_lng=boundary.lng,
                    region_type="boundary",
                    cell_size_km=cell_size,
                    priority=1 if radius >= 20 else 0,
                )
                logger.info(f"Created boundary region: {city.name} (OSM, {cell_size}km cells)")
            else:
                # Fall back to circle
                region = await self.add_region(
                    name=city.name,
                    state=state,
                    center_lat=city.lat,
                    center_lng=city.lng,
                    radius_km=radius,
                    region_type="city",
                    cell_size_km=cell_size,
                    priority=1 if radius >= 20 else 0,
                )
                logger.info(f"Created circular region: {city.name} ({radius}km radius, fallback)")
            
            regions.append(region)
        
        return regions

    async def scrape_regions(
        self,
        state: str,
        save_to_db: bool = True,
        region_names: Optional[List[str]] = None,
    ) -> List[ScrapedHotel]:
        """
        Scrape regions for a state.
        
        Args:
            state: State code (e.g., "FL")
            save_to_db: Whether to save hotels to database
            region_names: If provided, only scrape these specific regions
            
        Returns combined list of all hotels.
        """
        regions = await self.get_regions(state)
        if not regions:
            logger.warning(f"No regions defined for {state}. Use ingest_regions workflow first.")
            return []
        
        # Filter to specific regions if requested
        if region_names:
            name_set = {n.lower() for n in region_names}
            regions = [r for r in regions if r.name.lower() in name_set]
            if not regions:
                logger.warning(f"None of the specified regions found: {region_names}")
                return []
        
        all_hotels = []
        total_saved = 0
        seen_ids = set()  # Track across all regions for deduplication
        
        for region in regions:
            bounds = region.bounds
            if not bounds:
                logger.warning(f"Region {region.name} has no valid bounds, skipping")
                continue
            
            logger.info(
                f"Scraping region: {region.name} "
                f"({region.cell_size_km}km cells, priority={region.priority})"
            )
            
            scraper = GridScraper(
                api_key=self._api_key,
                cell_size_km=region.cell_size_km,
            )
            
            # Incremental save callback
            async def save_batch(batch_hotels):
                nonlocal total_saved
                # Deduplicate before saving
                unique_batch = []
                for h in batch_hotels:
                    if h.external_id:
                        if h.external_id in seen_ids:
                            continue
                        seen_ids.add(h.external_id)
                    unique_batch.append(h)
                
                if save_to_db and unique_batch:
                    count = await self._save_hotels(unique_batch, source=f"grid_{state.lower()}_{region.name.lower().replace(' ', '_')}")
                    total_saved += count
                    logger.info(f"  Saved batch: {count} hotels (total: {total_saved})")
                
                all_hotels.extend(unique_batch)
            
            hotels, stats = await scraper._scrape_bounds(
                lat_min=bounds["lat_min"],
                lat_max=bounds["lat_max"],
                lng_min=bounds["lng_min"],
                lng_max=bounds["lng_max"],
                on_batch_complete=save_batch,
            )
            
            logger.info(f"  Found {len(hotels)} hotels in {region.name}")
        
        logger.info(f"Total unique hotels across all regions: {len(all_hotels)} ({total_saved} saved)")
        
        return all_hotels

    async def estimate_regions(self, state: str) -> dict:
        """
        Estimate scraping all regions for a state.
        Returns combined estimate across all regions.
        """
        regions = await self.get_regions(state)
        if not regions:
            return {
                "regions": 0,
                "total_cells": 0,
                "total_api_calls": 0,
                "estimated_cost_usd": 0,
                "total_area_km2": 0,
                "message": f"No regions defined for {state}",
            }
        
        total_cells = 0
        total_area = await self.get_total_region_area(state)
        region_estimates = []
        
        for region in regions:
            bounds = region.bounds
            if not bounds:
                continue
            
            # Calculate cells manually (simpler than using GridScraper)
            center_lat = (bounds["lat_min"] + bounds["lat_max"]) / 2
            height_km = (bounds["lat_max"] - bounds["lat_min"]) * 111.0
            width_km = (bounds["lng_max"] - bounds["lng_min"]) * 111.0 * math.cos(math.radians(center_lat))
            
            cells_x = max(1, int(width_km / region.cell_size_km))
            cells_y = max(1, int(height_km / region.cell_size_km))
            region_cells = cells_x * cells_y
            
            total_cells += region_cells
            region_estimates.append({
                "name": region.name,
                "cells": region_cells,
                "cell_size_km": region.cell_size_km,
            })
        
        # Estimate with 2-4x API calls due to adaptive subdivision
        min_calls = total_cells
        max_calls = total_cells * 4
        
        return {
            "regions": len(regions),
            "total_cells": total_cells,
            "total_api_calls_range": f"{min_calls:,} - {max_calls:,}",
            "estimated_cost_usd_range": f"${min_calls * 0.0005:.2f} - ${max_calls * 0.0005:.2f}",
            "total_area_km2": round(total_area, 1),
            "region_breakdown": region_estimates,
        }

    def estimate_bbox(
        self,
        lat_min: float,
        lng_min: float,
        lat_max: float,
        lng_max: float,
        cell_size_km: float = 2.0,
        name: str = "bounding box",
    ) -> dict:
        """
        Estimate cost for scraping a bounding box.

        Args:
            lat_min, lng_min, lat_max, lng_max: Bounding box coordinates
            cell_size_km: Cell size for scraping grid
            name: Name for display

        Returns dict with estimate details.
        """
        center_lat = (lat_min + lat_max) / 2
        height_km = (lat_max - lat_min) * 111.0
        width_km = (lng_max - lng_min) * 111.0 * math.cos(math.radians(center_lat))
        area_km2 = height_km * width_km

        cells_x = max(1, int(math.ceil(width_km / cell_size_km)))
        cells_y = max(1, int(math.ceil(height_km / cell_size_km)))
        total_cells = cells_x * cells_y

        # Estimate API calls
        # Based on actual Orlando data: 5000 calls / 1536 cells = 3.25 calls per cell
        # Includes multiple query types (hotels, motels, lodging) + some pagination
        avg_queries_per_cell = 3.25
        estimated_api_calls = int(total_cells * avg_queries_per_cell)

        # Cost: $0.001 per query (Serper pricing)
        cost_per_query = 0.001
        estimated_cost = estimated_api_calls * cost_per_query

        # Estimate hotels: ~1.4 unique hotels per cell after dedup
        # Based on actual Palm Beach data: 689 hotels / 495 cells = 1.4
        # Many cells overlap, and dedup removes ~60% of raw results
        hotels_per_cell = 1.4
        estimated_hotels = int(total_cells * hotels_per_cell)

        return {
            "name": name,
            "lat_min": lat_min,
            "lng_min": lng_min,
            "lat_max": lat_max,
            "lng_max": lng_max,
            "cell_size_km": cell_size_km,
            "area_km2": round(area_km2, 1),
            "dimensions_km": f"{width_km:.1f} x {height_km:.1f}",
            "grid_cells": f"{cells_x} x {cells_y} = {total_cells}",
            "total_cells": total_cells,
            "estimated_api_calls": estimated_api_calls,
            "estimated_cost_usd": round(estimated_cost, 2),
            "estimated_hotels": estimated_hotels,
        }

    async def scrape_bbox(
        self,
        lat_min: float,
        lng_min: float,
        lat_max: float,
        lng_max: float,
        cell_size_km: float = 2.0,
        save_to_db: bool = True,
        source: str = "bbox",
        thorough: bool = False,
        max_pages: int = 1,
    ) -> Tuple[List[ScrapedHotel], dict]:
        """
        Scrape hotels in a bounding box.

        Args:
            lat_min, lng_min, lat_max, lng_max: Bounding box coordinates
            cell_size_km: Cell size for scraping grid
            save_to_db: Whether to save hotels to database
            source: Source identifier for database records
            thorough: Disable skipping for maximum coverage (more API calls)
            max_pages: Pages per query for pagination (1-5, each page ~20 results)

        Returns tuple of (hotels list, stats dict).
        """
        scraper = GridScraper(
            api_key=self._api_key,
            cell_size_km=cell_size_km,
            thorough=thorough,
            max_pages=max_pages,
        )

        # Preload existing hotels from DB to skip already-covered cells
        async with get_conn() as conn:
            existing = await queries.get_hotels_in_bbox(
                conn, lng_min=lng_min, lat_min=lat_min, lng_max=lng_max, lat_max=lat_max
            )

        # Filter to google_place external_ids
        existing_place_ids = {
            r['external_id'] for r in existing
            if r['external_id_type'] == 'google_place' and r['external_id']
        }
        existing_locations = {(round(r['lat'], 4), round(r['lng'], 4)) for r in existing if r['lat']}
        scraper.preload_existing(existing_place_ids, existing_locations)

        total_saved = 0
        all_hotels = []
        seen_ids = set(existing_place_ids)  # Also skip in batch saving

        async def save_batch(batch_hotels):
            nonlocal total_saved
            unique_batch = []
            for h in batch_hotels:
                if h.external_id:
                    if h.external_id in seen_ids:
                        continue
                    seen_ids.add(h.external_id)
                unique_batch.append(h)

            if save_to_db and unique_batch:
                count = await self._save_hotels(unique_batch, source=source)
                total_saved += count
                logger.info(f"  Saved batch: {count} hotels (total: {total_saved})")

            all_hotels.extend(unique_batch)

        hotels, stats = await scraper._scrape_bounds(
            lat_min=lat_min,
            lat_max=lat_max,
            lng_min=lng_min,
            lng_max=lng_max,
            on_batch_complete=save_batch,
        )

        return all_hotels, {
            "hotels_found": len(all_hotels),
            "hotels_saved": total_saved,
            "api_calls": stats.api_calls,
            "cells_searched": stats.cells_searched,
            "cells_subdivided": stats.cells_subdivided,
            "cells_skipped": stats.cells_skipped,
            "cells_sparse_skipped": stats.cells_sparse_skipped,
            "cells_duplicate_skipped": stats.cells_duplicate_skipped,
            "duplicates_skipped": stats.duplicates_skipped,
            "chains_skipped": stats.chains_skipped,
            "non_lodging_skipped": stats.non_lodging_skipped,
            "out_of_bounds": stats.out_of_bounds,
        }

    # =========================================================================
    # RETRY METHODS
    # =========================================================================

    async def get_hotels_for_retry(
        self,
        state: str,
        limit: int = 100,
        source_pattern: Optional[str] = None,
    ) -> List[dict]:
        """Get hotels with retryable errors (timeout, 5xx, browser exceptions)."""
        return await repo.get_hotels_for_retry(
            state=state,
            limit=limit,
            source_pattern=source_pattern,
        )

    async def reset_hotels_for_retry(self, hotel_ids: List[int]) -> int:
        """Reset hotel status and delete HBE records to allow retry.

        Returns number of hotels reset.
        """
        if not hotel_ids:
            return 0
        await repo.reset_hotels_for_retry(hotel_ids)
        logger.info(f"Reset {len(hotel_ids)} hotels for retry (status=0, HBE deleted)")
        return len(hotel_ids)

    # =========================================================================
    # BOOKING ENGINE ENUMERATION METHODS
    # =========================================================================

    async def enumerate_guestbook(
        self,
        bbox: Optional[Dict] = None,
        max_pages: Optional[int] = None,
        cloudbeds_only: bool = True,
    ) -> List[Dict]:
        """
        Enumerate hotels from TheGuestbook API (Cloudbeds partner directory).
        """
        async with GuestbookScraper() as scraper:
            properties = await scraper.fetch_all(
                bbox=bbox,
                max_pages=max_pages,
                cloudbeds_only=cloudbeds_only,
            )
        
        # Convert to dicts
        return [
            {
                "id": p.id,
                "name": p.name,
                "lat": p.lat,
                "lng": p.lng,
                "website": p.website,
                "bei_status": p.bei_status,
                "trust_you_score": p.trust_you_score,
                "review_count": p.review_count,
            }
            for p in properties
        ]

    async def enumerate_cloudbeds_sitemap(
        self,
        max_subdomains: Optional[int] = None,
        concurrency: int = 5,
        delay: float = 0.5,
    ) -> List[Dict]:
        """
        Enumerate hotels from Cloudbeds sitemap.
        """
        import random
        import re
        import asyncio
        
        SITEMAP_URL = "https://hotels.cloudbeds.com/sitemap.xml"
        
        async with httpx.AsyncClient() as client:
            # Fetch sitemap
            resp = await client.get(SITEMAP_URL)
            resp.raise_for_status()
            
            # Extract subdomains
            urls = re.findall(r'<loc>([^<]+)</loc>', resp.text)
            subdomains = set()
            for url in urls:
                match = re.match(r'https://([^.]+)\.cloudbeds\.com', url)
                if match:
                    subdomains.add(match.group(1))
            
            subdomains = list(subdomains)
            if max_subdomains:
                subdomains = subdomains[:max_subdomains]
            
            logger.info(f"Found {len(subdomains)} Cloudbeds subdomains in sitemap")
            
            # Scrape each subdomain for hotel name
            results = []
            semaphore = asyncio.Semaphore(concurrency)
            
            async def scrape_subdomain(subdomain: str) -> Optional[Dict]:
                async with semaphore:
                    url = f"https://{subdomain}.cloudbeds.com/"
                    try:
                        await asyncio.sleep(delay + random.uniform(0, delay * 0.5))
                        resp = await client.get(url, follow_redirects=True, timeout=10.0)
                        
                        if resp.status_code == 429:
                            await asyncio.sleep(30)
                            resp = await client.get(url, follow_redirects=True, timeout=10.0)
                        
                        if resp.status_code != 200:
                            return None
                        
                        # Extract name from title
                        title_match = re.search(r'<title>([^<]+)</title>', resp.text)
                        if title_match:
                            name = title_match.group(1).strip()
                            # Clean up title
                            name = re.sub(r'\s*[-|–]\s*Cloudbeds.*$', '', name, flags=re.IGNORECASE)
                            name = re.sub(r'\s*[-|–]\s*Book.*$', '', name, flags=re.IGNORECASE)
                            if name and len(name) > 2:
                                return {
                                    "subdomain": subdomain,
                                    "name": name,
                                    "url": url,
                                }
                        return None
                    except Exception:
                        return None
            
            tasks = [scrape_subdomain(s) for s in subdomains]
            scraped = await asyncio.gather(*tasks)
            results = [r for r in scraped if r is not None]
            
            logger.info(f"Scraped {len(results)} hotels from Cloudbeds sitemap")
            return results

    async def save_booking_engine_leads(
        self,
        leads: List[Dict],
        source: str,
        booking_engine: str,
    ) -> Dict:
        """
        Save leads with known booking engine to database.
        """
        stats = {
            "total": len(leads),
            "inserted": 0,
            "engines_linked": 0,
            "skipped_exists": 0,
            "skipped_no_website": 0,
            "errors": 0,
        }

        # Get booking engine ID (cached)
        engine = await repo.get_booking_engine_by_name(booking_engine)
        engine_id = engine.id if engine else None
        
        if not engine_id:
            # Create booking engine if it doesn't exist
            engine_id = await repo.insert_booking_engine(name=booking_engine, tier=2)
            logger.info(f"Created booking engine '{booking_engine}' with id {engine_id}")

        for i, lead in enumerate(leads):
            if (i + 1) % 100 == 0:
                logger.info(f"  Saved {i + 1}/{len(leads)}...")

            try:
                website = lead.get("website") or lead.get("url")
                if not website:
                    stats["skipped_no_website"] += 1
                    continue

                # Build external_id for deduplication
                external_id = lead.get("external_id")
                external_id_type = lead.get("external_id_type")
                
                if not external_id:
                    # Generate from source + subdomain or name
                    subdomain = lead.get("subdomain")
                    if subdomain:
                        external_id = f"{source}_{subdomain}"
                        external_id_type = source
                    else:
                        external_id = f"{source}_{lead.get('id', lead['name'][:50])}"
                        external_id_type = source

                # Check if already exists by querying
                async with get_conn() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id FROM hotels WHERE external_id = $1 AND external_id_type = $2",
                        external_id,
                        external_id_type,
                    )
                    if existing:
                        stats["skipped_exists"] += 1
                        continue

                # Insert hotel (uses latitude/longitude)
                hotel_id = await repo.insert_hotel(
                    name=lead["name"],
                    website=website,
                    source=source,
                    status=0,
                    latitude=lead.get("lat"),
                    longitude=lead.get("lng"),
                    external_id=external_id,
                    external_id_type=external_id_type,
                )
                stats["inserted"] += 1

                # Link to booking engine
                if engine_id:
                    await repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=engine_id,
                        detection_method=source,
                        status=1,
                    )
                    stats["engines_linked"] += 1

            except Exception as e:
                logger.error(f"Error saving lead {lead.get('name', 'unknown')}: {e}")
                stats["errors"] += 1

        logger.info(f"Save complete: {stats}")
        return stats

    async def enumerate_commoncrawl(
        self,
        max_indices: Optional[int] = None,
        year: Optional[int] = None,
        concurrency: int = 10,
        fetch_details: bool = True,
        use_archives: bool = True,
    ) -> List[Dict]:
        """
        Enumerate Cloudbeds hotels from Common Crawl CDX API.
        
        Args:
            use_archives: If True (default), fetch hotel details from CC's archived
                         HTML on S3 - NO rate limits, faster. If False, scrape
                         Cloudbeds directly (slower, rate limited).
        """
        async with CommonCrawlEnumerator() as enumerator:
            if fetch_details and use_archives:
                # Option B: Fetch from Common Crawl archives (recommended)
                # No rate limits, can parallelize freely
                hotels = await enumerator.enumerate_with_details(
                    max_indices=max_indices,
                    year=year,
                    concurrency=concurrency,
                )
                
                # Add external_id for DB dedup
                for h in hotels:
                    h["external_id"] = f"cloudbeds_{h['slug']}"
                    h["external_id_type"] = "commoncrawl"
                
                return hotels
            
            # Get slugs only
            slugs = await enumerator.enumerate_all(
                max_indices=max_indices,
                year=year,
                concurrency=concurrency,
            )
        
        logger.info(f"Found {len(slugs)} unique Cloudbeds slugs from Common Crawl")
        
        if not fetch_details:
            # Return just the slugs without fetching hotel details
            return [
                {
                    "slug": slug,
                    "booking_url": f"https://hotels.cloudbeds.com/reservation/{slug}",
                }
                for slug in slugs
            ]
        
        # Option A: Fetch hotel details by scraping Cloudbeds directly
        # (slower, rate limited - only use if archives don't work)
        logger.info(f"Fetching hotel details from Cloudbeds for {len(slugs)} slugs...")
        
        async with CloudbedsPropertyExtractor() as extractor:
            properties = await extractor.batch_fetch(slugs, concurrency=concurrency)
        
        logger.info(f"Successfully fetched details for {len(properties)} hotels")
        
        # Convert to dicts
        return [
            {
                "slug": p.slug,
                "name": p.name,
                "booking_url": p.booking_url,
                "property_id": p.property_id,
                "external_id": f"cloudbeds_{p.slug}",
                "external_id_type": "commoncrawl",
            }
            for p in properties
        ]

    async def ingest_commoncrawl_hotels(
        self,
        hotels: List[Dict],
        source_tag: str = "commoncrawl",
    ) -> Dict:
        """
        Ingest Common Crawl hotels into database with smart deduplication.
        
        Strategy:
        1. First try to match by name (exact, case-insensitive)
        2. If match found: append source (e.g., dbpr -> dbpr::commoncrawl)
        3. If no match: insert new hotel
        4. Always link to Cloudbeds booking engine using repo function
        
        Args:
            hotels: List of hotel dicts from enumerate_commoncrawl()
            source_tag: Source identifier to append (default: 'commoncrawl')
            
        Returns dict with counts: inserted, updated, engines_linked, errors
        """
        stats = {
            "total": len(hotels),
            "inserted": 0,
            "updated": 0,
            "engines_linked": 0,
            "skipped": 0,
            "errors": 0,
        }
        
        # Get Cloudbeds engine ID using repo
        engine = await repo.get_booking_engine_by_name("Cloudbeds")
        engine_id = engine.id if engine else None
        
        if not engine_id:
            engine_id = await repo.insert_booking_engine(name="Cloudbeds", tier=1)
            logger.info(f"Created Cloudbeds booking engine with id {engine_id}")
        
        for i, hotel in enumerate(hotels):
            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i + 1}/{len(hotels)}...")
            
            try:
                name = hotel.get("name")
                if not name or name == "Unknown":
                    stats["skipped"] += 1
                    continue
                
                slug = hotel.get("slug", "")
                city = hotel.get("city")
                country = hotel.get("country", "USA")
                booking_url = hotel.get("booking_url") or f"https://hotels.cloudbeds.com/reservation/{slug}"
                external_id = hotel.get("external_id") or f"cloudbeds_{slug}"
                
                async with get_conn() as conn:
                    # Try to find existing hotel by name (and city if available)
                    existing = None
                    if city:
                        existing = await conn.fetchrow(
                            """
                            SELECT id, name, source FROM sadie_gtm.hotels
                            WHERE LOWER(TRIM(name)) = LOWER(TRIM($1))
                              AND LOWER(TRIM(city)) = LOWER(TRIM($2))
                            LIMIT 1
                            """,
                            name, city
                        )
                    
                    if not existing:
                        # Try matching by name only
                        existing = await conn.fetchrow(
                            """
                            SELECT id, name, source FROM sadie_gtm.hotels
                            WHERE LOWER(TRIM(name)) = LOWER(TRIM($1))
                            LIMIT 1
                            """,
                            name
                        )
                    
                    if existing:
                        # Update existing hotel: append source
                        hotel_id = existing["id"]
                        current_source = existing["source"] or ""
                        
                        # Append source if not already present
                        if source_tag not in current_source:
                            new_source = f"{current_source}::{source_tag}" if current_source else source_tag
                            await conn.execute(
                                """
                                UPDATE sadie_gtm.hotels 
                                SET source = $1, updated_at = CURRENT_TIMESTAMP
                                WHERE id = $2
                                """,
                                new_source, hotel_id
                            )
                        
                        stats["updated"] += 1
                    else:
                        # Insert new hotel using repo pattern
                        hotel_id = await repo.insert_hotel(
                            name=name,
                            city=city,
                            country=country,
                            source=source_tag,
                            status=0,
                            external_id=external_id,
                            external_id_type="commoncrawl",
                        )
                        stats["inserted"] += 1
                
                # Link to Cloudbeds booking engine using repo function
                if hotel_id and engine_id:
                    await repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=engine_id,
                        booking_url=booking_url,
                        detection_method="commoncrawl",
                        status=1,
                    )
                    stats["engines_linked"] += 1
                        
            except Exception as e:
                logger.error(f"Error ingesting {hotel.get('name', 'unknown')}: {e}")
                stats["errors"] += 1
        
        logger.info(f"Ingest complete: {stats}")
        return stats

    async def ingest_crawled_urls(
        self,
        file_path: str,
        booking_engine: str,
        source_tag: str = "commoncrawl",
        scrape_names: bool = True,
        concurrency: int = 50,
        use_common_crawl: bool = True,
        fuzzy_match: bool = True,
        fuzzy_threshold: float = 0.7,
        checkpoint_file: Optional[str] = None,
    ) -> Dict:
        """
        Ingest crawled booking engine URLs/slugs from a file.
        
        Improvements:
        1. Uses Common Crawl S3 archives directly (50+ hotels/sec, no rate limits)
        2. Better name extraction (og:title, h1, meta tags, schema.org)
        3. Extracts actual hotel website when available
        4. Fuzzy deduplication with pg_trgm similarity
        5. Checkpoint/resume for large imports
        
        Args:
            file_path: Path to text file with slugs/URLs
            booking_engine: Engine name (cloudbeds, mews, rms, siteminder)
            source_tag: Source identifier for tracking
            scrape_names: If True, scrape hotel names (always True for quality)
            concurrency: Number of concurrent requests
            use_common_crawl: Use CC archives (fast) vs Wayback (slow)
            fuzzy_match: Enable fuzzy name matching for deduplication
            fuzzy_threshold: Similarity threshold (0.0-1.0, default 0.7)
            checkpoint_file: Path to save progress for resume
        """
        from pathlib import Path
        from .constants import BOOKING_ENGINE_URL_PATTERNS, BOOKING_ENGINE_TIERS
        from .booking_engines import CrawlIngester
        
        stats = {
            "total": 0,
            "inserted": 0,
            "updated": 0,
            "engines_linked": 0,
            "skipped_no_name": 0,
            "skipped_duplicate": 0,
            "fuzzy_matched": 0,
            "websites_found": 0,
            "errors": 0,
        }
        
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Get or create booking engine
        engine_name = booking_engine.title()
        if booking_engine.lower() == "rms":
            engine_name = "RMS Cloud"
        elif booking_engine.lower() == "siteminder":
            engine_name = "SiteMinder"
        
        engine = await repo.get_booking_engine_by_name(engine_name)
        engine_id = engine.id if engine else None
        
        if not engine_id:
            tier = BOOKING_ENGINE_TIERS.get(booking_engine.lower(), 2)
            engine_id = await repo.insert_booking_engine(name=engine_name, tier=tier)
            logger.info(f"Created {engine_name} booking engine with id {engine_id}")
        
        # Load slugs from file
        lines = file_path.read_text().strip().split("\n")
        all_slugs = list(set([s.strip().lower() for s in lines if s.strip()]))
        stats["total"] = len(all_slugs)
        
        logger.info(f"Loaded {len(all_slugs)} unique slugs")
        
        # Load checkpoint to skip already processed
        processed_slugs = set()
        if checkpoint_file:
            from pathlib import Path
            cp = Path(checkpoint_file)
            if cp.exists():
                processed_slugs = set(cp.read_text().strip().split('\n'))
                logger.info(f"Resuming: {len(processed_slugs)} already processed")
        
        slugs_to_process = [s for s in all_slugs if s not in processed_slugs]
        logger.info(f"Processing {len(slugs_to_process)} slugs...")
        
        if not slugs_to_process:
            logger.info("All slugs already processed!")
            return stats
        
        # Process incrementally in batches - fetch, extract, save immediately
        from .booking_engines import CommonCrawlEnumerator, CrawlIngester
        
        batch_size = 50  # Save to DB every 50 hotels for faster feedback
        
        if booking_engine.lower() == "cloudbeds" and use_common_crawl:
            logger.info("Using Common Crawl S3 archives (incremental save)...")
            
            async with CommonCrawlEnumerator() as enumerator:
                # Process in batches
                for batch_start in range(0, len(slugs_to_process), batch_size):
                    batch_slugs = slugs_to_process[batch_start:batch_start + batch_size]
                    
                    # Step 1: Look up in CDX (serial mode to avoid rate limits)
                    cdx_records = await enumerator.lookup_slugs_in_cdx(batch_slugs)
                    
                    # Step 2: Fetch HTML and extract
                    hotels = []
                    semaphore = asyncio.Semaphore(concurrency)
                    
                    async def fetch_and_extract(slug, record):
                        async with semaphore:
                            html = await enumerator.fetch_archived_html(record)
                            if html:
                                return enumerator.extract_hotel_info(html, slug)
                            return None
                    
                    tasks = [fetch_and_extract(s, r) for s, r in cdx_records.items()]
                    results = await asyncio.gather(*tasks)
                    hotels = [r for r in results if r and r.get('name')]
                    
                    # Step 3: Save to DB immediately
                    batch_stats = await self._save_hotels_batch(
                        hotels, engine_id, booking_engine, source_tag,
                        fuzzy_match, fuzzy_threshold
                    )
                    
                    # Update stats
                    for key in batch_stats:
                        stats[key] = stats.get(key, 0) + batch_stats[key]
                    
                    # Save checkpoint
                    if checkpoint_file:
                        with open(checkpoint_file, 'a') as f:
                            for slug in batch_slugs:
                                f.write(f"{slug}\n")
                    
                    pct = ((batch_start + len(batch_slugs)) / len(slugs_to_process)) * 100
                    total_saved = stats.get('inserted', 0) + stats.get('updated', 0)
                    logger.info(f"  [{pct:.1f}%] Batch {batch_start + len(batch_slugs)}/{len(slugs_to_process)}: "
                               f"+{batch_stats['inserted']} new, +{batch_stats['updated']} updated "
                               f"(total saved: {total_saved})")
        else:
            # Wayback fallback - also incremental
            hotels = await self._scrape_hotels_wayback(
                file_path, booking_engine, concurrency
            )
            batch_stats = await self._save_hotels_batch(
                hotels, engine_id, booking_engine, source_tag,
                fuzzy_match, fuzzy_threshold
            )
            for key in batch_stats:
                stats[key] = stats.get(key, 0) + batch_stats[key]
        
        logger.info(f"Ingest complete: {stats}")
        return stats
    
    async def _save_hotels_batch(
        self,
        hotels: List[Dict],
        engine_id: int,
        booking_engine: str,
        source_tag: str,
        fuzzy_match: bool,
        fuzzy_threshold: float,
    ) -> Dict:
        """Save a batch of hotels to DB with deduplication."""
        stats = {
            "inserted": 0,
            "updated": 0,
            "fuzzy_matched": 0,
            "websites_found": 0,
            "engines_linked": 0,
            "skipped_no_name": 0,
            "skipped_duplicate": 0,
            "errors": 0,
        }
        
        for hotel in hotels:
            try:
                name = hotel.get("name")
                if not name:
                    stats["skipped_no_name"] += 1
                    continue
                
                slug = hotel.get("slug", "")
                city = hotel.get("city")
                country = hotel.get("country", "USA")
                website = hotel.get("website")
                booking_url = hotel.get("booking_url", "")
                
                external_id = f"{booking_engine.lower()}_{slug}"
                external_id_type = f"{booking_engine.lower()}_crawl"
                
                # STEP 0: Check if booking URL already exists (most reliable match)
                existing_by_url = await repo.get_hotel_by_booking_url(booking_url)
                if existing_by_url:
                    # Hotel already has this booking URL - just update detection method
                    hotel_id = existing_by_url["hotel_id"]
                    await repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=engine_id,
                        booking_url=booking_url,
                        engine_property_id=slug,
                        detection_method="crawl_import",
                        status=1,
                    )
                    stats["updated"] += 1
                    stats["engines_linked"] += 1
                    continue
                
                async with get_conn() as conn:
                    existing = None
                    
                    # Try exact match first
                    existing = await conn.fetchrow(
                        "SELECT id, source, website FROM sadie_gtm.hotels WHERE LOWER(TRIM(name)) = LOWER(TRIM($1)) LIMIT 1",
                        name
                    )
                    
                    # Try fuzzy match if enabled and no exact match
                    if not existing and fuzzy_match:
                        try:
                            existing = await conn.fetchrow(
                                """
                                SELECT id, name, source, website, similarity(name, $1) as sim
                                FROM sadie_gtm.hotels
                                WHERE similarity(name, $1) > $2
                                  AND ($3::text IS NULL OR city IS NULL OR LOWER(city) = LOWER($3))
                                ORDER BY sim DESC LIMIT 1
                                """,
                                name, fuzzy_threshold, city
                            )
                            if existing:
                                stats["fuzzy_matched"] += 1
                        except Exception:
                            pass  # pg_trgm not installed
                    
                    if existing:
                        hotel_id = existing["id"]
                        current_source = existing["source"] or ""
                        current_website = existing["website"]
                        
                        updates = []
                        params = []
                        
                        if source_tag not in current_source:
                            new_source = f"{current_source}::{source_tag}" if current_source else source_tag
                            updates.append("source = $1")
                            params.append(new_source)
                        
                        if website and not current_website:
                            updates.append(f"website = ${len(params) + 1}")
                            params.append(website)
                            stats["websites_found"] += 1
                        
                        if updates:
                            params.append(hotel_id)
                            await conn.execute(
                                f"UPDATE sadie_gtm.hotels SET {', '.join(updates)}, updated_at = NOW() WHERE id = ${len(params)}",
                                *params
                            )
                        
                        stats["updated"] += 1
                    else:
                        # Insert at DETECTED status - we already know their booking engine
                        hotel_id = await repo.insert_hotel(
                            name=name,
                            website=website,
                            city=city,
                            country=country,
                            source=source_tag,
                            status=PipelineStage.DETECTED,
                            external_id=external_id,
                            external_id_type=external_id_type,
                        )
                        stats["inserted"] += 1
                        if website:
                            stats["websites_found"] += 1
                
                if hotel_id:
                    await repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=engine_id,
                        booking_url=booking_url,
                        engine_property_id=slug,
                        detection_method="crawl_import",
                        status=1,
                    )
                    stats["engines_linked"] += 1
                    
            except Exception as e:
                if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                    stats["skipped_duplicate"] += 1
                else:
                    stats["errors"] += 1
        
        return stats
    
    async def _scrape_hotels_wayback(
        self,
        file_path,
        booking_engine: str,
        concurrency: int,
    ) -> List[Dict]:
        """Fallback scraping via Wayback Machine for non-Cloudbeds engines."""
        import httpx
        import re
        import asyncio
        from pathlib import Path
        from .constants import BOOKING_ENGINE_URL_PATTERNS
        
        path = Path(file_path)
        slugs = list(set([s.strip() for s in path.read_text().strip().split('\n') if s.strip()]))
        
        url_pattern = BOOKING_ENGINE_URL_PATTERNS.get(booking_engine.lower())
        semaphore = asyncio.Semaphore(concurrency)
        hotels = []
        
        async def scrape_one(client: httpx.AsyncClient, slug: str) -> Optional[Dict]:
            async with semaphore:
                if url_pattern:
                    booking_url = url_pattern.replace("{slug}", slug)
                else:
                    booking_url = f"https://{slug}" if not slug.startswith("http") else slug
                
                try:
                    wayback_url = f"https://web.archive.org/web/2024/{booking_url}"
                    resp = await client.get(wayback_url, follow_redirects=True, timeout=15.0)
                    
                    if resp.status_code == 200:
                        html = resp.text
                        
                        # Better extraction - try multiple sources
                        name = None
                        for pattern in [
                            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
                            r'<h1[^>]*>([^<]+)</h1>',
                            r'<title>([^<]+)</title>',
                        ]:
                            match = re.search(pattern, html, re.IGNORECASE)
                            if match:
                                raw = match.group(1).strip()
                                parts = re.split(r'\s*[-|–]\s*', raw)
                                name = parts[0].strip()
                                if name.lower() not in ['book now', 'reservation', 'booking', 'home']:
                                    break
                                name = None
                        
                        if name:
                            return {
                                "slug": slug,
                                "name": name,
                                "booking_url": booking_url,
                            }
                except Exception:
                    pass
                
                return None
        
        async with httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0 (compatible; HotelBot/1.0)"},
        ) as client:
            batch_size = 100
            for i in range(0, len(slugs), batch_size):
                batch = slugs[i:i + batch_size]
                tasks = [scrape_one(client, s) for s in batch]
                results = await asyncio.gather(*tasks)
                hotels.extend([r for r in results if r])
                logger.info(f"  Scraped {i + len(batch)}/{len(slugs)}")
        
        return hotels
