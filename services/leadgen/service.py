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
from db.models.hotel import Hotel
from db.client import init_db
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
    async def save_detection_results(self, results: List[DetectionResult]) -> Tuple[int, int]:
        """
        Save detection results to database.
        Returns (detected_count, error_count) tuple.
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
                "google_place_id": h.google_place_id,  # Primary dedup key
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
        2. Run batch detection (visits websites, detects engines)
        3. Update database with results
        4. Return detection results
        """
        # Get hotels to process
        hotels = await repo.get_hotels_pending_detection(limit=limit)
        if not hotels:
            logger.info("No hotels pending detection")
            return []

        logger.info(f"Processing {len(hotels)} hotels for detection")

        # Convert to dicts for detector
        hotel_dicts = [
            {"id": h.id, "name": h.name, "website": h.website, "city": h.city or ""}
            for h in hotels
        ]

        # Run detection
        detector = BatchDetector(self.detection_config)
        results = await detector.detect_batch(hotel_dicts)

        # Update database with results
        for result in results:
            await self._save_detection_result(result)

        # Log summary
        detected = sum(1 for r in results if r.booking_engine and r.booking_engine not in ("", "unknown", "unknown_third_party"))
        failed = sum(1 for r in results if r.error)
        logger.info(f"Detection complete: {detected} detected, {failed} errors, {len(results) - detected - failed} no engine")

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

            # Handle non-retriable errors (timeout, precheck_failed, etc.)
            # Create a hotel_booking_engines record with status=-1 to prevent infinite retry
            if result.error and result.error not in ("location_mismatch",):
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

    async def save_detection_results(self, results: List[DetectionResult]) -> Tuple[int, int]:
        """Save detection results to database.

        Returns (detected_count, error_count) tuple.
        Note: Location mismatches are not counted as errors or detections.
        """
        detected = 0
        errors = 0

        for result in results:
            try:
                await self._save_detection_result(result)

                if result.error == "location_mismatch":
                    # Don't count as detected or error
                    pass
                elif result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "unknown_booking_api"):
                    detected += 1
                elif result.error:
                    errors += 1
            except Exception as e:
                logger.error(f"Error saving result for hotel {result.hotel_id}: {e}")
                errors += 1

        return (detected, errors)

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
                    if h.google_place_id:
                        if h.google_place_id in seen_ids:
                            continue
                        seen_ids.add(h.google_place_id)
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
        pool = await init_db()
        existing = await pool.fetch('''
            SELECT google_place_id, ST_Y(location::geometry) as lat, ST_X(location::geometry) as lng
            FROM sadie_gtm.hotels
            WHERE google_place_id IS NOT NULL
            AND ST_Within(
                location::geometry,
                ST_MakeEnvelope($1, $2, $3, $4, 4326)
            )
        ''', lng_min, lat_min, lng_max, lat_max)

        existing_place_ids = {r['google_place_id'] for r in existing if r['google_place_id']}
        existing_locations = {(round(r['lat'], 4), round(r['lng'], 4)) for r in existing}
        scraper.preload_existing(existing_place_ids, existing_locations)

        total_saved = 0
        all_hotels = []
        seen_ids = set(existing_place_ids)  # Also skip in batch saving

        async def save_batch(batch_hotels):
            nonlocal total_saved
            unique_batch = []
            for h in batch_hotels:
                if h.google_place_id:
                    if h.google_place_id in seen_ids:
                        continue
                    seen_ids.add(h.google_place_id)
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
