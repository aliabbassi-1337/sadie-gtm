"""LeadGen Service - Scraping and detection pipeline."""

from abc import ABC, abstractmethod
from optparse import Option
from typing import Dict, List, Tuple, Optional

from loguru import logger

from services.leadgen import repo
from services.leadgen.constants import HotelStatus
from services.leadgen.detector import BatchDetector, DetectionConfig, DetectionResult
from db.models.hotel import Hotel
from services.leadgen.grid_scraper import GridScraper, ScrapedHotel, ScrapeEstimate, CITY_COORDINATES, DEFAULT_CELL_SIZE_KM

# Re-export for public API
__all__ = ["IService", "Service", "ScrapeEstimate", "CITY_COORDINATES"]


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
    async def scrape_state(self, state: str, cell_size_km: float = DEFAULT_CELL_SIZE_KM) -> int:
        """
        Scrape hotels in an entire state using adaptive grid.
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
    def estimate_state(self, state: str, cell_size_km: float = DEFAULT_CELL_SIZE_KM) -> ScrapeEstimate:
        """Estimate cost for scraping a state."""
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
    async def enqueue_hotels_for_detection(self, limit: int = 1000, batch_size: int = 20) -> int:
        """
        Enqueue hotels for detection via SQS.
        Queries hotels with status=0 and no hotel_booking_engines record.
        Detection tracked by hotel_booking_engines presence, not status.
        Returns count of hotels enqueued.
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

    async def scrape_state(self, state: str, cell_size_km: float = DEFAULT_CELL_SIZE_KM) -> int:
        """
        Scrape hotels in an entire state using adaptive grid.
        Saves incrementally after each batch so progress isn't lost.

        Args:
            state: State name (e.g., "florida", "california")
            cell_size_km: Cell size in km (smaller = more thorough, default 2km)

        Returns:
            Number of hotels found and saved to database
        """
        logger.info(f"Starting state scrape: {state}")

        # Initialize scraper
        scraper = GridScraper(api_key=self._api_key, cell_size_km=cell_size_km)
        source = f"grid_{state.lower().replace(' ', '_')}"

        # Track total saved
        saved_count = 0

        # Incremental save callback
        async def save_batch(batch_hotels):
            nonlocal saved_count
            count = await self._save_hotels(batch_hotels, source=source)
            saved_count += count

        # Run the scrape with incremental saving
        hotels, stats = await scraper.scrape_state(state, on_batch_complete=save_batch)

        logger.info(
            f"State scrape complete ({state}): {stats.hotels_found} found, "
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

    async def enqueue_hotels_for_detection(self, limit: int = 1000, batch_size: int = 20) -> int:
        """Enqueue hotels for detection via SQS.

        Queries hotels with status=0 and no hotel_booking_engines record.
        Sends to SQS in batches. Does NOT update status - detection is tracked
        by presence of hotel_booking_engines record.

        Returns count of hotels enqueued.
        """
        from infra.sqs import send_messages_batch, get_queue_url

        # Get hotels pending detection (status=0, no booking engine record)
        hotels = await repo.get_hotels_pending_detection(limit=limit)
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
    async def detect_booking_engines(self, limit: int = 100) -> int:
        """
        Detect booking engines for hotels with status=0 (scraped).

        TODO: Integrate detect.py script

        Args:
            limit: Maximum number of hotels to process

        Returns:
            Number of hotels processed
        """
        # TODO: Integrate detect.py script
        logger.warning("detect_booking_engines not yet implemented")
        return 0

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
        return scraper.estimate_region(center_lat, center_lng, radius_km)

    def estimate_state(self, state: str, cell_size_km: float = DEFAULT_CELL_SIZE_KM) -> ScrapeEstimate:
        """Estimate cost for scraping a state."""
        scraper = GridScraper.__new__(GridScraper)  # Skip __init__ validation
        scraper.cell_size_km = cell_size_km
        return scraper.estimate_state(state)
