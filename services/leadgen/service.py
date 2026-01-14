from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

from loguru import logger

from services.leadgen import repo
from services.leadgen.detector import BatchDetector, DetectionConfig, DetectionResult
from db.models.hotel import Hotel


class IService(ABC):
    """LeadGen Service - Scraping pipeline."""

    @abstractmethod
    async def scrape_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float
    ) -> int:
        """
        Scrape hotels in a circular region using adaptive grid.
        Returns number of hotels found.
        """
        pass

    @abstractmethod
    async def scrape_state(self, state: str) -> int:
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
    async def get_pending_detection_count(self) -> int:
        """
        Count hotels waiting for detection (status=0).
        """
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
    async def claim_hotels_for_detection(self, limit: int = 100) -> List[Hotel]:
        """
        Atomically claim hotels for processing (multi-worker safe).
        Returns list of claimed Hotel models.
        """
        pass

    @abstractmethod
    async def reset_stale_processing_hotels(self) -> None:
        """
        Reset hotels stuck in processing state (status=10) for > 30 min.
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
        Queries status=0 hotels, sends to SQS in batches, sets status=10.
        Returns count of hotels enqueued.
        """
        pass


class Service(IService):
    def __init__(self, detection_config: DetectionConfig = None) -> None:
        self.detection_config = detection_config or DetectionConfig()

    async def scrape_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float
    ) -> int:
        # TODO: Integrate grid scraper
        return 0

    async def scrape_state(self, state: str) -> int:
        # TODO: Integrate grid scraper with state bounds
        return 0

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
            {"id": h.id, "name": h.name, "website": h.website}
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

                # Link hotel to booking engine
                await repo.insert_hotel_booking_engine(
                    hotel_id=result.hotel_id,
                    booking_engine_id=engine_id,
                    booking_url=result.booking_url or None,
                    detection_method=result.detection_method or None,
                )

                # Update hotel status to 1 (detected)
                await repo.update_hotel_status(
                    hotel_id=result.hotel_id,
                    status=1,
                    phone_website=result.phone_website or None,
                    email=result.email or None,
                )
            else:
                # No booking engine found - status 99
                await repo.update_hotel_status(
                    hotel_id=result.hotel_id,
                    status=99,
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
        """
        detected = 0
        errors = 0

        for result in results:
            try:
                await self._save_detection_result(result)
                if result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "unknown_booking_api"):
                    detected += 1
                elif result.error:
                    errors += 1
            except Exception as e:
                logger.error(f"Error saving result for hotel {result.hotel_id}: {e}")
                errors += 1

        return (detected, errors)

    async def claim_hotels_for_detection(self, limit: int = 100) -> List[Hotel]:
        """Atomically claim hotels for processing (multi-worker safe)."""
        return await repo.claim_hotels_for_detection(limit=limit)

    async def reset_stale_processing_hotels(self) -> None:
        """Reset hotels stuck in processing state (status=10) for > 30 min."""
        await repo.reset_stale_processing_hotels()

    async def get_hotels_by_ids(self, hotel_ids: List[int]) -> List[Hotel]:
        """Get hotels by list of IDs."""
        return await repo.get_hotels_by_ids(hotel_ids=hotel_ids)

    async def enqueue_hotels_for_detection(self, limit: int = 1000, batch_size: int = 20) -> int:
        """Enqueue hotels for detection via SQS.

        Queries status=0 hotels, sends to SQS in batches, sets status=10.
        Returns count of hotels enqueued.
        """
        from infra.sqs import send_messages_batch, get_queue_url

        # Get hotels pending detection
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

        # Update status to 10 (enqueued)
        await repo.update_hotels_status_batch(hotel_ids=hotel_ids, status=10)

        return len(hotel_ids)
