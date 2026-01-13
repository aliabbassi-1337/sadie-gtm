from abc import ABC, abstractmethod


class IService(ABC):
    """LeadGen Service - Scraping and detection pipeline."""

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
    async def detect_booking_engines(self, limit: int = 100) -> int:
        """
        Detect booking engines for hotels with status=0 (scraped).
        Updates status to 1 (detected) or 99 (no_booking_engine).
        Returns number of hotels processed.
        """
        pass

    @abstractmethod
    async def get_pending_detection_count(self) -> int:
        """
        Count hotels waiting for detection (status=0).
        """
        pass


class Service(IService):
    def __init__(self) -> None:
        pass

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

    async def detect_booking_engines(self, limit: int = 100) -> int:
        # TODO: Integrate detect.py script
        return 0

    async def get_pending_detection_count(self) -> int:
        # TODO: Query hotels WHERE status=0
        return 0
