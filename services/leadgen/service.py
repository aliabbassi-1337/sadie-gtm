"""LeadGen Service - Scraping and detection pipeline."""

from abc import ABC, abstractmethod
from typing import List, Optional

from loguru import logger

from services.leadgen import repo
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
    async def detect_booking_engines(self, limit: int = 100) -> int:
        """
        Detect booking engines for hotels with status=0 (scraped).
        Updates status to 1 (detected) or 2 (no_booking_engine).
        Returns number of hotels processed.
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


class Service(IService):
    """Implementation of the LeadGen service."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        """
        Initialize the LeadGen service.

        Args:
            api_key: Optional Serper API key. If not provided, uses SERPER_SAMI env var.
        """
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

        # Run the scrape
        hotels, stats = await scraper.scrape_region(center_lat, center_lng, radius_km)

        # Save to database
        saved_count = await self._save_hotels(hotels, source="grid_region")

        logger.info(
            f"Region scrape complete: {stats.hotels_found} found, "
            f"{saved_count} saved, {stats.api_calls} API calls, "
            f"{stats.cells_searched} cells ({stats.cells_subdivided} subdivided)"
        )

        return saved_count

    async def scrape_state(self, state: str, cell_size_km: float = DEFAULT_CELL_SIZE_KM) -> int:
        """
        Scrape hotels in an entire state using adaptive grid.

        Args:
            state: State name (e.g., "florida", "california")
            cell_size_km: Cell size in km (smaller = more thorough, default 2km)

        Returns:
            Number of hotels found and saved to database
        """
        logger.info(f"Starting state scrape: {state}")

        # Initialize scraper
        scraper = GridScraper(api_key=self._api_key, cell_size_km=cell_size_km)

        # Run the scrape
        hotels, stats = await scraper.scrape_state(state)

        # Save to database
        source = f"grid_{state.lower().replace(' ', '_')}"
        saved_count = await self._save_hotels(hotels, source=source)

        logger.info(
            f"State scrape complete ({state}): {stats.hotels_found} found, "
            f"{saved_count} saved, {stats.api_calls} API calls, "
            f"{stats.cells_searched} cells ({stats.cells_subdivided} subdivided)"
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
