from abc import ABC, abstractmethod


class IService(ABC):
    """Enrichment Service - Enrich hotel data with room counts and proximity."""

    @abstractmethod
    async def enrich_room_counts(self, limit: int = 100) -> int:
        """
        Get room counts for hotels with status=1 (detected).
        Uses Groq/Google to extract room count from website.
        Returns number of hotels enriched.
        """
        pass

    @abstractmethod
    async def calculate_customer_proximity(self, limit: int = 100) -> int:
        """
        Calculate distance to nearest Sadie customer for hotels.
        Updates hotel_customer_proximity table.
        Returns number of hotels processed.
        """
        pass

    @abstractmethod
    async def get_pending_enrichment_count(self) -> int:
        """
        Count hotels waiting for enrichment (status=1).
        """
        pass


class Service(IService):
    def __init__(self) -> None:
        pass

    async def enrich_room_counts(self, limit: int = 100) -> int:
        # TODO: Integrate room_count_groq.py
        return 0

    async def calculate_customer_proximity(self, limit: int = 100) -> int:
        # TODO: Integrate customer_match.py
        return 0

    async def get_pending_enrichment_count(self) -> int:
        # TODO: Query hotels WHERE status=1
        return 0
