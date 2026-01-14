from abc import ABC, abstractmethod
from decimal import Decimal
import asyncio

import httpx

from services.enrichment import repo
from services.enrichment.room_count_enricher import (
    enrich_hotel_room_count,
    get_groq_api_key,
    log,
)
from services.enrichment.customer_proximity import (
    log as proximity_log,
)


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
        Count hotels waiting for enrichment (status=1).
        """
        pass

    @abstractmethod
    async def get_pending_proximity_count(self) -> int:
        """
        Count hotels waiting for proximity calculation.
        """
        pass


class Service(IService):
    def __init__(self) -> None:
        pass

    async def enrich_room_counts(self, limit: int = 100) -> int:
        """
        Get room counts for hotels with status=1 (detected).
        Uses regex extraction first, then falls back to Groq LLM estimation.
        Returns number of hotels enriched.
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

        log(f"Claimed {len(hotels)} hotels for enrichment")

        enriched_count = 0

        # Use SSL context that's more permissive for older sites
        async with httpx.AsyncClient(verify=False) as client:
            for hotel in hotels:
                # Skip if no website
                if not hotel.website:
                    await repo.update_hotel_enrichment_status(hotel.id, status=1)
                    continue

                # Enrich this hotel
                room_count, source = await enrich_hotel_room_count(
                    client=client,
                    hotel_id=hotel.id,
                    hotel_name=hotel.name,
                    website=hotel.website,
                )

                if room_count:
                    # Set confidence based on source
                    confidence = Decimal("1.0") if source == "regex" else Decimal("0.7")

                    # Insert room count
                    await repo.insert_room_count(
                        hotel_id=hotel.id,
                        room_count=room_count,
                        source=source,
                        confidence=confidence,
                    )

                    # Update hotel status to enriched (3)
                    await repo.update_hotel_enrichment_status(hotel.id, status=3)
                    enriched_count += 1
                else:
                    # Reset back to detected (1) if enrichment failed
                    await repo.update_hotel_enrichment_status(hotel.id, status=1)

                # Delay to avoid Groq rate limits (30 RPM = 1 request every 2 seconds)
                await asyncio.sleep(2.5)

        log(f"Enrichment complete: {enriched_count}/{len(hotels)} hotels enriched")
        return enriched_count

    async def calculate_customer_proximity(
        self,
        limit: int = 100,
        max_distance_km: float = 100.0,
    ) -> int:
        """
        Calculate distance to nearest Sadie customer for hotels.
        Uses PostGIS for efficient spatial queries.
        Returns number of hotels processed.
        """
        # Get hotels needing proximity calculation
        hotels = await repo.get_hotels_pending_proximity(limit=limit)

        if not hotels:
            proximity_log("No hotels pending proximity calculation")
            return 0

        proximity_log(f"Processing {len(hotels)} hotels for proximity calculation")

        processed_count = 0

        for hotel in hotels:
            # Skip if hotel has no location
            if hotel.latitude is None or hotel.longitude is None:
                continue

            # Find nearest customer using PostGIS
            nearest = await repo.find_nearest_customer(
                hotel_id=hotel.id,
                max_distance_km=max_distance_km,
            )

            if nearest:
                # Insert proximity record
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
                proximity_log(f"  {hotel.name}: no customer within {max_distance_km}km")

        proximity_log(
            f"Proximity calculation complete: {processed_count}/{len(hotels)} "
            f"hotels have nearby customers"
        )
        return processed_count

    async def get_pending_enrichment_count(self) -> int:
        """Count hotels waiting for enrichment (status=1, not yet enriched)."""
        return await repo.get_pending_enrichment_count()

    async def get_pending_proximity_count(self) -> int:
        """Count hotels waiting for proximity calculation."""
        return await repo.get_pending_proximity_count()
