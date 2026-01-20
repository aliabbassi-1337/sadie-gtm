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
from services.enrichment.website_enricher import WebsiteEnricher


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


class Service(IService):
    def __init__(self) -> None:
        pass

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
        """Count hotels waiting for enrichment (has website, not yet in hotel_room_count)."""
        return await repo.get_pending_enrichment_count()

    async def get_pending_proximity_count(self) -> int:
        """Count hotels waiting for proximity calculation."""
        return await repo.get_pending_proximity_count()

    async def enrich_websites(
        self,
        api_key: str,
        limit: int = 100,
        source_filter: str = None,
        state_filter: str = None,
        delay: float = 0.1,
    ) -> dict:
        """
        Find websites for hotels that don't have them via Serper search.

        Uses claim pattern for multi-worker safety.

        Args:
            api_key: Serper API key
            limit: Max hotels to process
            source_filter: Filter by source (e.g., 'dbpr')
            state_filter: Filter by state (e.g., 'FL')
            delay: Delay between API calls

        Returns:
            Stats dict with found/not_found/errors counts
        """
        # Claim hotels atomically (multi-worker safe)
        hotels = await repo.claim_hotels_for_website_enrichment(
            limit=limit,
            source_filter=source_filter,
            state_filter=state_filter,
        )

        if not hotels:
            log("No hotels found needing website enrichment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0}

        log(f"Claimed {len(hotels)} hotels for website enrichment")

        enricher = WebsiteEnricher(api_key=api_key, delay_between_requests=delay)

        found = 0
        not_found = 0
        errors = 0

        for i, hotel in enumerate(hotels):
            if (i + 1) % 50 == 0:
                log(f"  Progress: {i + 1}/{len(hotels)} ({found} found)")

            result = await enricher.find_website(
                name=hotel["name"],
                city=hotel["city"],
                state=hotel.get("state", "FL"),
            )

            if result.website:
                found += 1
                await repo.update_hotel_website(hotel["id"], result.website)
                await repo.update_website_enrichment_status(
                    hotel["id"], status=1, source="serper"
                )
            elif result.error == "no_match":
                not_found += 1
                await repo.update_website_enrichment_status(
                    hotel["id"], status=0, source="serper"
                )
            else:
                errors += 1
                await repo.update_website_enrichment_status(
                    hotel["id"], status=0, source="serper"
                )

        log(f"Website enrichment complete: {found} found, {not_found} not found, {errors} errors")

        return {
            "total": len(hotels),
            "found": found,
            "not_found": not_found,
            "errors": errors,
            "api_calls": len(hotels),
        }
