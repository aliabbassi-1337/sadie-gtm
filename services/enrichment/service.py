from abc import ABC, abstractmethod
from decimal import Decimal
import asyncio
import os

import httpx
from dotenv import load_dotenv

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

load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY")


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
