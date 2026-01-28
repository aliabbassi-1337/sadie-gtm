"""
Enrichment Service - Enrich hotel data.

Uses lib.rms for shared code, local rms_repo.py and rms_queue.py for service-specific code.
"""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List, Dict, Callable
import asyncio
import os

import httpx
from pydantic import BaseModel
from loguru import logger
from dotenv import load_dotenv
from playwright.async_api import async_playwright, BrowserContext
from playwright_stealth import Stealth

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
from services.enrichment.rms_repo import RMSRepo
from services.enrichment.rms_queue import RMSQueue, MockQueue
from lib.rms import RMSScraper, RMSHotelRecord, QueueStats

load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
MAX_QUEUE_DEPTH = 1000


# =============================================================================
# Result Models
# =============================================================================

class EnrichResult(BaseModel):
    """Result of RMS enrichment."""
    processed: int
    enriched: int
    failed: int


class EnqueueResult(BaseModel):
    """Result of enqueueing hotels."""
    total_found: int
    enqueued: int
    skipped: bool
    reason: Optional[str] = None


class ConsumeResult(BaseModel):
    """Result of consuming from queue."""
    messages_processed: int
    hotels_processed: int
    hotels_enriched: int
    hotels_failed: int


# =============================================================================
# Service Interface
# =============================================================================

class IService(ABC):
    """Enrichment Service Interface."""

    @abstractmethod
    async def enrich_room_counts(self, limit: int = 100) -> int:
        pass

    @abstractmethod
    async def calculate_customer_proximity(self, limit: int = 100, max_distance_km: float = 100.0) -> int:
        pass

    @abstractmethod
    async def get_pending_enrichment_count(self) -> int:
        pass

    @abstractmethod
    async def get_pending_proximity_count(self) -> int:
        pass

    @abstractmethod
    async def enrich_rms_hotels(self, hotels: List[RMSHotelRecord], concurrency: int = 6) -> EnrichResult:
        pass

    @abstractmethod
    async def enqueue_rms_for_enrichment(self, limit: int = 5000, batch_size: int = 10) -> EnqueueResult:
        pass

    @abstractmethod
    async def consume_rms_enrichment_queue(self, concurrency: int = 6, max_messages: int = 0, should_stop: Optional[Callable[[], bool]] = None) -> ConsumeResult:
        pass


# =============================================================================
# Service Implementation
# =============================================================================

class Service(IService):
    def __init__(self, rms_repo: Optional[RMSRepo] = None, rms_queue = None) -> None:
        self._rms_repo = rms_repo or RMSRepo()
        self._rms_queue = rms_queue or RMSQueue()
        self._shutdown_requested = False

    def request_shutdown(self):
        self._shutdown_requested = True
        logger.info("Shutdown requested")

    # =========================================================================
    # Room Count Enrichment
    # =========================================================================

    async def enrich_room_counts(self, limit: int = 100, free_tier: bool = False, concurrency: int = 15) -> int:
        if not get_groq_api_key():
            log("Error: ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY not found in .env")
            return 0
        hotels = await repo.claim_hotels_for_enrichment(limit=limit)
        if not hotels:
            log("No hotels pending enrichment")
            return 0
        mode = "free tier (sequential)" if free_tier else f"paid tier ({concurrency} concurrent)"
        log(f"Claimed {len(hotels)} hotels for enrichment ({mode})")

        async def process_hotel(client: httpx.AsyncClient, hotel, semaphore: asyncio.Semaphore = None):
            if semaphore:
                async with semaphore:
                    return await self._enrich_single_hotel(client, hotel)
            else:
                result = await self._enrich_single_hotel(client, hotel)
                await asyncio.sleep(2.5)
                return result

        enriched_count = 0
        async with httpx.AsyncClient(verify=False) as client:
            if free_tier:
                for hotel in hotels:
                    success = await process_hotel(client, hotel)
                    if success:
                        enriched_count += 1
            else:
                semaphore = asyncio.Semaphore(concurrency)
                tasks = [process_hotel(client, hotel, semaphore) for hotel in hotels]
                results = await asyncio.gather(*tasks)
                enriched_count = sum(1 for r in results if r)
        log(f"Enrichment complete: {enriched_count}/{len(hotels)} hotels enriched")
        return enriched_count

    async def _enrich_single_hotel(self, client: httpx.AsyncClient, hotel) -> bool:
        room_count, source = await enrich_hotel_room_count(
            client=client, hotel_id=hotel.id, hotel_name=hotel.name, website=hotel.website
        )
        if room_count:
            confidence = Decimal("1.0") if source == "regex" else Decimal("0.7")
            await repo.insert_room_count(hotel_id=hotel.id, room_count=room_count, source=source, confidence=confidence, status=1)
            return True
        else:
            await repo.insert_room_count(hotel_id=hotel.id, room_count=None, source=None, confidence=None, status=0)
            return False

    # =========================================================================
    # Customer Proximity
    # =========================================================================

    async def calculate_customer_proximity(self, limit: int = 100, max_distance_km: float = 100.0, concurrency: int = 20) -> int:
        hotels = await repo.get_hotels_pending_proximity(limit=limit)
        if not hotels:
            proximity_log("No hotels pending proximity calculation")
            return 0
        proximity_log(f"Processing {len(hotels)} hotels for proximity calculation (concurrency={concurrency})")
        semaphore = asyncio.Semaphore(concurrency)
        processed_count = 0

        async def process_hotel(hotel):
            nonlocal processed_count
            if hotel.latitude is None or hotel.longitude is None:
                return
            async with semaphore:
                nearest = await repo.find_nearest_customer(hotel_id=hotel.id, max_distance_km=max_distance_km)
                if nearest:
                    await repo.insert_customer_proximity(hotel_id=hotel.id, existing_customer_id=nearest["existing_customer_id"], distance_km=Decimal(str(round(nearest["distance_km"], 1))))
                    proximity_log(f"  {hotel.name}: nearest customer is {nearest['customer_name']} ({round(nearest['distance_km'], 1)}km)")
                    processed_count += 1
                else:
                    await repo.insert_customer_proximity_none(hotel_id=hotel.id)
                    proximity_log(f"  {hotel.name}: no customer within {max_distance_km}km")

        await asyncio.gather(*[process_hotel(h) for h in hotels])
        proximity_log(f"Proximity calculation complete: {processed_count}/{len(hotels)} hotels have nearby customers")
        return processed_count

    async def get_pending_enrichment_count(self) -> int:
        return await repo.get_pending_enrichment_count()

    async def get_pending_proximity_count(self) -> int:
        return await repo.get_pending_proximity_count()

    # =========================================================================
    # Website Enrichment
    # =========================================================================

    async def enrich_websites(self, limit: int = 100, source_filter: str = None, state_filter: str = None, concurrency: int = 10) -> dict:
        if not SERPER_API_KEY:
            log("Error: SERPER_API_KEY not found in environment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0}
        hotels = await repo.claim_hotels_for_website_enrichment(limit=limit, source_filter=source_filter, state_filter=state_filter)
        if not hotels:
            log("No hotels found needing website enrichment")
            return {"total": 0, "found": 0, "not_found": 0, "errors": 0, "api_calls": 0}
        log(f"Claimed {len(hotels)} hotels for website enrichment (concurrency={concurrency})")
        enricher = WebsiteEnricher(api_key=SERPER_API_KEY, delay_between_requests=0)
        semaphore = asyncio.Semaphore(concurrency)
        found = not_found = errors = skipped_chains = api_calls = completed = 0

        async def process_hotel(hotel: dict):
            nonlocal found, not_found, errors, skipped_chains, api_calls, completed
            async with semaphore:
                result = await enricher.find_website(name=hotel["name"], city=hotel["city"], state=hotel.get("state", "FL"), address=hotel.get("address"))
                if result.website:
                    found += 1; api_calls += 1
                    await repo.update_hotel_website(hotel["id"], result.website)
                    await repo.update_website_enrichment_status(hotel["id"], status=1, source="serper")
                elif result.error == "chain_hotel":
                    skipped_chains += 1
                    await repo.update_website_enrichment_status(hotel["id"], status=0, source="chain_skip")
                elif result.error == "no_match":
                    not_found += 1; api_calls += 1
                    await repo.update_website_enrichment_status(hotel["id"], status=0, source="serper")
                else:
                    errors += 1; api_calls += 1
                    await repo.update_website_enrichment_status(hotel["id"], status=0, source="serper")
                completed += 1
                if completed % 50 == 0:
                    log(f"  Progress: {completed}/{len(hotels)} ({found} found, {skipped_chains} chains skipped)")

        await asyncio.gather(*[process_hotel(h) for h in hotels])
        log(f"Website enrichment complete: {found} found, {not_found} not found, {skipped_chains} chains skipped, {errors} errors")
        return {"total": len(hotels), "found": found, "not_found": not_found, "skipped_chains": skipped_chains, "errors": errors, "api_calls": api_calls}

    # =========================================================================
    # RMS Enrichment
    # =========================================================================

    async def enrich_rms_hotels(self, hotels: List[RMSHotelRecord], concurrency: int = 6) -> EnrichResult:
        processed = enriched = failed = 0
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            contexts: List[BrowserContext] = []
            scrapers: List[RMSScraper] = []
            for _ in range(concurrency):
                ctx = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                page = await ctx.new_page()
                stealth = Stealth()
                await stealth.apply_stealth_async(page)
                contexts.append(ctx)
                scrapers.append(RMSScraper(page))
            semaphore = asyncio.Semaphore(concurrency)

            async def enrich_one(hotel: RMSHotelRecord, idx: int) -> tuple[bool, bool]:
                async with semaphore:
                    scraper = scrapers[idx % len(scrapers)]
                    url = hotel.booking_url if hotel.booking_url.startswith("http") else f"https://{hotel.booking_url}"
                    slug = url.split("/")[-1]
                    data = await scraper.extract(url, slug)
                    if data and data.has_data():
                        await self._rms_repo.update_hotel(hotel_id=hotel.hotel_id, name=data.name, address=data.address, city=data.city, state=data.state, country=data.country, phone=data.phone, email=data.email, website=data.website)
                        await self._rms_repo.update_enrichment_status(hotel.booking_url, "enriched")
                        logger.info(f"Enriched {hotel.hotel_id}: {data.name}")
                        return (True, True)
                    else:
                        await self._rms_repo.update_enrichment_status(hotel.booking_url, "no_data")
                        return (True, False)

            tasks = [enrich_one(h, i) for i, h in enumerate(hotels)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Enrichment error: {result}"); failed += 1
                else:
                    pr, en = result
                    processed += 1 if pr else 0
                    enriched += 1 if en else 0
                    if pr and not en:
                        failed += 1
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        return EnrichResult(processed=processed, enriched=enriched, failed=failed)

    async def enqueue_rms_for_enrichment(self, limit: int = 5000, batch_size: int = 10) -> EnqueueResult:
        stats = self._rms_queue.get_stats()
        logger.info(f"Queue: {stats.pending} pending, {stats.in_flight} in flight")
        if stats.pending > MAX_QUEUE_DEPTH:
            return EnqueueResult(total_found=0, enqueued=0, skipped=True, reason=f"Queue depth exceeds {MAX_QUEUE_DEPTH}")
        hotels = await self._rms_repo.get_hotels_needing_enrichment(limit)
        logger.info(f"Found {len(hotels)} hotels needing enrichment")
        if not hotels:
            return EnqueueResult(total_found=0, enqueued=0, skipped=False)
        enqueued = self._rms_queue.enqueue_hotels(hotels, batch_size)
        logger.success(f"Enqueued {enqueued} hotels")
        return EnqueueResult(total_found=len(hotels), enqueued=enqueued, skipped=False)

    async def consume_rms_enrichment_queue(self, concurrency: int = 6, max_messages: int = 0, should_stop: Optional[Callable[[], bool]] = None) -> ConsumeResult:
        messages_processed = hotels_processed = hotels_enriched = hotels_failed = 0
        should_stop = should_stop or (lambda: self._shutdown_requested)
        logger.info(f"Starting consumer (concurrency={concurrency})")
        while not should_stop():
            if max_messages > 0 and messages_processed >= max_messages:
                break
            stats = self._rms_queue.get_stats()
            if stats.pending == 0 and stats.in_flight == 0:
                if max_messages > 0:
                    break
                logger.info("Queue empty, waiting...")
                await asyncio.sleep(30)
                continue
            messages = self._rms_queue.receive_messages(min(concurrency, 10))
            if not messages:
                continue
            logger.info(f"Processing {len(messages)} messages")
            for msg in messages:
                if should_stop():
                    break
                if not msg.hotels:
                    self._rms_queue.delete_message(msg.receipt_handle)
                    continue
                try:
                    result = await self.enrich_rms_hotels(msg.hotels, concurrency)
                    hotels_processed += result.processed
                    hotels_enriched += result.enriched
                    hotels_failed += result.failed
                    self._rms_queue.delete_message(msg.receipt_handle)
                    messages_processed += 1
                except Exception as e:
                    logger.error(f"Error: {e}")
                    hotels_failed += len(msg.hotels)
            logger.info(f"Progress: {hotels_processed} processed, {hotels_enriched} enriched")
        return ConsumeResult(messages_processed=messages_processed, hotels_processed=hotels_processed, hotels_enriched=hotels_enriched, hotels_failed=hotels_failed)

    # =========================================================================
    # RMS Query Methods
    # =========================================================================

    async def get_rms_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        return await self._rms_repo.get_hotels_needing_enrichment(limit)

    async def get_rms_stats(self) -> Dict[str, int]:
        return await self._rms_repo.get_stats()

    async def count_rms_needing_enrichment(self) -> int:
        return await self._rms_repo.count_needing_enrichment()

    def get_rms_queue_stats(self) -> QueueStats:
        return self._rms_queue.get_stats()
