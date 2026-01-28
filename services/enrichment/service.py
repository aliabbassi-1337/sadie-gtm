"""
Enrichment Service - Enrich hotel data.

Self-contained - no imports from other services.
"""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List, Dict, Callable, Protocol, runtime_checkable
import asyncio
import os
import re

import httpx
from pydantic import BaseModel
from loguru import logger
from dotenv import load_dotenv
from playwright.async_api import async_playwright, BrowserContext, Page
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
from db.client import queries, get_conn
from infra.sqs import (
    send_message,
    receive_messages,
    delete_message,
    get_queue_url,
    get_queue_attributes,
)

load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
MAX_QUEUE_DEPTH = 1000
SCRAPE_TIMEOUT = 20000
RMS_QUEUE_NAME = "sadie-gtm-rms-enrichment"


# =============================================================================
# RMS Models (self-contained)
# =============================================================================

class RMSHotelRecord(BaseModel):
    """RMS hotel record from database."""
    hotel_id: int
    booking_url: str


class ExtractedRMSData(BaseModel):
    """Data extracted from RMS booking page."""
    slug: str
    booking_url: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    
    def has_data(self) -> bool:
        return bool(self.name and self.name.lower() not in ['online bookings', 'search', 'error', 'loading', ''])


class QueueStats(BaseModel):
    """Queue statistics."""
    pending: int
    in_flight: int


class QueueMessage(BaseModel):
    """Message from queue."""
    receipt_handle: str
    hotels: List[RMSHotelRecord]


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
# RMS Helpers
# =============================================================================

def decode_cloudflare_email(encoded: str) -> str:
    try:
        r = int(encoded[:2], 16)
        return ''.join(chr(int(encoded[i:i+2], 16) ^ r) for i in range(2, len(encoded), 2))
    except Exception:
        return ""


def normalize_country(country: str) -> str:
    if not country:
        return ""
    country_map = {
        "united states": "USA", "us": "USA", "usa": "USA",
        "australia": "AU", "canada": "CA", "new zealand": "NZ",
        "united kingdom": "GB", "uk": "GB", "mexico": "MX",
    }
    return country_map.get(country.lower().strip(), country.upper()[:2])


# =============================================================================
# RMS Scraper (self-contained)
# =============================================================================

class RMSScraper:
    """Extracts hotel data from RMS booking pages."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def extract(self, url: str, slug: str) -> Optional[ExtractedRMSData]:
        data = ExtractedRMSData(slug=slug, booking_url=url)
        try:
            await self._page.goto(url, timeout=SCRAPE_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            content = await self._page.content()
            body_text = await self._page.evaluate("document.body.innerText")
            
            if not self._is_valid(content, body_text):
                return None
            
            data.name = await self._extract_name()
            data.phone = self._extract_phone(body_text)
            data.email = self._extract_email(content, body_text)
            data.website = await self._extract_website()
            data.address = self._extract_address(body_text)
            if data.address:
                data.state, data.country = self._parse_address(data.address)
            
            return data if data.has_data() else None
        except Exception as e:
            logger.debug(f"Error extracting {url}: {e}")
            return None
    
    def _is_valid(self, content: str, body_text: str) -> bool:
        if "Error" in content[:500] and "application issues" in content:
            return False
        if "Page Not Found" in content or "404" in content[:1000]:
            return False
        return bool(body_text and len(body_text) >= 100)
    
    async def _extract_name(self) -> Optional[str]:
        for selector in ['h1', '.property-name', '.header-title']:
            try:
                el = await self._page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and 2 < len(text) < 100 and text.lower() not in ['online bookings', 'search', 'book now']:
                        return text
            except Exception:
                pass
        title = await self._page.title()
        if title and title.lower() not in ['online bookings', 'search', '']:
            title = re.sub(r'\s*[-|]\s*RMS.*$', '', title, flags=re.IGNORECASE)
            if title and len(title) > 2:
                return title.strip()
        return None
    
    def _extract_phone(self, body_text: str) -> Optional[str]:
        patterns = [
            r'(?:tel|phone|call)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
        ]
        for pattern in patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                if len(re.sub(r'\D', '', phone)) >= 7:
                    return phone
        return None
    
    def _extract_email(self, content: str, body_text: str) -> Optional[str]:
        cf_match = re.search(r'data-cfemail="([a-f0-9]+)"', content)
        if cf_match:
            return decode_cloudflare_email(cf_match.group(1))
        email_match = re.search(r'[\w\.\-+]+@[\w\.-]+\.\w{2,}', body_text)
        if email_match:
            email = email_match.group(0)
            if not any(x in email.lower() for x in ['rmscloud', 'example', 'test', 'noreply']):
                return email
        return None
    
    async def _extract_website(self) -> Optional[str]:
        try:
            links = await self._page.query_selector_all('a[href^="http"]')
            for link in links[:10]:
                href = await link.get_attribute('href')
                if href and 'rmscloud' not in href and 'google' not in href:
                    if any(x in href.lower() for x in ['.com', '.com.au', '.co.nz', '.co.uk']):
                        return href
        except Exception:
            pass
        return None
    
    def _extract_address(self, body_text: str) -> Optional[str]:
        patterns = [
            r'(?:address|location)[:\s]*([^\n]{10,100})',
            r'(\d+\s+[A-Za-z]+\s+(?:St|Street|Rd|Road|Ave|Avenue)[^\n]{0,50})',
        ]
        for pattern in patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                addr = match.group(1).strip()
                if len(addr) > 10:
                    return addr
        return None
    
    def _parse_address(self, address: str) -> tuple[Optional[str], Optional[str]]:
        state = None
        country = None
        state_match = re.search(r',\s*([A-Z]{2,3})\s*(?:\d|$)', address)
        if state_match:
            state = state_match.group(1)
        country_match = re.search(r'(?:Australia|USA|Canada|New Zealand|UK)', address, re.IGNORECASE)
        if country_match:
            country = normalize_country(country_match.group(0))
        return state, country


# =============================================================================
# RMS Repo (self-contained)
# =============================================================================

class RMSRepo:
    """Database operations for RMS enrichment."""
    
    def __init__(self):
        self._booking_engine_id: Optional[int] = None
    
    async def get_booking_engine_id(self) -> int:
        if self._booking_engine_id is None:
            async with get_conn() as conn:
                result = await queries.get_rms_booking_engine_id(conn)
                if result:
                    self._booking_engine_id = result["id"]
                else:
                    raise ValueError("RMS Cloud booking engine not found")
        return self._booking_engine_id
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            results = await queries.get_rms_hotels_needing_enrichment(
                conn, booking_engine_id=booking_engine_id, limit=limit
            )
            return [RMSHotelRecord(hotel_id=r["hotel_id"], booking_url=r["booking_url"]) for r in results]
    
    async def update_hotel(
        self, hotel_id: int, name: Optional[str] = None, address: Optional[str] = None,
        city: Optional[str] = None, state: Optional[str] = None, country: Optional[str] = None,
        phone: Optional[str] = None, email: Optional[str] = None, website: Optional[str] = None,
    ) -> None:
        async with get_conn() as conn:
            await queries.update_rms_hotel(
                conn, hotel_id=hotel_id, name=name, address=address, city=city,
                state=state, country=country, phone=phone, email=email, website=website,
            )
    
    async def update_enrichment_status(self, booking_url: str, status: str) -> None:
        async with get_conn() as conn:
            await queries.update_rms_enrichment_status(conn, booking_url=booking_url, status=status)
    
    async def get_stats(self) -> Dict[str, int]:
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            result = await queries.get_rms_stats(conn, booking_engine_id=booking_engine_id)
            if result:
                return dict(result)
            return {"total": 0, "with_name": 0, "with_city": 0, "with_email": 0, "with_phone": 0, "enriched": 0, "no_data": 0, "dead": 0}
    
    async def count_needing_enrichment(self) -> int:
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            result = await queries.count_rms_needing_enrichment(conn, booking_engine_id=booking_engine_id)
            return result["count"] if result else 0


# =============================================================================
# RMS Queue (self-contained)
# =============================================================================

class RMSQueue:
    """SQS operations for RMS enrichment."""
    
    def __init__(self):
        self._queue_url: Optional[str] = None
    
    @property
    def queue_url(self) -> str:
        if not self._queue_url:
            self._queue_url = get_queue_url(RMS_QUEUE_NAME)
        return self._queue_url
    
    def get_stats(self) -> QueueStats:
        attrs = get_queue_attributes(self.queue_url)
        return QueueStats(
            pending=int(attrs.get("ApproximateNumberOfMessages", 0)),
            in_flight=int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        )
    
    def enqueue_hotels(self, hotels: List[RMSHotelRecord], batch_size: int = 10) -> int:
        enqueued = 0
        for i in range(0, len(hotels), batch_size):
            batch = hotels[i:i + batch_size]
            message = {"hotels": [{"hotel_id": h.hotel_id, "booking_url": h.booking_url} for h in batch]}
            send_message(self.queue_url, message)
            enqueued += len(batch)
        return enqueued
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        raw_messages = receive_messages(self.queue_url, max_messages=min(max_messages, 10), visibility_timeout=3600, wait_time=20)
        messages = []
        for msg in raw_messages:
            hotels_data = msg["body"].get("hotels", [])
            hotels = [RMSHotelRecord(hotel_id=h["hotel_id"], booking_url=h["booking_url"]) for h in hotels_data]
            messages.append(QueueMessage(receipt_handle=msg["receipt_handle"], hotels=hotels))
        return messages
    
    def delete_message(self, receipt_handle: str) -> None:
        delete_message(self.queue_url, receipt_handle)


class MockQueue:
    """Mock queue for testing."""
    
    def __init__(self):
        self._messages: List[QueueMessage] = []
        self.pending = 0
        self.in_flight = 0
    
    def get_stats(self) -> QueueStats:
        return QueueStats(pending=self.pending, in_flight=self.in_flight)
    
    def enqueue_hotels(self, hotels: List[RMSHotelRecord], batch_size: int = 10) -> int:
        self.pending += len(hotels)
        return len(hotels)
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        messages = self._messages[:max_messages]
        self._messages = self._messages[max_messages:]
        self.pending -= len(messages)
        self.in_flight += len(messages)
        return messages
    
    def delete_message(self, receipt_handle: str) -> None:
        self.in_flight = max(0, self.in_flight - 1)


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
