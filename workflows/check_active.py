"""Check if hotel properties are still active/operating.

Multi-phase approach (cheapest signals first):
1. Booking URL check (FREE) — if the booking page returns HTTP 200, hotel is active.
2. Website check (FREE) — if the hotel website is alive and not parked, hotel is active.
3. Serper search ($0.001) — parse Knowledge Graph / Yelp for "Permanently closed".
4. LLM fallback ($) — GPT-4o interprets search results (only for ambiguous cases).
5. OTA confirmation — if marked closed, verify it's not bookable on major OTAs.

Usage:
    uv run python -m workflows.check_active --limit 100
    uv run python -m workflows.check_active --country Australia --engine "rms cloud" --limit 50
    uv run python -m workflows.check_active --limit 50 --dry-run
"""

import asyncio
import argparse
import json
import os
import re
from typing import Optional

import httpx
from loguru import logger

from db.client import init_db, close_db, get_conn


# Azure OpenAI config
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

# Serper config
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")

TARGET_ENGINES = ("cloudbeds", "mews", "rms cloud", "siteminder")

SYSTEM_PROMPT = """You are a hotel status checker. Given a hotel name, location, and search results, determine if the hotel is still operating.

Respond with EXACTLY one of these JSON objects (no other text):
{"status": "active", "reason": "brief reason"}
{"status": "closed", "reason": "brief reason"}
{"status": "unknown", "reason": "brief reason"}

Rules:
- "active" = hotel is currently operating and accepting guests
- "closed" = hotel has permanently closed, been demolished, converted to other use
- "unknown" = cannot determine with confidence
- Default to "active" unless you find STRONG evidence of permanent closure
- If listed on Booking.com, Expedia, Hotels.com, or any OTA = "active"
- A hotel that was rebranded but still operates as a hotel at the same location is "active"
- A hotel temporarily closed for renovation is "active"
- If the hotel website is down but it's listed on booking sites, it's "active"
- RV parks, campgrounds, retreats, marinas, and other non-traditional lodging that accept bookings are "active"
- Recovery centers, colleges, or other non-hotel businesses that use booking software are "active"
"""



# ============================================================================
# Phase 1: Booking URL check (FREE)
# ============================================================================

async def check_booking_url(client: httpx.AsyncClient, booking_url: str) -> Optional[bool]:
    """Check if booking URL is alive. Returns True=active, None=inconclusive."""
    if not booking_url:
        return None

    # Skip generic URLs that aren't property-specific
    skip_patterns = [
        "cloudbeds.com/hotel-management-software",
        "cloudbeds.com/pms",
        "mews.com/en",
    ]
    for pattern in skip_patterns:
        if pattern in booking_url:
            return None

    try:
        resp = await client.get(booking_url, follow_redirects=True, timeout=15.0)
        if resp.status_code == 200:
            return True
        # 404 = booking page removed, likely closed
        return None
    except (httpx.TimeoutException, httpx.ConnectError, httpx.ConnectTimeout):
        return None
    except Exception:
        return None


# ============================================================================
# Phase 2: Website check (FREE)
# ============================================================================

PARKED_SIGNALS = [
    "domain is for sale", "buy this domain", "parked by", "parked free",
    "this domain is available", "acquire this domain", "domain may be for sale",
    "hugedomains", "godaddy", "sedo.com", "afternic", "dan.com",
    "sedoparking", "domainmarket", "is for sale on",
]

PARKED_REDIRECT_HOSTS = {
    "hugedomains.com", "sedo.com", "afternic.com", "dan.com",
    "godaddy.com", "domainmarket.com", "sedoparking.com",
}


async def check_website(client: httpx.AsyncClient, website: str) -> Optional[bool]:
    """Check if hotel website is alive and not parked. Returns True=active, None=inconclusive."""
    if not website:
        return None

    try:
        resp = await client.get(website, follow_redirects=True, timeout=15.0)

        # Redirected to a domain seller = dead
        final_host = str(resp.url.host).lower()
        for parked_host in PARKED_REDIRECT_HOSTS:
            if parked_host in final_host:
                return None

        if resp.status_code != 200:
            return None

        # Check for parked domain content
        body = resp.text[:5000].lower()
        for signal in PARKED_SIGNALS:
            if signal in body:
                return None

        # Website is alive with real content
        return True
    except Exception:
        return None


# ============================================================================
# Phase 3: Serper search → Knowledge Graph parsing → LLM fallback
# ============================================================================

async def serper_search_raw(client: httpx.AsyncClient, query: str) -> Optional[dict]:
    """Execute a Serper search and return raw JSON response."""
    try:
        resp = await client.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": 10},
            timeout=15.0,
        )
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _format_search_results(data: dict) -> str:
    """Format Serper JSON into readable text for LLM consumption."""
    parts = []
    kg = data.get("knowledgeGraph", {})
    if kg:
        parts.append(f"Knowledge Graph: {kg.get('title', '')} - {kg.get('type', '')} - {kg.get('description', '')}")
        for k, v in kg.get("attributes", {}).items():
            parts.append(f"  {k}: {v}")
    for r in data.get("organic", [])[:5]:
        parts.append(f"Result: {r.get('title', '')} - {r.get('snippet', '')}")
    return "\n".join(parts) if parts else "No results found"


def _parse_knowledge_graph(data: dict) -> Optional[str]:
    """Parse Serper Knowledge Graph for definitive business status.

    Returns "closed" if Google says permanently closed, None otherwise.
    """
    kg = data.get("knowledgeGraph", {})
    if not kg:
        return None

    # Check KG type/description for closure signals
    kg_type = (kg.get("type") or "").lower()
    kg_desc = (kg.get("description") or "").lower()
    kg_title = (kg.get("title") or "").lower()

    if "permanently closed" in kg_type or "permanently closed" in kg_desc:
        return "closed"

    # Check KG attributes
    for k, v in kg.get("attributes", {}).items():
        val = str(v).lower()
        if "permanently closed" in val:
            return "closed"

    return None


def _check_organic_for_closure(data: dict) -> Optional[str]:
    """Check organic results for strong closure signals.

    Returns "closed" only for very strong signals, None otherwise.
    """
    for r in data.get("organic", [])[:5]:
        title = (r.get("title") or "").lower()
        snippet = (r.get("snippet") or "").lower()
        link = (r.get("link") or "").lower()

        # Yelp uses "CLOSED" in the title like "HOTEL NAME - CLOSED - Updated..."
        if "yelp.com" in link and " - closed - " in title:
            return "closed"

        # Google Maps / organic results with "permanently closed" in snippet
        if "permanently closed" in snippet:
            return "closed"

    return None


async def check_hotel_serper(
    client: httpx.AsyncClient,
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
    stats: dict,
) -> dict:
    """Check hotel status: Serper Knowledge Graph first, LLM fallback for ambiguous cases.

    Returns {"status": "active"|"closed"|"unknown", "reason": "..."}.
    """
    location_parts = [p for p in [city, state] if p]
    location = ", ".join(location_parts) if location_parts else ""
    query = f"{hotel_name} {location} hotel"

    # Step 1: Serper search
    data = await serper_search_raw(client, query)
    if data is None:
        return {"status": "unknown", "reason": "Search failed"}

    # Step 2: Check Knowledge Graph for "Permanently closed"
    kg_status = _parse_knowledge_graph(data)
    if kg_status == "closed":
        stats["kg_closed"] += 1
        return {"status": "closed", "reason": "Google Knowledge Graph: Permanently closed"}

    # Step 3: Check Yelp organic results for "CLOSED" tag
    yelp_status = _check_organic_for_closure(data)
    if yelp_status == "closed":
        stats["yelp_closed"] += 1
        return {"status": "closed", "reason": "Yelp: marked as CLOSED"}

    # Step 4: LLM fallback — pass search results we already have (no extra Serper call)
    search_text = _format_search_results(data)
    llm_result = await _llm_interpret(client, hotel_name, location, search_text)
    stats["llm_checked"] += 1
    return llm_result


async def _llm_interpret(
    client: httpx.AsyncClient,
    hotel_name: str,
    location: str,
    search_results: str,
) -> dict:
    """Ask GPT-4o to interpret pre-fetched search results. No tool use needed."""
    user_msg = f'Is "{hotel_name}" in {location} still operating?\n\nHere are search results:\n{search_results}' if location else f'Is "{hotel_name}" still operating?\n\nHere are search results:\n{search_results}'

    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version={AZURE_OPENAI_API_VERSION}"
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}

    for attempt in range(3):
        try:
            resp = await client.post(
                url,
                headers=headers,
                json={
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                    "max_tokens": 200,
                    "temperature": 0.1,
                },
                timeout=30.0,
            )

            if resp.status_code == 429:
                wait = (attempt + 1) * 5
                logger.warning(f"Rate limited, waiting {wait}s (attempt {attempt + 1}/3)")
                await asyncio.sleep(wait)
                continue

            if resp.status_code != 200:
                logger.error(f"Azure OpenAI error {resp.status_code}: {resp.text[:200]}")
                return {"status": "unknown", "reason": f"API error {resp.status_code}"}

            data = resp.json()
            text = data["choices"][0]["message"].get("content", "")
            return _parse_response(text)

        except httpx.TimeoutException:
            logger.warning(f"Timeout on LLM for {hotel_name} (attempt {attempt + 1}/3)")
            if attempt < 2:
                await asyncio.sleep(5)
                continue
            return {"status": "unknown", "reason": "Request timeout"}
        except Exception as e:
            logger.error(f"LLM error for {hotel_name}: {e}")
            return {"status": "unknown", "reason": str(e)[:100]}

    return {"status": "unknown", "reason": "Max retries exceeded"}


def _parse_response(text: str) -> dict:
    """Parse the LLM response into a status dict."""
    text = text.strip()

    json_match = re.search(r'\{[^}]+\}', text)
    if json_match:
        try:
            result = json.loads(json_match.group())
            status = result.get("status", "").lower()
            reason = result.get("reason", "")
            if status in ("active", "closed", "unknown"):
                return {"status": status, "reason": reason}
        except json.JSONDecodeError:
            pass

    lower = text.lower()
    if "permanently closed" in lower or "no longer operat" in lower or "has closed" in lower:
        return {"status": "closed", "reason": text[:100]}
    if "still operat" in lower or "currently open" in lower or "is active" in lower:
        return {"status": "active", "reason": text[:100]}

    return {"status": "unknown", "reason": text[:100]}


# ============================================================================
# Phase 3b: OTA confirmation (catches LLM false positives)
# ============================================================================

OTA_DOMAINS = ("booking.com", "expedia.com", "hotels.com", "tripadvisor.com", "kayak.com")


async def confirm_closed_via_ota(
    client: httpx.AsyncClient,
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
) -> bool:
    """Search OTAs to confirm a hotel is actually closed.

    Returns True if the hotel appears bookable on major OTAs (i.e. NOT closed).
    Returns False if no OTA presence found (confirms closure).
    """
    location_parts = [p for p in [city, state] if p]
    location = ", ".join(location_parts) if location_parts else ""
    query = f'"{hotel_name}" {location} hotel book room'

    try:
        resp = await client.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": 10},
            timeout=15.0,
        )
        if resp.status_code != 200:
            return False

        data = resp.json()

        # Check organic results for OTA listings
        for r in data.get("organic", []):
            url = r.get("link", "").lower()
            title = r.get("title", "").lower()
            snippet = r.get("snippet", "").lower()

            # Skip results that mention "closed"
            if "closed" in title or "permanently closed" in snippet:
                continue

            for ota in OTA_DOMAINS:
                if ota in url:
                    # Found on a booking platform — likely still active
                    logger.info(f"  OTA override: {hotel_name} found on {ota}")
                    return True

        return False
    except Exception:
        return False


# ============================================================================
# Orchestrator
# ============================================================================

async def check_hotel(
    client: httpx.AsyncClient,
    hotel: dict,
    semaphore: asyncio.Semaphore,
    rate_limiter: asyncio.Semaphore,
    stats: dict,
) -> dict:
    """Check a single hotel: booking URL → website → LLM → OTA confirmation."""
    async with semaphore:
        hotel_id = hotel["id"]
        name = hotel["name"]
        city = hotel.get("city")
        state = hotel.get("state")
        booking_url = hotel.get("booking_url")
        website = hotel.get("website")

        # Phase 1: Check booking URL (FREE)
        url_result = await check_booking_url(client, booking_url)
        if url_result is True:
            stats["url_active"] += 1
            return {
                "hotel_id": hotel_id,
                "hotel_name": name,
                "is_active": True,
                "status": "active",
                "reason": "booking page alive",
            }

        # Phase 2: Check hotel website (FREE)
        web_result = await check_website(client, website)
        if web_result is True:
            stats["website_active"] += 1
            return {
                "hotel_id": hotel_id,
                "hotel_name": name,
                "is_active": True,
                "status": "active",
                "reason": "website alive",
            }

        # Phase 3: Serper → Knowledge Graph → LLM fallback (rate limited)
        async with rate_limiter:
            result = await check_hotel_serper(client, name, city, state, stats)

        # Phase 3b: If marked closed, double-check against OTAs
        if result["status"] == "closed":
            async with rate_limiter:
                stats["ota_checked"] += 1
                on_ota = await confirm_closed_via_ota(client, name, city, state)
            if on_ota:
                stats["ota_override"] += 1
                return {
                    "hotel_id": hotel_id,
                    "hotel_name": name,
                    "is_active": True,
                    "status": "active",
                    "reason": "marked closed but found on OTA",
                }

        is_active = None
        if result["status"] == "active":
            is_active = True
        elif result["status"] == "closed":
            is_active = False

        return {
            "hotel_id": hotel_id,
            "hotel_name": name,
            "is_active": is_active,
            "status": result["status"],
            "reason": result["reason"],
        }


async def _refill_rate_limiter(rate_limiter: asyncio.Semaphore, rpm: int, stop_event: asyncio.Event):
    """Refill the rate limiter semaphore at a steady rate."""
    interval = 60.0 / rpm
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        try:
            rate_limiter.release()
        except ValueError:
            pass


async def run(limit: int = 500, concurrency: int = 50, rpm: int = 200, dry_run: bool = False, country: str = "United States", engine: Optional[str] = None):
    """Run active status check on hotels by country and optionally by booking engine."""
    if not AZURE_OPENAI_API_KEY:
        logger.error("AZURE_OPENAI_API_KEY not set")
        return
    if not SERPER_API_KEY:
        logger.error("SERPER_API_KEY not set")
        return

    await init_db()

    engines = [engine.lower()] if engine else [e for e in TARGET_ENGINES]

    async with get_conn() as conn:
        hotels = await conn.fetch("""
            SELECT DISTINCT ON (h.id)
                h.id, h.name, h.city, h.state, h.active_checked_at,
                h.website, hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE h.country = $2
              AND h.status >= 1
              AND LOWER(be.name) = ANY($3)
              AND (h.is_active IS NULL OR h.active_checked_at < NOW() - INTERVAL '30 days')
            ORDER BY h.id, h.active_checked_at NULLS FIRST
            LIMIT $1
        """, limit, country, engines)

    total = len(hotels)
    if total == 0:
        logger.info("No hotels to check")
        await close_db()
        return

    engine_label = engine or "all target engines"
    logger.info(f"Checking {total} {country} hotels [{engine_label}] (concurrency={concurrency}, rpm={rpm}, dry_run={dry_run})")

    semaphore = asyncio.Semaphore(concurrency)
    rate_limiter = asyncio.Semaphore(concurrency)
    stop_event = asyncio.Event()
    refiller = asyncio.create_task(_refill_rate_limiter(rate_limiter, rpm, stop_event))

    stats = {"url_active": 0, "website_active": 0, "kg_closed": 0, "yelp_closed": 0, "llm_checked": 0, "ota_checked": 0, "ota_override": 0}
    all_results = []
    batch_size = 200

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency + 10),
    ) as client:
        for batch_start in range(0, total, batch_size):
            batch = hotels[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            logger.info(f"Batch {batch_num}/{total_batches} ({len(batch)} hotels)")

            tasks = [
                check_hotel(client, dict(h), semaphore, rate_limiter, stats)
                for h in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            valid_results = []
            errors = 0
            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                    logger.error(f"Hotel check error: {r}")
                else:
                    valid_results.append(r)

            active = sum(1 for r in valid_results if r["status"] == "active")
            closed = sum(1 for r in valid_results if r["status"] == "closed")
            unknown = sum(1 for r in valid_results if r["status"] == "unknown")
            logger.info(f"  Batch results: {active} active, {closed} closed, {unknown} unknown, {errors} errors")

            for r in valid_results:
                if r["status"] == "closed":
                    logger.info(f"  CLOSED: {r['hotel_name']} (id={r['hotel_id']}) reason={r['reason']}")

            if not dry_run:
                update_results = [r for r in valid_results if r["is_active"] is not None]
                if update_results:
                    ids = [r["hotel_id"] for r in update_results]
                    statuses = [r["is_active"] for r in update_results]
                    async with get_conn() as conn:
                        await conn.execute("""
                            UPDATE sadie_gtm.hotels AS h
                            SET is_active = m.is_active,
                                active_checked_at = NOW(),
                                updated_at = NOW()
                            FROM (
                                SELECT unnest($1::int[]) AS id,
                                       unnest($2::boolean[]) AS is_active
                            ) AS m
                            WHERE h.id = m.id
                        """, ids, statuses)

            all_results.extend(valid_results)

    stop_event.set()
    refiller.cancel()

    total_active = sum(1 for r in all_results if r["status"] == "active")
    total_closed = sum(1 for r in all_results if r["status"] == "closed")
    total_unknown = sum(1 for r in all_results if r["status"] == "unknown")
    logger.info(f"TOTAL: {total_active} active, {total_closed} closed, {total_unknown} unknown")
    logger.info(f"  Booking URL alive (FREE): {stats['url_active']}")
    logger.info(f"  Website alive (FREE): {stats['website_active']}")
    logger.info(f"  Knowledge Graph closed: {stats['kg_closed']}")
    logger.info(f"  Yelp closed: {stats['yelp_closed']}")
    logger.info(f"  LLM fallback: {stats['llm_checked']}")
    logger.info(f"  OTA confirmation checks: {stats['ota_checked']}")
    logger.info(f"  OTA overrides (saved from false positive): {stats['ota_override']}")

    if dry_run:
        logger.info("Dry run - no database updates were made")

    await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check hotel active status")
    parser.add_argument("--limit", type=int, default=500, help="Max hotels to check")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent checks")
    parser.add_argument("--rpm", type=int, default=200, help="Azure OpenAI requests per minute limit")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument("--country", type=str, default="United States", help="Country to check (default: United States)")
    parser.add_argument("--engine", type=str, default=None, help="Single booking engine to filter by (e.g. 'rms cloud')")

    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, concurrency=args.concurrency, rpm=args.rpm, dry_run=args.dry_run, country=args.country, engine=args.engine))
