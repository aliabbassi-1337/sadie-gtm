"""Check if hotel properties are still active/operating.

Multi-phase approach (cheapest signals first):
1. Booking URL + Website check (FREE, parallel) — combined signal determines next step.
   - Both alive → Active (high confidence, skip paid checks)
   - Booking alive + website dead/missing → Needs Serper verification
   - Booking dead + website alive → Active (switched engines)
   - Both dead/missing → Needs Serper verification
2. Serper search ($0.001) — parse Knowledge Graph / Yelp for "Permanently closed".
3. LLM fallback ($) — GPT-4o interprets search results (only for ambiguous cases).
4. OTA confirmation — if marked closed, verify it's not bookable on major OTAs.

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
from urllib.parse import urlparse

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
# URL sanitization & per-host rate limiting
# ============================================================================

PLACEHOLDER_URLS = {"no.website", "nowebsite", "no-website"}

_host_semaphores: dict[str, asyncio.Semaphore] = {}
_HOST_CONCURRENCY = 10


def _sanitize_url(url: str) -> Optional[str]:
    """Sanitize and normalize a URL. Returns None if garbage."""
    if not url:
        return None

    url = url.strip()
    if not url:
        return None

    # Reject emails stored as URLs (e.g. "info@kmva.com.au")
    if "@" in url and "//" not in url:
        return None

    # Remove spaces (e.g. "http:// www.gulfhaven.com.au")
    url = url.replace(" ", "")

    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    # Reject known placeholder domains
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if not host or "." not in host:
            return None
        for p in PLACEHOLDER_URLS:
            if p in host:
                return None
    except Exception:
        return None

    return url


def _get_host_semaphore(url: str) -> asyncio.Semaphore:
    """Get or create a per-host semaphore to cap concurrent requests per host."""
    try:
        host = urlparse(url).hostname or "unknown"
    except Exception:
        host = "unknown"
    if host not in _host_semaphores:
        _host_semaphores[host] = asyncio.Semaphore(_HOST_CONCURRENCY)
    return _host_semaphores[host]


# ============================================================================
# Phase 1: Booking URL check (FREE)
# ============================================================================

BOOKING_SKIP_PATTERNS = [
    "cloudbeds.com/hotel-management-software",
    "cloudbeds.com/pms",
    "mews.com/en",
]

BOOKING_DEAD_BODY_SIGNALS = [
    "property not found",
    "page not found",
    "no longer available",
    "this property is no longer",
    "is currently not available for online booking",
]


async def check_booking_url(client: httpx.AsyncClient, booking_url: str) -> Optional[bool]:
    """Check if booking URL is alive. Returns True=active, None=inconclusive."""
    booking_url = _sanitize_url(booking_url)
    if not booking_url:
        return None

    for pattern in BOOKING_SKIP_PATTERNS:
        if pattern in booking_url:
            return None

    try:
        async with _get_host_semaphore(booking_url):
            resp = await client.get(booking_url, follow_redirects=True, timeout=15.0)

        if resp.status_code != 200:
            return None

        # Check body for dead booking page signals (200 but empty/error)
        body = resp.text[:5000].lower()
        for signal in BOOKING_DEAD_BODY_SIGNALS:
            if signal in body:
                return None

        return True
    except Exception:
        return None


# ============================================================================
# Phase 1: Website check (FREE)
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
    website = _sanitize_url(website)
    if not website:
        return None

    try:
        async with _get_host_semaphore(website):
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
# Phase 2: Serper search → Knowledge Graph parsing → LLM fallback
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

    kg_type = (kg.get("type") or "").lower()
    kg_desc = (kg.get("description") or "").lower()

    if "permanently closed" in kg_type or "permanently closed" in kg_desc:
        return "closed"

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
    llm_limiter: asyncio.Semaphore,
) -> dict:
    """Check hotel status via Serper search + LLM fallback.

    Returns {"status": ..., "reason": ..., "source": "kg"|"yelp"|"llm"|"error"}.
    """
    location_parts = [p for p in [city, state] if p]
    location = ", ".join(location_parts) if location_parts else ""
    query = f"{hotel_name} {location} hotel"

    # Step 1: Serper search
    data = await serper_search_raw(client, query)
    if data is None:
        return {"status": "unknown", "reason": "Search failed", "source": "error"}

    # Step 2: Check Knowledge Graph for "Permanently closed"
    kg_status = _parse_knowledge_graph(data)
    if kg_status == "closed":
        return {"status": "closed", "reason": "Google Knowledge Graph: Permanently closed", "source": "kg"}

    # Step 3: Check Yelp organic results for "CLOSED" tag
    yelp_status = _check_organic_for_closure(data)
    if yelp_status == "closed":
        return {"status": "closed", "reason": "Yelp: marked as CLOSED", "source": "yelp"}

    # Step 4: LLM fallback — rate limited separately from Serper
    search_text = _format_search_results(data)
    async with llm_limiter:
        llm_result = await _llm_interpret(client, hotel_name, location, search_text)
    llm_result["source"] = "llm"
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
# OTA confirmation (catches false positives)
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

        for r in data.get("organic", []):
            url = r.get("link", "").lower()
            title = r.get("title", "").lower()
            snippet = r.get("snippet", "").lower()

            if "closed" in title or "permanently closed" in snippet:
                continue

            for ota in OTA_DOMAINS:
                if ota in url:
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
    llm_limiter: asyncio.Semaphore,
    stats: dict,
) -> dict:
    """Check a single hotel using combined booking URL + website signals."""
    async with semaphore:
        hotel_id = hotel["id"]
        name = hotel["name"]
        city = hotel.get("city")
        state = hotel.get("state")
        booking_url = hotel.get("booking_url")
        website = hotel.get("website")

        # Phase 1: Check booking URL AND website in parallel (FREE)
        url_result, web_result = await asyncio.gather(
            check_booking_url(client, booking_url),
            check_website(client, website),
        )

        booking_alive = url_result is True
        website_alive = web_result is True

        # Decision matrix
        if booking_alive and website_alive:
            stats["both_alive"] += 1
            return {
                "hotel_id": hotel_id,
                "hotel_name": name,
                "is_active": True,
                "status": "active",
                "reason": "booking page + website both alive",
            }

        if not booking_alive and website_alive:
            stats["website_only"] += 1
            return {
                "hotel_id": hotel_id,
                "hotel_name": name,
                "is_active": True,
                "status": "active",
                "reason": "website alive (booking URL dead/missing)",
            }

        # Booking alive + website dead/missing → needs verification
        # Both dead/missing → needs verification
        # Either way, fall through to Serper

        # Phase 2: Serper → KG → Yelp → LLM fallback
        stats["serper_checked"] += 1
        result = await check_hotel_serper(client, name, city, state, llm_limiter)
        source = result.get("source", "")

        # Track signal source
        if source == "kg":
            stats["kg_closed"] += 1
        elif source == "yelp":
            stats["yelp_closed"] += 1
        elif source == "llm":
            stats["llm_checked"] += 1

        # Phase 3: If marked closed, double-check against OTAs
        if result["status"] == "closed":
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


async def _refill_rate_limiter(limiter: asyncio.Semaphore, rpm: int, stop_event: asyncio.Event):
    """Refill the rate limiter semaphore at a steady rate."""
    interval = 60.0 / rpm
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        try:
            limiter.release()
        except ValueError:
            pass


async def run(limit: int = 500, concurrency: int = 50, rpm: int = 1000, dry_run: bool = False, country: str = "United States", engine: Optional[str] = None):
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
                h.id, h.name, h.city, h.state,
                h.website, hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE h.country = $2
              AND h.status >= 1
              AND LOWER(be.name) = ANY($3)
              AND (
                (h.is_active IS NULL AND h.active_checked_at IS NULL)
                OR h.active_checked_at < NOW() - INTERVAL '30 days'
              )
            ORDER BY h.id, hbe.booking_url IS NULL, be.name
            LIMIT $1
        """, limit, country, engines)

    total = len(hotels)
    if total == 0:
        logger.info("No hotels to check")
        await close_db()
        return

    engine_label = engine or "all target engines"
    logger.info(f"Checking {total} {country} hotels [{engine_label}] (concurrency={concurrency}, rpm={rpm}, dry_run={dry_run})")

    # Concurrency semaphore for total parallel hotel checks
    semaphore = asyncio.Semaphore(concurrency)

    # LLM rate limiter — small initial burst, refilled at RPM rate
    llm_limiter = asyncio.Semaphore(min(10, rpm // 60))
    stop_event = asyncio.Event()
    refiller = asyncio.create_task(_refill_rate_limiter(llm_limiter, rpm, stop_event))

    # Reset per-host semaphores
    _host_semaphores.clear()

    stats = {
        "both_alive": 0,
        "website_only": 0,
        "serper_checked": 0,
        "kg_closed": 0,
        "yelp_closed": 0,
        "llm_checked": 0,
        "ota_checked": 0,
        "ota_override": 0,
    }
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
                check_hotel(client, dict(h), semaphore, llm_limiter, stats)
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

            # Update ALL results including unknowns (sets active_checked_at so
            # they aren't rechecked until the 30-day window passes)
            if not dry_run and valid_results:
                ids = [r["hotel_id"] for r in valid_results]
                statuses = [r["is_active"] for r in valid_results]
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
    logger.info(f"  Both alive (FREE): {stats['both_alive']}")
    logger.info(f"  Website only (FREE): {stats['website_only']}")
    logger.info(f"  Serper searches: {stats['serper_checked']}")
    logger.info(f"  KG closure signals: {stats['kg_closed']}")
    logger.info(f"  Yelp closure signals: {stats['yelp_closed']}")
    logger.info(f"  LLM fallback: {stats['llm_checked']}")
    logger.info(f"  OTA confirmation checks: {stats['ota_checked']}")
    logger.info(f"  OTA overrides: {stats['ota_override']}")

    if dry_run:
        logger.info("Dry run - no database updates were made")

    await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check hotel active status")
    parser.add_argument("--limit", type=int, default=500, help="Max hotels to check")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent checks")
    parser.add_argument("--rpm", type=int, default=1000, help="Azure OpenAI requests per minute limit")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument("--country", type=str, default="United States", help="Country to check (default: United States)")
    parser.add_argument("--engine", type=str, default=None, help="Single booking engine to filter by (e.g. 'rms cloud')")

    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, concurrency=args.concurrency, rpm=args.rpm, dry_run=args.dry_run, country=args.country, engine=args.engine))
