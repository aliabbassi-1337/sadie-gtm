"""Check if hotel properties are still active/operating.

Uses Azure OpenAI (gpt-35-turbo) with Serper web search as a custom tool
to verify each hotel's operational status.

Targets US hotels on Cloudbeds, Mews, RMS Cloud, and SiteMinder.

Usage:
    uv run python -m workflows.check_active --limit 100
    uv run python -m workflows.check_active --limit 50 --dry-run
    uv run python -m workflows.check_active --limit 4200 --concurrency 10
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
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")

# Serper config
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")

TARGET_ENGINES = ("cloudbeds", "mews", "rms cloud", "siteminder")

SYSTEM_PROMPT = """You are a hotel status checker. You have a search tool to look up current information.

Given a hotel name and location, search for it and determine if the hotel is still operating.

Respond with EXACTLY one of these JSON objects (no other text):
{"status": "active", "reason": "brief reason"}
{"status": "closed", "reason": "brief reason"}
{"status": "unknown", "reason": "brief reason"}

Rules:
- "active" = hotel is currently operating and accepting guests
- "closed" = hotel has permanently closed, been demolished, converted to other use, or rebranded under a completely different name
- "unknown" = cannot determine with confidence
- A hotel that was rebranded but still operates as a hotel at the same location is "active"
- A hotel temporarily closed for renovation is "active"
- If the hotel website is down but it's listed on booking sites, it's "active"
"""

SEARCH_TOOL = {
    "type": "function",
    "function": {
        "name": "search",
        "description": "Search Google for information about a hotel to determine if it is still operating",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query about the hotel",
                }
            },
            "required": ["query"],
        },
    },
}


async def serper_search(client: httpx.AsyncClient, query: str) -> str:
    """Execute a search via Serper API and return formatted results."""
    try:
        resp = await client.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": 5},
            timeout=15.0,
        )
        if resp.status_code != 200:
            return f"Search failed: HTTP {resp.status_code}"

        data = resp.json()
        parts = []

        # Knowledge graph (most useful)
        kg = data.get("knowledgeGraph", {})
        if kg:
            parts.append(f"Knowledge Graph: {kg.get('title', '')} - {kg.get('type', '')} - {kg.get('description', '')}")
            attrs = kg.get("attributes", {})
            for k, v in attrs.items():
                parts.append(f"  {k}: {v}")

        # Top organic results
        for r in data.get("organic", [])[:3]:
            parts.append(f"Result: {r.get('title', '')} - {r.get('snippet', '')}")

        return "\n".join(parts) if parts else "No results found"
    except Exception as e:
        return f"Search error: {e}"


async def check_hotel_status(
    client: httpx.AsyncClient,
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
) -> dict:
    """Check a single hotel's status using Azure GPT + Serper search.

    Returns {"status": "active"|"closed"|"unknown", "reason": "..."}.
    """
    location_parts = [p for p in [city, state] if p]
    location = ", ".join(location_parts) if location_parts else ""
    user_msg = f"Is \"{hotel_name}\" in {location} still operating as a hotel?" if location else f"Is \"{hotel_name}\" still operating as a hotel?"

    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version={AZURE_OPENAI_API_VERSION}"
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]

    # Step 1: Ask GPT (it will call the search tool)
    for attempt in range(3):
        try:
            resp = await client.post(
                url,
                headers=headers,
                json={
                    "messages": messages,
                    "tools": [SEARCH_TOOL],
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
            choice = data["choices"][0]
            msg = choice["message"]

            # If GPT wants to call the search tool
            if msg.get("tool_calls"):
                messages.append(msg)

                # Handle all tool calls (GPT sometimes makes multiple)
                for tool_call in msg["tool_calls"]:
                    search_args = json.loads(tool_call["function"]["arguments"])
                    search_query = search_args.get("query", f"{hotel_name} {location}")
                    search_results = await serper_search(client, search_query)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call["id"],
                        "content": search_results,
                    })

                resp2 = await client.post(
                    url,
                    headers=headers,
                    json={
                        "messages": messages,
                        "max_tokens": 200,
                        "temperature": 0.1,
                    },
                    timeout=30.0,
                )

                if resp2.status_code == 429:
                    wait = (attempt + 1) * 5
                    logger.warning(f"Rate limited on follow-up, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                if resp2.status_code != 200:
                    logger.error(f"Azure OpenAI follow-up error {resp2.status_code}: {resp2.text[:200]}")
                    return {"status": "unknown", "reason": f"API error {resp2.status_code}"}

                data2 = resp2.json()
                text = data2["choices"][0]["message"]["content"]
                return _parse_response(text)

            # GPT answered directly without searching
            text = msg.get("content", "")
            return _parse_response(text)

        except httpx.TimeoutException:
            logger.warning(f"Timeout checking {hotel_name} (attempt {attempt + 1}/3)")
            if attempt < 2:
                await asyncio.sleep(5)
                continue
            return {"status": "unknown", "reason": "Request timeout"}
        except Exception as e:
            logger.error(f"Error checking {hotel_name}: {e}")
            return {"status": "unknown", "reason": str(e)[:100]}

    return {"status": "unknown", "reason": "Max retries exceeded"}


def _parse_response(text: str) -> dict:
    """Parse the LLM response into a status dict."""
    text = text.strip()

    # Try to extract JSON from the response
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

    # Fallback: look for keywords
    lower = text.lower()
    if "permanently closed" in lower or "no longer operat" in lower or "has closed" in lower:
        return {"status": "closed", "reason": text[:100]}
    if "still operat" in lower or "currently open" in lower or "is active" in lower:
        return {"status": "active", "reason": text[:100]}

    return {"status": "unknown", "reason": text[:100]}


async def check_hotel(
    client: httpx.AsyncClient,
    hotel: dict,
    semaphore: asyncio.Semaphore,
    rate_limiter: asyncio.Semaphore,
) -> dict:
    """Check a single hotel with concurrency and rate limit control."""
    async with semaphore:
        # Rate limit: acquire and release after delay to pace requests
        async with rate_limiter:
            hotel_id = hotel["id"]
            name = hotel["name"]
            city = hotel.get("city")
            state = hotel.get("state")

            result = await check_hotel_status(client, name, city, state)

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
            pass  # Already at max


async def run(limit: int = 500, concurrency: int = 50, rpm: int = 200, dry_run: bool = False):
    """Run active status check on US hotels (Cloudbeds, Mews, RMS Cloud, SiteMinder)."""
    if not AZURE_OPENAI_API_KEY:
        logger.error("AZURE_OPENAI_API_KEY not set")
        return
    if not SERPER_API_KEY:
        logger.error("SERPER_API_KEY not set")
        return

    await init_db()

    async with get_conn() as conn:
        hotels = await conn.fetch("""
            SELECT DISTINCT h.id, h.name, h.city, h.state, h.active_checked_at
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE h.country = 'United States'
              AND h.status >= 1
              AND LOWER(be.name) IN ('cloudbeds', 'mews', 'rms cloud', 'siteminder')
              AND (h.is_active IS NULL OR h.active_checked_at < NOW() - INTERVAL '30 days')
            ORDER BY h.active_checked_at NULLS FIRST
            LIMIT $1
        """, limit)

    total = len(hotels)
    if total == 0:
        logger.info("No hotels to check")
        await close_db()
        return

    logger.info(f"Checking {total} US hotels (concurrency={concurrency}, rpm={rpm}, dry_run={dry_run})")

    semaphore = asyncio.Semaphore(concurrency)
    # Token bucket rate limiter — start with a burst of `concurrency` tokens
    rate_limiter = asyncio.Semaphore(concurrency)
    stop_event = asyncio.Event()
    refiller = asyncio.create_task(_refill_rate_limiter(rate_limiter, rpm, stop_event))

    all_results = []
    batch_size = 200

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency + 10),
    ) as client:
        # Process in batches, commit each batch to DB
        for batch_start in range(0, total, batch_size):
            batch = hotels[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            logger.info(f"Batch {batch_num}/{total_batches} ({len(batch)} hotels)")

            tasks = [
                check_hotel(client, dict(h), semaphore, rate_limiter)
                for h in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out exceptions
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

            # Commit batch to DB immediately
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

    # Final summary
    total_active = sum(1 for r in all_results if r["status"] == "active")
    total_closed = sum(1 for r in all_results if r["status"] == "closed")
    total_unknown = sum(1 for r in all_results if r["status"] == "unknown")
    logger.info(f"TOTAL: {total_active} active, {total_closed} closed, {total_unknown} unknown")

    if dry_run:
        logger.info("Dry run - no database updates were made")

    await close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check hotel active status (US - Cloudbeds/Mews/RMS Cloud/SiteMinder)")
    parser.add_argument("--limit", type=int, default=500, help="Max hotels to check")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent checks")
    parser.add_argument("--rpm", type=int, default=200, help="Azure OpenAI requests per minute limit")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")

    args = parser.parse_args()
    asyncio.run(run(limit=args.limit, concurrency=args.concurrency, rpm=args.rpm, dry_run=args.dry_run))
