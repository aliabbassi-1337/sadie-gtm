#!/usr/bin/env python3
"""RMS Availability Enrichment Workflow (Optimized)

Checks if Australia RMS hotels have availability with optimized performance.
Uses adaptive rate limiting, host-aware parallelism, and smart early exits.

Usage:
    # Check availability for 100 hotels
    uv run python -m workflows.rms_availability --limit 100

    # Check 3.7k hotels with optimized settings
    uv run python -m workflows.rms_availability --limit 3700 --concurrency 30
"""

import asyncio
import argparse
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse, urlencode

import httpx
from loguru import logger

from db.client import init_db, close_db, get_conn

# RMS API configuration
RMS_BASE_URL = "https://bookings12.rmscloud.com/Rates/Index"

# Multiple date ranges to check (days ahead)
CHECK_DATE_RANGES = [
    (30, 32),  # 30 days ahead
    (60, 62),  # 60 days ahead
    (75, 77),  # 75 days ahead (different interval)
    (90, 92),  # 90 days ahead
]

# OPTIMIZED Rate limiting - adaptive approach
_host_stats: dict[str, dict] = defaultdict(
    lambda: {
        "last_request": datetime.min,
        "consecutive_429s": 0,
        "current_delay": 0.5,  # Start aggressive at 0.5s
    }
)

# Per-host semaphores - different RMS hosts can be hit in parallel
_host_semaphores: dict[str, asyncio.Semaphore] = {}
_host_lock = asyncio.Lock()

# Global adaptive delay
_adaptive_delay = 0.5
_adaptive_lock = asyncio.Lock()

# Optimized retry config
_max_retries = 3
_base_retry_delay = 2.0  # Faster retries


async def _get_or_create_host_semaphore(
    host: str, max_concurrent: int = 5
) -> asyncio.Semaphore:
    """Get or create a semaphore for a specific host with proper locking."""
    async with _host_lock:
        if host not in _host_semaphores:
            _host_semaphores[host] = asyncio.Semaphore(max_concurrent)
        return _host_semaphores[host]


async def _adapt_rate_limit(host: str, got_429: bool = False):
    """Adaptively adjust rate limiting based on server response.

    If we get 429s, increase delay. If successful, gradually decrease.
    """
    global _adaptive_delay

    async with _adaptive_lock:
        stats = _host_stats[host]

        if got_429:
            stats["consecutive_429s"] += 1
            # Exponential backoff for this host
            stats["current_delay"] = min(5.0, stats["current_delay"] * 1.5)
            _adaptive_delay = max(_adaptive_delay, stats["current_delay"])
            logger.debug(
                f"Rate limit hit on {host}, increased delay to {stats['current_delay']:.2f}s"
            )
        else:
            if stats["consecutive_429s"] > 0:
                stats["consecutive_429s"] = 0
                # Gradually decrease delay on success
                stats["current_delay"] = max(0.3, stats["current_delay"] * 0.9)


async def _respect_rate_limit(host: str):
    """Wait appropriate time before making request to host."""
    stats = _host_stats[host]
    now = datetime.now()

    elapsed = (now - stats["last_request"]).total_seconds()
    delay_needed = stats["current_delay"] - elapsed

    if delay_needed > 0:
        await asyncio.sleep(delay_needed)

    stats["last_request"] = datetime.now()


def _extract_rms_host_and_ids(
    booking_url: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract RMS host, client_id, and prop_id from booking URL.

    Returns (host, client_id, prop_id) or (None, None, None) if invalid.
    """
    if not booking_url or "rmscloud.com" not in booking_url:
        return None, None, None

    try:
        parsed = urlparse(booking_url)
        host = parsed.hostname
        path_parts = parsed.path.split("/")

        # Extract client_id and prop_id from URL like /Rates/Index/317/1 or /Search/Index/317/1
        if len(path_parts) >= 5:
            client_id = path_parts[3]
            prop_id = path_parts[4] if len(path_parts) > 4 else "1"
            return host, client_id, prop_id
        else:
            # Fallback - try to use the path
            return host, "317", "1"
    except Exception:
        return None, None, None


async def check_rms_availability_single_date(
    client: httpx.AsyncClient,
    host: str,
    client_id: str,
    prop_id: str,
    arrival_days_ahead: int,
    departure_days_ahead: int,
) -> Optional[bool]:
    """Check availability for a specific date range with optimized rate limiting."""

    # Calculate dates
    arrival_date = datetime.now() + timedelta(days=arrival_days_ahead)
    departure_date = datetime.now() + timedelta(days=departure_days_ahead)

    arrival_str = arrival_date.strftime("%m/%d/%Y")
    departure_str = departure_date.strftime("%m/%d/%Y")

    # Construct URL using the actual host
    base_url = f"https://{host}/Rates/Index"
    params = {
        "A": arrival_str,
        "D": departure_str,
        "Ad": "2",
        "Mp": "0",
        "M": "0",
        "Y": "0",
        "Z": "0",
        "Rv": "1",
    }
    availability_url = f"{base_url}/{client_id}/{prop_id}?{urlencode(params)}"

    # Get semaphore for this specific host
    semaphore = await _get_or_create_host_semaphore(host)

    async with semaphore:
        await _respect_rate_limit(host)

        for attempt in range(_max_retries):
            try:
                resp = await client.get(
                    availability_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate",
                        "DNT": "1",
                        "Connection": "keep-alive",
                    },
                    follow_redirects=True,
                    timeout=15.0,  # Reduced timeout for faster failures
                )

                if resp.status_code == 429:
                    await _adapt_rate_limit(host, got_429=True)
                    retry_delay = _base_retry_delay * (2**attempt)
                    logger.warning(
                        f"429 on {host}, retrying in {retry_delay}s (attempt {attempt + 1}/{_max_retries})"
                    )
                    await asyncio.sleep(retry_delay)
                    continue

                # Success - adapt rate limit positively
                await _adapt_rate_limit(host, got_429=False)

                if resp.status_code != 200:
                    return None

                return _parse_availability_from_html(resp.text)

            except httpx.TimeoutException:
                return None
            except Exception:
                return None

        logger.warning(f"All retries exhausted for {host}")
        return None


def _parse_availability_from_html(html_content: str) -> Optional[bool]:
    """Fast HTML parsing for availability indicators."""
    html_lower = html_content.lower()

    # Quick check for "No Available Rates" (fastest path)
    if (
        'value="No Available Rates"'.lower() in html_lower
        or "lblNoAvailableRates".lower() in html_lower
    ):
        return False

    # Check for "no available rates" in text
    if "no available rates" in html_lower:
        return False

    # Check for rate data (positive signals)
    if '"LeadInRate"' in html_content:
        match = re.search(r'id="LeadInRate"[^>]+value="(\d+)"', html_content)
        if match and int(match.group(1)) > 0:
            return True

    # Check DataCollection for room data
    if '"DataCollection":' in html_content:
        # Quick regex check for non-empty array
        if re.search(r'"DataCollection":\s*\[[^\]]', html_content):
            return True

    # Check for availability CSS classes
    if any(
        indicator in html_lower
        for indicator in ["room-available", "rate-available", "has-availability"]
    ):
        return True

    return None


async def check_rms_availability_optimized(
    client: httpx.AsyncClient,
    booking_url: str,
) -> tuple[Optional[bool], list[dict], int]:
    """Check availability with optimized early exit strategy.

    Returns: (has_availability, check_details, checks_performed)
    """
    host, client_id, prop_id = _extract_rms_host_and_ids(booking_url)

    if not host or not client_id or not prop_id:
        return None, [], 0

    check_details = []
    checks_performed = 0

    for i, (arrival_days, departure_days) in enumerate(CHECK_DATE_RANGES):
        result = await check_rms_availability_single_date(
            client, host, client_id, prop_id, arrival_days, departure_days
        )
        checks_performed += 1

        detail = {
            "arrival_days_ahead": arrival_days,
            "departure_days_ahead": departure_days,
            "has_availability": result,
        }
        check_details.append(detail)

        # Early exit on availability found
        if result is True:
            return True, check_details, checks_performed

        # Smart early exit: if first 2 checks are inconclusive,
        # the hotel likely has parsing issues - skip remaining
        if i == 1 and result is None:
            logger.debug(f"  First 2 checks inconclusive, skipping remaining dates")
            break

        # Small delay only if we're continuing
        if i < len(CHECK_DATE_RANGES) - 1:
            await asyncio.sleep(0.3)

    # Determine final result
    has_definitive_false = any(d["has_availability"] is False for d in check_details)
    all_inconclusive = all(d["has_availability"] is None for d in check_details)

    if all_inconclusive:
        return None, check_details, checks_performed
    elif has_definitive_false:
        return False, check_details, checks_performed
    else:
        return None, check_details, checks_performed


async def process_hotel_optimized(
    client: httpx.AsyncClient,
    hotel: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Process a single hotel with optimized checking."""
    async with semaphore:
        hotel_id = hotel["hotel_id"]
        name = hotel.get("name", "Unknown")
        booking_url = hotel.get("booking_url")

        if not booking_url:
            return {
                "hotel_id": hotel_id,
                "hotel_name": name,
                "has_availability": None,
                "check_details": [],
                "checks_performed": 0,
                "status": "inconclusive",
            }

        (
            has_availability,
            check_details,
            checks_performed,
        ) = await check_rms_availability_optimized(client, booking_url)

        result = {
            "hotel_id": hotel_id,
            "hotel_name": name,
            "has_availability": has_availability,
            "check_details": check_details,
            "checks_performed": checks_performed,
            "status": "checked",
        }

        if has_availability is True:
            result["status"] = "has_availability"
            logger.info(f"  ✓ {name}: HAS availability ({checks_performed} checks)")
        elif has_availability is False:
            checked_ranges = [f"{d['arrival_days_ahead']}d" for d in check_details]
            result["status"] = "no_availability"
            logger.info(
                f"  ✗ {name}: NO availability (checked: {', '.join(checked_ranges)})"
            )
        else:
            result["status"] = "inconclusive"
            logger.debug(f"  ? {name}: Inconclusive ({checks_performed} checks)")

        return result


async def get_pending_hotels(limit: int, force: bool = False) -> list:
    """Get Australia RMS leads that need availability check."""
    async with get_conn() as conn:
        where_clause = """
            be.name = 'RMS Cloud'
            AND h.country IN ('AU', 'Australia')
            AND h.status = 1
            AND hbe.status = 1
        """
        if not force:
            where_clause += " AND hbe.has_availability IS NULL"

        rows = await conn.fetch(
            f"""
            SELECT
                h.id AS hotel_id,
                h.name,
                h.city,
                h.state,
                hbe.booking_url
            FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE {where_clause}
            ORDER BY h.id
            LIMIT $1
            """,
            limit,
        )
        return [dict(row) for row in rows]


async def update_availability_results(results: list, dry_run: bool = False) -> int:
    """Update database with availability check results."""
    if dry_run:
        logger.info(f"[DRY RUN] Would update {len(results)} hotels")
        return 0

    valid_results = [r for r in results if r["has_availability"] is not None]
    if not valid_results:
        return 0

    hotel_ids = [r["hotel_id"] for r in valid_results]
    availability_values = [r["has_availability"] for r in valid_results]

    async with get_conn() as conn:
        await conn.execute(
            """
            UPDATE sadie_gtm.hotel_booking_engines AS hbe
            SET has_availability = m.has_availability,
                availability_checked_at = NOW()
            FROM (
                SELECT unnest($1::int[]) AS hotel_id,
                       unnest($2::boolean[]) AS has_availability
            ) AS m
            WHERE hbe.hotel_id = m.hotel_id
            """,
            hotel_ids,
            availability_values,
        )

    return len(valid_results)


async def show_status():
    """Display availability check status for Australia RMS hotels."""
    async with get_conn() as conn:
        stats = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE hbe.has_availability IS NULL) AS pending,
                COUNT(*) FILTER (WHERE hbe.has_availability = TRUE) AS has_availability,
                COUNT(*) FILTER (WHERE hbe.has_availability = FALSE) AS no_availability
            FROM sadie_gtm.hotel_booking_engines hbe
            JOIN sadie_gtm.hotels h ON hbe.hotel_id = h.id
            JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
            WHERE be.name = 'RMS Cloud'
              AND h.country IN ('AU', 'Australia')
              AND h.status = 1
              AND hbe.status = 1
        """)

    logger.info("=" * 60)
    logger.info("RMS AUSTRALIA AVAILABILITY STATUS")
    logger.info("=" * 60)
    logger.info(f"Total Australia RMS leads: {stats['total']}")
    logger.info(f"Pending checks: {stats['pending']}")
    logger.info(f"Has availability: {stats['has_availability']}")
    logger.info(f"No availability: {stats['no_availability']}")
    logger.info("=" * 60)


async def run(
    limit: int = 100,
    concurrency: int = 30,  # Increased default for optimization
    force: bool = False,
    dry_run: bool = False,
):
    """Run optimized RMS availability enrichment."""
    await init_db()

    try:
        hotels = await get_pending_hotels(limit, force)

        if not hotels:
            logger.info("No Australia RMS leads pending availability check")
            return

        mode = "FORCE RECHECK" if force else "pending only"

        # Better ETA calculation with optimizations
        total_requests = len(hotels) * 2.5  # Avg 2.5 checks per hotel with early exit
        requests_per_second = concurrency * 1.5  # Optimized throughput
        estimated_seconds = total_requests / requests_per_second
        eta_minutes = int(estimated_seconds / 60)
        eta_seconds = int(estimated_seconds % 60)

        logger.info(
            f"Processing {len(hotels)} Australia RMS leads ({mode}, concurrency={concurrency})"
        )
        logger.info(
            f"Optimized: adaptive rate limiting, host-aware parallelism, smart early exit"
        )
        logger.info(f"Estimated time: ~{eta_minutes}m {eta_seconds}s")

        semaphore = asyncio.Semaphore(concurrency)

        async with httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=concurrency * 2,
                max_keepalive_connections=concurrency * 2,
            ),
            http2=False,
        ) as client:
            tasks = [
                process_hotel_optimized(client, hotel, semaphore) for hotel in hotels
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

        updated = await update_availability_results(valid_results, dry_run)

        has_avail = sum(1 for r in valid_results if r.get("has_availability") is True)
        no_avail = sum(1 for r in valid_results if r.get("has_availability") is False)
        inconclusive = sum(
            1 for r in valid_results if r.get("has_availability") is None
        )
        total_checks = sum(r.get("checks_performed", 0) for r in valid_results)
        avg_checks = total_checks / len(valid_results) if valid_results else 0

        logger.info("=" * 60)
        logger.info("RMS AVAILABILITY ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Leads processed: {len(valid_results)}")
        logger.info(f"  Has availability: {has_avail}")
        logger.info(f"  No availability: {no_avail}")
        logger.info(f"  Inconclusive: {inconclusive}")
        logger.info(f"  Errors: {errors}")
        logger.info(f"  Total HTTP requests: {total_checks}")
        logger.info(f"  Avg checks per hotel: {avg_checks:.1f}")
        logger.info(
            f"  Efficiency: {(1 - avg_checks / 4) * 100:.0f}% saved by early exit"
        )
        if not dry_run:
            logger.info(f"Database updated: {updated} leads")
        else:
            logger.info("[DRY RUN] No database updates made")
        logger.info("=" * 60)

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Check RMS Australia hotel availability (Optimized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check 100 leads (fast)
  uv run python -m workflows.rms_availability --limit 100

  # Check 3.7k leads with high concurrency
  uv run python -m workflows.rms_availability --limit 3700 --concurrency 30

  # Force re-check all leads
  uv run python -m workflows.rms_availability --limit 3700 --force

  # Check status
  uv run python -m workflows.rms_availability --status
        """,
    )

    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=100,
        help="Max leads to process (default: 100)",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=30,
        help="Concurrent requests (default: 30, optimized for speed)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-check all leads (ignore previous checks)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument(
        "--status", action="store_true", help="Show availability check status"
    )

    args = parser.parse_args()

    if args.status:
        asyncio.run(show_status())
    else:
        asyncio.run(
            run(
                limit=args.limit,
                concurrency=args.concurrency,
                force=args.force,
                dry_run=args.dry_run,
            )
        )


if __name__ == "__main__":
    main()
