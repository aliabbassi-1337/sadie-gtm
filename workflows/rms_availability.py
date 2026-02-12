#!/usr/bin/env python3
"""RMS Availability Enrichment Workflow

Checks if Australia RMS hotels have availability by calling their booking API
with future dates and scraping the HTML response.

Tries multiple date ranges (30, 60, 90 days ahead) to ensure we don't miss
availability due to seasonal closures or specific date restrictions.

The RMS booking API URL format:
https://bookings12.rmscloud.com/Rates/Index/{client_id}/{prop_id}?A={arrival}&D={departure}&Ad=2

Usage:
    # Check availability for 100 hotels
    uv run python -m workflows.rms_availability --limit 100

    # Force re-check all hotels (even if already checked)
    uv run python -m workflows.rms_availability --limit 100 --force

    # Dry run (don't update database)
    uv run python -m workflows.rms_availability --limit 100 --dry-run

    # Check status
    uv run python -m workflows.rms_availability --status
"""

import asyncio
import argparse
import re
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse, urlencode

import httpx
from loguru import logger

from db.client import init_db, close_db, get_conn


# RMS API configuration
RMS_BASE_URL = "https://bookings12.rmscloud.com/Rates/Index"

# Multiple date ranges to check (days ahead)
# Strategy: 1 check in 30-day range, 2 checks in 60-day range, 1 check in 90-day range
CHECK_DATE_RANGES = [
    (30, 32),  # 30 days ahead (30-day range)
    (60, 62),  # 60 days ahead (60-day range - first check)
    (75, 77),  # 75 days ahead (60-day range - second check, different interval)
    (90, 92),  # 90 days ahead (90-day range)
]


async def check_rms_availability_single_date(
    client: httpx.AsyncClient,
    booking_url: str,
    arrival_days_ahead: int,
    departure_days_ahead: int,
) -> Optional[bool]:
    """Check availability for a specific date range.

    Uses httpx to fetch the HTML and parses it for availability indicators.

    Args:
        client: HTTP client for making requests
        booking_url: The hotel's RMS booking URL
        arrival_days_ahead: Days from now for arrival date
        departure_days_ahead: Days from now for departure date

    Returns:
        True if hotel has availability, False if no availability, None if inconclusive
    """
    if not booking_url or "rmscloud.com" not in booking_url:
        return None

    # Parse the existing URL to extract client_id and prop_id
    try:
        parsed = urlparse(booking_url)
        path_parts = parsed.path.split("/")

        # Extract client_id and prop_id from URL like /Rates/Index/317/1
        if len(path_parts) >= 4:
            client_id = path_parts[3]
            prop_id = path_parts[4] if len(path_parts) > 4 else "1"
        else:
            # Try to extract from query params or use defaults
            client_id = "317"
            prop_id = "1"
    except Exception:
        logger.warning(f"Could not parse booking URL: {booking_url}")
        return None

    # Calculate future dates for availability check
    arrival_date = datetime.now() + timedelta(days=arrival_days_ahead)
    departure_date = datetime.now() + timedelta(days=departure_days_ahead)

    # Format dates as MM/DD/YYYY for RMS API
    arrival_str = arrival_date.strftime("%m/%d/%Y")
    departure_str = departure_date.strftime("%m/%d/%Y")

    # Construct availability check URL
    params = {
        "A": arrival_str,
        "D": departure_str,
        "Ad": "2",  # 2 adults
        "Mp": "0",
        "M": "0",
        "Y": "0",
        "Z": "0",
        "Rv": "1",
    }

    availability_url = f"{RMS_BASE_URL}/{client_id}/{prop_id}?{urlencode(params)}"

    try:
        resp = await client.get(
            availability_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
            timeout=30.0,
        )

        if resp.status_code != 200:
            logger.debug(f"Non-200 status {resp.status_code} for {booking_url}")
            return None

        html_content = resp.text

        # Parse HTML for availability indicators
        return _parse_availability_from_html(html_content, booking_url)

    except httpx.TimeoutException:
        logger.debug(f"Timeout checking availability for {booking_url}")
        return None
    except Exception as e:
        logger.debug(f"Error checking availability for {booking_url}: {e}")
        return None


def _parse_availability_from_html(
    html_content: str, booking_url: str
) -> Optional[bool]:
    """Parse RMS booking page HTML for availability indicators.

    Looks for:
    - LeadInRate value (positive rate = has availability)
    - "No Available Rates" text or hidden fields
    - Room/category data in the page

    Args:
        html_content: The HTML response from the booking page
        booking_url: URL for logging purposes

    Returns:
        True if has availability, False if no availability, None if inconclusive
    """
    # Check for "No Available Rates" signal in the HTML
    # This is typically in a hidden input field or displayed text
    no_avail_patterns = [
        'value="No Available Rates"',
        "lblNoAvailableRates",
        "no available rates",
        "no rates available",
    ]

    for pattern in no_avail_patterns:
        if pattern.lower() in html_content.lower():
            return False

    # Check for actual rate/availability data in the page
    # Look for room/category data in the VM (view model) JSON
    has_data_indicators = [
        '"DataCollection":',
        '"LeadInRate"',
        '"Category"',
        '"RoomName"',
    ]

    has_data = any(indicator in html_content for indicator in has_data_indicators)

    if has_data:
        # Check if the LeadInRate has a valid value (positive rate)
        leadin_match = re.search(r'id="LeadInRate"[^>]+value="(\d+)"', html_content)
        if leadin_match:
            rate = int(leadin_match.group(1))
            if rate > 0:
                return True

        # Even without LeadInRate, if we have DataCollection, likely has availability
        if '"DataCollection":' in html_content:
            # Check if DataCollection is not empty
            data_match = re.search(
                r'"DataCollection":\s*(\[.*?\])', html_content, re.DOTALL
            )
            if data_match:
                try:
                    import json

                    data = json.loads(data_match.group(1))
                    if data and len(data) > 0:
                        return True
                except:
                    pass

    # Check for availability-related CSS classes or elements
    avail_indicators = [
        "room-available",
        "rate-available",
        "has-availability",
        "availability-calendar",
    ]

    for indicator in avail_indicators:
        if indicator in html_content.lower():
            return True

    # Inconclusive - couldn't determine availability
    logger.debug(f"Could not determine availability for {booking_url}")
    return None


async def check_rms_availability_multi_date(
    client: httpx.AsyncClient,
    booking_url: str,
) -> tuple[Optional[bool], list[dict]]:
    """Check availability across multiple date ranges.

    Tries each date range in sequence. Returns True immediately if any range
    shows availability. Only returns False if ALL ranges show no availability.

    Args:
        client: HTTP client
        booking_url: The hotel's RMS booking URL

    Returns:
        Tuple of (has_availability, check_details)
        - has_availability: True/False/None
        - check_details: List of dicts with results for each date range checked
    """
    check_details = []

    for arrival_days, departure_days in CHECK_DATE_RANGES:
        result = await check_rms_availability_single_date(
            client, booking_url, arrival_days, departure_days
        )

        detail = {
            "arrival_days_ahead": arrival_days,
            "departure_days_ahead": departure_days,
            "has_availability": result,
        }
        check_details.append(detail)

        # If we found availability, return immediately (no need to check other dates)
        if result is True:
            logger.debug(f"  Found availability at {arrival_days} days ahead")
            return True, check_details

        # Small delay between checks to be nice to the server
        await asyncio.sleep(0.5)

    # If we get here, either all were False or all were inconclusive
    # Return False only if at least one check was definitive False
    has_definitive_false = any(d["has_availability"] is False for d in check_details)
    all_inconclusive = all(d["has_availability"] is None for d in check_details)

    if all_inconclusive:
        return None, check_details
    elif has_definitive_false:
        return False, check_details
    else:
        return None, check_details


async def process_hotel(
    client: httpx.AsyncClient,
    hotel: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Process a single hotel for availability check across multiple dates.

    Args:
        client: HTTP client
        hotel: Hotel dict with id, name, booking_url
        semaphore: Concurrency semaphore

    Returns:
        Dict with hotel_id, has_availability, check_details, and status
    """
    async with semaphore:
        hotel_id = hotel["hotel_id"]
        name = hotel.get("name", "Unknown")
        booking_url = hotel.get("booking_url")

        logger.debug(f"Checking availability for {name} (id={hotel_id})")

        # Handle missing booking_url
        if not booking_url:
            return {
                "hotel_id": hotel_id,
                "hotel_name": name,
                "has_availability": None,
                "check_details": [],
                "status": "inconclusive",
            }

        has_availability, check_details = await check_rms_availability_multi_date(
            client, booking_url
        )

        result = {
            "hotel_id": hotel_id,
            "hotel_name": name,
            "has_availability": has_availability,
            "check_details": check_details,
            "status": "checked",
        }

        if has_availability is True:
            result["status"] = "has_availability"
            logger.info(f"  ✓ {name}: HAS availability")
        elif has_availability is False:
            # Log which date ranges were checked
            checked_ranges = [
                f"{d['arrival_days_ahead']}d"
                for d in check_details
                if d["has_availability"] is not None
            ]
            result["status"] = "no_availability"
            logger.info(
                f"  ✗ {name}: NO availability (checked: {', '.join(checked_ranges)})"
            )
        else:
            result["status"] = "inconclusive"
            logger.debug(f"  ? {name}: Inconclusive (all date ranges failed)")

        return result


async def get_pending_hotels(limit: int, force: bool = False) -> list:
    """Get Australia RMS leads that need availability check.

    Only fetches hotels that:
    - Use RMS Cloud booking engine
    - Are located in Australia
    - Have not been checked yet (or force=True)

    Args:
        limit: Maximum number of hotels to return
        force: If True, return all Australia RMS leads (for re-check)

    Returns:
        List of hotel dicts
    """
    async with get_conn() as conn:
        if force:
            rows = await conn.fetch(
                """
                SELECT 
                    h.id AS hotel_id,
                    h.name,
                    h.city,
                    h.state,
                    hbe.booking_url
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
                WHERE be.name = 'RMS Cloud'
                  AND h.country IN ('AU', 'Australia')
                ORDER BY h.id
                LIMIT $1
            """,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT 
                    h.id AS hotel_id,
                    h.name,
                    h.city,
                    h.state,
                    hbe.booking_url
                FROM sadie_gtm.hotels h
                JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
                JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
                WHERE be.name = 'RMS Cloud'
                  AND hbe.has_availability IS NULL
                  AND h.country IN ('AU', 'Australia')
                ORDER BY h.id
                LIMIT $1
            """,
                limit,
            )

        return [dict(row) for row in rows]


async def update_availability_results(results: list, dry_run: bool = False) -> int:
    """Update database with availability check results.

    Args:
        results: List of result dicts from process_hotel
        dry_run: If True, don't actually update database

    Returns:
        Number of hotels updated
    """
    if dry_run:
        logger.info(f"[DRY RUN] Would update {len(results)} hotels")
        return 0

    # Filter out inconclusive results
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
    concurrency: int = 20,
    force: bool = False,
    dry_run: bool = False,
):
    """Run RMS availability enrichment for Australia hotels.

    Checks multiple date ranges (30, 60, 90 days ahead) to ensure accurate
    availability detection. Only marks a hotel as unavailable if ALL date
    ranges show no availability.

    Args:
        limit: Maximum number of hotels to process
        concurrency: Number of concurrent requests
        force: If True, re-check all hotels regardless of previous checks
        dry_run: If True, don't update database
    """
    await init_db()

    try:
        # Get hotels to process
        hotels = await get_pending_hotels(limit, force)

        if not hotels:
            logger.info("No Australia RMS leads pending availability check")
            return

        mode = "FORCE RECHECK" if force else "pending only"
        logger.info(
            f"Processing {len(hotels)} Australia RMS leads ({mode}, concurrency={concurrency})"
        )
        logger.info(f"Checking {len(CHECK_DATE_RANGES)} date ranges per hotel")

        # Process hotels concurrently
        semaphore = asyncio.Semaphore(concurrency)

        async with httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=concurrency + 10,
                max_keepalive_connections=concurrency + 10,
            ),
        ) as client:
            tasks = [process_hotel(client, hotel, semaphore) for hotel in hotels]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle results
        valid_results = []
        errors = 0
        for r in results:
            if isinstance(r, Exception):
                errors += 1
                logger.error(f"Hotel check error: {r}")
            else:
                valid_results.append(r)

        # Update database
        updated = await update_availability_results(valid_results, dry_run)

        # Summary
        has_avail = sum(1 for r in valid_results if r.get("has_availability") is True)
        no_avail = sum(1 for r in valid_results if r.get("has_availability") is False)
        inconclusive = sum(
            1 for r in valid_results if r.get("has_availability") is None
        )

        # Count how many required multi-date checks
        multi_date_checks = sum(
            1
            for r in valid_results
            if r.get("check_details") and len(r["check_details"]) > 1
        )

        logger.info("=" * 60)
        logger.info("RMS AVAILABILITY ENRICHMENT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Leads processed: {len(valid_results)}")
        logger.info(f"  Has availability: {has_avail}")
        logger.info(f"  No availability: {no_avail}")
        logger.info(f"  Inconclusive: {inconclusive}")
        logger.info(f"  Errors: {errors}")
        logger.info(f"  Multi-date checks needed: {multi_date_checks}")
        if not dry_run:
            logger.info(f"Database updated: {updated} leads")
        else:
            logger.info("[DRY RUN] No database updates made")
        logger.info("=" * 60)

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Check RMS Australia hotel availability",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check 100 leads
  uv run python -m workflows.rms_availability --limit 100
  
  # Force re-check all leads
  uv run python -m workflows.rms_availability --limit 100 --force
  
  # Dry run (don't update database)
  uv run python -m workflows.rms_availability --limit 100 --dry-run
  
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
        default=20,
        help="Concurrent requests (default: 20)",
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
