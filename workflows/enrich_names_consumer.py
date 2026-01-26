"""Hotel enrichment worker - Polls SQS and scrapes names/addresses from booking pages.

Run continuously on EC2 instances to process enrichment jobs.
Extracts hotel names and location data (city, state, country) from booking pages.

Usage:
    uv run python -m workflows.enrich_names_consumer
    uv run python -m workflows.enrich_names_consumer --delay 0.5
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import json
import os
import re
import signal
from dataclasses import dataclass
from typing import Dict, Any, Optional
from loguru import logger
import httpx

from db.client import init_db, close_db, get_conn, queries
from infra.sqs import receive_messages, delete_message, get_queue_attributes

# Queue URL from environment
QUEUE_URL = os.getenv("SQS_NAME_ENRICHMENT_QUEUE_URL", "")

# Global flag for graceful shutdown
shutdown_requested = False


@dataclass
class ExtractedData:
    """Data extracted from a booking page."""
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


def extract_json_ld(html: str) -> Optional[Dict[str, Any]]:
    """Extract JSON-LD structured data from HTML."""
    try:
        # Find all JSON-LD scripts
        pattern = r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            try:
                data = json.loads(match.strip())
                # Handle array of objects
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                            return item
                # Handle single object
                elif data.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                    return data
                # Handle nested @graph
                elif "@graph" in data:
                    for item in data["@graph"]:
                        if item.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                            return item
            except json.JSONDecodeError:
                continue
    except Exception:
        pass
    return None


def parse_address_from_json_ld(json_ld: Dict[str, Any]) -> ExtractedData:
    """Parse address from JSON-LD structured data."""
    data = ExtractedData()
    
    # Get name
    if "name" in json_ld:
        data.name = json_ld["name"].strip()
    
    # Get address (can be string or PostalAddress object)
    address = json_ld.get("address", {})
    if isinstance(address, str):
        data.address = address
    elif isinstance(address, dict):
        data.address = address.get("streetAddress")
        data.city = address.get("addressLocality")
        data.state = address.get("addressRegion")
        data.country = address.get("addressCountry")
        
        # Country might be an object
        if isinstance(data.country, dict):
            data.country = data.country.get("name") or data.country.get("@id")
    
    return data


def extract_from_meta_tags(html: str) -> ExtractedData:
    """Extract data from meta tags."""
    data = ExtractedData()
    
    # og:title for name
    match = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if not match:
        match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']', html, re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        parts = re.split(r'\s*[-|–]\s*', raw)
        name = parts[0].strip()
        if name.lower() not in ['book now', 'reservation', 'booking', 'home', 'unknown']:
            data.name = name
    
    # Fallback to <title>
    if not data.name:
        match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
        if match:
            raw = match.group(1).strip()
            parts = re.split(r'\s*[-|–]\s*', raw)
            name = parts[0].strip()
            if name.lower() not in ['book now', 'reservation', 'booking', 'home', 'unknown']:
                data.name = name
    
    # og:locality / og:region for city/state
    city_match = re.search(r'<meta[^>]+property=["\'](?:og:locality|business:contact_data:locality)["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if city_match:
        data.city = city_match.group(1).strip()
    
    state_match = re.search(r'<meta[^>]+property=["\'](?:og:region|business:contact_data:region)["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if state_match:
        data.state = state_match.group(1).strip()
    
    country_match = re.search(r'<meta[^>]+property=["\'](?:og:country-name|business:contact_data:country_name)["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if country_match:
        data.country = country_match.group(1).strip()
    
    return data


async def extract_data_from_page(
    client: httpx.AsyncClient,
    booking_url: str,
    engine: str,
) -> Optional[ExtractedData]:
    """Scrape hotel name and address from booking page."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        
        resp = await client.get(booking_url, headers=headers, follow_redirects=True, timeout=30.0)
        
        if resp.status_code != 200:
            logger.warning(f"Failed to fetch {booking_url}: {resp.status_code}")
            return None
        
        html = resp.text
        
        # Try JSON-LD first (most structured)
        json_ld = extract_json_ld(html)
        if json_ld:
            data = parse_address_from_json_ld(json_ld)
            if data.name or data.city:
                return data
        
        # Fall back to meta tags
        data = extract_from_meta_tags(html)
        if data.name or data.city:
            return data
        
        return None
        
    except Exception as e:
        logger.warning(f"Error scraping {booking_url}: {e}")
        return None


async def get_hotel_current_state(hotel_id: int) -> Optional[Dict[str, Any]]:
    """Get current hotel data to determine what needs enrichment."""
    try:
        async with get_conn() as conn:
            result = await queries.get_hotel_by_id(conn, hotel_id=hotel_id)
            return dict(result) if result else None
    except Exception as e:
        logger.error(f"Failed to get hotel {hotel_id}: {e}")
        return None


def needs_name_enrichment(hotel: Dict[str, Any]) -> bool:
    """Check if hotel needs name enrichment."""
    name = hotel.get("name", "")
    return not name or name.startswith("Unknown")


def needs_address_enrichment(hotel: Dict[str, Any]) -> bool:
    """Check if hotel needs address enrichment."""
    city = hotel.get("city", "")
    state = hotel.get("state", "")
    return not city or not state


async def update_hotel(
    hotel_id: int,
    data: ExtractedData,
    update_name: bool,
    update_address: bool,
) -> tuple:
    """Update hotel in database with extracted data.
    
    Returns (success, name_updated, address_updated).
    """
    try:
        async with get_conn() as conn:
            name_to_update = data.name if update_name and data.name else None
            has_location = data.city or data.state
            
            if update_address and has_location:
                # Update both name and location (query uses COALESCE to preserve existing)
                await queries.update_hotel_name_and_location(
                    conn,
                    hotel_id=hotel_id,
                    name=name_to_update,
                    address=data.address,
                    city=data.city,
                    state=data.state,
                    country=data.country,
                )
                return (True, bool(name_to_update), True)
            elif update_name and data.name:
                # Just update name
                await queries.update_hotel_name(conn, hotel_id=hotel_id, name=data.name)
                return (True, True, False)
            else:
                # Nothing to update
                return (True, False, False)
                
    except Exception as e:
        logger.error(f"Failed to update hotel {hotel_id}: {e}")
        return (False, False, False)


async def process_message(
    client: httpx.AsyncClient,
    message: Dict[str, Any],
    queue_url: str,
    delay: float,
) -> tuple:
    """Process a single SQS message.
    
    Auto-detects what the hotel needs based on current DB state.
    Returns (success, found_data, name_updated, address_updated).
    """
    receipt_handle = message["receipt_handle"]
    body = message["body"]
    
    hotel_id = body.get("hotel_id")
    booking_url = body.get("booking_url")
    engine = body.get("engine", "unknown")
    
    if not hotel_id or not booking_url:
        # Invalid message, delete it
        delete_message(queue_url, receipt_handle)
        return (False, False, False, False)
    
    # Get current hotel state to determine what needs enrichment
    hotel = await get_hotel_current_state(hotel_id)
    if not hotel:
        # Hotel not found, delete message
        delete_message(queue_url, receipt_handle)
        return (False, False, False, False)
    
    # Auto-detect what needs enrichment
    enrich_name = needs_name_enrichment(hotel)
    enrich_address = needs_address_enrichment(hotel)
    
    if not enrich_name and not enrich_address:
        # Already fully enriched, skip
        delete_message(queue_url, receipt_handle)
        return (True, False, False, False)
    
    # Rate limiting delay
    await asyncio.sleep(delay)
    
    # Scrape the data
    data = await extract_data_from_page(client, booking_url, engine)
    
    if data:
        # Update database (only fields that need updating)
        db_success, name_updated, addr_updated = await update_hotel(
            hotel_id, data, enrich_name, enrich_address
        )
        
        if db_success:
            parts = []
            if name_updated:
                parts.append(f"name={data.name}")
            if addr_updated:
                parts.append(f"city={data.city}, state={data.state}")
            if parts:
                logger.info(f"  Updated hotel {hotel_id}: {', '.join(parts)}")
            delete_message(queue_url, receipt_handle)
            return (True, True, name_updated, addr_updated)
        else:
            # DB error - don't delete, will retry
            return (False, True, False, False)
    else:
        # Couldn't extract data - delete anyway (won't help to retry)
        delete_message(queue_url, receipt_handle)
        return (True, False, False, False)


async def run_worker(delay: float = 0.5, poll_interval: int = 5):
    """Main worker loop - poll SQS and process messages."""
    global shutdown_requested
    
    if not QUEUE_URL:
        logger.error("SQS_NAME_ENRICHMENT_QUEUE_URL not set")
        return
    
    await init_db()
    
    # Stats
    processed = 0
    data_found = 0
    names_updated = 0
    addresses_updated = 0
    errors = 0
    
    logger.info(f"Starting enrichment worker (delay={delay}s)")
    logger.info(f"Queue: {QUEUE_URL}")
    
    async with httpx.AsyncClient() as client:
        try:
            while not shutdown_requested:
                # Get queue stats
                attrs = get_queue_attributes(QUEUE_URL)
                pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                
                if pending == 0 and in_flight == 0:
                    logger.info(f"Queue empty. Processed: {processed}, Names: {names_updated}, Addresses: {addresses_updated}, Errors: {errors}")
                    logger.info(f"Waiting {poll_interval}s...")
                    await asyncio.sleep(poll_interval)
                    continue
                
                # Receive messages
                messages = receive_messages(
                    QUEUE_URL,
                    max_messages=10,
                    wait_time_seconds=20,
                    visibility_timeout=60,
                )
                
                if not messages:
                    continue
                
                logger.info(f"Processing {len(messages)} messages (pending: {pending}, in_flight: {in_flight})")
                
                # Process each message
                for msg in messages:
                    if shutdown_requested:
                        break
                    
                    success, found, name_updated, addr_updated = await process_message(client, msg, QUEUE_URL, delay)
                    processed += 1
                    if found:
                        data_found += 1
                    if name_updated:
                        names_updated += 1
                    if addr_updated:
                        addresses_updated += 1
                    if not success:
                        errors += 1
                
                # Log progress
                if processed % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {names_updated} names, {addresses_updated} addresses, {errors} errors")
                    
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            await close_db()
            logger.info(f"Final stats: {processed} processed, {names_updated} names, {addresses_updated} addresses, {errors} errors")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Name enrichment worker - scrape hotel names from booking pages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run worker with default settings (0.5s delay)
    uv run python -m workflows.enrich_names_consumer

    # Run with faster rate (use with caution)
    uv run python -m workflows.enrich_names_consumer --delay 0.2

Environment:
    SQS_NAME_ENRICHMENT_QUEUE_URL - Required. The SQS queue URL.
        """
    )
    
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds to wait when queue is empty (default: 5)"
    )
    
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    asyncio.run(run_worker(args.delay, args.poll_interval))


if __name__ == "__main__":
    main()
