#!/usr/bin/env python3
"""
Scan RMS Cloud for hotel properties by ID enumeration.

RMS uses numeric IDs that are sparse - we scan ranges to find valid properties.
Supports distributed scanning across multiple EC2 instances.

RATE LIMITING:
- RMS will rate limit aggressive scanning
- Use --delay and low --concurrency to avoid bans
- For faster scanning, distribute across EC2 instances

Usage:
    # Conservative scan (low risk, ~2 hours for 20k IDs)
    uv run python -m workflows.scan_rms --start 1 --end 20000 --concurrency 5 --delay 0.5
    
    # Distributed scan (split across 7 EC2 instances)
    # Instance 1: uv run python -m workflows.scan_rms --start 1 --end 3000 --save-db
    # Instance 2: uv run python -m workflows.scan_rms --start 3001 --end 6000 --save-db
    # ...etc
    
    # Dry run to test
    uv run python -m workflows.scan_rms --start 1 --end 100 --dry-run
"""

import argparse
import asyncio
import logging
import json
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description="Scan RMS Cloud for hotel properties"
    )
    
    # Range
    parser.add_argument(
        "--start",
        type=int,
        default=1,
        help="Start ID (default: 1)"
    )
    parser.add_argument(
        "--end",
        type=int,
        default=20000,
        help="End ID (default: 20000)"
    )
    
    # Rate limiting
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Concurrent requests (default: 10, lower = safer)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay between requests in seconds (default: 0.2)"
    )
    
    # Subdomain
    parser.add_argument(
        "--subdomain",
        type=str,
        default="ibe13.rmscloud.com",
        help="RMS subdomain to scan (default: ibe13.rmscloud.com)"
    )
    parser.add_argument(
        "--all-subdomains",
        action="store_true",
        help="Scan all known RMS subdomains (slower but more complete)"
    )
    
    # Output
    parser.add_argument(
        "--output",
        type=str,
        help="Path to save found properties as JSON"
    )
    parser.add_argument(
        "--save-db",
        action="store_true",
        help="Save to database incrementally"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just test without saving"
    )
    
    args = parser.parse_args()
    
    # Estimate time
    total_ids = args.end - args.start + 1
    time_per_id = args.delay + 0.1  # delay + overhead
    est_time = (total_ids / args.concurrency) * time_per_id / 60
    
    logger.info(f"Scanning RMS IDs {args.start}-{args.end} ({total_ids} IDs)")
    logger.info(f"Settings: concurrency={args.concurrency}, delay={args.delay}s")
    logger.info(f"Estimated time: ~{est_time:.1f} minutes")
    
    if args.dry_run:
        logger.info("Dry run - will scan but not save")
    
    # Initialize scanner - use fast API-based scanner
    from lib.rms.scanner import RMSScanner
    
    found_hotels = []
    
    # DB save callback
    save_callback = None
    if args.save_db and not args.dry_run:
        from db.client import init_db
        await init_db()
        
        from services.leadgen import repo
        
        # Get or create RMS Cloud booking engine
        engine = await repo.get_booking_engine_by_name("RMS Cloud")
        engine_id = engine.id if engine else None
        
        if not engine_id:
            engine_id = await repo.insert_booking_engine(name="RMS Cloud", tier=2)
            logger.info(f"Created RMS Cloud booking engine with id {engine_id}")
        
        async def save_to_db(hotel: dict):
            """Save hotel to DB immediately when found."""
            try:
                hotel_id = await repo.insert_hotel(
                    name=hotel["name"],
                    source="rms_scan",
                    status=0,
                    external_id=f"rms_{hotel['id']}",
                    external_id_type="rms_scan",
                )
                
                if hotel_id:
                    await repo.insert_hotel_booking_engine(
                        hotel_id=hotel_id,
                        booking_engine_id=engine_id,
                        booking_url=hotel["booking_url"],
                        engine_property_id=str(hotel["id"]),
                        detection_method="rms_scan",
                        status=1,
                    )
                    logger.debug(f"Saved: {hotel['name']} (ID {hotel['id']})")
            except Exception as e:
                if "duplicate" not in str(e).lower():
                    logger.error(f"Error saving {hotel['name']}: {e}")
        
        save_callback = save_to_db
    
    # Scan
    async with RMSScanner(
        concurrency=args.concurrency,
        delay=args.delay,
    ) as scanner:
        if args.all_subdomains:
            found_hotels = await scanner.scan_all_subdomains(
                start_id=args.start,
                end_id=args.end,
                on_found=save_callback,
            )
        else:
            found_hotels = await scanner.scan_range(
                start_id=args.start,
                end_id=args.end,
                subdomain=args.subdomain,
                on_found=save_callback,
            )
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("SCAN COMPLETE")
    logger.info("=" * 50)
    logger.info(f"IDs scanned: {total_ids}")
    logger.info(f"Properties found: {len(found_hotels)}")
    logger.info(f"Hit rate: {len(found_hotels) / total_ids * 100:.2f}%")
    
    # Save to file
    if args.output and found_hotels:
        output_path = Path(args.output)
        output_path.write_text(json.dumps(found_hotels, indent=2))
        logger.info(f"Saved to {output_path}")
    
    # Show sample
    if found_hotels:
        logger.info("\nSample properties found:")
        for hotel in found_hotels[:5]:
            logger.info(f"  ID {hotel['id']}: {hotel['name']}")
        if len(found_hotels) > 5:
            logger.info(f"  ... and {len(found_hotels) - 5} more")


if __name__ == "__main__":
    asyncio.run(main())
