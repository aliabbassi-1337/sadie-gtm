#!/usr/bin/env python3
"""
TheGuestbook Enumeration Workflow - Discover Cloudbeds hotels via TheGuestbook API.

TheGuestbook is Cloudbeds' rewards program with 800+ partner hotels.
This workflow fetches all hotels from their API and saves them to the database.

Usage:
    # Fetch all US hotels
    uv run python -m workflows.guestbook_enum

    # Fetch Florida only
    uv run python -m workflows.guestbook_enum --florida

    # Limit pages (for testing)
    uv run python -m workflows.guestbook_enum --max-pages 2

    # Include non-Cloudbeds hotels
    uv run python -m workflows.guestbook_enum --all-engines

    # Output to JSON
    uv run python -m workflows.guestbook_enum --output hotels.json

    # Save to database
    uv run python -m workflows.guestbook_enum --save-db
"""

import argparse
import asyncio
import json
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.leadgen.service import Service


# State bounding boxes for targeted scraping
STATE_BBOXES = {
    "florida": {
        "type": "Polygon",
        "coordinates": [[
            [-87.6, 24.5],   # SW (Key West area)
            [-80.0, 24.5],   # SE (Miami area)
            [-80.0, 31.0],   # NE (Jacksonville area)
            [-87.6, 31.0],   # NW (Pensacola area)
            [-87.6, 24.5],   # Close polygon
        ]]
    },
    "california": {
        "type": "Polygon",
        "coordinates": [[
            [-124.5, 32.5],  # SW
            [-114.0, 32.5],  # SE
            [-114.0, 42.0],  # NE
            [-124.5, 42.0],  # NW
            [-124.5, 32.5],  # Close
        ]]
    },
    "texas": {
        "type": "Polygon",
        "coordinates": [[
            [-106.6, 25.8],  # SW
            [-93.5, 25.8],   # SE
            [-93.5, 36.5],   # NE
            [-106.6, 36.5],  # NW
            [-106.6, 25.8],  # Close
        ]]
    },
    "new_york": {
        "type": "Polygon",
        "coordinates": [[
            [-79.8, 40.5],   # SW
            [-71.8, 40.5],   # SE
            [-71.8, 45.0],   # NE
            [-79.8, 45.0],   # NW
            [-79.8, 40.5],   # Close
        ]]
    },
    "hawaii": {
        "type": "Polygon",
        "coordinates": [[
            [-160.5, 18.5],  # SW
            [-154.5, 18.5],  # SE
            [-154.5, 22.5],  # NE
            [-160.5, 22.5],  # NW
            [-160.5, 18.5],  # Close
        ]]
    },
}


async def main():
    parser = argparse.ArgumentParser(
        description="Enumerate Cloudbeds hotels via TheGuestbook API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
TheGuestbook is Cloudbeds' rewards program with 800+ partner hotels.
This workflow fetches their hotel directory and extracts:
- Hotel name and coordinates
- Website URL
- Integration status (Cloudbeds vs other)
- Review scores

Examples:
    # Fetch all Cloudbeds hotels in the US
    uv run python -m workflows.guestbook_enum

    # Fetch Florida hotels only
    uv run python -m workflows.guestbook_enum --florida

    # Fetch Texas hotels
    uv run python -m workflows.guestbook_enum --state texas

    # Save to database as leads
    uv run python -m workflows.guestbook_enum --florida --save-db
"""
    )

    parser.add_argument(
        "--florida",
        action="store_true",
        help="Only fetch Florida hotels",
    )
    parser.add_argument(
        "--state",
        type=str,
        choices=list(STATE_BBOXES.keys()),
        help="Fetch hotels in a specific state",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Limit number of pages to fetch (for testing)",
    )
    parser.add_argument(
        "--all-engines",
        action="store_true",
        help="Include hotels not using Cloudbeds (beiStatus != 'automated')",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--save-db",
        action="store_true",
        help="Save results to database as hotel leads",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Determine bounding box
    bbox = None
    region_name = "US"
    
    if args.florida:
        bbox = STATE_BBOXES["florida"]
        region_name = "Florida"
    elif args.state:
        bbox = STATE_BBOXES[args.state]
        region_name = args.state.replace("_", " ").title()

    cloudbeds_only = not args.all_engines

    logger.info(f"Fetching hotels from TheGuestbook API")
    logger.info(f"Region: {region_name}")
    logger.info(f"Cloudbeds only: {cloudbeds_only}")
    if args.max_pages:
        logger.info(f"Max pages: {args.max_pages}")

    # Use service for enumeration
    service = Service()
    properties = await service.enumerate_guestbook(
        bbox=bbox,
        max_pages=args.max_pages,
        cloudbeds_only=cloudbeds_only,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("Results")
    logger.info("=" * 60)
    logger.info(f"Total properties found: {len(properties)}")

    # Count by status
    by_status = {}
    for p in properties:
        status = p.get("bei_status", "unknown")
        by_status[status] = by_status.get(status, 0) + 1
    
    logger.info("By integration status:")
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        logger.info(f"  {status}: {count}")

    # Count with websites
    with_website = sum(1 for p in properties if p.get("website"))
    logger.info(f"Properties with website: {with_website}")

    # Output to JSON
    if args.output:
        with open(args.output, "w") as f:
            json.dump(properties, f, indent=2)
        logger.info(f"Saved {len(properties)} properties to {args.output}")

    # Save to database using service
    if args.save_db:
        from db.client import init_db
        
        await init_db()
        
        source = f"guestbook_{region_name.lower().replace(' ', '_')}"
        
        # Prepare leads with external_id
        leads = []
        for p in properties:
            if p.get("website"):
                leads.append({
                    "name": p["name"],
                    "website": p["website"],
                    "lat": p.get("lat"),
                    "lng": p.get("lng"),
                    "external_id": f"guestbook_{p['id']}",
                    "external_id_type": "guestbook",
                })
        
        logger.info(f"Saving {len(leads)} leads to database...")
        stats = await service.save_booking_engine_leads(
            leads=leads,
            source=source,
            booking_engine="Cloudbeds",
        )
        
        logger.info(f"Database results:")
        logger.info(f"  Inserted: {stats['inserted']}")
        logger.info(f"  Engines linked: {stats['engines_linked']}")
        logger.info(f"  Skipped (exists): {stats['skipped_exists']}")
        logger.info(f"  Skipped (no website): {stats['skipped_no_website']}")
        logger.info(f"  Errors: {stats['errors']}")

    # Show sample results
    if properties and not args.output:
        logger.info("")
        logger.info("Sample properties (first 10):")
        for p in properties[:10]:
            status_icon = "✓" if p.get("bei_status") == "automated" else "○"
            score = f" ({p.get('trust_you_score')}★)" if p.get("trust_you_score") else ""
            website = p.get("website", "")
            website_str = f" → {website[:50]}..." if website and len(website) > 50 else f" → {website}" if website else ""
            logger.info(f"  {status_icon} {p['name']}{score}{website_str}")


if __name__ == "__main__":
    asyncio.run(main())
