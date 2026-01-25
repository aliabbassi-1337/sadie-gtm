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

from services.leadgen.booking_engines import GuestbookScraper, GuestbookProperty


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

    # Fetch hotels
    async with GuestbookScraper() as scraper:
        properties = await scraper.fetch_all(
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
        by_status[p.bei_status] = by_status.get(p.bei_status, 0) + 1
    
    logger.info("By integration status:")
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        logger.info(f"  {status}: {count}")

    # Count with websites
    with_website = sum(1 for p in properties if p.website)
    logger.info(f"Properties with website: {with_website}")

    # Output to JSON
    if args.output:
        output_data = [p.model_dump() for p in properties]
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Saved {len(properties)} properties to {args.output}")

    # Save to database
    if args.save_db:
        from db.client import init_db, get_conn
        from services.leadgen import repo
        
        await init_db()
        
        logger.info("Saving to database...")
        source = f"guestbook_{region_name.lower().replace(' ', '_')}"
        
        inserted = 0
        skipped = 0
        errors = 0
        
        for prop in properties:
            if not prop.website:
                skipped += 1
                continue
            
            try:
                # Create hotel record
                hotel_data = {
                    "name": prop.name,
                    "website": prop.website,
                    "lat": prop.lat,
                    "lng": prop.lng,
                    "source": source,
                    "external_id": f"guestbook_{prop.id}",
                    "external_id_type": "guestbook",
                }
                
                # Check if hotel already exists
                async with get_conn() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id FROM hotels WHERE external_id = $1 AND external_id_type = $2",
                        hotel_data["external_id"],
                        hotel_data["external_id_type"],
                    )
                    
                    if existing:
                        skipped += 1
                        continue
                    
                    # Insert hotel
                    hotel_id = await conn.fetchval(
                        """
                        INSERT INTO hotels (name, website, lat, lng, source, external_id, external_id_type)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        RETURNING id
                        """,
                        hotel_data["name"],
                        hotel_data["website"],
                        hotel_data["lat"],
                        hotel_data["lng"],
                        hotel_data["source"],
                        hotel_data["external_id"],
                        hotel_data["external_id_type"],
                    )
                    
                    # Link to Cloudbeds engine if automated
                    if prop.bei_status == "automated":
                        await conn.execute(
                            """
                            INSERT INTO hotel_booking_engines (hotel_id, booking_engine_id, detected_at)
                            SELECT $1, id, NOW()
                            FROM booking_engines
                            WHERE name ILIKE 'cloudbeds'
                            ON CONFLICT (hotel_id, booking_engine_id) DO NOTHING
                            """,
                            hotel_id,
                        )
                    
                    inserted += 1
                    
            except Exception as e:
                logger.error(f"Error saving {prop.name}: {e}")
                errors += 1

        logger.info(f"Database results:")
        logger.info(f"  Inserted: {inserted}")
        logger.info(f"  Skipped (exists or no website): {skipped}")
        logger.info(f"  Errors: {errors}")

    # Show sample results
    if properties and not args.output:
        logger.info("")
        logger.info("Sample properties (first 10):")
        for p in properties[:10]:
            status_icon = "✓" if p.bei_status == "automated" else "○"
            score = f" ({p.trust_you_score}★)" if p.trust_you_score else ""
            website = f" → {p.website[:50]}..." if p.website and len(p.website) > 50 else f" → {p.website}" if p.website else ""
            logger.info(f"  {status_icon} {p.name}{score}{website}")


if __name__ == "__main__":
    asyncio.run(main())
