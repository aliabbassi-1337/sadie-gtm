#!/usr/bin/env python3
"""
Mark non-hotel businesses in the database with status=-4.

Uses the same filtering logic as the scraper/detector to identify
restaurants, stores, and other non-hotel businesses that slipped through.
"""

import asyncio
import argparse
from loguru import logger

from db.client import init_db
from services.leadgen.constants import HotelStatus

# Non-hotel name keywords (same as detector.py)
NON_HOTEL_KEYWORDS = [
    # Restaurants (generic)
    "restaurant", "grill", "sushi", "pizza", "taco", "burrito", "bbq", "barbecue",
    "steakhouse", "seafood", "buffet", "diner", "bakery", "deli", "cafe",
    "bistro", "eatery", "cantina", "tavern", "pub", "brewery", "bar & grill",
    "ramen", "noodle", "pho", "wings", "wingstop", "hot pot",
    "korean bbq", "hibachi", "teriyaki", "shawarma", "falafel", "kebab",
    # Restaurant chains
    "mcdonald", "burger king", "wendy", "taco bell", "chick-fil-a",
    "starbucks", "dunkin", "subway", "pizza hut", "domino", "papa john",
    "olive garden", "applebee", "chili", "ihop", "denny", "waffle house",
    "cracker barrel", "outback", "longhorn", "red lobster", "texas roadhouse",
    "buffalo wild wings", "hooters", "carrabba", "bonefish", "cheesecake factory",
    "pf chang", "benihana", "shake shack", "in-n-out", "whataburger",
    "jack in the box", "hardee", "carl's jr", "krispy kreme", "baskin",
    "cold stone", "dairy queen", "culver", "popeyes", "five guys", "arby",
    # Medical
    "pharmacy", "hospital", "clinic", "medical center", "dental", "urgent care",
    "doctor", "physician", "healthcare", "laboratory",
    # Retail
    "publix", "walmart", "target", "cvs", "walgreens", "kroger", "whole foods",
    "costco", "safeway", "dollar general", "dollar tree", "best buy",
    "warby parker", "eyewear", "optical", "mattress",
    # Banks
    "bank of america", "chase bank", "wells fargo", "citibank",
    "credit union", "western union", "moneygram",
    # Gas stations
    "gas station", "chevron", "exxon", "shell", "speedway",
    "wawa", "sheetz", "racetrac", "quiktrip", "circle k", "7-eleven",
    # Auto
    "autozone", "o'reilly auto", "jiffy lube", "valvoline", "car wash",
    # Religious/Education
    "church", "temple", "mosque", "synagogue", "chapel",
    "school", "university", "college", "academy",
    # Fitness
    "gym", "fitness", "planet fitness", "la fitness", "ymca", "crossfit",
    # Personal services
    "salon", "nail", "tattoo", "barbershop",
    # Pet
    "pet", "grooming", "doggy", "veterinar",
    # Childcare
    "daycare", "childcare", "preschool", "kindergarten",
    # Entertainment
    "cinema", "theater", "theatre", "bowling", "arcade", "escape room",
    "trampoline", "skating rink", "mini golf", "laser tag",
    # Storage
    "storage", "self storage", "u-haul",
    # Car rental
    "sixt", "hertz", "avis", "enterprise rent", "budget car", "national car",
    "rent a car", "car rental",
    # Apartments/Senior Living (not short-term)
    "apartment", "the palace", "senior living", "assisted living", "nursing home",
    "retirement", "memory care", "eldercare",
    # Construction/Services
    "exteriors", "roofing", "plumbing", "electric", "hvac", "landscaping",
    "construction", "contractor", "remodeling", "renovation",
    # Coffee/Bagel/Food that slipped through
    "coffee", "bagel", "donut", "smoothie", "juice bar", "ice cream",
    "frozen yogurt", "cupcake", "cookie",
]


async def mark_non_hotels(dry_run: bool = True, batch_size: int = 1000) -> int:
    """Mark non-hotel businesses with status=-4."""
    pool = await init_db()
    
    # Build LIKE conditions for each keyword (escape apostrophes for SQL)
    like_conditions = " OR ".join([f"LOWER(name) LIKE '%{kw.replace(chr(39), chr(39)+chr(39))}%'" for kw in NON_HOTEL_KEYWORDS])
    
    async with pool.acquire() as conn:
        if dry_run:
            # Count how many would be affected
            count = await conn.fetchval(f"""
                SELECT COUNT(*) FROM sadie_gtm.hotels 
                WHERE status >= 0 AND ({like_conditions})
            """)
            logger.info(f"Would mark {count} non-hotels with status={HotelStatus.NON_HOTEL}")
            
            # Show sample
            samples = await conn.fetch(f"""
                SELECT name FROM sadie_gtm.hotels 
                WHERE status >= 0 AND ({like_conditions})
                LIMIT 20
            """)
            logger.info("Sample non-hotels:")
            for s in samples:
                logger.info(f"  - {s['name']}")
            
            return count
        else:
            # Actually update
            result = await conn.execute(f"""
                UPDATE sadie_gtm.hotels 
                SET status = $1, updated_at = NOW()
                WHERE status >= 0 AND ({like_conditions})
            """, HotelStatus.NON_HOTEL)
            
            count = int(result.split()[-1])
            logger.info(f"Marked {count} non-hotels with status={HotelStatus.NON_HOTEL}")
            return count


async def get_stats() -> None:
    """Show current status distribution."""
    pool = await init_db()
    async with pool.acquire() as conn:
        stats = await conn.fetch("""
            SELECT status, COUNT(*) as cnt 
            FROM sadie_gtm.hotels 
            GROUP BY status 
            ORDER BY status
        """)
        
        logger.info("Current hotel status distribution:")
        for s in stats:
            status = s['status']
            cnt = s['cnt']
            label = {
                -4: "non_hotel",
                -3: "duplicate", 
                -2: "location_mismatch",
                -1: "no_booking_engine",
                0: "pending",
                1: "launched",
            }.get(status, f"unknown_{status}")
            logger.info(f"  {status:>3} ({label}): {cnt}")


async def main():
    parser = argparse.ArgumentParser(description="Mark non-hotel businesses")
    parser.add_argument("--run", action="store_true", help="Actually update (default: dry-run)")
    parser.add_argument("--stats", action="store_true", help="Show status distribution")
    args = parser.parse_args()
    
    if args.stats:
        await get_stats()
    else:
        await mark_non_hotels(dry_run=not args.run)


if __name__ == "__main__":
    asyncio.run(main())
