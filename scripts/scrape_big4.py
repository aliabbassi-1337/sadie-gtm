"""Run the BIG4 scraper and save results to DB.

Usage:
    python scripts/scrape_big4.py                  # Full scrape with contact enrichment
    python scripts/scrape_big4.py --no-contacts     # Algolia only (fast)
    python scripts/scrape_big4.py --test 5          # Test with N parks
"""

import argparse
import asyncio

from loguru import logger

from lib.big4 import Big4Scraper
from services.enrichment import repo


async def main(enrich_contacts: bool = True, test_limit: int = 0):
    async with Big4Scraper(
        concurrency=15,
        delay=0.3,
        enrich_contacts=enrich_contacts,
    ) as scraper:
        parks = await scraper.scrape_all()

    if test_limit:
        parks = parks[:test_limit]
        logger.info(f"Test mode: limited to {test_limit} parks")

    if not parks:
        logger.error("No parks fetched")
        return

    # Print sample
    for p in parks[:5]:
        logger.info(
            f"  {p.name} | {p.city}, {p.state} | "
            f"addr={p.address} | phone={p.phone} | email={p.email}"
        )

    # Stats before DB
    logger.info(f"--- Pre-DB Stats ---")
    logger.info(f"Total: {len(parks)}")
    logger.info(f"With coords: {sum(1 for p in parks if p.has_location())}")
    logger.info(f"With address: {sum(1 for p in parks if p.address)}")
    logger.info(f"With phone: {sum(1 for p in parks if p.phone)}")
    logger.info(f"With email: {sum(1 for p in parks if p.email)}")
    logger.info(f"With postcode: {sum(1 for p in parks if p.postcode)}")

    # Upsert to DB
    logger.info("Upserting to DB...")
    await repo.upsert_big4_parks(
        names=[p.name for p in parks],
        slugs=[p.slug for p in parks],
        phones=[p.phone for p in parks],
        emails=[p.email for p in parks],
        websites=[p.full_url for p in parks],
        addresses=[p.address for p in parks],
        cities=[p.city for p in parks],
        states=[p.state for p in parks],
        postcodes=[p.postcode for p in parks],
        lats=[p.latitude for p in parks],
        lons=[p.longitude for p in parks],
    )

    total = await repo.get_big4_count()
    logger.info(f"Total BIG4 parks in DB: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape BIG4 parks")
    parser.add_argument("--no-contacts", action="store_true", help="Skip contact page enrichment")
    parser.add_argument("--test", type=int, default=0, help="Limit to N parks for testing")
    args = parser.parse_args()

    asyncio.run(main(
        enrich_contacts=not args.no_contacts,
        test_limit=args.test,
    ))
