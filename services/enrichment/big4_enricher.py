"""BIG4 Australia - scrape and upsert holiday parks."""

from typing import Dict, Any

from loguru import logger

from lib.big4 import Big4Scraper
from services.enrichment import repo


async def scrape_and_upsert(
    concurrency: int = 10,
    delay: float = 0.5,
) -> Dict[str, Any]:
    """Scrape all BIG4 holiday parks and upsert into hotels table."""
    logger.info("Starting BIG4 scrape")

    async with Big4Scraper(concurrency=concurrency, delay=delay) as scraper:
        parks = await scraper.scrape_all()

    if not parks:
        logger.warning("No parks discovered")
        return {"discovered": 0, "total_big4": 0, "with_email": 0, "with_phone": 0, "with_address": 0}

    logger.info(f"Scraped {len(parks)} parks, deduplicating and importing to DB...")

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
    total_big4 = await repo.get_big4_count()

    result = {
        "discovered": len(parks),
        "total_big4": total_big4,
        "with_email": sum(1 for p in parks if p.email),
        "with_phone": sum(1 for p in parks if p.phone),
        "with_address": sum(1 for p in parks if p.address),
    }

    logger.info(
        f"BIG4 scrape complete: {result['discovered']} discovered, "
        f"{result['total_big4']} total in DB, "
        f"{result['with_email']} with email, "
        f"{result['with_phone']} with phone"
    )

    return result
