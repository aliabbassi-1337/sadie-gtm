"""
Workflow: Scrape State Cities
==============================
Scrapes hotels for all target cities in a state.

USAGE
-----
    # Estimate cost for all Maryland cities
    uv run python -m workflows.scrape_state_cities --state Maryland --estimate

    # Scrape all Maryland cities
    uv run python -m workflows.scrape_state_cities --state Maryland

    # Scrape with custom radius (default 15km)
    uv run python -m workflows.scrape_state_cities --state Maryland --radius-km 20
"""

import sys
import asyncio
import argparse
from typing import List, Tuple

from loguru import logger

from db.client import init_db, close_db, get_conn
from services.leadgen.service import Service


async def get_target_cities(state: str) -> List[Tuple[str, float, float, int]]:
    """Get target cities for a state from database."""
    async with get_conn() as conn:
        rows = await conn.fetch('''
            SELECT name, lat, lng, population
            FROM sadie_gtm.scrape_target_cities
            WHERE state = $1
            ORDER BY population DESC NULLS LAST
        ''', state)
        return [(r['name'], r['lat'], r['lng'], r['population'] or 0) for r in rows]


async def estimate_state(state: str, radius_km: float, cell_size_km: float):
    """Estimate cost for scraping all cities in a state."""
    await init_db()
    
    cities = await get_target_cities(state)
    if not cities:
        logger.error(f"No target cities found for {state}")
        return
    
    service = Service()
    
    total_cost = 0.0
    total_calls = 0
    total_hotels = 0
    
    logger.info(f"Estimating {len(cities)} cities in {state}")
    logger.info("=" * 60)
    
    for name, lat, lng, pop in cities:
        estimate = service.estimate_region(lat, lng, radius_km, cell_size_km)
        total_cost += estimate.estimated_cost_usd
        total_calls += estimate.estimated_api_calls
        total_hotels += estimate.estimated_hotels
        logger.info(f"  {name}: ~{estimate.estimated_hotels} hotels, ${estimate.estimated_cost_usd:.2f}")
    
    logger.info("=" * 60)
    logger.info(f"TOTAL ESTIMATE for {state}:")
    logger.info(f"  Cities: {len(cities)}")
    logger.info(f"  Est. hotels: {total_hotels:,}")
    logger.info(f"  Est. API calls: {total_calls:,}")
    logger.info(f"  Est. cost: ${total_cost:.2f}")
    logger.info(f"  Est. time: ~{total_calls / 4 / 60:.1f} minutes")
    logger.info("=" * 60)
    
    await close_db()


async def scrape_state(state: str, radius_km: float, cell_size_km: float):
    """Scrape all cities in a state."""
    await init_db()
    
    cities = await get_target_cities(state)
    if not cities:
        logger.error(f"No target cities found for {state}")
        return
    
    service = Service()
    
    total_hotels = 0
    
    logger.info(f"Scraping {len(cities)} cities in {state}")
    logger.info("=" * 60)
    
    for i, (name, lat, lng, pop) in enumerate(cities, 1):
        logger.info(f"[{i}/{len(cities)}] Scraping {name} (pop {pop:,})...")
        
        try:
            count = await service.scrape_region(lat, lng, radius_km, cell_size_km)
            total_hotels += count
            logger.success(f"  {name}: {count} hotels")
        except Exception as e:
            logger.error(f"  {name}: failed - {e}")
    
    logger.info("=" * 60)
    logger.info(f"SCRAPE COMPLETE for {state}:")
    logger.info(f"  Cities scraped: {len(cities)}")
    logger.info(f"  Total hotels: {total_hotels:,}")
    logger.info("=" * 60)
    
    await close_db()


def main():
    parser = argparse.ArgumentParser(description="Scrape hotels for all cities in a state")
    
    parser.add_argument("--state", required=True, help="State name (e.g., 'Maryland')")
    parser.add_argument("--radius-km", type=float, default=15, help="Radius around each city in km (default: 15)")
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2)")
    parser.add_argument("--estimate", action="store_true", help="Only show cost estimate, don't scrape")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    if args.debug:
        logger.add(sys.stderr, level="DEBUG", format="<level>{level: <8}</level> | {message}")
    else:
        logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    if args.estimate:
        asyncio.run(estimate_state(args.state, args.radius_km, args.cell_size))
    else:
        asyncio.run(scrape_state(args.state, args.radius_km, args.cell_size))


if __name__ == "__main__":
    main()
