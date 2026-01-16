"""
Workflow: Scrape Cities
=======================
Scrapes hotels for top cities in a state using target cities from database.

USAGE
-----

1. List target cities for a state:
    uv run python workflows/scrape_cities.py --state FL --list

2. Cost estimate:
    uv run python workflows/scrape_cities.py --state FL --estimate

3. Run scrape:
    uv run python workflows/scrape_cities.py --state FL

4. Add a new target city:
    uv run python workflows/scrape_cities.py --state FL --add "Naples"

SETUP
-----
Target cities must be configured in the database first.
Use --add to add cities, or run the seed migration.
"""

import sys
import asyncio
import argparse

from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service, CityLocation
from infra import slack


async def scrape_cities_workflow(
    state: str,
    cell_size_km: float = 2.0,
    limit: int = 100,
    notify: bool = True,
) -> int:
    """Scrape target cities in a state."""
    await init_db()
    
    service = Service()
    cities = await service.get_target_cities(state, limit=limit)
    
    if not cities:
        logger.error(f"No target cities found for {state}. Add cities with --add")
        await close_db()
        return 0
    
    logger.info(f"Scraping {len(cities)} cities in {state}")
    total_hotels = 0
    
    try:
        for city in cities:
            radius = city.radius_km
            logger.info(f"Scraping {city.name} (r={radius}km, cell={cell_size_km}km)")
            count = await service.scrape_region(city.lat, city.lng, radius, cell_size_km)
            total_hotels += count
            logger.info(f"  → {count} hotels")
        
        logger.info(f"Complete: {total_hotels} hotels from {len(cities)} cities")
        
        if notify and total_hotels > 0:
            slack.send_message(
                f"*City Scrape Complete*\n"
                f"• State: {state}\n"
                f"• Cities: {len(cities)}\n"
                f"• Hotels: {total_hotels}"
            )
        
        return total_hotels
    
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        if notify:
            slack.send_error("City Scrape", str(e))
        raise
    finally:
        await close_db()


async def print_estimate(state: str, cell_size_km: float, limit: int):
    """Print cost estimate for cities in a state."""
    await init_db()
    
    service = Service()
    cities = await service.get_target_cities(state, limit=limit)
    
    if not cities:
        logger.error(f"No target cities found for {state}. Add cities with --add")
        await close_db()
        return
    
    total_cost = 0.0
    total_hotels = 0
    total_calls = 0
    
    print()
    print("=" * 70)
    print(f"CITY SCRAPE ESTIMATE: {state} ({len(cities)} cities, {cell_size_km}km cells)")
    print("=" * 70)
    print(f"{'City':<20} {'Radius':>8} {'Cost':>10} {'Hotels':>12}")
    print("-" * 70)
    
    for city in cities:
        radius = city.radius_km
        estimate = service.estimate_region(city.lat, city.lng, radius, cell_size_km)
        total_cost += estimate.estimated_cost_usd
        total_hotels += estimate.estimated_hotels
        total_calls += estimate.estimated_api_calls
        print(f"{city.name:<20} {radius:>6.0f}km ${estimate.estimated_cost_usd:>8.2f} {estimate.estimated_hotels:>10,}")
    
    print("-" * 70)
    print(f"{'TOTAL':<20} {'':<8} ${total_cost:>8.2f} {total_hotels:>10,}")
    print()
    print(f"API calls:  {total_calls:,}")
    print(f"Est. time:  ~{total_calls / 4 / 60:.1f} minutes")
    print("=" * 70)
    print()
    
    await close_db()


async def list_cities(state: str, limit: int):
    """List target cities for a state."""
    await init_db()
    
    service = Service()
    cities = await service.get_target_cities(state, limit=limit)
    
    if not cities:
        print(f"\nNo target cities found for {state}.")
        print(f"Add cities with: uv run python workflows/scrape_cities.py --state {state} --add \"Miami\"")
        await close_db()
        return
    
    print(f"\nTarget cities for {state} ({len(cities)} cities):\n")
    for city in cities:
        short_display = city.display_name[:55] + "..." if city.display_name and len(city.display_name) > 55 else city.display_name
        print(f"• {city.name:<18} ({city.lat:.4f}, {city.lng:.4f}) r={city.radius_km:.0f}km")
        if city.display_name:
            print(f"  └─ {short_display}")
    print()
    
    await close_db()


async def add_city(name: str, state: str, radius_km: float = None):
    """Add a target city."""
    await init_db()
    
    service = Service()
    city = await service.add_target_city(name, state, radius_km=radius_km)
    
    print(f"\nAdded target city:")
    print(f"• {city.name}, {city.state}")
    print(f"• Coordinates: ({city.lat:.4f}, {city.lng:.4f})")
    print(f"• Radius: {city.radius_km:.0f}km")
    if city.display_name:
        print(f"• Verified: {city.display_name}")
    print()
    
    await close_db()


def main():
    parser = argparse.ArgumentParser(description="Scrape hotels for target cities in a state")
    
    parser.add_argument("--state", type=str, required=True, help="State code (e.g., FL)")
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2)")
    parser.add_argument("--limit", type=int, default=100, help="Max cities to process (default: 100)")
    parser.add_argument("--estimate", action="store_true", help="Show cost estimate only")
    parser.add_argument("--list", action="store_true", help="List target cities")
    parser.add_argument("--add", type=str, metavar="CITY", help="Add a target city")
    parser.add_argument("--radius", type=float, help="Radius for --add (optional)")
    parser.add_argument("--no-notify", action="store_true", help="Disable Slack notification")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    level = "DEBUG" if args.debug else "INFO"
    logger.add(sys.stderr, level=level, format="<level>{level: <8}</level> | {message}")
    
    if args.add:
        asyncio.run(add_city(args.add, args.state, args.radius))
        return
    
    if args.list:
        asyncio.run(list_cities(args.state, args.limit))
        return
    
    if args.estimate:
        asyncio.run(print_estimate(args.state, args.cell_size, args.limit))
        return
    
    asyncio.run(scrape_cities_workflow(args.state, args.cell_size, args.limit, not args.no_notify))


if __name__ == "__main__":
    main()
