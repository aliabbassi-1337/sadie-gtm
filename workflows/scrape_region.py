"""
Workflow: Scrape Region
=======================
Scrapes hotels in a given region using the adaptive grid scraper.

USAGE
-----

1. Scrape entire state:
    uv run python workflows/scrape_region.py --state florida --estimate  # estimate first
    uv run python workflows/scrape_region.py --state florida             # run scrape

2. Scrape state with HYBRID mode (recommended - uses small cells near cities, large elsewhere):
    uv run python workflows/scrape_region.py --state florida --hybrid --estimate
    uv run python workflows/scrape_region.py --state florida --hybrid

3. Scrape a built-in city (with radius):
    uv run python workflows/scrape_region.py --city miami_beach --radius-km 10 --estimate
    uv run python workflows/scrape_region.py --city miami_beach --radius-km 10

4. Scrape any custom location (by coordinates):
    uv run python workflows/scrape_region.py --center-lat 25.7617 --center-lng -80.1918 --radius-km 20

5. Debug mode (shows filtered hotels):
    uv run python workflows/scrape_region.py --city miami_beach --radius-km 10 --debug

AVAILABLE REGIONS
-----------------

States: florida, california, texas, new_york, tennessee, north_carolina,
        georgia, arizona, nevada, colorado

Cities: 60+ cities across US (see CITY_COORDINATES in grid_scraper.py)

ADDING NEW REGIONS
------------------

Edit services/leadgen/grid_scraper.py:

    # Add city
    CITY_COORDINATES = {
        "chicago": (41.8781, -87.6298),
        ...
    }

    # Add state (lat_min, lat_max, lng_min, lng_max)
    STATE_BOUNDS = {
        "illinois": (36.970298, 42.508481, -91.513079, -87.019935),
        ...
    }

OPTIONS
-------

--cell-size   Cell size in km (default: 2). Smaller = more thorough but more API calls.
--hybrid      Use variable cell sizes: 2km near cities, 10km elsewhere. Best cost/coverage tradeoff.
--estimate    Show cost estimate without running scrape.
--debug       Enable debug logging (shows skipped hotels).
"""

import sys
import asyncio
import argparse

from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service, ScrapeEstimate, CITY_COORDINATES
from infra import slack


def print_estimate(estimate: ScrapeEstimate, region_name: str):
    """Print a formatted cost estimate."""
    print()
    print("=" * 60)
    print(f"COST ESTIMATE: {region_name}")
    print("=" * 60)
    print(f"Region size:          {estimate.region_size_km2:,.1f} km²")
    print(f"Initial cells:        {estimate.initial_cells:,}")
    print(f"Est. total cells:     {estimate.estimated_cells_after_subdivision:,} (after subdivision)")
    print(f"Avg queries/cell:     {estimate.avg_queries_per_cell:.1f} (adaptive: 2-12)")
    print(f"Est. API calls:       {estimate.estimated_api_calls:,}")
    print(f"Est. cost:            ${estimate.estimated_cost_usd:.2f}")
    print(f"Est. hotels:          {estimate.estimated_hotels:,}")
    print()
    print("Pricing: $1 per 1,000 credits ($50 = 50k credits)")
    print("Rate limit: 4 queries/second")
    print(f"Est. time:            ~{estimate.estimated_api_calls / 4 / 60:.1f} minutes")
    print("=" * 60)
    print()


async def scrape_region_workflow(
    center_lat: float,
    center_lng: float,
    radius_km: float,
    cell_size_km: float,
    region_name: str = "Region",
    notify: bool = True,
) -> int:
    """Scrape hotels in a circular region."""
    await init_db()

    try:
        service = Service()
        count = await service.scrape_region(center_lat, center_lng, radius_km, cell_size_km)
        logger.info(f"Scrape complete: {count} hotels saved to database")

        if notify and count > 0:
            slack.send_message(
                f"*Scrape Complete*\n"
                f"• Region: {region_name}\n"
                f"• Hotels scraped: {count}"
            )

        return count
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        if notify:
            slack.send_error("Region Scrape", str(e))
        raise
    finally:
        await close_db()


async def scrape_state_workflow(state: str, cell_size_km: float, hybrid: bool = False, aggressive: bool = False, notify: bool = True) -> int:
    """Scrape hotels in a state."""
    await init_db()

    try:
        service = Service()
        count = await service.scrape_state(state, cell_size_km, hybrid=hybrid, aggressive=aggressive)
        logger.info(f"Scrape complete: {count} hotels saved to database")

        if aggressive:
            mode = "hybrid-aggressive"
        elif hybrid:
            mode = "hybrid"
        else:
            mode = f"{cell_size_km}km cells"
        if notify and count > 0:
            slack.send_message(
                f"*Scrape Complete*\n"
                f"• State: {state.title()}\n"
                f"• Mode: {mode}\n"
                f"• Hotels scraped: {count}"
            )

        return count
    except Exception as e:
        logger.error(f"State scrape failed: {e}")
        if notify:
            slack.send_error("State Scrape", str(e))
        raise
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Scrape hotels in a region")

    # Region by city name (uses CITY_COORDINATES lookup)
    city_names = list(CITY_COORDINATES.keys())
    parser.add_argument("--city", type=str, choices=city_names, help=f"City name: {', '.join(city_names)}")

    # Region by center + radius
    parser.add_argument("--center-lat", type=float, help="Center latitude")
    parser.add_argument("--center-lng", type=float, help="Center longitude")
    parser.add_argument("--radius-km", type=float, default=10, help="Radius in km (default: 10)")

    # Or by state
    parser.add_argument("--state", type=str, help="State name (e.g., florida)")

    # Scraper settings
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2, smaller=denser)")
    parser.add_argument("--hybrid", action="store_true", help="Use variable cell sizes: 2km near cities, 10km elsewhere (state only)")
    parser.add_argument("--aggressive", action="store_true", help="Aggressive hybrid: 2km within 20km of cities, 15km elsewhere (cheapest)")

    # Estimate only
    parser.add_argument("--estimate", action="store_true", help="Only show cost estimate, don't scrape")

    # Debug logging
    parser.add_argument("--debug", action="store_true", help="Enable debug logging (shows filtered hotels)")

    # Slack notification
    parser.add_argument("--no-notify", action="store_true", help="Disable Slack notification")

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    if args.debug:
        logger.add(sys.stderr, level="DEBUG", format="<level>{level: <8}</level> | {message}")
    else:
        logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Service for estimates and scraping
    service = Service()
    cell_size = args.cell_size

    # Resolve city to coordinates
    if args.city:
        lat, lng = CITY_COORDINATES[args.city]
        if args.estimate:
            estimate = service.estimate_region(lat, lng, args.radius_km, cell_size)
            print_estimate(estimate, f"City: {args.city.replace('_', ' ').title()} r={args.radius_km}km cell={cell_size}km")
        else:
            region_name = args.city.replace('_', ' ').title()
            asyncio.run(scrape_region_workflow(lat, lng, args.radius_km, cell_size, region_name, not args.no_notify))

    elif args.state:
        if args.estimate:
            estimate = service.estimate_state(args.state, cell_size, hybrid=args.hybrid, aggressive=args.aggressive)
            if args.aggressive:
                mode = "hybrid-aggressive"
            elif args.hybrid:
                mode = "hybrid"
            else:
                mode = f"cell={cell_size}km"
            print_estimate(estimate, f"State: {args.state.title()} {mode}")
        else:
            asyncio.run(scrape_state_workflow(args.state, cell_size, hybrid=args.hybrid, aggressive=args.aggressive, notify=not args.no_notify))

    elif args.center_lat and args.center_lng:
        if args.estimate:
            estimate = service.estimate_region(args.center_lat, args.center_lng, args.radius_km, cell_size)
            print_estimate(estimate, f"Region: ({args.center_lat}, {args.center_lng}) r={args.radius_km}km cell={cell_size}km")
        else:
            region_name = f"({args.center_lat}, {args.center_lng})"
            asyncio.run(scrape_region_workflow(args.center_lat, args.center_lng, args.radius_km, cell_size, region_name, not args.no_notify))

    else:
        parser.error("Provide --city, --state, or --center-lat and --center-lng")


if __name__ == "__main__":
    main()
