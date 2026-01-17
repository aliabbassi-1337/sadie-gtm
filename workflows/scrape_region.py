"""
Workflow: Scrape Region
=======================
Scrapes hotels in a circular region using the adaptive grid scraper.

For scraping multiple dense areas (cities, tourist zones), use scrape_polygon.py instead.

USAGE
-----

1. Scrape by coordinates:
    uv run python -m workflows.scrape_region --lat 25.7617 --lng -80.1918 --radius-km 20 --estimate
    uv run python -m workflows.scrape_region --lat 25.7617 --lng -80.1918 --radius-km 20

2. Adjust cell size (smaller = more thorough, more expensive):
    uv run python -m workflows.scrape_region --lat 25.7617 --lng -80.1918 --radius-km 20 --cell-size 1.0

OPTIONS
-------

--lat           Center latitude (required)
--lng           Center longitude (required)
--radius-km     Radius in km (default: 10)
--cell-size     Cell size in km (default: 2). Smaller = more thorough.
--estimate      Show cost estimate without running scrape.
--debug         Enable debug logging.
--no-notify     Disable Slack notification.
"""

import sys
import asyncio
import argparse

from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service, ScrapeEstimate
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


def main():
    parser = argparse.ArgumentParser(description="Scrape hotels in a circular region")

    # Required: center coordinates
    parser.add_argument("--lat", type=float, required=True, help="Center latitude")
    parser.add_argument("--lng", type=float, required=True, help="Center longitude")
    parser.add_argument("--radius-km", type=float, default=10, help="Radius in km (default: 10)")

    # Scraper settings
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2)")

    # Estimate only
    parser.add_argument("--estimate", action="store_true", help="Only show cost estimate, don't scrape")

    # Debug logging
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Slack notification
    parser.add_argument("--no-notify", action="store_true", help="Disable Slack notification")

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    if args.debug:
        logger.add(sys.stderr, level="DEBUG", format="<level>{level: <8}</level> | {message}")
    else:
        logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    service = Service()

    if args.estimate:
        estimate = service.estimate_region(args.lat, args.lng, args.radius_km, args.cell_size)
        print_estimate(estimate, f"({args.lat}, {args.lng}) r={args.radius_km}km cell={args.cell_size}km")
    else:
        region_name = f"({args.lat}, {args.lng})"
        asyncio.run(scrape_region_workflow(args.lat, args.lng, args.radius_km, args.cell_size, region_name, not args.no_notify))


if __name__ == "__main__":
    main()
