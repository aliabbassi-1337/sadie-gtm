"""
Workflow: Scrape Region
=======================
Scrapes hotels in a given region using the adaptive grid scraper.

Usage:
    # Estimate costs first
    uv run python workflows/scrape_region.py --center-lat 25.7907 --center-lng -80.1300 --radius-km 3 --estimate
    uv run python workflows/scrape_region.py --state florida --estimate

    # Run scrape
    uv run python workflows/scrape_region.py --center-lat 25.7907 --center-lng -80.1300 --radius-km 3
    uv run python workflows/scrape_region.py --state florida
"""

import asyncio
import argparse
import logging

from db.client import init_db, close_db
from services.leadgen.service import Service
from services.leadgen.grid_scraper import GridScraper, ScrapeEstimate

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def print_estimate(estimate: ScrapeEstimate, region_name: str):
    """Print a formatted cost estimate."""
    print()
    print("=" * 60)
    print(f"COST ESTIMATE: {region_name}")
    print("=" * 60)
    print(f"Region size:          {estimate.region_size_km2:,.1f} km²")
    print(f"Initial cells:        {estimate.initial_cells:,}")
    print(f"Est. total cells:     {estimate.estimated_cells_after_subdivision:,} (after subdivision)")
    print(f"API calls/cell:       {estimate.api_calls_per_cell}")
    print(f"Est. API calls:       {estimate.estimated_api_calls:,}")
    print(f"Est. cost:            ${estimate.estimated_cost_usd:.2f}")
    print(f"Est. hotels:          {estimate.estimated_hotels:,}")
    print()
    print("Pricing: $1 per 1,000 credits ($50 = 50k credits)")
    print("Rate limit: 50 queries/second")
    print(f"Est. time:            ~{estimate.estimated_api_calls / 50 / 60:.1f} minutes")
    print("=" * 60)
    print()


async def scrape_region_workflow(
    center_lat: float,
    center_lng: float,
    radius_km: float,
) -> int:
    """Scrape hotels in a circular region."""
    await init_db()

    try:
        service = Service()
        count = await service.scrape_region(center_lat, center_lng, radius_km)
        logger.info(f"Scrape complete: {count} hotels saved to database")
        return count
    finally:
        await close_db()


async def scrape_state_workflow(state: str) -> int:
    """Scrape hotels in a state."""
    await init_db()

    try:
        service = Service()
        count = await service.scrape_state(state)
        logger.info(f"Scrape complete: {count} hotels saved to database")
        return count
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Scrape hotels in a region")

    # Region by center + radius
    parser.add_argument("--center-lat", type=float, help="Center latitude")
    parser.add_argument("--center-lng", type=float, help="Center longitude")
    parser.add_argument("--radius-km", type=float, default=5, help="Radius in km (default: 5)")

    # Or by state
    parser.add_argument("--state", type=str, help="State name (e.g., florida)")

    # Estimate only
    parser.add_argument("--estimate", action="store_true", help="Only show cost estimate, don't scrape")

    args = parser.parse_args()

    # Create scraper for estimates (doesn't need API key for estimates)
    try:
        scraper = GridScraper()
    except ValueError:
        # No API key - OK for estimates
        scraper = None

    if args.state:
        if args.estimate:
            if scraper is None:
                # Create scraper without validation for estimate
                import os
                os.environ.setdefault("SERPER_SAMI", "dummy")
                scraper = GridScraper()
            estimate = scraper.estimate_state(args.state)
            print_estimate(estimate, f"State: {args.state.title()}")
        else:
            asyncio.run(scrape_state_workflow(args.state))

    elif args.center_lat and args.center_lng:
        if args.estimate:
            if scraper is None:
                import os
                os.environ.setdefault("SERPER_SAMI", "dummy")
                scraper = GridScraper()
            estimate = scraper.estimate_region(args.center_lat, args.center_lng, args.radius_km)
            print_estimate(estimate, f"Region: ({args.center_lat}, {args.center_lng}) r={args.radius_km}km")
        else:
            asyncio.run(scrape_region_workflow(args.center_lat, args.center_lng, args.radius_km))

    else:
        parser.error("Provide --state OR --center-lat and --center-lng")


if __name__ == "__main__":
    main()
