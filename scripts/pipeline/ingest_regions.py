#!/usr/bin/env python3
"""
Ingest region boundaries from OpenStreetMap.

Fetches real city boundary polygons and stores them in scrape_regions table.
Run once per state to set up optimized scraping masks.

Usage:
    uv run python -m scripts.pipeline.ingest_regions --state FL
    uv run python -m scripts.pipeline.ingest_regions --state FL --dry-run
"""

import argparse
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from loguru import logger
from db.client import init_db, close_db
from services.leadgen.service import Service
from services.leadgen.geocoding import fetch_city_boundary


async def ingest_regions(state: str, dry_run: bool = False) -> None:
    """Ingest OSM boundaries for all target cities in a state."""
    await init_db()
    service = Service(api_key="")  # No API key needed for ingestion
    
    cities = await service.get_target_cities(state)
    if not cities:
        print(f"No target cities configured for {state}.")
        print(f"Add cities first: uv run python -m workflows.scrape_cities --state {state} --add 'City Name'")
        await close_db()
        return
    
    print(f"\nIngesting boundaries for {len(cities)} cities in {state}")
    print("=" * 50)
    
    if not dry_run:
        await service.clear_regions(state)
    
    boundary_count = 0
    fallback_count = 0
    
    for i, city in enumerate(cities, 1):
        # Rate limit: Nominatim allows 1 req/sec
        await asyncio.sleep(1.1)
        
        print(f"[{i}/{len(cities)}] {city.name}...", end=" ", flush=True)
        
        boundary = await fetch_city_boundary(city.name, state)
        
        radius = city.radius_km or 12.0
        cell_size = 2.0 if radius >= 20 else 1.5 if radius >= 12 else 1.0
        
        if boundary:
            if not dry_run:
                await service.add_region_geojson(
                    name=city.name,
                    state=state,
                    polygon_geojson=boundary.polygon_geojson,
                    center_lat=boundary.lat,
                    center_lng=boundary.lng,
                    region_type="boundary",
                    cell_size_km=cell_size,
                    priority=1 if radius >= 20 else 0,
                )
            print(f"✓ OSM boundary")
            boundary_count += 1
        else:
            if not dry_run:
                await service.add_region(
                    name=city.name,
                    state=state,
                    center_lat=city.lat,
                    center_lng=city.lng,
                    radius_km=radius,
                    region_type="city",
                    cell_size_km=cell_size,
                    priority=1 if radius >= 20 else 0,
                )
            print(f"○ {radius}km circle (fallback)")
            fallback_count += 1
    
    print()
    print("=" * 50)
    print(f"Done! {boundary_count} OSM boundaries, {fallback_count} circle fallbacks")
    
    if not dry_run:
        total_area = await service.get_total_region_area(state)
        print(f"Total coverage: {total_area:,.1f} km²")
    
    await close_db()


async def main():
    parser = argparse.ArgumentParser(description="Ingest region boundaries from OpenStreetMap")
    parser.add_argument("--state", required=True, help="State code (e.g., FL)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    
    args = parser.parse_args()
    await ingest_regions(args.state.upper(), args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
