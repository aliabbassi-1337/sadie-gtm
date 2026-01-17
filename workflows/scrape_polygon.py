#!/usr/bin/env python3
"""
Polygon-based region scraping workflow.

Scrapes hotels in pre-defined polygon regions (dense areas like cities).
Regions must be ingested first using ingest_regions.py.

Usage:
    # List configured regions
    uv run python -m workflows.scrape_polygon --state FL --list

    # Estimate cost for all regions
    uv run python -m workflows.scrape_polygon --state FL --estimate

    # Scrape all regions
    uv run python -m workflows.scrape_polygon --state FL

    # Show GeoJSON for a region (paste at geojson.io)
    uv run python -m workflows.scrape_polygon --state FL --show-geojson "Miami Beach"

    # Add a custom circular region
    uv run python -m workflows.scrape_polygon --state FL --add "Keys" --lat 24.7 --lng -81.1 --radius 50

    # Add a custom polygon from GeoJSON file
    uv run python -m workflows.scrape_polygon --state FL --add "Custom Zone" --geojson zone.geojson

    # Remove a region
    uv run python -m workflows.scrape_polygon --state FL --remove "Keys"

    # Clear all regions
    uv run python -m workflows.scrape_polygon --state FL --clear
"""

import argparse
import asyncio
import json
import os
import sys
from typing import List, Optional

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.client import init_db, close_db
from services.leadgen.service import Service
from infra import slack


async def list_regions(service: Service, state: str) -> None:
    """List all regions for a state."""
    regions = await service.get_regions(state)
    
    if not regions:
        logger.warning(f"No regions configured for {state}.")
        logger.info("Ingest regions first: uv run python -m workflows.ingest_regions --state {state}")
        return
    
    total_area = await service.get_total_region_area(state)
    
    logger.info("=" * 60)
    logger.info(f"Scrape Regions for {state}")
    logger.info("=" * 60)
    logger.info(f"Total regions: {len(regions)}")
    logger.info(f"Total area: {total_area:,.1f} km²")
    logger.info("")
    logger.info(f"{'Name':<25} {'Type':<10} {'Radius':<10} {'Cell Size':<10} {'Priority':<8}")
    logger.info("-" * 73)
    
    for region in regions:
        radius_str = f"{region.radius_km:.1f} km" if region.radius_km else "polygon"
        logger.info(
            f"{region.name:<25} "
            f"{region.region_type:<10} "
            f"{radius_str:<10} "
            f"{region.cell_size_km:.1f} km     "
            f"{region.priority:<8}"
        )


async def estimate_regions(service: Service, state: str) -> None:
    """Estimate cost for scraping all regions."""
    estimate = await service.estimate_regions(state)
    
    if estimate["regions"] == 0:
        logger.warning(estimate['message'])
        return
    
    logger.info("=" * 60)
    logger.info(f"Estimate for {state} Region Scraping")
    logger.info("=" * 60)
    logger.info(f"Regions: {estimate['regions']}")
    logger.info(f"Total area: {estimate['total_area_km2']:,.1f} km²")
    logger.info(f"Total cells: {estimate['total_cells']:,}")
    logger.info(f"API calls: {estimate['total_api_calls_range']}")
    logger.info(f"Cost: {estimate['estimated_cost_usd_range']}")
    logger.info("")
    logger.info("Region breakdown:")
    for r in estimate["region_breakdown"]:
        logger.info(f"  • {r['name']}: {r['cells']:,} cells ({r['cell_size_km']}km)")


async def scrape_regions(service: Service, state: str, region_names: Optional[List[str]] = None, notify: bool = True) -> None:
    """Scrape regions for a state."""
    if region_names:
        regions_desc = ", ".join(region_names)
        logger.info(f"Scraping {len(region_names)} specific regions: {regions_desc}")
    else:
        region_count = await service.count_regions(state)
        if region_count == 0:
            logger.warning(f"No regions configured for {state}.")
            logger.info(f"Ingest regions first: uv run python -m workflows.ingest_regions --state {state}")
            return
        regions_desc = f"all {region_count} regions"
        logger.info(f"Scraping {regions_desc} for {state}...")
    
    # Notify start
    if notify:
        slack.send_message(f"🔍 *Scraping Started*\n• State: {state}\n• Regions: {regions_desc}")
    
    try:
        hotels = await service.scrape_regions(state, save_to_db=True, region_names=region_names)
        
        logger.info("=" * 60)
        logger.info("Scraping Complete")
        logger.info("=" * 60)
        logger.info(f"Total unique hotels found: {len(hotels)}")
        
        # Notify completion
        if notify:
            slack.send_message(
                f"✅ *Scraping Complete*\n"
                f"• State: {state}\n"
                f"• Regions: {regions_desc}\n"
                f"• Hotels found: {len(hotels)}"
            )
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        if notify:
            slack.send_error(f"Scraping {state}", str(e))
        raise


async def add_region(
    service: Service,
    state: str,
    name: str,
    lat: float,
    lng: float,
    radius: float,
    cell_size: float,
) -> None:
    """Add a custom region."""
    region = await service.add_region(
        name=name,
        state=state,
        center_lat=lat,
        center_lng=lng,
        radius_km=radius,
        region_type="custom",
        cell_size_km=cell_size,
    )
    logger.info(f"Added region: {region.name}")
    logger.info(f"  Center: ({lat}, {lng})")
    logger.info(f"  Radius: {radius} km")
    logger.info(f"  Cell size: {cell_size} km")


async def remove_region(service: Service, state: str, name: str) -> None:
    """Remove a region."""
    region = await service.get_region(name, state)
    if not region:
        logger.warning(f"Region '{name}' not found in {state}.")
        return
    
    await service.remove_region(name, state)
    logger.info(f"Removed region: {name}")


async def clear_regions(service: Service, state: str) -> None:
    """Clear all regions for a state."""
    count = await service.count_regions(state)
    if count == 0:
        logger.warning(f"No regions to clear for {state}.")
        return
    
    await service.clear_regions(state)
    logger.info(f"Cleared {count} regions for {state}.")


async def add_region_from_geojson(
    service: Service,
    state: str,
    name: str,
    geojson_path: str,
    cell_size: float,
) -> None:
    """Add a region from a GeoJSON file."""
    with open(geojson_path) as f:
        data = json.load(f)
    
    # Handle Feature, FeatureCollection, or raw geometry
    if data.get("type") == "Feature":
        geom = data["geometry"]
    elif data.get("type") == "FeatureCollection":
        geom = data["features"][0]["geometry"]
    else:
        geom = data
    
    if geom.get("type") not in ("Polygon", "MultiPolygon"):
        logger.error(f"GeoJSON must be a Polygon or MultiPolygon, got {geom.get('type')}")
        return
    
    # Calculate center from coordinates
    if geom.get("type") == "MultiPolygon":
        # For MultiPolygon, use first polygon's exterior ring
        coords = geom["coordinates"][0][0]
    else:
        coords = geom["coordinates"][0]
    lngs = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    center_lat = sum(lats) / len(lats)
    center_lng = sum(lngs) / len(lngs)
    
    region = await service.add_region_geojson(
        name=name,
        state=state,
        polygon_geojson=json.dumps(geom),
        center_lat=center_lat,
        center_lng=center_lng,
        region_type="custom",
        cell_size_km=cell_size,
    )
    
    logger.info(f"Added custom polygon region: {region.name}")
    logger.info(f"  Center: ({center_lat:.4f}, {center_lng:.4f})")
    logger.info(f"  Bounds: lat [{min(lats):.4f}, {max(lats):.4f}], lng [{min(lngs):.4f}, {max(lngs):.4f}]")
    logger.info(f"  Points: {len(coords)}")
    logger.info(f"  Cell size: {cell_size} km")


async def show_geojson(service: Service, state: str, name: str) -> None:
    """Output GeoJSON for a region (can paste into geojson.io for visualization)."""
    region = await service.get_region(name, state)
    if not region:
        logger.warning(f"Region '{name}' not found in {state}.")
        return
    
    if not region.polygon_geojson:
        logger.warning(f"Region '{name}' has no polygon data.")
        return
    
    geom = json.loads(region.polygon_geojson)
    
    # Wrap in Feature for geojson.io compatibility
    feature = {
        "type": "Feature",
        "properties": {
            "name": region.name,
            "state": region.state,
            "cell_size_km": region.cell_size_km,
            "radius_km": region.radius_km,
        },
        "geometry": geom
    }
    
    logger.info(f"GeoJSON for {region.name}")
    logger.info("Paste at https://geojson.io to visualize")
    logger.info(json.dumps(feature, indent=2))


async def main():
    parser = argparse.ArgumentParser(
        description="Polygon-based region scraping for targeted hotel discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List regions
  uv run python -m workflows.scrape_polygon --state FL --list

  # Estimate cost
  uv run python -m workflows.scrape_polygon --state FL --estimate

  # Scrape all regions
  uv run python -m workflows.scrape_polygon --state FL

  # Add custom circular region
  uv run python -m workflows.scrape_polygon --state FL --add "Keys" --lat 24.7 --lng -81.1 --radius 50
        """
    )
    
    parser.add_argument("--state", required=True, help="State code (e.g., FL)")
    
    # Actions
    parser.add_argument("--list", action="store_true", help="List configured regions")
    parser.add_argument("--estimate", action="store_true", help="Estimate cost for all regions")
    parser.add_argument("--clear", action="store_true", help="Clear all regions")
    parser.add_argument("--only", nargs="+", metavar="REGION", help="Only scrape these specific regions")
    
    # Region management
    parser.add_argument("--add", metavar="NAME", help="Add a custom region")
    parser.add_argument("--remove", metavar="NAME", help="Remove a region")
    parser.add_argument("--lat", type=float, help="Latitude for custom region")
    parser.add_argument("--lng", type=float, help="Longitude for custom region")
    parser.add_argument("--radius", type=float, default=15.0, help="Radius in km (default: 15)")
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2)")
    parser.add_argument("--geojson", type=str, help="Path to GeoJSON file for custom polygon shape")
    parser.add_argument("--show-geojson", metavar="NAME", help="Output GeoJSON for a region (for visualization)")
    parser.add_argument("--no-notify", action="store_true", help="Disable Slack notifications")
    
    args = parser.parse_args()
    state = args.state.upper()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    # Initialize
    await init_db()
    api_key = os.environ.get("SERPER_API_KEY")
    no_api_key_needed = (
        args.list or args.clear or args.add or 
        args.remove or args.show_geojson
    )
    if not api_key and not no_api_key_needed:
        logger.error("SERPER_API_KEY environment variable required for scraping")
        await close_db()
        sys.exit(1)
    
    service = Service(api_key=api_key or "")
    
    try:
        if args.list:
            await list_regions(service, state)
        elif args.estimate:
            await estimate_regions(service, state)
        elif args.clear:
            await clear_regions(service, state)
        elif args.show_geojson:
            await show_geojson(service, state, args.show_geojson)
        elif args.add:
            if args.geojson:
                # Add from GeoJSON file
                await add_region_from_geojson(service, state, args.add, args.geojson, args.cell_size)
            elif args.lat is not None and args.lng is not None:
                # Add circular region
                await add_region(service, state, args.add, args.lat, args.lng, args.radius, args.cell_size)
            else:
                logger.error("Either --geojson or --lat/--lng required when adding a region")
                sys.exit(1)
        elif args.remove:
            await remove_region(service, state, args.remove)
        else:
            # Default action: scrape (optionally filtered)
            await scrape_regions(service, state, region_names=args.only, notify=not args.no_notify)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
