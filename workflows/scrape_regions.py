#!/usr/bin/env python3
"""
Polygon-based region scraping workflow.

Instead of scraping an entire state with uniform cells, this workflow:
1. Uses predefined polygon regions (generated from target cities)
2. Each region has its own optimized cell size
3. Only areas within regions are scraped - no wasted API calls on empty rural areas

Usage:
    # Generate regions from target cities
    python -m workflows.scrape_regions --state FL --generate

    # List configured regions
    python -m workflows.scrape_regions --state FL --list

    # Estimate cost for all regions
    python -m workflows.scrape_regions --state FL --estimate

    # Scrape all regions
    python -m workflows.scrape_regions --state FL

    # Add a custom region
    python -m workflows.scrape_regions --state FL --add "Keys Corridor" --lat 24.7 --lng -81.1 --radius 50

    # Remove a region
    python -m workflows.scrape_regions --state FL --remove "Keys Corridor"

    # Clear all regions
    python -m workflows.scrape_regions --state FL --clear
"""

import argparse
import asyncio
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.client import init_db, close_db
from services.leadgen.service import Service


async def list_regions(service: Service, state: str) -> None:
    """List all regions for a state."""
    regions = await service.get_regions(state)
    
    if not regions:
        print(f"\nNo regions configured for {state}.")
        print(f"Run with --generate to create regions from target cities.")
        return
    
    total_area = await service.get_total_region_area(state)
    
    print(f"\n{'='*60}")
    print(f"Scrape Regions for {state}")
    print(f"{'='*60}")
    print(f"Total regions: {len(regions)}")
    print(f"Total area: {total_area:,.1f} km²")
    print()
    
    print(f"{'Name':<25} {'Type':<10} {'Radius':<10} {'Cell Size':<10} {'Priority':<8}")
    print("-" * 73)
    
    for region in regions:
        radius_str = f"{region.radius_km:.1f} km" if region.radius_km else "custom"
        print(
            f"{region.name:<25} "
            f"{region.region_type:<10} "
            f"{radius_str:<10} "
            f"{region.cell_size_km:.1f} km     "
            f"{region.priority:<8}"
        )


async def generate_regions(service: Service, state: str) -> None:
    """Generate circular regions from target cities."""
    city_count = await service.count_target_cities(state)
    
    if city_count == 0:
        print(f"\nNo target cities configured for {state}.")
        print(f"Add cities first with: python -m workflows.scrape_cities --state {state} --add 'City Name'")
        return
    
    print(f"\nGenerating circular regions from {city_count} target cities...")
    regions = await service.generate_regions_from_cities(state)
    
    print(f"\nCreated {len(regions)} regions:")
    for region in regions:
        print(f"  • {region.name}: {region.radius_km:.1f}km radius, {region.cell_size_km:.1f}km cells")
    
    total_area = await service.get_total_region_area(state)
    print(f"\nTotal coverage: {total_area:,.1f} km²")


async def generate_optimized_regions(service: Service, state: str) -> None:
    """Generate optimized regions using real OSM city boundaries."""
    city_count = await service.count_target_cities(state)
    
    if city_count == 0:
        print(f"\nNo target cities configured for {state}.")
        print(f"Add cities first with: python -m workflows.scrape_cities --state {state} --add 'City Name'")
        return
    
    print(f"\nFetching real city boundaries from OpenStreetMap for {city_count} cities...")
    print("(This takes ~1 second per city due to API rate limits)")
    print()
    
    regions = await service.generate_regions_from_boundaries(state)
    
    boundary_count = sum(1 for r in regions if r.region_type == "boundary")
    circle_count = len(regions) - boundary_count
    
    print(f"\nCreated {len(regions)} regions:")
    print(f"  • {boundary_count} with real OSM boundaries (optimal)")
    print(f"  • {circle_count} with circular fallback")
    print()
    
    for region in regions:
        if region.region_type == "boundary":
            print(f"  ✓ {region.name}: OSM boundary, {region.cell_size_km:.1f}km cells")
        else:
            print(f"  ○ {region.name}: {region.radius_km:.1f}km circle (fallback)")
    
    total_area = await service.get_total_region_area(state)
    print(f"\nTotal coverage: {total_area:,.1f} km²")


async def estimate_regions(service: Service, state: str) -> None:
    """Estimate cost for scraping all regions."""
    estimate = await service.estimate_regions(state)
    
    if estimate["regions"] == 0:
        print(f"\n{estimate['message']}")
        return
    
    print(f"\n{'='*60}")
    print(f"Estimate for {state} Region Scraping")
    print(f"{'='*60}")
    print(f"Regions: {estimate['regions']}")
    print(f"Total area: {estimate['total_area_km2']:,.1f} km²")
    print(f"Total cells: {estimate['total_cells']:,}")
    print(f"API calls: {estimate['total_api_calls_range']}")
    print(f"Cost: {estimate['estimated_cost_usd_range']}")
    print()
    
    print("Region breakdown:")
    for r in estimate["region_breakdown"]:
        print(f"  • {r['name']}: {r['cells']:,} cells ({r['cell_size_km']}km)")


async def scrape_regions(service: Service, state: str) -> None:
    """Scrape all regions for a state."""
    region_count = await service.count_regions(state)
    
    if region_count == 0:
        print(f"\nNo regions configured for {state}.")
        print(f"Run with --generate to create regions from target cities.")
        return
    
    print(f"\nScraping {region_count} regions for {state}...")
    hotels = await service.scrape_regions(state, save_to_db=True)
    
    print(f"\n{'='*60}")
    print(f"Scraping Complete")
    print(f"{'='*60}")
    print(f"Total unique hotels found: {len(hotels)}")


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
    print(f"\nAdded region: {region.name}")
    print(f"  Center: ({lat}, {lng})")
    print(f"  Radius: {radius} km")
    print(f"  Cell size: {cell_size} km")


async def remove_region(service: Service, state: str, name: str) -> None:
    """Remove a region."""
    region = await service.get_region(name, state)
    if not region:
        print(f"\nRegion '{name}' not found in {state}.")
        return
    
    await service.remove_region(name, state)
    print(f"\nRemoved region: {name}")


async def clear_regions(service: Service, state: str) -> None:
    """Clear all regions for a state."""
    count = await service.count_regions(state)
    if count == 0:
        print(f"\nNo regions to clear for {state}.")
        return
    
    await service.clear_regions(state)
    print(f"\nCleared {count} regions for {state}.")


async def add_region_from_geojson(
    service: Service,
    state: str,
    name: str,
    geojson_path: str,
    cell_size: float,
) -> None:
    """Add a region from a GeoJSON file."""
    import json
    
    with open(geojson_path) as f:
        data = json.load(f)
    
    # Handle both Feature and raw Polygon
    if data.get("type") == "Feature":
        geom = data["geometry"]
    elif data.get("type") == "FeatureCollection":
        geom = data["features"][0]["geometry"]
    else:
        geom = data
    
    if geom.get("type") != "Polygon":
        print(f"Error: GeoJSON must be a Polygon, got {geom.get('type')}")
        return
    
    # Calculate center from coordinates
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
    
    # Calculate area
    area_km2 = (max(lats) - min(lats)) * 111.0 * (max(lngs) - min(lngs)) * 111.0 * 0.785  # rough
    
    print(f"\nAdded custom polygon region: {region.name}")
    print(f"  Center: ({center_lat:.4f}, {center_lng:.4f})")
    print(f"  Bounds: lat [{min(lats):.4f}, {max(lats):.4f}], lng [{min(lngs):.4f}, {max(lngs):.4f}]")
    print(f"  Points: {len(coords)}")
    print(f"  Cell size: {cell_size} km")


async def show_geojson(service: Service, state: str, name: str) -> None:
    """Output GeoJSON for a region (can paste into geojson.io for visualization)."""
    region = await service.get_region(name, state)
    if not region:
        print(f"\nRegion '{name}' not found in {state}.")
        return
    
    if not region.polygon_geojson:
        print(f"\nRegion '{name}' has no polygon data.")
        return
    
    import json
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
    
    print(f"\n# GeoJSON for {region.name}")
    print(f"# Paste at https://geojson.io to visualize\n")
    print(json.dumps(feature, indent=2))


async def main():
    parser = argparse.ArgumentParser(
        description="Polygon-based region scraping for targeted hotel discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate regions from cities
  python -m workflows.scrape_regions --state FL --generate

  # Estimate cost
  python -m workflows.scrape_regions --state FL --estimate

  # Scrape all regions
  python -m workflows.scrape_regions --state FL

  # Add custom region (e.g., Keys corridor)
  python -m workflows.scrape_regions --state FL --add "Keys" --lat 24.7 --lng -81.1 --radius 50
        """
    )
    
    parser.add_argument("--state", required=True, help="State code (e.g., FL)")
    
    # Actions
    parser.add_argument("--list", action="store_true", help="List configured regions")
    parser.add_argument("--generate", action="store_true", help="Generate circular regions from target cities")
    parser.add_argument("--generate-optimized", action="store_true", 
                       help="Generate optimized regions using real OSM city boundaries (slower, but efficient)")
    parser.add_argument("--estimate", action="store_true", help="Estimate cost for all regions")
    parser.add_argument("--clear", action="store_true", help="Clear all regions")
    
    # Region management
    parser.add_argument("--add", metavar="NAME", help="Add a custom region")
    parser.add_argument("--remove", metavar="NAME", help="Remove a region")
    parser.add_argument("--lat", type=float, help="Latitude for custom region")
    parser.add_argument("--lng", type=float, help="Longitude for custom region")
    parser.add_argument("--radius", type=float, default=15.0, help="Radius in km (default: 15)")
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2)")
    parser.add_argument("--geojson", type=str, help="Path to GeoJSON file for custom polygon shape")
    parser.add_argument("--show-geojson", metavar="NAME", help="Output GeoJSON for a region (for visualization)")
    
    args = parser.parse_args()
    state = args.state.upper()
    
    # Initialize
    await init_db()
    api_key = os.environ.get("SERPER_API_KEY")
    no_api_key_needed = (
        args.list or args.generate or args.clear or 
        args.add or args.remove or args.show_geojson
    )
    if not api_key and not no_api_key_needed:
        print("Error: SERPER_API_KEY environment variable required for scraping")
        await close_db()
        sys.exit(1)
    
    service = Service(api_key=api_key or "")
    
    try:
        if args.generate:
            await generate_regions(service, state)
        elif args.generate_optimized:
            await generate_optimized_regions(service, state)
        elif args.list:
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
                print("Error: Either --geojson or --lat/--lng required when adding a region")
                sys.exit(1)
        elif args.remove:
            await remove_region(service, state, args.remove)
        else:
            # Default action: scrape
            await scrape_regions(service, state)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
