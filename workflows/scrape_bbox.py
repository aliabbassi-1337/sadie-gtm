#!/usr/bin/env python3
"""
Bounding box scraping workflow.

Scrape hotels in rectangular bounding boxes - simpler than polygons,
works great with sparse-cell skipping for water/empty areas.

Usage:
    # Estimate cost for a box
    uv run python -m workflows.scrape_bbox --estimate --bbox 25.35,-80.50,26.30,-80.05

    # Estimate all Florida metro boxes from GeoJSON file
    uv run python -m workflows.scrape_bbox --estimate --geojson context/florida_metro_boxes.geojson

    # Scrape a specific box
    uv run python -m workflows.scrape_bbox --bbox 25.35,-80.50,26.30,-80.05 --name "Miami Metro"

    # Scrape a box from the GeoJSON file by name
    uv run python -m workflows.scrape_bbox --geojson context/florida_metro_boxes.geojson --name "Tampa Bay"

    # List boxes in a GeoJSON file
    uv run python -m workflows.scrape_bbox --list --geojson context/florida_metro_boxes.geojson
"""

import argparse
import asyncio
import json
import os
import sys
from typing import List, Optional, Tuple

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.client import init_db, close_db
from services.leadgen.service import Service


def parse_bbox(bbox_str: str) -> Tuple[float, float, float, float]:
    """Parse bbox string 'lat_min,lng_min,lat_max,lng_max' into tuple."""
    parts = [float(x.strip()) for x in bbox_str.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be 'lat_min,lng_min,lat_max,lng_max'")
    return tuple(parts)


def load_boxes_from_geojson(path: str) -> List[dict]:
    """Load bounding boxes from a GeoJSON FeatureCollection."""
    with open(path) as f:
        data = json.load(f)

    boxes = []
    features = data.get("features", [])

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})

        if geom.get("type") != "Polygon":
            continue

        # Extract bounds from polygon coordinates
        coords = geom.get("coordinates", [[]])[0]
        if not coords:
            continue

        lngs = [c[0] for c in coords]
        lats = [c[1] for c in coords]

        boxes.append({
            "name": props.get("name", "unnamed"),
            "lat_min": min(lats),
            "lat_max": max(lats),
            "lng_min": min(lngs),
            "lng_max": max(lngs),
        })

    return boxes


async def list_boxes(boxes: List[dict]) -> None:
    """List all boxes from GeoJSON."""
    logger.info("=" * 70)
    logger.info("Bounding Boxes")
    logger.info("=" * 70)
    logger.info(f"{'Name':<30} {'Lat Range':<20} {'Lng Range':<20}")
    logger.info("-" * 70)

    for box in boxes:
        lat_range = f"{box['lat_min']:.2f} - {box['lat_max']:.2f}"
        lng_range = f"{box['lng_min']:.2f} - {box['lng_max']:.2f}"
        logger.info(f"{box['name']:<30} {lat_range:<20} {lng_range:<20}")


async def estimate_boxes(service: Service, boxes: List[dict], cell_size: float) -> None:
    """Estimate cost for all boxes."""
    logger.info("=" * 70)
    logger.info("Scraping Estimates")
    logger.info("=" * 70)

    total_cells = 0
    total_api_calls = 0
    total_cost = 0
    total_hotels = 0

    for box in boxes:
        est = service.estimate_bbox(
            lat_min=box["lat_min"],
            lng_min=box["lng_min"],
            lat_max=box["lat_max"],
            lng_max=box["lng_max"],
            cell_size_km=cell_size,
            name=box["name"],
        )

        logger.info(f"\n{box['name']}:")
        logger.info(f"  Area: {est['area_km2']} km² ({est['dimensions_km']})")
        logger.info(f"  Grid: {est['grid_cells']} cells ({cell_size}km)")
        logger.info(f"  API calls: ~{est['estimated_api_calls']:,}")
        logger.info(f"  Cost: ~${est['estimated_cost_usd']:.2f}")
        logger.info(f"  Hotels: ~{est['estimated_hotels']:,}")

        total_cells += est["total_cells"]
        total_api_calls += est["estimated_api_calls"]
        total_cost += est["estimated_cost_usd"]
        total_hotels += est["estimated_hotels"]

    logger.info("")
    logger.info("=" * 70)
    logger.info("TOTAL ESTIMATE")
    logger.info("=" * 70)
    logger.info(f"  Boxes: {len(boxes)}")
    logger.info(f"  Total cells: {total_cells:,}")
    logger.info(f"  Total API calls: ~{total_api_calls:,}")
    logger.info(f"  Total cost: ~${total_cost:.2f}")
    logger.info(f"  Total hotels: ~{total_hotels:,}")


async def scrape_box(
    service: Service,
    lat_min: float,
    lng_min: float,
    lat_max: float,
    lng_max: float,
    name: str,
    cell_size: float,
    state: str,
    thorough: bool = False,
    max_pages: int = 1,
) -> None:
    """Scrape a single bounding box."""
    # Show estimate first
    est = service.estimate_bbox(
        lat_min=lat_min,
        lng_min=lng_min,
        lat_max=lat_max,
        lng_max=lng_max,
        cell_size_km=cell_size,
        name=name,
    )

    # Adjust estimate for pagination
    if max_pages > 1:
        est_calls_with_pages = est['estimated_api_calls'] * max_pages
        est_cost_with_pages = est['estimated_cost_usd'] * max_pages
    else:
        est_calls_with_pages = est['estimated_api_calls']
        est_cost_with_pages = est['estimated_cost_usd']

    logger.info("=" * 70)
    logger.info(f"Scraping: {name}")
    logger.info("=" * 70)
    logger.info(f"Bounds: ({lat_min}, {lng_min}) to ({lat_max}, {lng_max})")
    logger.info(f"Area: {est['area_km2']} km² ({est['dimensions_km']})")
    logger.info(f"Grid: {est['grid_cells']} cells ({cell_size}km)")
    logger.info(f"Pagination: {max_pages} page(s) per query")
    logger.info(f"Estimated API calls: ~{est_calls_with_pages:,}")
    logger.info(f"Estimated cost: ~${est_cost_with_pages:.2f}")
    logger.info("")

    # Create source name for database
    source_name = f"bbox_{state.lower()}_{name.lower().replace(' ', '_').replace('/', '_')}"

    hotels, stats = await service.scrape_bbox(
        lat_min=lat_min,
        lng_min=lng_min,
        lat_max=lat_max,
        lng_max=lng_max,
        cell_size_km=cell_size,
        save_to_db=True,
        source=source_name,
        thorough=thorough,
        max_pages=max_pages,
    )

    logger.info("")
    logger.info("=" * 70)
    logger.info("Scraping Complete")
    logger.info("=" * 70)
    logger.info(f"Hotels found: {stats['hotels_found']}")
    logger.info(f"Hotels saved: {stats['hotels_saved']}")
    logger.info(f"API calls: {stats['api_calls']}")
    logger.info(f"Cells searched: {stats['cells_searched']}")
    logger.info(f"Cells sparse-skipped: {stats['cells_sparse_skipped']}")
    logger.info(f"Cells duplicate-skipped: {stats['cells_duplicate_skipped']}")
    logger.info(f"Cells subdivided: {stats['cells_subdivided']}")
    logger.info(f"Duplicates skipped: {stats['duplicates_skipped']}")
    logger.info(f"Chains skipped: {stats['chains_skipped']}")
    logger.info(f"Non-lodging skipped: {stats['non_lodging_skipped']}")
    logger.info(f"Out of bounds: {stats['out_of_bounds']}")


async def main():
    parser = argparse.ArgumentParser(
        description="Bounding box hotel scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List boxes in GeoJSON
  uv run python -m workflows.scrape_bbox --list --geojson context/florida_metro_boxes.geojson

  # Estimate all boxes
  uv run python -m workflows.scrape_bbox --estimate --geojson context/florida_metro_boxes.geojson

  # Estimate a single box
  uv run python -m workflows.scrape_bbox --estimate --bbox 25.35,-80.50,26.30,-80.05

  # Scrape a box by coordinates
  uv run python -m workflows.scrape_bbox --bbox 25.35,-80.50,26.30,-80.05 --name "Miami Metro" --state FL

  # Scrape a box from GeoJSON by name
  uv run python -m workflows.scrape_bbox --geojson context/florida_metro_boxes.geojson --name "Tampa Bay" --state FL
        """
    )

    # Input sources
    parser.add_argument("--bbox", type=str, help="Bounding box: lat_min,lng_min,lat_max,lng_max")
    parser.add_argument("--geojson", type=str, help="Path to GeoJSON file with boxes")

    # Actions
    parser.add_argument("--list", action="store_true", help="List boxes in GeoJSON file")
    parser.add_argument("--estimate", action="store_true", help="Estimate cost (don't scrape)")

    # Scraping options
    parser.add_argument("--name", type=str, help="Name of box (for scraping specific box from GeoJSON)")
    parser.add_argument("--state", type=str, default="FL", help="State code for source tracking (default: FL)")
    parser.add_argument("--cell-size", type=float, default=2.0, help="Cell size in km (default: 2.0)")
    parser.add_argument("--thorough", action="store_true", help="Disable skipping for maximum coverage (more API calls)")
    parser.add_argument("--pages", type=int, default=1, help="Pages per query for pagination (1-5, each page ~20 results, default: 1)")

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Validate inputs
    if not args.bbox and not args.geojson:
        parser.error("Either --bbox or --geojson is required")

    # Load boxes
    boxes = []
    if args.geojson:
        boxes = load_boxes_from_geojson(args.geojson)
        if not boxes:
            logger.error(f"No valid boxes found in {args.geojson}")
            sys.exit(1)
    elif args.bbox:
        try:
            lat_min, lng_min, lat_max, lng_max = parse_bbox(args.bbox)
            boxes = [{
                "name": args.name or "custom",
                "lat_min": lat_min,
                "lng_min": lng_min,
                "lat_max": lat_max,
                "lng_max": lng_max,
            }]
        except ValueError as e:
            logger.error(f"Invalid bbox format: {e}")
            sys.exit(1)

    # Filter to specific box if name provided
    if args.name and args.geojson:
        name_lower = args.name.lower()
        boxes = [b for b in boxes if b["name"].lower() == name_lower]
        if not boxes:
            logger.error(f"Box '{args.name}' not found in {args.geojson}")
            sys.exit(1)

    # Handle list action (no API key needed)
    if args.list:
        await list_boxes(boxes)
        return

    # Initialize for estimate/scrape
    await init_db()
    api_key = os.environ.get("SERPER_API_KEY")

    if not api_key and not args.estimate:
        logger.error("SERPER_API_KEY environment variable required for scraping")
        await close_db()
        sys.exit(1)

    service = Service(api_key=api_key or "")

    try:
        if args.estimate:
            await estimate_boxes(service, boxes, args.cell_size)
        else:
            # Scrape mode - process one box at a time
            for box in boxes:
                await scrape_box(
                    service=service,
                    lat_min=box["lat_min"],
                    lng_min=box["lng_min"],
                    lat_max=box["lat_max"],
                    lng_max=box["lng_max"],
                    name=box["name"],
                    cell_size=args.cell_size,
                    state=args.state,
                    thorough=args.thorough,
                    max_pages=args.pages,
                )
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
