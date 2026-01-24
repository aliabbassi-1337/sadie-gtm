#!/usr/bin/env python3
"""
Enrich hotel data by coordinates - Find hotel names/websites using lat/lon.

For parcel/assessor data that has coordinates but no hotel names.

Usage:
    # Enrich SF hotels from geojson
    uv run python -m workflows.enrich_by_coords --source sf_hotels

    # Dry run (don't save)
    uv run python -m workflows.enrich_by_coords --source sf_hotels --dry-run
"""

import argparse
import asyncio
import csv
import json
import os
import sys

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.enrichment.website_enricher import WebsiteEnricher


async def enrich_sf_hotels(dry_run: bool = False):
    """Enrich SF hotels from geojson with hotel names via Serper."""
    import boto3

    # Download geojson from S3
    s3 = boto3.client("s3")
    logger.info("Downloading SF hotels geojson from S3...")
    response = s3.get_object(
        Bucket="sadie-gtm",
        Key="hotel-sources/us/california/san_francisco_hotels.geojson",
    )
    geojson = json.loads(response["Body"].read().decode("utf-8"))

    features = geojson.get("features", [])
    logger.info(f"Loaded {len(features)} features from geojson")

    # Filter to hotels with coordinates and room count > 1
    hotels = []
    for f in features:
        coords = f.get("geometry", {}).get("coordinates", [])
        props = f.get("properties", {})

        if len(coords) != 2:
            continue

        rooms = props.get("number_of_rooms", "0")
        try:
            rooms = int(float(rooms)) if rooms else 0
        except (ValueError, TypeError):
            rooms = 0

        if rooms <= 1:
            continue

        hotels.append({
            "lon": coords[0],
            "lat": coords[1],
            "parcel_number": props.get("parcel_number"),
            "address": props.get("property_location"),
            "category": props.get("property_class_code_definition", "hotel"),
            "neighborhood": props.get("analysis_neighborhood"),
            "room_count": rooms,
        })

    logger.info(f"Filtered to {len(hotels)} hotels with room count > 1")

    if dry_run:
        logger.info("Dry run - showing first 5 hotels:")
        for h in hotels[:5]:
            logger.info(f"  {h['parcel_number']}: ({h['lat']}, {h['lon']}) - {h['room_count']} rooms")
        return

    # Enrich via Serper
    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        logger.error("SERPER_API_KEY not set")
        return

    enriched = []
    found = 0
    not_found = 0

    async with WebsiteEnricher(api_key=api_key, max_concurrent=10) as enricher:
        for i, hotel in enumerate(hotels):
            if i > 0 and i % 50 == 0:
                logger.info(f"Progress: {i}/{len(hotels)} ({found} found, {not_found} not found)")

            result = await enricher.find_by_coordinates(
                lat=hotel["lat"],
                lon=hotel["lon"],
                category=hotel["category"].lower() if "motel" in hotel["category"].lower() else "hotel",
            )

            if result and result.get("name"):
                found += 1
                enriched.append({
                    **hotel,
                    "name": result["name"],
                    "website": result.get("website"),
                    "phone": result.get("phone"),
                    "rating": result.get("rating"),
                    "cid": result.get("cid"),
                    "matched": True,
                })
            else:
                not_found += 1
                enriched.append({
                    **hotel,
                    "name": None,
                    "website": None,
                    "phone": None,
                    "rating": None,
                    "cid": None,
                    "matched": False,
                })

            # Rate limit
            await asyncio.sleep(0.1)

    logger.info(f"Enrichment complete: {found} found, {not_found} not found")

    # Save to CSV
    output_file = "/tmp/sf_hotels_enriched.csv"
    with open(output_file, "w", newline="") as f:
        fieldnames = [
            "parcel_number", "name", "address", "neighborhood", "room_count",
            "lat", "lon", "category", "website", "phone", "rating", "cid", "matched",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched)

    logger.info(f"Saved enriched data to {output_file}")

    # Upload to S3
    logger.info("Uploading to S3...")
    s3.upload_file(
        output_file,
        "sadie-gtm",
        "hotel-sources/us/california/san_francisco_hotels_enriched.csv",
    )
    logger.info("Uploaded to s3://sadie-gtm/hotel-sources/us/california/san_francisco_hotels_enriched.csv")

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Enrichment Summary")
    logger.info("=" * 60)
    logger.info(f"Total hotels: {len(hotels)}")
    logger.info(f"Found names: {found} ({100*found/len(hotels):.1f}%)")
    logger.info(f"Not found: {not_found}")

    # Show sample
    logger.info("")
    logger.info("Sample enriched hotels:")
    for h in enriched[:10]:
        if h["matched"]:
            logger.info(f"  {h['name']} - {h['room_count']} rooms")
            if h["website"]:
                logger.info(f"    {h['website']}")


async def main():
    parser = argparse.ArgumentParser(description="Enrich hotel data by coordinates")
    parser.add_argument("--source", "-s", type=str, default="sf_hotels", help="Source to enrich")
    parser.add_argument("--dry-run", action="store_true", help="Don't save results")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    if args.source == "sf_hotels":
        await enrich_sf_hotels(dry_run=args.dry_run)
    else:
        logger.error(f"Unknown source: {args.source}")


if __name__ == "__main__":
    asyncio.run(main())
