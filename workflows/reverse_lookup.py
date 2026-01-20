#!/usr/bin/env python3
"""
Reverse Lookup Workflow - Find hotels by their booking engine URLs.

Instead of scraping hotels and then detecting their booking engines,
this approach searches for booking engine URLs directly via Google dorks.
The engine is already known from the search, so we skip detection.

Usage:
    # Search a single location
    uv run python -m workflows.reverse_lookup --location "Palm Beach Florida"

    # Search multiple locations
    uv run python -m workflows.reverse_lookup --location "Miami FL" --location "Orlando FL"

    # Filter to specific engines
    uv run python -m workflows.reverse_lookup --location "Florida" --engine cloudbeds --engine guesty

    # Dry run (show what would be searched)
    uv run python -m workflows.reverse_lookup --location "Tampa FL" --dry-run

    # Save to database
    uv run python -m workflows.reverse_lookup --location "Palm Beach Florida" --save-db
"""

import argparse
import asyncio
import json
import os
import sys

from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.leadgen.service import Service


def list_engines():
    """List all supported booking engines."""
    service = Service()
    dorks = service.get_reverse_lookup_dorks()
    engines = set()
    for engine, _, _ in dorks:
        engines.add(engine)
    return sorted(engines)


async def main():
    parser = argparse.ArgumentParser(
        description="Find hotels by their booking engine URLs (reverse lookup)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Search Palm Beach Florida for all engines
    uv run python -m workflows.reverse_lookup --location "Palm Beach Florida"

    # Search multiple locations
    uv run python -m workflows.reverse_lookup -l "Miami FL" -l "Orlando FL" -l "Tampa FL"

    # Only search for Cloudbeds and Guesty hotels
    uv run python -m workflows.reverse_lookup -l "Florida" -e cloudbeds -e guesty

    # Save to database
    uv run python -m workflows.reverse_lookup -l "Palm Beach Florida" --save-db

Supported engines: """ + ", ".join(list_engines())
    )

    parser.add_argument(
        "-l", "--location",
        action="append",
        help="Location to search (can be specified multiple times)",
    )
    parser.add_argument(
        "-e", "--engine",
        action="append",
        help="Filter to specific engine(s) (can be specified multiple times)",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Max results per dork query (default: 100)",
    )
    parser.add_argument(
        "--save-db",
        action="store_true",
        help="Save results to database",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show dorks that would be run without executing",
    )
    parser.add_argument(
        "--list-engines",
        action="store_true",
        help="List all supported booking engines and exit",
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    # Handle --list-engines
    if args.list_engines:
        logger.info("Supported booking engines:")
        for engine in list_engines():
            logger.info(f"  - {engine}")
        return

    # Require --location for all other operations
    if not args.location:
        parser.error("the following arguments are required: -l/--location")

    locations = args.location
    engines = args.engine

    # Initialize service (handles API key internally)
    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key and not args.dry_run:
        logger.error("SERPER_API_KEY environment variable not set")
        sys.exit(1)

    service = Service(api_key=api_key)

    # Dry run - show what would be searched
    if args.dry_run:
        dorks_to_run = service.get_reverse_lookup_dorks(engines)
        logger.info(f"Locations: {locations}")
        logger.info(f"Engines: {engines or 'all'}")
        logger.info(f"Dorks ({len(dorks_to_run)}):")
        for engine, dork_template, _ in dorks_to_run:
            for loc in locations:
                dork = dork_template.format(location=loc)
                logger.info(f"  [{engine}] {dork}")
        logger.info(f"Total API calls: {len(dorks_to_run) * len(locations)}")
        logger.info(f"Estimated cost: ${len(dorks_to_run) * len(locations) * 0.001:.3f}")
        return

    logger.info(f"Starting reverse lookup for {len(locations)} location(s)")
    if engines:
        logger.info(f"Filtering to engines: {engines}")

    # Run search
    results, stats = await service.reverse_lookup(
        locations=locations,
        engines=engines,
        max_results_per_dork=args.max_results,
    )

    # Output summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Reverse Lookup Results")
    logger.info("=" * 60)
    logger.info(f"Dorks run: {stats.dorks_run}")
    logger.info(f"API calls: {stats.api_calls}")
    logger.info(f"Cost: ${stats.api_calls * 0.001:.3f}")
    logger.info(f"Raw results: {stats.results_found}")
    logger.info(f"Unique results: {stats.unique_results}")
    logger.info("")
    logger.info("By engine:")
    for engine, count in sorted(stats.by_engine.items(), key=lambda x: -x[1]):
        logger.info(f"  {engine}: {count}")

    # Save to JSON file
    if args.output:
        output_data = [r.model_dump() for r in results]
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Saved {len(results)} results to {args.output}")

    # Save to database
    if args.save_db:
        logger.info("Saving to database...")
        source = "reverse_lookup"
        if locations:
            loc_slug = locations[0].lower().replace(" ", "_").replace(",", "")[:30]
            source = f"reverse_lookup_{loc_slug}"

        # Convert results to dicts for service
        results_dicts = [r.model_dump() for r in results]
        db_stats = await service.save_reverse_lookup_results(results_dicts, source=source)

        logger.info(f"Database results:")
        logger.info(f"  Hotels inserted: {db_stats['inserted']}")
        logger.info(f"  Engines linked: {db_stats['engines_linked']}")
        logger.info(f"  Skipped (no website): {db_stats['skipped_no_website']}")
        logger.info(f"  Errors: {db_stats['errors']}")

    # Show sample results
    if results and not args.output:
        logger.info("")
        logger.info("Sample results (first 5):")
        for r in results[:5]:
            logger.info(f"  {r.name} [{r.booking_engine}]")
            logger.info(f"    URL: {r.booking_url}")


if __name__ == "__main__":
    asyncio.run(main())
