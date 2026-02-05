#!/usr/bin/env python3
"""
Workflow: Export Leads
======================
Exports hotel leads to Excel and uploads to S3.

Supports two export modes:
1. By booking engine: cloudbeds_leads.xlsx, rms_leads.xlsx
2. By country/state: USA/California/California.xlsx, Australia/Victoria/Victoria.xlsx

Usage:
    # Export Cloudbeds leads (engine-based)
    uv run python -m workflows.export --engine cloudbeds

    # Export RMS leads
    uv run python -m workflows.export --engine rms

    # Export all engines (cloudbeds, rms, siteminder, mews)
    uv run python -m workflows.export --engines-all

    # Export a specific state
    uv run python -m workflows.export --state California

    # Export all US states
    uv run python -m workflows.export --country USA

    # Export all Australian states
    uv run python -m workflows.export --country Australia

    # Export everything (all engines + all countries)
    uv run python -m workflows.export --all

    # Dry run (show what would be exported)
    uv run python -m workflows.export --all --dry-run
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

from db.client import init_db, close_db
from services.reporting.service import Service

BOOKING_ENGINES = ["cloudbeds", "rms", "siteminder", "mews"]
# Map display name to DB value
COUNTRIES = {
    "USA": "United States",
    "Australia": "Australia",
}

# Limit concurrent exports to avoid overwhelming S3/DB
MAX_CONCURRENT_EXPORTS = 10


async def export_by_engine(service: Service, engine: str, dry_run: bool = False) -> tuple[str, int]:
    """Export leads for a specific booking engine."""
    # Use '%' as wildcard to match all sources
    source_pattern = "%"
    
    if dry_run:
        # Just count
        from services.reporting import repo
        engine_name = engine.title()
        if engine.lower() == "rms":
            engine_name = "RMS Cloud"
        elif engine.lower() == "siteminder":
            engine_name = "SiteMinder"
        leads = await repo.get_leads_by_booking_engine(engine_name, source_pattern=source_pattern)
        logger.info(f"[DRY RUN] {engine}: {len(leads)} leads")
        return "", len(leads)
    
    return await service.export_by_booking_engine(engine, source_pattern=source_pattern)


async def export_by_state(service: Service, state: str, country: str = "USA", dry_run: bool = False) -> tuple[str, int]:
    """Export leads for a specific state."""
    if dry_run:
        from services.reporting import repo
        leads = await repo.get_leads_for_state(state, source_pattern=None)
        logger.info(f"[DRY RUN] {country}/{state}: {len(leads)} leads")
        return "", len(leads)
    
    return await service.export_state(state, country, source_pattern=None)


async def export_all_engines(service: Service, dry_run: bool = False) -> dict:
    """Export all booking engines concurrently."""
    results = {"engines": 0, "total_leads": 0, "exports": []}
    
    async def export_one(engine: str) -> tuple[str, str, int]:
        try:
            s3_uri, count = await export_by_engine(service, engine, dry_run)
            return engine, s3_uri, count
        except Exception as e:
            logger.error(f"  {engine}: failed - {e}")
            return engine, "", 0
    
    # Run all engine exports concurrently
    tasks = [export_one(engine) for engine in BOOKING_ENGINES]
    export_results = await asyncio.gather(*tasks)
    
    for engine, s3_uri, count in export_results:
        if count > 0:
            results["exports"].append((engine, s3_uri, count))
            results["engines"] += 1
            results["total_leads"] += count
            if not dry_run:
                logger.success(f"  {engine}: {count} leads -> {s3_uri}")
    
    return results


async def export_country(service: Service, country: str, dry_run: bool = False) -> dict:
    """Export all states for a country concurrently."""
    from services.reporting import repo
    
    # Map display name to DB value
    db_country = COUNTRIES.get(country, country)
    
    # Get states for this country
    states = await repo.get_distinct_states_for_country(db_country)
    
    if not states:
        logger.warning(f"No states found for {country}")
        return {"states": 0, "total_leads": 0, "exports": []}
    
    logger.info(f"Found {len(states)} states for {country}")
    
    results = {"states": 0, "total_leads": 0, "exports": []}
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXPORTS)
    
    async def export_one(state: str) -> tuple[str, str, int]:
        async with semaphore:
            try:
                s3_uri, count = await export_by_state(service, state, country, dry_run)
                return state, s3_uri, count
            except Exception as e:
                logger.error(f"  {country}/{state}: failed - {e}")
                return state, "", 0
    
    # Run all state exports concurrently (with semaphore limit)
    valid_states = [s for s in states if s]
    tasks = [export_one(state) for state in valid_states]
    export_results = await asyncio.gather(*tasks)
    
    for state, s3_uri, count in export_results:
        if count > 0:
            results["exports"].append((state, s3_uri, count))
            results["states"] += 1
            results["total_leads"] += count
            if not dry_run:
                logger.success(f"  {country}/{state}: {count} leads")
    
    return results


async def export_all(dry_run: bool = False) -> dict:
    """Export everything: all engines + all countries concurrently."""
    service = Service()
    
    all_results = {
        "engines": {"count": 0, "leads": 0},
        "countries": {},
        "total_leads": 0,
    }
    
    logger.info("Exporting all engines and countries concurrently...")
    
    # Create tasks for engines and all countries
    async def do_engines():
        return await export_all_engines(service, dry_run)
    
    async def do_country(country: str):
        return country, await export_country(service, country, dry_run)
    
    # Run everything concurrently
    tasks = [do_engines()] + [do_country(c) for c in COUNTRIES.keys()]
    results = await asyncio.gather(*tasks)
    
    # First result is engines
    engine_results = results[0]
    all_results["engines"]["count"] = engine_results["engines"]
    all_results["engines"]["leads"] = engine_results["total_leads"]
    all_results["total_leads"] += engine_results["total_leads"]
    
    # Rest are countries
    for country, country_results in results[1:]:
        all_results["countries"][country] = {
            "states": country_results["states"],
            "leads": country_results["total_leads"],
        }
        all_results["total_leads"] += country_results["total_leads"]
    
    return all_results


def print_summary(results: dict):
    """Print export summary."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("EXPORT SUMMARY")
    logger.info("=" * 50)
    
    if "engines" in results and results["engines"]["count"] > 0:
        logger.info(f"Engines: {results['engines']['count']} ({results['engines']['leads']} leads)")
    
    for country, data in results.get("countries", {}).items():
        if data["states"] > 0:
            logger.info(f"{country}: {data['states']} states ({data['leads']} leads)")
    
    logger.info(f"Total leads exported: {results['total_leads']}")


async def main():
    parser = argparse.ArgumentParser(description="Export hotel leads to Excel")
    
    # Engine-based export
    parser.add_argument("--engine", "-e", type=str, choices=BOOKING_ENGINES, help="Export specific engine")
    parser.add_argument("--engines-all", action="store_true", help="Export all booking engines")
    
    # Country/state-based export
    parser.add_argument("--state", "-s", type=str, help="Export specific state")
    parser.add_argument("--country", "-c", type=str, choices=list(COUNTRIES.keys()), help="Export all states in country")
    
    # Export everything
    parser.add_argument("--all", "-a", action="store_true", help="Export everything (all engines + all countries)")
    
    # Options
    parser.add_argument("--dry-run", action="store_true", help="Show what would be exported without uploading")
    parser.add_argument("--no-notify", action="store_true", help="Disable Slack notifications")
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    # Initialize database
    await init_db()
    
    try:
        service = Service()
        
        if args.all:
            results = await export_all(args.dry_run)
            print_summary(results)
            
        elif args.engines_all:
            results = await export_all_engines(service, args.dry_run)
            logger.info(f"\nExported {results['engines']} engines, {results['total_leads']} total leads")
            
        elif args.engine:
            s3_uri, count = await export_by_engine(service, args.engine, args.dry_run)
            if not args.dry_run:
                logger.success(f"Exported {count} {args.engine} leads to {s3_uri}")
                
        elif args.country:
            results = await export_country(service, args.country, args.dry_run)
            logger.info(f"\nExported {results['states']} states, {results['total_leads']} total leads")
            
        elif args.state:
            country = "USA"  # Default to USA
            s3_uri, count = await export_by_state(service, args.state, country, args.dry_run)
            if not args.dry_run:
                logger.success(f"Exported {count} leads for {args.state} to {s3_uri}")
                
        else:
            parser.error("Provide --all, --engines-all, --engine, --country, or --state")
            
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
