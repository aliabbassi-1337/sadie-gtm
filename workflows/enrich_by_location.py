#!/usr/bin/env python3
"""
Coordinate-based Enrichment Workflow - Find hotel details using coordinates.

For parcel data (SF, Maryland) that has coordinates but no real hotel names,
use Serper Places API to find the actual hotel at those coordinates.

Usage:
    uv run python -m workflows.enrich_by_location --limit 100
    uv run python -m workflows.enrich_by_location --status
    uv run python -m workflows.enrich_by_location --source sf_assessor --source md_sdat_cama
"""

import argparse
import asyncio
import sys

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service as EnrichmentService
from infra import slack


async def run():
    parser = argparse.ArgumentParser(
        description="Enrich hotels using coordinates (Serper Places API)",
    )
    parser.add_argument("-l", "--limit", type=int, default=100)
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Source name(s) to filter (can specify multiple times)",
    )
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    parser.add_argument("--concurrency", type=int, default=10)

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    await init_db()

    try:
        service = EnrichmentService()

        if args.status:
            pending = await service.get_pending_coordinate_enrichment_count(
                sources=args.sources,
            )
            logger.info(f"Hotels pending coordinate enrichment: {pending}")
            return

        logger.info(f"Running coordinate enrichment (limit={args.limit})")
        if args.sources:
            logger.info(f"  Sources: {', '.join(args.sources)}")
        stats = await service.enrich_by_coordinates(
            limit=args.limit,
            sources=args.sources,
            concurrency=args.concurrency,
        )

        if not args.no_notify and stats["enriched"] > 0:
            slack.send_message(
                f"*Coordinate Enrichment Complete*\n"
                f"• Enriched: {stats['enriched']}\n"
                f"• Not found: {stats['not_found']}\n"
                f"• API calls: {stats['api_calls']}"
            )

    except Exception as e:
        logger.error(f"Coordinate enrichment failed: {e}")
        if not args.no_notify:
            slack.send_error("Coordinate Enrichment", str(e))
        raise
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(run())
