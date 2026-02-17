"""Owner enrichment workflow - find hotel decision makers (owners, GMs).

USAGE:
    # Direct mode (local testing, no SQS needed)
    uv run python workflows/enrich_owners.py run --limit 5
    uv run python workflows/enrich_owners.py run --limit 10 --layer rdap
    uv run python workflows/enrich_owners.py run --limit 10 --layer website

    # Show enrichment status
    uv run python workflows/enrich_owners.py status

    # For production SQS-based processing, use:
    #   uv run python workflows/enrich_owners_enqueue.py --limit 500
    #   uv run python workflows/enrich_owners_consumer.py --concurrency 5
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service

LAYER_CHOICES = ["rdap", "whois-history", "dns", "website", "reviews", "email-verify", "all"]


async def run_enrichment(
    limit: int = 20,
    concurrency: int = 5,
    layer: str = "all",
) -> None:
    """Run owner enrichment (direct mode, no SQS)."""
    await init_db()
    try:
        svc = Service()
        result = await svc.run_owner_enrichment(
            limit=limit,
            concurrency=concurrency,
            layer=layer if layer != "all" else None,
        )

        if result["processed"]:
            logger.info(
                f"\nOwner Enrichment Complete:\n"
                f"  Hotels processed: {result['processed']}\n"
                f"  Hotels with contacts: {result['found']}\n"
                f"  Total contacts found: {result['contacts']}\n"
                f"  Verified emails: {result['verified']}\n"
                f"  Hit rate: {result['found']/result['processed']*100:.1f}%"
            )
    except Exception as e:
        logger.error(f"Owner enrichment failed: {e}")
        raise
    finally:
        await close_db()


async def show_status() -> None:
    """Show owner enrichment pipeline statistics."""
    await init_db()
    try:
        svc = Service()
        stats = await svc.get_owner_enrichment_stats()
        if not stats:
            logger.info("No enrichment data yet")
            return

        logger.info(
            "\n=== Owner Enrichment Status ===\n"
            f"  Hotels with website:    {stats.get('total_with_website', 0):,}\n"
            f"  Complete:               {stats.get('complete', 0):,}\n"
            f"  No results:             {stats.get('no_results', 0):,}\n"
            f"  ---\n"
            f"  Hotels with contacts:   {stats.get('hotels_with_contacts', 0):,}\n"
            f"  Total contacts:         {stats.get('total_contacts', 0):,}\n"
            f"  Verified emails:        {stats.get('verified_emails', 0):,}"
        )

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Hotel owner/GM enrichment pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    run_parser = subparsers.add_parser("run", help="Run owner enrichment (direct mode)")
    run_parser.add_argument("--limit", type=int, default=20, help="Max hotels to process")
    run_parser.add_argument("--concurrency", type=int, default=5, help="Max concurrent enrichments")
    run_parser.add_argument(
        "--layer", choices=LAYER_CHOICES, default="all",
        help="Run specific layer only (default: all)",
    )

    subparsers.add_parser("status", help="Show enrichment pipeline status")

    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(run_enrichment(
            limit=args.limit,
            concurrency=args.concurrency,
            layer=args.layer,
        ))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
