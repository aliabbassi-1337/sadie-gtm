"""Owner enrichment workflow - find hotel decision makers (owners, GMs).

USAGE:
    # Run full waterfall enrichment
    uv run python workflows/enrich_owners.py run --limit 20

    # Run specific layer only
    uv run python workflows/enrich_owners.py run --limit 50 --layer rdap
    uv run python workflows/enrich_owners.py run --limit 50 --layer whois-history
    uv run python workflows/enrich_owners.py run --limit 50 --layer dns
    uv run python workflows/enrich_owners.py run --limit 50 --layer website
    uv run python workflows/enrich_owners.py run --limit 50 --layer reviews
    uv run python workflows/enrich_owners.py run --limit 50 --layer email-verify

    # Show enrichment status
    uv run python workflows/enrich_owners.py status

    # Preview hotels that would be enriched
    uv run python workflows/enrich_owners.py preview --limit 10

    # Reset stale claims from crashed workers
    uv run python workflows/enrich_owners.py reset-claims
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from db.client import init_db, close_db
from services.enrichment import owner_repo as repo
from services.enrichment.owner_enricher import enrich_batch
from lib.owner_discovery.models import (
    LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
    LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
)

LAYER_MAP = {
    "rdap": LAYER_RDAP,
    "whois-history": LAYER_WHOIS_HISTORY,
    "dns": LAYER_DNS,
    "website": LAYER_WEBSITE,
    "reviews": LAYER_REVIEWS,
    "email-verify": LAYER_EMAIL_VERIFY,
    "all": 0xFF,
}


async def run_enrichment(
    limit: int = 20,
    concurrency: int = 5,
    layer: str = "all",
) -> None:
    """Run owner enrichment waterfall."""
    await init_db()
    try:
        layer_mask = LAYER_MAP.get(layer, 0xFF)
        layer_filter = layer_mask if layer != "all" else None

        logger.info(f"Owner enrichment: limit={limit}, concurrency={concurrency}, layer={layer}")

        # Claim hotels
        hotels = await repo.claim_hotels_for_owner_enrichment(
            limit=limit, layer=layer_filter,
        )
        if not hotels:
            logger.info("No hotels pending owner enrichment")
            return

        logger.info(f"Claimed {len(hotels)} hotels for enrichment")

        # Run waterfall
        results = await enrich_batch(
            hotels=hotels,
            concurrency=concurrency,
            layers=layer_mask,
        )

        # Summary
        found = sum(1 for r in results if r.found_any)
        total_contacts = sum(len(r.decision_makers) for r in results)
        verified = sum(
            sum(1 for dm in r.decision_makers if dm.email_verified)
            for r in results
        )

        logger.info(
            f"\nOwner Enrichment Complete:\n"
            f"  Hotels processed: {len(results)}\n"
            f"  Hotels with contacts: {found}\n"
            f"  Total contacts found: {total_contacts}\n"
            f"  Verified emails: {verified}\n"
            f"  Hit rate: {found/len(results)*100:.1f}%" if results else ""
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
        stats = await repo.get_enrichment_stats()
        if not stats:
            logger.info("No enrichment data yet")
            return

        print("\n=== Owner Enrichment Status ===")
        print(f"  Hotels with website:    {stats.get('total_with_website', 0):,}")
        print(f"  Pending:                {stats.get('pending', 0):,}")
        print(f"  Claimed (in progress):  {stats.get('claimed', 0):,}")
        print(f"  Complete:               {stats.get('complete', 0):,}")
        print(f"  No results:             {stats.get('no_results', 0):,}")
        print(f"  ---")
        print(f"  Hotels with contacts:   {stats.get('hotels_with_contacts', 0):,}")
        print(f"  Total contacts:         {stats.get('total_contacts', 0):,}")
        print(f"  Verified emails:        {stats.get('verified_emails', 0):,}")
        print()

    finally:
        await close_db()


async def preview(limit: int = 10) -> None:
    """Preview hotels that would be enriched."""
    await init_db()
    try:
        hotels = await repo.get_hotels_pending_owner_enrichment(limit=limit)
        if not hotels:
            logger.info("No hotels pending enrichment")
            return

        print(f"\n=== Next {len(hotels)} Hotels for Owner Enrichment ===")
        for h in hotels:
            print(f"  [{h['hotel_id']}] {h['name']}")
            print(f"    Website: {h.get('website', 'N/A')}")
            print(f"    Location: {h.get('city', '?')}, {h.get('state', '?')}")
            print()

    finally:
        await close_db()


async def reset_claims() -> None:
    """Reset stale claims from crashed workers."""
    await init_db()
    try:
        count = await repo.reset_stale_owner_claims()
        logger.info(f"Reset {count} stale claims")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Hotel owner/GM enrichment pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # run command
    run_parser = subparsers.add_parser("run", help="Run owner enrichment waterfall")
    run_parser.add_argument("--limit", type=int, default=20, help="Max hotels to process")
    run_parser.add_argument("--concurrency", type=int, default=5, help="Max concurrent enrichments")
    run_parser.add_argument(
        "--layer", choices=list(LAYER_MAP.keys()), default="all",
        help="Run specific layer only (default: all)",
    )

    # status command
    subparsers.add_parser("status", help="Show enrichment pipeline status")

    # preview command
    preview_parser = subparsers.add_parser("preview", help="Preview next hotels to enrich")
    preview_parser.add_argument("--limit", type=int, default=10, help="Number of hotels to preview")

    # reset-claims command
    subparsers.add_parser("reset-claims", help="Reset stale claims from crashed workers")

    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(run_enrichment(
            limit=args.limit,
            concurrency=args.concurrency,
            layer=args.layer,
        ))
    elif args.command == "status":
        asyncio.run(show_status())
    elif args.command == "preview":
        asyncio.run(preview(limit=args.limit))
    elif args.command == "reset-claims":
        asyncio.run(reset_claims())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
