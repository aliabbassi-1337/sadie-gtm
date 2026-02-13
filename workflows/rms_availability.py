#!/usr/bin/env python3
"""RMS Availability Enrichment Workflow

Checks if Australia RMS hotels have availability via the ibe12 API.

Usage:
    uv run python -m workflows.rms_availability --limit 100
    uv run python -m workflows.rms_availability --limit 3800 --concurrency 30
    uv run python -m workflows.rms_availability --proxy brightdata
    uv run python -m workflows.rms_availability --status
    uv run python -m workflows.rms_availability --reset
"""

import asyncio
import argparse

from db.client import init_db, close_db
from services.enrichment.service import Service


async def run(
    limit: int = 100,
    concurrency: int = 30,
    force: bool = False,
    dry_run: bool = False,
    proxy_mode: str = "auto",
):
    """Run RMS availability enrichment."""
    await init_db()
    try:
        svc = Service()
        await svc.check_rms_availability(
            limit=limit,
            concurrency=concurrency,
            force=force,
            dry_run=dry_run,
            proxy_mode=proxy_mode,
        )
    finally:
        await close_db()


async def status():
    """Show availability check status."""
    await init_db()
    try:
        svc = Service()
        await svc.get_rms_availability_status()
    finally:
        await close_db()


async def reset():
    """Reset all availability results."""
    await init_db()
    try:
        svc = Service()
        await svc.reset_rms_availability()
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Check RMS Australia hotel availability",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -m workflows.rms_availability --limit 100
  uv run python -m workflows.rms_availability --limit 3800 --concurrency 30
  uv run python -m workflows.rms_availability --proxy brightdata
  uv run python -m workflows.rms_availability --proxy direct
  uv run python -m workflows.rms_availability --status
  uv run python -m workflows.rms_availability --reset
        """,
    )

    parser.add_argument("--limit", "-l", type=int, default=100, help="Max leads to process (default: 100)")
    parser.add_argument("--concurrency", "-c", type=int, default=30, help="Concurrent requests (default: 30)")
    parser.add_argument("--proxy", "-p", default="auto",
                        choices=["auto", "direct", "brightdata", "free", "proxy"],
                        help="Proxy mode: auto (direct+free), direct, brightdata, free (ProxyScrape), proxy (RMS_PROXY_URLS)")
    parser.add_argument("--force", action="store_true", help="Re-check all leads (ignore previous checks)")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument("--status", action="store_true", help="Show availability check status")
    parser.add_argument("--reset", action="store_true", help="Reset all results to NULL (clear bad data)")

    args = parser.parse_args()

    if args.status:
        asyncio.run(status())
    elif args.reset:
        asyncio.run(reset())
    else:
        asyncio.run(
            run(
                limit=args.limit,
                concurrency=args.concurrency,
                force=args.force,
                dry_run=args.dry_run,
                proxy_mode=args.proxy,
            )
        )


if __name__ == "__main__":
    main()
