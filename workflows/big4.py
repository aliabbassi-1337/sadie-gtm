"""BIG4 Australia workflow - scrape parks from big4.com.au.

USAGE:
    uv run python workflows/big4.py scrape
    uv run python workflows/big4.py scrape --concurrency 20 --delay 0.3
    uv run python workflows/big4.py status
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse

from loguru import logger

from db.client import init_db, close_db
from services.enrichment.service import Service
from infra import slack


async def run_scrape(
    concurrency: int = 10,
    delay: float = 0.5,
    notify: bool = True,
) -> None:
    await init_db()
    try:
        service = Service()
        result = await service.scrape_big4_parks(
            concurrency=concurrency,
            delay=delay,
        )
        if notify and result["discovered"] > 0:
            slack.send_message(
                f"*BIG4 Scrape Complete*\n"
                f"- Parks discovered: {result['discovered']}\n"
                f"- Total BIG4 in DB: {result['total_big4']}\n"
                f"- With email: {result['with_email']}\n"
                f"- With phone: {result['with_phone']}\n"
                f"- With address: {result['with_address']}"
            )
    except Exception as e:
        logger.error(f"BIG4 scrape failed: {e}")
        if notify:
            slack.send_error("BIG4 Scrape", str(e))
        raise
    finally:
        await close_db()


async def show_status() -> None:
    await init_db()
    try:
        from services.enrichment import repo
        count = await repo.get_big4_count()
        logger.info("=" * 60)
        logger.info("BIG4 STATUS")
        logger.info("=" * 60)
        logger.info(f"BIG4 parks in database: {count}")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="BIG4 Australia park scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    scrape_parser = subparsers.add_parser("scrape")
    scrape_parser.add_argument("--concurrency", "-c", type=int, default=10)
    scrape_parser.add_argument("--delay", "-d", type=float, default=0.5)
    scrape_parser.add_argument("--no-notify", action="store_true")

    subparsers.add_parser("status")

    args = parser.parse_args()

    if args.command == "scrape":
        asyncio.run(run_scrape(
            concurrency=args.concurrency,
            delay=args.delay,
            notify=not args.no_notify,
        ))
    elif args.command == "status":
        asyncio.run(show_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
