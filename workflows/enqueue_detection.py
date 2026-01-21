"""Enqueue hotels for detection via SQS.

Run this after scraping or on a schedule to queue hotels for detection workers.

Usage:
    uv run python workflows/enqueue_detection.py --limit 1000
    uv run python workflows/enqueue_detection.py --limit 5000 --batch-size 20
    uv run python workflows/enqueue_detection.py --limit 5000 --categories hotel motel
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
from typing import List, Optional
from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service
from infra import slack


async def run(
    limit: int = 1000,
    batch_size: int = 20,
    categories: Optional[List[str]] = None,
    notify: bool = True,
):
    """Enqueue hotels for detection."""
    await init_db()
    try:
        service = Service()
        count = await service.enqueue_hotels_for_detection(
            limit=limit, batch_size=batch_size, categories=categories
        )
        logger.info(f"Enqueued {count} hotels for detection")

        if notify and count > 0:
            msg = f"*Detection Queue Updated*\n• Hotels enqueued: {count}\n• Batch size: {batch_size}"
            if categories:
                msg += f"\n• Categories: {', '.join(categories)}"
            slack.send_message(msg)
    except Exception as e:
        logger.error(f"Enqueue failed: {e}")
        if notify:
            slack.send_error("Detection Enqueue", str(e))
        raise
    finally:
        await close_db()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Enqueue hotels for detection via SQS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Enqueue up to 1000 hotels
    uv run python workflows/enqueue_detection.py --limit 1000

    # Enqueue 5000 hotels with 20 per message
    uv run python workflows/enqueue_detection.py --limit 5000 --batch-size 20

    # Enqueue only hotels and motels (DBPR leads)
    uv run python workflows/enqueue_detection.py --limit 5000 --categories hotel motel

Environment:
    SQS_DETECTION_QUEUE_URL - Required. The SQS queue URL.
    AWS_REGION - Optional. Defaults to us-east-1.
        """
    )

    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=1000,
        help="Max hotels to enqueue (default: 1000)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=20,
        help="Hotels per SQS message (default: 20)"
    )
    parser.add_argument(
        "--categories", "-c",
        nargs="+",
        help="Filter by categories (e.g., --categories hotel motel)"
    )
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable Slack notification"
    )

    args = parser.parse_args()

    cat_str = f", categories={args.categories}" if args.categories else ""
    logger.info(f"Enqueuing up to {args.limit} hotels (batch_size={args.batch_size}{cat_str})")
    asyncio.run(run(
        limit=args.limit,
        batch_size=args.batch_size,
        categories=args.categories,
        notify=not args.no_notify,
    ))


if __name__ == "__main__":
    main()
