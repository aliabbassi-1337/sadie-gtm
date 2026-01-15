"""Enqueue hotels for detection via SQS.

Run this after scraping or on a schedule to queue hotels for detection workers.

Usage:
    uv run python workflows/enqueue_detection.py --limit 1000
    uv run python workflows/enqueue_detection.py --limit 5000 --batch-size 20
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service
from infra import slack


async def run(limit: int = 1000, batch_size: int = 20, notify: bool = False):
    """Enqueue hotels for detection."""
    await init_db()
    try:
        service = Service()
        count = await service.enqueue_hotels_for_detection(limit=limit, batch_size=batch_size)
        logger.info(f"Enqueued {count} hotels for detection")

        if notify and count > 0:
            slack.send_message(
                f"*Detection Queue Updated*\n"
                f"• Hotels enqueued: {count}\n"
                f"• Batch size: {batch_size}"
            )
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
        "--notify",
        action="store_true",
        help="Send Slack notification after enqueue"
    )

    args = parser.parse_args()

    logger.info(f"Enqueuing up to {args.limit} hotels (batch_size={args.batch_size})")
    asyncio.run(run(limit=args.limit, batch_size=args.batch_size, notify=args.notify))


if __name__ == "__main__":
    main()
