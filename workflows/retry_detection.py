"""
Workflow: Retry Failed Detections
=================================
Retries hotels that failed detection due to timeout, server errors, or browser exceptions.

Usage:
    # Show hotels that can be retried
    uv run python workflows/retry_detection.py --state FL --source dbpr --dry-run

    # Retry up to 100 hotels
    uv run python workflows/retry_detection.py --state FL --source dbpr --limit 100
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse

from loguru import logger

from db.client import init_db, close_db
from services.leadgen import repo


async def retry_workflow(
    state: str,
    source: str = None,
    limit: int = 100,
    dry_run: bool = False,
) -> int:
    """Reset hotels with retryable errors so they can be detected again.

    Returns number of hotels reset for retry.
    """
    await init_db()

    try:
        source_pattern = f"%{source}%" if source else None

        # Get hotels with retryable errors
        hotels = await repo.get_hotels_for_retry(
            state=state,
            limit=limit,
            source_pattern=source_pattern,
        )

        if not hotels:
            logger.info("No hotels found for retry")
            return 0

        # Group by error type for reporting
        error_counts = {}
        for h in hotels:
            method = h["detection_method"]
            if "timeout" in method:
                err = "timeout"
            elif "HTTP 5" in method:
                err = "5xx"
            elif "exception" in method:
                err = "browser_exception"
            else:
                err = "other"
            error_counts[err] = error_counts.get(err, 0) + 1

        logger.info(f"Found {len(hotels)} hotels for retry:")
        for err, count in sorted(error_counts.items(), key=lambda x: -x[1]):
            logger.info(f"  {err}: {count}")

        if dry_run:
            logger.info("Dry run - not deleting HBE records")
            logger.info("Sample hotels:")
            for h in hotels[:10]:
                logger.info(f"  {h['id']}: {h['name']} - {h['detection_method'][:60]}")
            return 0

        # Delete HBE records and reset status to allow retry
        hotel_ids = [h["id"] for h in hotels]
        await repo.reset_hotels_for_retry(hotel_ids)

        logger.info(f"Reset {len(hotel_ids)} hotels for retry (status=0, HBE deleted)")
        logger.info("Run detection workflow to retry these hotels")

        return len(hotel_ids)

    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Retry failed hotel detections")

    parser.add_argument("--state", type=str, required=True, help="State code (e.g., 'FL')")
    parser.add_argument("--source", type=str, help="Filter by source (e.g., 'dbpr')")
    parser.add_argument("--limit", type=int, default=100, help="Max hotels to retry")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be retried without doing it")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    count = asyncio.run(retry_workflow(
        args.state,
        args.source,
        args.limit,
        args.dry_run,
    ))

    print(f"\nReset {count} hotels for retry")


if __name__ == "__main__":
    main()
