"""Detection workflow - Fetch hotels and detect their booking engines.

SCALING MODES:

1. Single run (default):
   uv run python workflows/detection.py --preset medium --limit 500

2. Worker mode (for multiple EC2 instances):
   uv run python workflows/detection.py --worker --preset medium --batch-size 50

   In worker mode:
   - Uses FOR UPDATE SKIP LOCKED to claim hotels atomically
   - Multiple workers can run on different machines simultaneously
   - Each worker claims and processes batches until no hotels left
   - If a worker crashes, hotels reset after 30 min (status=10 -> 0)

RAM PRESETS:
- 8GB RAM:  --preset small  (3 concurrent, batch 50)
- 12GB RAM: --preset medium (5 concurrent, batch 100)
- 16GB RAM: --preset large  (8 concurrent, batch 200)
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import socket
from typing import List
from loguru import logger

from db.client import init_db, close_db
from services.leadgen import repo
from services.leadgen.detector import DetectionConfig, DetectionResult, BatchDetector


# RAM-based presets
PRESETS = {
    "small": {      # 8GB RAM
        "concurrency": 3,
        "batch_size": 50,
        "description": "8GB RAM - conservative",
    },
    "medium": {     # 12GB RAM
        "concurrency": 5,
        "batch_size": 100,
        "description": "12GB RAM - balanced",
    },
    "large": {      # 16GB+ RAM
        "concurrency": 8,
        "batch_size": 200,
        "description": "16GB RAM - aggressive",
    },
    "xlarge": {     # 32GB+ RAM
        "concurrency": 12,
        "batch_size": 500,
        "description": "32GB RAM - maximum throughput",
    },
}


async def process_batch(hotels: List, concurrency: int, debug: bool) -> List[DetectionResult]:
    """Run detection on a batch of hotels."""
    if not hotels:
        return []

    hotel_dicts = [
        {"id": h.id, "name": h.name, "website": h.website}
        for h in hotels
    ]

    config = DetectionConfig(
        concurrency=concurrency,
        headless=True,
        debug=debug,
    )

    detector = BatchDetector(config)
    return await detector.detect_batch(hotel_dicts)


async def save_results(results: List[DetectionResult]) -> tuple:
    """Save detection results to database. Returns (detected, errors) counts."""
    detected = 0
    errors = 0

    for result in results:
        try:
            if result.booking_engine and result.booking_engine not in ("", "unknown", "unknown_third_party", "unknown_booking_api"):
                # Found a booking engine
                engine = await repo.get_booking_engine_by_name(result.booking_engine)
                if engine:
                    engine_id = engine.id
                else:
                    engine_id = await repo.insert_booking_engine(
                        name=result.booking_engine,
                        domains=[result.booking_engine_domain] if result.booking_engine_domain else None,
                        tier=2,
                    )

                await repo.insert_hotel_booking_engine(
                    hotel_id=result.hotel_id,
                    booking_engine_id=engine_id,
                    booking_url=result.booking_url or None,
                    detection_method=result.detection_method or None,
                )

                await repo.update_hotel_status(
                    hotel_id=result.hotel_id,
                    status=1,
                    phone_website=result.phone_website or None,
                    email=result.email or None,
                )
                detected += 1
            else:
                # No booking engine found - status 99
                await repo.update_hotel_status(
                    hotel_id=result.hotel_id,
                    status=99,
                    phone_website=result.phone_website or None,
                    email=result.email or None,
                )
                if result.error:
                    errors += 1

        except Exception as e:
            logger.error(f"Error saving result for hotel {result.hotel_id}: {e}")
            errors += 1

    return detected, errors


async def worker_loop(
    concurrency: int,
    batch_size: int,
    debug: bool,
    max_batches: int = 0,
):
    """
    Worker mode: continuously claim and process batches until queue is empty.

    Args:
        concurrency: Parallel browser contexts
        batch_size: Hotels per batch
        debug: Enable debug logging
        max_batches: Max batches to process (0 = unlimited)
    """
    worker_id = socket.gethostname()
    logger.info(f"Worker {worker_id} starting (concurrency={concurrency}, batch_size={batch_size})")

    total_processed = 0
    total_detected = 0
    total_errors = 0
    batch_num = 0

    while True:
        # Claim a batch of hotels atomically
        hotels = await repo.claim_hotels_for_detection(limit=batch_size)

        if not hotels:
            logger.info(f"Worker {worker_id}: No more hotels to process")
            break

        batch_num += 1
        logger.info(f"Worker {worker_id} batch {batch_num}: claimed {len(hotels)} hotels")

        # Process the batch
        results = await process_batch(hotels, concurrency, debug)

        # Save results
        detected, errors = await save_results(results)

        total_processed += len(results)
        total_detected += detected
        total_errors += errors

        logger.info(f"Worker {worker_id} batch {batch_num}: {detected} detected, {errors} errors")

        # Check max batches limit
        if max_batches > 0 and batch_num >= max_batches:
            logger.info(f"Worker {worker_id}: Reached max batches limit ({max_batches})")
            break

        # Small pause to prevent tight loop if something is wrong
        await asyncio.sleep(0.5)

    # Final summary
    logger.info("=" * 60)
    logger.info(f"WORKER {worker_id} COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Batches processed:   {batch_num}")
    logger.info(f"Hotels processed:    {total_processed}")
    logger.info(f"Engines detected:    {total_detected}")
    logger.info(f"Errors:              {total_errors}")
    if total_processed > 0:
        logger.info(f"Hit rate:            {total_detected / total_processed * 100:.1f}%")
    logger.info("=" * 60)


async def single_run(
    limit: int,
    concurrency: int,
    batch_size: int,
    debug: bool,
):
    """
    Single run mode: process up to `limit` hotels, then exit.
    """
    # Get all pending hotels up to limit
    all_hotels = await repo.get_hotels_pending_detection(limit=limit)
    total_hotels = len(all_hotels)

    if not all_hotels:
        logger.info("No hotels pending detection")
        return

    logger.info(f"Found {total_hotels} hotels pending detection")
    logger.info(f"Config: concurrency={concurrency}, batch_size={batch_size}")

    total_detected = 0
    total_errors = 0
    batch_num = 0

    # Process in batches
    for i in range(0, total_hotels, batch_size):
        batch_num += 1
        batch = all_hotels[i:i + batch_size]
        logger.info(f"Processing batch {batch_num}: {len(batch)} hotels")

        results = await process_batch(batch, concurrency, debug)
        detected, errors = await save_results(results)

        total_detected += detected
        total_errors += errors

        logger.info(f"Batch {batch_num}: {detected} detected, {errors} errors")

        if i + batch_size < total_hotels:
            await asyncio.sleep(1)

    # Final summary
    no_engine = total_hotels - total_detected - total_errors
    logger.info("=" * 60)
    logger.info("DETECTION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Hotels processed:    {total_hotels}")
    logger.info(f"Engines detected:    {total_detected}")
    logger.info(f"No engine found:     {no_engine}")
    logger.info(f"Errors:              {total_errors}")
    if total_hotels > 0:
        logger.info(f"Hit rate:            {total_detected / total_hotels * 100:.1f}%")
    logger.info("=" * 60)


async def run(
    worker: bool = False,
    limit: int = 100,
    concurrency: int = 5,
    batch_size: int = 100,
    max_batches: int = 0,
    debug: bool = False,
):
    """Initialize DB and run workflow."""
    await init_db()
    try:
        if worker:
            await worker_loop(
                concurrency=concurrency,
                batch_size=batch_size,
                debug=debug,
                max_batches=max_batches,
            )
        else:
            await single_run(
                limit=limit,
                concurrency=concurrency,
                batch_size=batch_size,
                debug=debug,
            )
    finally:
        await close_db()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Run booking engine detection workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
SCALING MODES:

1. Single run (default):
   uv run python workflows/detection.py --preset medium --limit 500

2. Worker mode (multiple EC2 instances):
   uv run python workflows/detection.py --worker --preset medium

   Workers use FOR UPDATE SKIP LOCKED to safely claim hotels.
   Run the same command on multiple machines!

RAM Presets:
  --preset small   8GB RAM  (3 concurrent, batch 50)
  --preset medium  12GB RAM (5 concurrent, batch 100)
  --preset large   16GB RAM (8 concurrent, batch 200)

Examples:
  # Single run - process 500 hotels
  uv run python workflows/detection.py --preset medium --limit 500

  # Worker mode - run on each EC2 instance
  uv run python workflows/detection.py --worker --preset medium

  # Worker with max batches (for testing)
  uv run python workflows/detection.py --worker --preset small --max-batches 5
        """
    )

    parser.add_argument(
        "--worker", "-w",
        action="store_true",
        help="Run in worker mode (claims hotels atomically, runs until queue empty)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="Max hotels to process in single-run mode (default: 100)"
    )
    parser.add_argument(
        "--preset", "-p",
        choices=list(PRESETS.keys()),
        help="RAM preset (small=8GB, medium=12GB, large=16GB, xlarge=32GB)"
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        help="Parallel browser contexts (overrides preset)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        help="Hotels per batch (overrides preset)"
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Max batches in worker mode, 0=unlimited (default: 0)"
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--reset-stale",
        action="store_true",
        help="Reset hotels stuck in processing state (status=10) before starting"
    )

    args = parser.parse_args()

    # Apply preset defaults
    if args.preset:
        preset = PRESETS[args.preset]
        concurrency = preset["concurrency"]
        batch_size = preset["batch_size"]
        logger.info(f"Using preset '{args.preset}': {preset['description']}")
    else:
        concurrency = 5
        batch_size = 100

    # Override with explicit args
    if args.concurrency:
        concurrency = args.concurrency
    if args.batch_size:
        batch_size = args.batch_size

    mode = "worker" if args.worker else "single-run"
    logger.info(f"Starting detection ({mode}): concurrency={concurrency}, batch_size={batch_size}")

    # Handle reset-stale flag
    if args.reset_stale:
        async def reset():
            await init_db()
            await repo.reset_stale_processing_hotels()
            logger.info("Reset stale processing hotels (status=10 -> 0)")
            await close_db()
        asyncio.run(reset())

    asyncio.run(run(
        worker=args.worker,
        limit=args.limit,
        concurrency=concurrency,
        batch_size=batch_size,
        max_batches=args.max_batches,
        debug=args.debug,
    ))


if __name__ == "__main__":
    main()
