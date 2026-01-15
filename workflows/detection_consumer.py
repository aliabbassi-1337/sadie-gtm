"""Detection worker - Polls SQS and processes hotel batches.

Run continuously on EC2 instances to process detection jobs from SQS.

Usage:
    uv run python workflows/detection_worker.py --concurrency 6
    uv run python workflows/detection_worker.py --concurrency 6 --preset medium

RAM Presets:
    --preset small   8GB RAM  (concurrency 5, batch concurrency 3)
    --preset medium  12GB RAM (concurrency 6, batch concurrency 5)
    --preset large   16GB RAM (concurrency 8, batch concurrency 5)
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import signal
from typing import Dict, Any
from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service
from services.leadgen.detector import DetectionConfig, BatchDetector, set_engine_patterns
from infra.sqs import receive_messages, delete_message, get_queue_url, get_queue_attributes
from infra import slack


# RAM-based presets
PRESETS = {
    "small": {      # 8GB RAM
        "concurrency": 5,           # concurrent SQS messages
        "batch_concurrency": 3,     # concurrent hotels per batch
        "description": "8GB RAM",
    },
    "medium": {     # 12GB RAM
        "concurrency": 6,
        "batch_concurrency": 5,
        "description": "12GB RAM",
    },
    "large": {      # 16GB+ RAM
        "concurrency": 8,
        "batch_concurrency": 5,
        "description": "16GB RAM",
    },
}

# Global flag for graceful shutdown
shutdown_requested = False


def handle_shutdown(signum, frame):
    """Handle shutdown signal gracefully."""
    global shutdown_requested
    logger.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


async def process_message(
    service: Service,
    message: Dict[str, Any],
    queue_url: str,
    batch_concurrency: int,
    debug: bool,
) -> tuple:
    """Process a single SQS message containing hotel IDs.

    Returns (processed_count, detected_count, error_count).
    On exception, does NOT delete message so SQS can retry.
    """
    receipt_handle = message["receipt_handle"]
    hotel_ids = message["body"].get("hotel_ids", [])

    if not hotel_ids:
        # Empty message, delete it
        delete_message(queue_url, receipt_handle)
        return (0, 0, 0)

    try:
        # Fetch hotels from DB
        hotels = await service.get_hotels_by_ids(hotel_ids)
        if not hotels:
            # Hotels not found (maybe deleted), delete message
            delete_message(queue_url, receipt_handle)
            return (0, 0, 0)

        # Convert to dicts for detector (include city for location filtering)
        hotel_dicts = [
            {"id": h.id, "name": h.name, "website": h.website, "city": h.city or ""}
            for h in hotels
        ]

        # Run detection
        config = DetectionConfig(
            concurrency=batch_concurrency,
            headless=True,
            debug=debug,
        )
        detector = BatchDetector(config)
        results = await detector.detect_batch(hotel_dicts)

        # Save results
        detected, errors = await service.save_detection_results(results)

        # Delete message from SQS (successful processing)
        delete_message(queue_url, receipt_handle)

        return (len(results), detected, errors)

    except Exception as e:
        # Don't delete message - SQS will retry after visibility timeout
        logger.error(f"Error processing message (will retry): {e}")
        raise  # Re-raise so worker_loop knows it failed


async def worker_loop(
    concurrency: int = 6,
    batch_concurrency: int = 5,
    debug: bool = False,
    max_messages: int = 0,
    notify: bool = False,
):
    """Main worker loop - poll SQS and process messages.

    Args:
        concurrency: Max concurrent SQS messages to process
        batch_concurrency: Concurrent hotels within each batch
        debug: Enable debug logging
        max_messages: Max messages to process (0 = unlimited)
        notify: Send Slack notification on completion
    """
    global shutdown_requested

    await init_db()
    try:
        service = Service()
        queue_url = get_queue_url()

        # Load engine patterns (cached)
        patterns = await service.get_engine_patterns()
        set_engine_patterns(patterns)

        logger.info(f"Consumer starting (concurrency={concurrency}, batch_concurrency={batch_concurrency})")
        logger.info(f"Queue: {queue_url}")

        total_processed = 0
        total_detected = 0
        total_errors = 0
        message_count = 0

        # Semaphore to limit concurrent message processing
        semaphore = asyncio.Semaphore(concurrency)

        async def process_with_semaphore(msg):
            async with semaphore:
                return await process_message(
                    service=service,
                    message=msg,
                    queue_url=queue_url,
                    batch_concurrency=batch_concurrency,
                    debug=debug,
                )

        while not shutdown_requested:
            # Check max messages limit
            if max_messages > 0 and message_count >= max_messages:
                logger.info(f"Reached max messages limit ({max_messages})")
                break

            # Poll for messages
            messages = receive_messages(
                queue_url=queue_url,
                max_messages=min(concurrency, 10),  # SQS max is 10
                wait_time_seconds=20,
                visibility_timeout=7200,  # 2 hours - safe buffer for retries
            )

            if not messages:
                # No messages, check queue depth
                attrs = get_queue_attributes(queue_url)
                pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))

                if pending == 0 and in_flight == 0:
                    logger.info("Queue empty, waiting...")

                continue

            # Process messages concurrently
            tasks = [process_with_semaphore(msg) for msg in messages]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing message: {result}")
                    total_errors += 1
                else:
                    processed, detected, errors = result
                    total_processed += processed
                    total_detected += detected
                    total_errors += errors

            message_count += len(messages)
            logger.info(
                f"Processed {len(messages)} messages | "
                f"Total: {total_processed} hotels, {total_detected} detected, {total_errors} errors"
            )

        # Final summary
        logger.info("=" * 60)
        logger.info("CONSUMER COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Messages processed: {message_count}")
        logger.info(f"Hotels processed:   {total_processed}")
        logger.info(f"Engines detected:   {total_detected}")
        logger.info(f"Errors:             {total_errors}")
        if total_processed > 0:
            logger.info(f"Hit rate:           {total_detected / total_processed * 100:.1f}%")
        logger.info("=" * 60)

        # Send Slack notification
        if notify and total_processed > 0:
            hit_rate = total_detected / total_processed * 100 if total_processed > 0 else 0
            slack.send_message(
                f"*Detection Consumer Complete*\n"
                f"• Hotels processed: {total_processed}\n"
                f"• Engines detected: {total_detected}\n"
                f"• Hit rate: {hit_rate:.1f}%\n"
                f"• Errors: {total_errors}"
            )

    except Exception as e:
        logger.error(f"Detection consumer failed: {e}")
        if notify:
            slack.send_error("Detection Consumer", str(e))
        raise
    finally:
        await close_db()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Detection worker - poll SQS and process hotels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
RAM Presets:
  --preset small   8GB RAM  (5 concurrent messages, 3 concurrent hotels)
  --preset medium  12GB RAM (6 concurrent messages, 5 concurrent hotels)
  --preset large   16GB RAM (8 concurrent messages, 5 concurrent hotels)

Examples:
  # Run with medium preset
  uv run python workflows/detection_worker.py --preset medium

  # Run with custom concurrency
  uv run python workflows/detection_worker.py --concurrency 6 --batch-concurrency 5

  # Process max 100 messages then exit (for testing)
  uv run python workflows/detection_worker.py --preset small --max-messages 100

Environment:
  SQS_DETECTION_QUEUE_URL - Required. The SQS queue URL.
  AWS_REGION - Optional. Defaults to us-east-1.
        """
    )

    parser.add_argument(
        "--preset", "-p",
        choices=list(PRESETS.keys()),
        help="RAM preset (small=8GB, medium=12GB, large=16GB)"
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        help="Concurrent SQS messages to process (overrides preset)"
    )
    parser.add_argument(
        "--batch-concurrency",
        type=int,
        help="Concurrent hotels per batch (overrides preset)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Max messages to process, 0=unlimited (default: 0)"
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Send Slack notification on completion"
    )

    args = parser.parse_args()

    # Apply preset defaults
    if args.preset:
        preset = PRESETS[args.preset]
        concurrency = preset["concurrency"]
        batch_concurrency = preset["batch_concurrency"]
        logger.info(f"Using preset '{args.preset}': {preset['description']}")
    else:
        concurrency = 6
        batch_concurrency = 5

    # Override with explicit args
    if args.concurrency:
        concurrency = args.concurrency
    if args.batch_concurrency:
        batch_concurrency = args.batch_concurrency

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info(f"Starting worker (concurrency={concurrency}, batch_concurrency={batch_concurrency})")

    asyncio.run(worker_loop(
        concurrency=concurrency,
        batch_concurrency=batch_concurrency,
        debug=args.debug,
        max_messages=args.max_messages,
        notify=args.notify,
    ))


if __name__ == "__main__":
    main()
