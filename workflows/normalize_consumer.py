"""
Workflow: Normalization Consumer
=================================
SQS consumer that runs normalization when triggered by a message.

Send a message to the queue to trigger normalization:
    {"action": "normalize"}

The consumer long-polls the queue, runs normalization on each message,
then deletes the message. Exits cleanly on SIGINT/SIGTERM.

USAGE:
    # Run the consumer (production — Fargate)
    uv run python -m workflows.normalize_consumer

    # Dry run (process messages but don't apply changes)
    uv run python -m workflows.normalize_consumer --dry-run

    # Send a normalize trigger message
    uv run python -m workflows.normalize_consumer --send
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import signal

from loguru import logger


QUEUE_URL = os.getenv("SQS_NORMALIZATION_QUEUE_URL", "")

shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current normalization run...")
    shutdown_requested = True


async def run_consumer(dry_run: bool = False):
    """Poll SQS for normalization trigger messages."""
    from infra.sqs import receive_messages, delete_message
    from workflows.normalize import run as normalize_run

    if not QUEUE_URL:
        logger.error("SQS_NORMALIZATION_QUEUE_URL not set")
        return

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("Normalization consumer started, waiting for messages...")

    while not shutdown_requested:
        # Long-poll (20s) — blocks until a message arrives or timeout
        messages = await asyncio.to_thread(
            receive_messages, QUEUE_URL,
            max_messages=1, wait_time_seconds=20, visibility_timeout=900,
        )

        if not messages or shutdown_requested:
            continue

        msg = messages[0]
        body = msg["body"]
        action = body.get("action", "")

        if action != "normalize":
            logger.warning(f"Unknown action: {action}, deleting message")
            await asyncio.to_thread(delete_message, QUEUE_URL, msg["receipt_handle"])
            continue

        logger.info("Received normalize trigger, running normalization...")
        try:
            await normalize_run(dry_run=dry_run)
            logger.success("Normalization complete")
        except Exception:
            logger.exception("Normalization failed")

        await asyncio.to_thread(delete_message, QUEUE_URL, msg["receipt_handle"])

    logger.info("Consumer stopped")


def send_trigger():
    """Send a normalize trigger message to the queue."""
    from infra.sqs import send_message

    if not QUEUE_URL:
        logger.error("SQS_NORMALIZATION_QUEUE_URL not set")
        return

    msg_id = send_message(QUEUE_URL, {"action": "normalize"})
    logger.info(f"Sent normalize trigger (message ID: {msg_id})")


def main():
    parser = argparse.ArgumentParser(description="Normalization SQS consumer")
    parser.add_argument("--dry-run", action="store_true", help="Run normalization in dry-run mode")
    parser.add_argument("--send", action="store_true", help="Send a normalize trigger message and exit")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    if args.send:
        send_trigger()
    else:
        asyncio.run(run_consumer(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
