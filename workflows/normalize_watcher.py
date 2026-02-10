"""
Workflow: Normalize Watcher
============================
Polls all enrichment SQS queues. When ALL are empty for N consecutive checks
(default: 2 checks, 30s apart), runs normalization automatically.

This ensures dirty data written by enrichers (abbreviated states, etc.)
gets normalized without manual intervention.

USAGE:
    # One-shot: wait for queues to drain, normalize once
    uv run python -m workflows.normalize_watcher

    # Continuous: loop forever (wait -> normalize -> wait -> ...)
    uv run python -m workflows.normalize_watcher --loop

    # Dry run (polls queues, shows what normalization would do)
    uv run python -m workflows.normalize_watcher --dry-run

    # Custom poll interval and stability checks
    uv run python -m workflows.normalize_watcher --poll-interval 60 --stable-checks 3
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
from typing import Dict, Tuple

from loguru import logger


# Enrichment queue env vars to monitor
QUEUE_ENV_VARS = [
    "SQS_RMS_ENRICHMENT_QUEUE_URL",
    "SQS_BOOKING_ENRICHMENT_QUEUE_URL",
    "SQS_SITEMINDER_ENRICHMENT_QUEUE_URL",
    "SQS_MEWS_ENRICHMENT_QUEUE_URL",
    "SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL",
]


def get_configured_queues() -> Dict[str, str]:
    """Return {env_var_name: queue_url} for all configured queue env vars."""
    queues = {}
    for var in QUEUE_ENV_VARS:
        url = os.getenv(var)
        if url:
            # Use a short name for logging (e.g. "RMS" from "SQS_RMS_ENRICHMENT_QUEUE_URL")
            name = var.replace("SQS_", "").replace("_ENRICHMENT_QUEUE_URL", "")
            queues[name] = url
    return queues


def get_all_queue_stats() -> Dict[str, Tuple[int, int]]:
    """Get (pending, in_flight) for each configured queue.

    Returns:
        {queue_name: (approximate_pending, approximate_in_flight)}
    """
    from infra.sqs import get_queue_attributes

    queues = get_configured_queues()
    if not queues:
        logger.warning("No enrichment queue URLs configured — nothing to watch")
        return {}

    stats = {}
    for name, url in queues.items():
        try:
            attrs = get_queue_attributes(url)
            pending = int(attrs.get("ApproximateNumberOfMessages", 0))
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            stats[name] = (pending, in_flight)
        except Exception as e:
            logger.error(f"Failed to get stats for {name}: {e}")
            # Treat errors as non-empty to be safe
            stats[name] = (-1, -1)

    return stats


def all_queues_empty(stats: Dict[str, Tuple[int, int]]) -> bool:
    """True if all queues have 0 pending and 0 in-flight messages."""
    if not stats:
        return False
    return all(pending == 0 and in_flight == 0 for pending, in_flight in stats.values())


async def wait_for_queues_empty(
    poll_interval: int = 30,
    stable_checks: int = 2,
) -> None:
    """Poll queues until all are empty for `stable_checks` consecutive checks."""
    consecutive_empty = 0

    while consecutive_empty < stable_checks:
        # boto3 is sync — run in thread to not block the event loop
        stats = await asyncio.to_thread(get_all_queue_stats)

        if not stats:
            logger.warning("No queues configured, exiting wait loop")
            return

        # Log current state
        parts = []
        for name, (pending, in_flight) in sorted(stats.items()):
            if pending == -1:
                parts.append(f"{name}=ERROR")
            else:
                parts.append(f"{name}={pending}+{in_flight}")
        logger.info(f"Queue stats: {', '.join(parts)}")

        if all_queues_empty(stats):
            consecutive_empty += 1
            if consecutive_empty < stable_checks:
                logger.info(
                    f"All queues empty ({consecutive_empty}/{stable_checks} checks) "
                    f"— waiting {poll_interval}s for stability..."
                )
            else:
                logger.info(f"All queues empty for {stable_checks} consecutive checks")
        else:
            if consecutive_empty > 0:
                logger.info("Queue activity detected, resetting empty counter")
            consecutive_empty = 0

        if consecutive_empty < stable_checks:
            await asyncio.sleep(poll_interval)


async def run(
    loop: bool = False,
    dry_run: bool = False,
    poll_interval: int = 30,
    stable_checks: int = 2,
):
    """Wait for queues to drain, then run normalization."""
    from workflows.normalize import run as normalize_run

    queues = get_configured_queues()
    if not queues:
        logger.error("No enrichment queue URLs configured. Set at least one of:")
        for var in QUEUE_ENV_VARS:
            logger.error(f"  {var}")
        return

    logger.info(f"Watching {len(queues)} queues: {', '.join(queues.keys())}")
    logger.info(f"Poll interval: {poll_interval}s, stable checks: {stable_checks}")
    if loop:
        logger.info("Running in continuous loop mode")

    iteration = 0
    while True:
        iteration += 1
        if loop and iteration > 1:
            logger.info("")
            logger.info(f"--- Iteration {iteration} ---")

        logger.info("Waiting for all enrichment queues to drain...")
        await wait_for_queues_empty(
            poll_interval=poll_interval,
            stable_checks=stable_checks,
        )

        logger.info("")
        logger.info("All queues drained — running normalization...")
        await normalize_run(dry_run=dry_run)

        if not loop:
            break

        logger.info("Normalization complete. Resuming queue watch...")


def main():
    parser = argparse.ArgumentParser(
        description="Watch enrichment queues and auto-run normalization when drained"
    )
    parser.add_argument("--loop", action="store_true", help="Continuously watch and normalize")
    parser.add_argument("--dry-run", action="store_true", help="Run normalization in dry-run mode")
    parser.add_argument(
        "--poll-interval", type=int, default=30,
        help="Seconds between queue polls (default: 30)",
    )
    parser.add_argument(
        "--stable-checks", type=int, default=2,
        help="Consecutive empty checks required before normalizing (default: 2)",
    )

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    asyncio.run(run(
        loop=args.loop,
        dry_run=args.dry_run,
        poll_interval=args.poll_interval,
        stable_checks=args.stable_checks,
    ))


if __name__ == "__main__":
    main()
