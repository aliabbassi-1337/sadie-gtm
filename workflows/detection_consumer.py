"""Detection consumer optimised for ECS Fargate.

Single persistent Chromium browser, shared context pool, website dedup,
idle self-exit.

Usage:
    uv run python -m workflows.detection_consumer --pool-size 10 --idle-timeout 120
    uv run python -m workflows.detection_consumer --pool-size 3 --max-messages 2 --debug
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import signal
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from loguru import logger

from db.client import init_db, close_db
from services.leadgen.service import Service
from services.leadgen.detector import (
    DetectionConfig,
    DetectionResult,
    HotelProcessor,
    batch_precheck,
    get_chain_name,
    is_junk_domain,
    is_non_hotel_domain,
    is_non_hotel_name,
    normalize_url,
    set_engine_patterns,
)
from services.leadgen.browser_pool import BrowserPool
from infra.sqs import (
    delete_message,
    get_queue_attributes,
    get_queue_url,
    receive_messages,
)
from infra import slack


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_POOL_SIZE = 10
DEFAULT_IDLE_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------

shutdown_requested = False


def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info(f"Signal {signum} received, requesting graceful shutdown...")
    shutdown_requested = True


# ---------------------------------------------------------------------------
# Website dedup
# ---------------------------------------------------------------------------


def normalize_url_for_dedup(url: str) -> str:
    """Normalise URL for dedup: strip protocol, www, trailing slash, query."""
    url = normalize_url(url)
    if not url:
        return ""
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/")
    return f"{host}{path}"


def group_hotels_by_website(hotels: List[Dict]) -> Dict[str, List[Dict]]:
    """Group hotels by normalised website URL for dedup."""
    groups: Dict[str, List[Dict]] = {}
    no_url_counter = 0
    for hotel in hotels:
        norm = normalize_url_for_dedup(hotel.get("website", ""))
        if not norm:
            no_url_counter += 1
            groups[f"__no_url_{no_url_counter}"] = [hotel]
        else:
            groups.setdefault(norm, []).append(hotel)
    return groups


def fan_out_result(result: DetectionResult, hotel_ids: List[int]) -> List[DetectionResult]:
    """Copy a detection result to multiple hotel IDs."""
    out = []
    for hid in hotel_ids:
        r = result.model_copy()
        r.hotel_id = hid
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# Per-hotel processing via shared pool
# ---------------------------------------------------------------------------


async def process_hotel_with_pool(
    hotel: Dict,
    pool: BrowserPool,
    config: DetectionConfig,
    timeout_sec: int = 90,
    max_retries: int = 2,
) -> DetectionResult:
    """Process a single hotel using the shared browser pool.

    On browser crash: restarts pool and retries.
    """
    hotel_id = hotel["id"]

    for attempt in range(max_retries + 1):
        gen_before = pool.generation
        ctx = await pool.acquire_context()

        try:
            # Feed context to HotelProcessor via a single-item queue
            ctx_queue: asyncio.Queue = asyncio.Queue()
            await ctx_queue.put(ctx)
            semaphore = asyncio.Semaphore(1)

            processor = HotelProcessor(
                config=config,
                browser=pool.browser,
                semaphore=semaphore,
                context_queue=ctx_queue,
            )

            result = await asyncio.wait_for(
                processor.process(
                    hotel_id=hotel_id,
                    name=hotel["name"],
                    website=hotel.get("website", ""),
                    expected_city=hotel.get("city", ""),
                    skip_precheck=True,
                ),
                timeout=timeout_sec,
            )

            # HotelProcessor returns context to its queue — retrieve it
            try:
                ctx = await asyncio.wait_for(ctx_queue.get(), timeout=5)
            except asyncio.TimeoutError:
                ctx = None

            if ctx:
                await pool.release_context(ctx)

            return result

        except asyncio.TimeoutError:
            logger.warning(f"Hotel {hotel_id} timed out after {timeout_sec}s")
            try:
                await pool.release_context(ctx)
            except Exception:
                pass
            return DetectionResult(
                hotel_id=hotel_id,
                error=f"timeout: processing exceeded {timeout_sec}s",
            )

        except Exception as e:
            error_str = str(e).lower()
            is_browser_crash = any(
                x in error_str
                for x in [
                    "browser has been closed",
                    "target page",
                    "target closed",
                    "connection refused",
                    "browser disconnected",
                    "protocol error",
                    "session closed",
                ]
            )

            if is_browser_crash and attempt < max_retries:
                logger.warning(
                    f"Browser crash for hotel {hotel_id} "
                    f"(attempt {attempt + 1}/{max_retries + 1}), restarting..."
                )
                if pool.generation == gen_before:
                    await pool.restart_browser()
                continue
            else:
                try:
                    await pool.release_context(ctx)
                except Exception:
                    pass
                return DetectionResult(
                    hotel_id=hotel_id,
                    error=f"exception: {str(e)[:100]}",
                )

    return DetectionResult(hotel_id=hotel_id, error="max_retries_exhausted")


# ---------------------------------------------------------------------------
# Message processing
# ---------------------------------------------------------------------------


async def process_message(
    service: Service,
    message: Dict,
    queue_url: str,
    pool: BrowserPool,
    config: DetectionConfig,
) -> Tuple[int, int, int]:
    """Process a single SQS message. Returns (processed, detected, errors)."""
    receipt_handle = message["receipt_handle"]
    hotel_ids = message["body"].get("hotel_ids", [])

    if not hotel_ids:
        delete_message(queue_url, receipt_handle)
        return (0, 0, 0)

    try:
        # 1. Fetch from DB
        hotels = await service.get_hotels_by_ids(hotel_ids)
        if not hotels:
            delete_message(queue_url, receipt_handle)
            return (0, 0, 0)

        hotel_dicts = [
            {"id": h.id, "name": h.name, "website": h.website, "city": h.city or ""}
            for h in hotels
        ]

        all_results: List[DetectionResult] = []

        # 2. Pre-browser filters
        filtered_hotels = []
        for h in hotel_dicts:
            if is_non_hotel_name(h["name"]):
                all_results.append(DetectionResult(hotel_id=h["id"], error="non_hotel_name"))
            elif is_non_hotel_domain(h.get("website", "")):
                all_results.append(DetectionResult(hotel_id=h["id"], error="non_hotel_domain"))
            elif not h.get("website") or is_junk_domain(h.get("website", "")):
                all_results.append(DetectionResult(hotel_id=h["id"], error="junk_domain"))
            else:
                chain = get_chain_name(h.get("website", ""))
                if chain:
                    all_results.append(
                        DetectionResult(
                            hotel_id=h["id"],
                            booking_engine=chain,
                            detection_method="chain_domain",
                        )
                    )
                else:
                    filtered_hotels.append(h)

        if not filtered_hotels:
            await _save_and_ack(service, all_results, queue_url, receipt_handle)
            return (len(all_results), 0, 0)

        # 3. HTTP precheck
        urls_to_check = [(h["id"], normalize_url(h["website"])) for h in filtered_hotels]
        precheck_results = await batch_precheck(urls_to_check, concurrency=30)

        reachable_hotels = []
        for h in filtered_hotels:
            hid = h["id"]
            if hid in precheck_results:
                reachable, error = precheck_results[hid]
                if not reachable:
                    all_results.append(
                        DetectionResult(hotel_id=hid, error=f"precheck_failed: {error}")
                    )
                    continue
            reachable_hotels.append(h)

        if not reachable_hotels:
            await _save_and_ack(service, all_results, queue_url, receipt_handle)
            return (len(all_results), 0, 0)

        # 4. Dedup by website URL
        groups = group_hotels_by_website(reachable_hotels)
        dedup_saved = len(reachable_hotels) - len(groups)
        if dedup_saved > 0:
            logger.info(
                f"  Dedup: {len(reachable_hotels)} hotels -> "
                f"{len(groups)} unique URLs (saved {dedup_saved} visits)"
            )

        # 5. Process unique URLs via shared browser pool
        async def process_group(group_hotels: List[Dict]) -> List[DetectionResult]:
            rep = group_hotels[0]
            result = await process_hotel_with_pool(
                hotel=rep, pool=pool, config=config,
            )
            if len(group_hotels) > 1:
                return [result] + fan_out_result(
                    result, [h["id"] for h in group_hotels[1:]]
                )
            return [result]

        group_tasks = [process_group(gh) for gh in groups.values()]
        group_results = await asyncio.gather(*group_tasks, return_exceptions=True)

        for gr in group_results:
            if isinstance(gr, Exception):
                logger.error(f"Group processing error: {gr}")
            else:
                all_results.extend(gr)

        # 6. Save and ack
        detected, errors, retriable = await _save_and_ack(
            service, all_results, queue_url, receipt_handle
        )
        return (len(all_results), detected, errors)

    except Exception as e:
        logger.error(f"Error processing message (will retry): {e}")
        error_str = str(e).lower()
        if any(x in error_str for x in ["database", "connection", "asyncpg", "postgres"]):
            slack.send_error("Detection Consumer", f"DB error: {e}")
        raise


async def _save_and_ack(
    service: Service,
    results: List[DetectionResult],
    queue_url: str,
    receipt_handle: str,
) -> Tuple[int, int, int]:
    """Save results with retry, then ack SQS message. Returns (detected, errors, retriable)."""
    detected, errors, retriable = 0, 0, 0

    for attempt in range(3):
        try:
            detected, errors, retriable = await service.save_detection_results(results)
            break
        except Exception as e:
            error_str = str(e).lower()
            is_transient = any(
                x in error_str for x in ["connection", "closed", "timeout", "reset"]
            )
            if is_transient and attempt < 2:
                logger.warning(f"DB save failed (attempt {attempt + 1}), retrying: {e}")
                await asyncio.sleep(2 ** (attempt + 1))
            else:
                raise

    if retriable > 0:
        logger.info(f"  {retriable} retriable errors — leaving for SQS retry")
    else:
        delete_message(queue_url, receipt_handle)

    return (detected, errors, retriable)


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------


async def worker_loop(
    pool_size: int = DEFAULT_POOL_SIZE,
    idle_timeout: int = DEFAULT_IDLE_TIMEOUT,
    max_messages: int = 0,
    debug: bool = False,
    notify: bool = True,
):
    """Poll SQS, process hotels via persistent browser, exit on idle."""
    global shutdown_requested

    await init_db()
    pool = BrowserPool(pool_size=pool_size, headless=True)

    try:
        service = Service()
        queue_url = get_queue_url()

        # Load engine patterns
        patterns = await service.get_engine_patterns()
        set_engine_patterns(patterns)

        # Start browser
        await pool.start()

        config = DetectionConfig(
            concurrency=pool_size,
            headless=True,
            debug=debug,
        )

        logger.info(f"Consumer ready (pool_size={pool_size}, idle_timeout={idle_timeout}s)")
        logger.info(f"Queue: {queue_url}")

        total_processed = 0
        total_detected = 0
        total_errors = 0
        message_count = 0
        idle_start: Optional[float] = None

        while not shutdown_requested:
            if max_messages > 0 and message_count >= max_messages:
                logger.info(f"Reached max messages limit ({max_messages})")
                break

            messages = receive_messages(
                queue_url=queue_url,
                max_messages=min(10, pool_size),
                wait_time_seconds=20,
                visibility_timeout=900,
            )

            if not messages:
                if idle_start is None:
                    idle_start = time.time()
                    attrs = get_queue_attributes(queue_url)
                    pending = int(attrs.get("ApproximateNumberOfMessages", 0))
                    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                    logger.info(f"No messages (pending={pending}, in_flight={in_flight})")

                elapsed = time.time() - idle_start
                if elapsed >= idle_timeout:
                    logger.info(f"Idle for {elapsed:.0f}s >= {idle_timeout}s, exiting")
                    break
                continue

            # Reset idle timer
            idle_start = None

            # Process messages concurrently
            tasks = [
                process_message(
                    service=service,
                    message=msg,
                    queue_url=queue_url,
                    pool=pool,
                    config=config,
                )
                for msg in messages
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    total_errors += 1
                    error_str = str(result).lower()
                    if any(
                        x in error_str
                        for x in ["database", "connection", "asyncpg", "postgres"]
                    ):
                        slack.send_error("Detection Consumer", f"Critical: {result}")
                else:
                    processed, detected, errors = result
                    total_processed += processed
                    total_detected += detected
                    total_errors += errors

            message_count += len(messages)
            logger.info(
                f"Batch done ({len(messages)} msgs) | "
                f"Total: {total_processed} hotels, {total_detected} detected, "
                f"{total_errors} errors"
            )

        # Summary
        logger.info("=" * 60)
        logger.info("CONSUMER EXITING")
        logger.info(
            f"Messages: {message_count} | Hotels: {total_processed} | "
            f"Detected: {total_detected} | Errors: {total_errors}"
        )
        if total_processed > 0:
            logger.info(f"Hit rate: {total_detected / total_processed * 100:.1f}%")
        logger.info("=" * 60)

        if notify and total_processed > 0:
            hit_rate = (
                total_detected / total_processed * 100 if total_processed > 0 else 0
            )
            slack.send_message(
                f"*Detection Consumer Exit*\n"
                f"- Hotels: {total_processed}\n"
                f"- Detected: {total_detected} ({hit_rate:.1f}%)\n"
                f"- Errors: {total_errors}"
            )

    except Exception as e:
        logger.error(f"Consumer fatal error: {e}")
        if notify:
            slack.send_error("Detection Consumer", str(e))
        raise
    finally:
        await pool.close()
        await close_db()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Detection consumer for ECS Fargate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -m workflows.detection_consumer --pool-size 10
  uv run python -m workflows.detection_consumer --pool-size 3 --max-messages 2 --debug
  uv run python -m workflows.detection_consumer --idle-timeout 60

Environment:
  SQS_DETECTION_QUEUE_URL - Required.
  AWS_REGION - Optional. Defaults to eu-north-1.
        """,
    )

    parser.add_argument(
        "--pool-size",
        type=int,
        default=DEFAULT_POOL_SIZE,
        help=f"Browser context pool size (default: {DEFAULT_POOL_SIZE})",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=DEFAULT_IDLE_TIMEOUT,
        help=f"Exit after N seconds idle (default: {DEFAULT_IDLE_TIMEOUT})",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Process at most N messages then exit (0=unlimited)",
    )
    parser.add_argument("--debug", "-d", action="store_true")
    parser.add_argument("--no-notify", action="store_true")

    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info(f"Starting consumer (pool_size={args.pool_size}, idle_timeout={args.idle_timeout}s)")

    asyncio.run(
        worker_loop(
            pool_size=args.pool_size,
            idle_timeout=args.idle_timeout,
            max_messages=args.max_messages,
            debug=args.debug,
            notify=not args.no_notify,
        )
    )


if __name__ == "__main__":
    main()
