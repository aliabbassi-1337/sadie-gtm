#!/usr/bin/env python3
"""
RMS scan consumer - pulls ID range jobs from SQS and processes them.

Run this on each EC2 instance. Workers automatically pull jobs from the queue,
so work is distributed evenly without manual coordination.

Usage:
    # Run consumer (pulls jobs until queue is empty)
    uv run python -m workflows.rms_consumer
    
    # Run with custom settings
    uv run python -m workflows.rms_consumer --concurrency 10 --delay 0.2
    
    # Process a fixed number of jobs then exit
    uv run python -m workflows.rms_consumer --max-jobs 10
"""

import argparse
import asyncio
import os
import signal
import sys
from typing import Optional

from loguru import logger


# Queue URL
RMS_QUEUE_URL = os.getenv(
    "SQS_RMS_QUEUE_URL",
    os.getenv("SQS_DETECTION_QUEUE_URL", "")
).replace("detection-queue", "rms-scan-queue")


# Graceful shutdown
shutdown_requested = False

def handle_signal(signum, frame):
    global shutdown_requested
    logger.info("Shutdown requested, finishing current job...")
    shutdown_requested = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


async def process_job(job: dict, concurrency: int, delay: float) -> dict:
    """Process a single RMS scan job."""
    from services.leadgen.booking_engines import RMSScanner
    from services.leadgen import repo
    from db.client import get_conn
    
    start_id = job["start_id"]
    end_id = job["end_id"]
    subdomain = job.get("subdomain", "ibe13.rmscloud.com")
    
    logger.info(f"Processing IDs {start_id}-{end_id} on {subdomain}")
    
    # Get or create RMS booking engine
    engine = await repo.get_booking_engine_by_name("RMS Cloud")
    engine_id = engine.id if engine else None
    
    if not engine_id:
        engine_id = await repo.insert_booking_engine(name="RMS Cloud", tier=2)
    
    stats = {"scanned": 0, "found": 0, "saved": 0, "errors": 0}
    
    async def save_hotel(hotel: dict):
        """Save hotel to DB immediately when found."""
        nonlocal stats
        try:
            async with get_conn() as conn:
                # Check if already exists
                existing = await conn.fetchrow(
                    "SELECT id FROM sadie_gtm.hotels WHERE external_id = $1 AND external_id_type = 'rms_scan'",
                    f"rms_{hotel['id']}"
                )
                
                if existing:
                    stats["found"] += 1
                    return
                
            hotel_id = await repo.insert_hotel(
                name=hotel["name"],
                source="rms_scan",
                status=0,
                external_id=f"rms_{hotel['id']}",
                external_id_type="rms_scan",
            )
            
            if hotel_id:
                await repo.insert_hotel_booking_engine(
                    hotel_id=hotel_id,
                    booking_engine_id=engine_id,
                    booking_url=hotel["booking_url"],
                    engine_property_id=str(hotel["id"]),
                    detection_method="rms_scan",
                    status=1,
                )
                stats["saved"] += 1
                stats["found"] += 1
                
        except Exception as e:
            if "duplicate" not in str(e).lower():
                logger.error(f"Error saving hotel: {e}")
                stats["errors"] += 1
    
    # Run scan
    async with RMSScanner(concurrency=concurrency, delay=delay) as scanner:
        await scanner.scan_range(
            start_id=start_id,
            end_id=end_id,
            subdomain=subdomain,
            on_found=save_hotel,
        )
    
    stats["scanned"] = end_id - start_id + 1
    return stats


async def run_consumer(
    concurrency: int = 10,
    delay: float = 0.2,
    max_jobs: Optional[int] = None,
):
    """Main consumer loop - pulls jobs from SQS and processes them."""
    from db.client import init_db
    from infra.sqs import receive_messages, delete_message, get_queue_attributes
    
    await init_db()
    
    queue_url = RMS_QUEUE_URL
    if not queue_url:
        logger.error("SQS_RMS_QUEUE_URL or SQS_DETECTION_QUEUE_URL not set")
        return
    
    logger.info(f"Starting RMS consumer")
    logger.info(f"Queue: {queue_url}")
    logger.info(f"Settings: concurrency={concurrency}, delay={delay}s")
    
    jobs_processed = 0
    total_found = 0
    
    while not shutdown_requested:
        # Check if we've hit max jobs
        if max_jobs and jobs_processed >= max_jobs:
            logger.info(f"Reached max jobs ({max_jobs}), exiting")
            break
        
        # Receive messages
        try:
            messages = receive_messages(
                queue_url=queue_url,
                max_messages=1,  # Process one at a time for simplicity
                wait_time_seconds=20,
                visibility_timeout=600,  # 10 min timeout per job
            )
        except Exception as e:
            logger.error(f"Failed to receive messages: {e}")
            await asyncio.sleep(5)
            continue
        
        if not messages:
            # Check if queue is empty
            try:
                attrs = get_queue_attributes(queue_url)
                waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
                inflight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
                
                if waiting == 0 and inflight == 0:
                    logger.info("Queue empty, exiting")
                    break
                else:
                    logger.debug(f"Waiting for messages (queue: {waiting} waiting, {inflight} in-flight)")
            except Exception:
                pass
            continue
        
        # Process message
        for msg in messages:
            job = msg["body"]
            receipt_handle = msg["receipt_handle"]
            
            if job.get("type") != "rms_scan":
                logger.warning(f"Unknown job type: {job.get('type')}")
                delete_message(queue_url, receipt_handle)
                continue
            
            try:
                stats = await process_job(job, concurrency, delay)
                jobs_processed += 1
                total_found += stats["found"]
                
                logger.info(
                    f"Job complete: scanned {stats['scanned']}, "
                    f"found {stats['found']}, saved {stats['saved']}, "
                    f"errors {stats['errors']}"
                )
                
                # Delete from queue on success
                delete_message(queue_url, receipt_handle)
                
            except Exception as e:
                logger.error(f"Job failed: {e}")
                # Don't delete - will be retried after visibility timeout
    
    # Summary
    logger.info("=" * 50)
    logger.info("CONSUMER SHUTDOWN")
    logger.info(f"Jobs processed: {jobs_processed}")
    logger.info(f"Total properties found: {total_found}")


def main():
    parser = argparse.ArgumentParser(description="RMS scan consumer")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent requests per job")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between requests")
    parser.add_argument("--max-jobs", type=int, help="Max jobs to process before exiting")
    
    args = parser.parse_args()
    
    asyncio.run(run_consumer(
        concurrency=args.concurrency,
        delay=args.delay,
        max_jobs=args.max_jobs,
    ))


if __name__ == "__main__":
    main()
