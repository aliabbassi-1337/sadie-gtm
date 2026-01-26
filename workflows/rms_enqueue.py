#!/usr/bin/env python3
"""
Enqueue RMS ID ranges to SQS for distributed scanning.

This creates small ID range jobs that workers can pull and process.
Workers automatically distribute the work - no manual range splitting.

Usage:
    # Enqueue 25000 IDs in chunks of 500
    uv run python -m workflows.rms_enqueue --start 1 --end 25000 --chunk-size 500
    
    # Check queue status
    uv run python -m workflows.rms_enqueue --status
    
    # Clear the queue
    uv run python -m workflows.rms_enqueue --purge
"""

import argparse
import os
from typing import List, Dict

from loguru import logger


# Use a separate queue for RMS scanning (or reuse detection queue)
RMS_QUEUE_URL = os.getenv(
    "SQS_RMS_QUEUE_URL",
    os.getenv("SQS_DETECTION_QUEUE_URL", "")
).replace("detection-queue", "rms-scan-queue")


def main():
    parser = argparse.ArgumentParser(
        description="Enqueue RMS ID ranges to SQS"
    )
    
    parser.add_argument("--start", type=int, default=1, help="Start ID")
    parser.add_argument("--end", type=int, default=25000, help="End ID")
    parser.add_argument("--chunk-size", type=int, default=500, help="IDs per job (default: 500)")
    parser.add_argument("--subdomain", type=str, default="ibe13.rmscloud.com", help="RMS subdomain")
    parser.add_argument("--status", action="store_true", help="Show queue status")
    parser.add_argument("--purge", action="store_true", help="Clear the queue")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enqueued")
    
    args = parser.parse_args()
    
    from infra.sqs import send_messages_batch, get_queue_attributes, get_sqs_client
    
    queue_url = RMS_QUEUE_URL
    if not queue_url:
        logger.error("SQS_RMS_QUEUE_URL or SQS_DETECTION_QUEUE_URL not set")
        return
    
    # Status check
    if args.status:
        try:
            attrs = get_queue_attributes(queue_url)
            waiting = attrs.get("ApproximateNumberOfMessages", "?")
            inflight = attrs.get("ApproximateNumberOfMessagesNotVisible", "?")
            logger.info(f"Queue: {queue_url}")
            logger.info(f"Waiting: {waiting} | In-flight: {inflight}")
        except Exception as e:
            logger.error(f"Failed to get queue status: {e}")
        return
    
    # Purge queue
    if args.purge:
        try:
            client = get_sqs_client()
            client.purge_queue(QueueUrl=queue_url)
            logger.info(f"Purged queue: {queue_url}")
        except Exception as e:
            logger.error(f"Failed to purge queue: {e}")
        return
    
    # Create job messages
    jobs: List[Dict] = []
    for chunk_start in range(args.start, args.end + 1, args.chunk_size):
        chunk_end = min(chunk_start + args.chunk_size - 1, args.end)
        jobs.append({
            "type": "rms_scan",
            "start_id": chunk_start,
            "end_id": chunk_end,
            "subdomain": args.subdomain,
        })
    
    logger.info(f"Created {len(jobs)} jobs for IDs {args.start}-{args.end}")
    logger.info(f"Chunk size: {args.chunk_size} IDs per job")
    
    if args.dry_run:
        logger.info("Dry run - not sending to queue")
        logger.info(f"Sample jobs: {jobs[:3]}")
        return
    
    # Send to SQS
    logger.info(f"Sending to queue: {queue_url}")
    sent = send_messages_batch(queue_url, jobs)
    logger.info(f"Sent {sent}/{len(jobs)} jobs to queue")
    
    # Show queue status
    attrs = get_queue_attributes(queue_url)
    logger.info(f"Queue now has {attrs.get('ApproximateNumberOfMessages', '?')} messages waiting")


if __name__ == "__main__":
    main()
