#!/usr/bin/env python3
"""Enqueue RMS ID ranges for distributed scanning.

Usage:
    # Enqueue ranges 0-100000 in chunks of 1000
    uv run python workflows/ingest_rms_enqueue.py --start 0 --end 100000 --chunk-size 1000
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add project root to path for imports when run as script
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from loguru import logger


def get_queue_url() -> str:
    url = os.getenv("SQS_RMS_INGEST_QUEUE_URL")
    if not url:
        raise ValueError("SQS_RMS_INGEST_QUEUE_URL environment variable not set")
    return url


def enqueue_ranges(start: int, end: int, chunk_size: int, dry_run: bool = False) -> int:
    """Enqueue ID ranges to SQS."""
    queue_url = get_queue_url()
    sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "eu-north-1"))
    
    ranges = []
    for chunk_start in range(start, end, chunk_size):
        chunk_end = min(chunk_start + chunk_size, end)
        ranges.append({"start": chunk_start, "end": chunk_end})
    
    logger.info(f"Enqueueing {len(ranges)} ranges ({start}-{end}, chunk_size={chunk_size})")
    
    if dry_run:
        for r in ranges[:5]:
            logger.info(f"  Would enqueue: {r['start']}-{r['end']}")
        if len(ranges) > 5:
            logger.info(f"  ... and {len(ranges) - 5} more")
        return 0
    
    # Send in batches of 10 (SQS limit)
    enqueued = 0
    for i in range(0, len(ranges), 10):
        batch = ranges[i:i + 10]
        entries = [
            {
                "Id": str(idx),
                "MessageBody": json.dumps(msg),
            }
            for idx, msg in enumerate(batch)
        ]
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        enqueued += len(response.get("Successful", []))
        
        if response.get("Failed"):
            for f in response["Failed"]:
                logger.error(f"Failed: {f}")
    
    logger.success(f"Enqueued {enqueued} ranges")
    return enqueued


def main():
    parser = argparse.ArgumentParser(description="Enqueue RMS ID ranges for scanning")
    parser.add_argument("--start", type=int, required=True, help="Start ID")
    parser.add_argument("--end", type=int, required=True, help="End ID")
    parser.add_argument("--chunk-size", type=int, default=1000, help="IDs per chunk (default: 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually enqueue")
    
    args = parser.parse_args()
    
    enqueue_ranges(args.start, args.end, args.chunk_size, args.dry_run)


if __name__ == "__main__":
    main()
