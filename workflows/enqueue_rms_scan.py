#!/usr/bin/env python3
"""Enqueue RMS ID ranges to SQS for distributed scanning.

Splits a large ID range into chunks and pushes them to SQS.
Each message contains a start/end range for a worker to scan.
Designed to be consumed by 7+ EC2 instances in parallel.

Usage:
    # Enqueue range 1-100000 in chunks of 1000 IDs
    uv run python -m workflows.enqueue_rms_scan --start 1 --end 100000 --chunk-size 1000
    
    # Dry run to see what would be enqueued
    uv run python -m workflows.enqueue_rms_scan --start 1 --end 50000 --dry-run
    
    # Check queue status
    uv run python -m workflows.enqueue_rms_scan --status
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from loguru import logger


SQS_QUEUE_NAME = "sadie-gtm-rms-scan"
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")


def get_or_create_queue() -> str:
    """Get or create the SQS queue for RMS scanning."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    
    try:
        response = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        return response["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        logger.info(f"Creating queue: {SQS_QUEUE_NAME}")
        response = sqs.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "1800",  # 30 min per chunk
                "MessageRetentionPeriod": "86400",  # 1 day
            }
        )
        return response["QueueUrl"]


def get_queue_status() -> dict:
    """Get queue status."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    
    try:
        queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
        )["Attributes"]
        
        return {
            "pending": int(attrs.get("ApproximateNumberOfMessages", 0)),
            "in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        }
    except sqs.exceptions.QueueDoesNotExist:
        return {"pending": 0, "in_flight": 0, "exists": False}


def enqueue_ranges(
    start_id: int,
    end_id: int,
    chunk_size: int = 1000,
    dry_run: bool = False,
) -> int:
    """Enqueue ID ranges to SQS.
    
    Args:
        start_id: First ID to scan
        end_id: Last ID to scan (inclusive)
        chunk_size: IDs per chunk/message
        dry_run: Preview without sending
        
    Returns:
        Number of messages enqueued
    """
    total_ids = end_id - start_id + 1
    num_chunks = (total_ids + chunk_size - 1) // chunk_size
    
    logger.info(f"Splitting {total_ids} IDs into {num_chunks} chunks of ~{chunk_size} IDs")
    
    if not dry_run:
        queue_url = get_or_create_queue()
        sqs = boto3.client("sqs", region_name=AWS_REGION)
    
    messages_sent = 0
    
    for chunk_start in range(start_id, end_id + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_id)
        
        message = {
            "start_id": chunk_start,
            "end_id": chunk_end,
            "chunk_size": chunk_end - chunk_start + 1,
        }
        
        if dry_run:
            logger.debug(f"Would enqueue: {chunk_start}-{chunk_end} ({chunk_end - chunk_start + 1} IDs)")
        else:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
            )
        
        messages_sent += 1
    
    logger.info(f"{'Would enqueue' if dry_run else 'Enqueued'} {messages_sent} messages")
    return messages_sent


async def main():
    parser = argparse.ArgumentParser(description="Enqueue RMS ID ranges to SQS for distributed scanning")
    
    parser.add_argument("--start", type=int, default=1, help="Start ID (default: 1)")
    parser.add_argument("--end", type=int, default=100000, help="End ID (default: 100000)")
    parser.add_argument("--chunk-size", type=int, default=1000, help="IDs per chunk (default: 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    parser.add_argument("--status", action="store_true", help="Just show queue status")
    
    args = parser.parse_args()
    
    if args.status:
        status = get_queue_status()
        logger.info(f"Queue status: {status['pending']} pending, {status['in_flight']} in-flight")
        return
    
    # Check queue first
    status = get_queue_status()
    if status["pending"] > 0 or status["in_flight"] > 0:
        logger.warning(f"Queue has {status['pending']} pending, {status['in_flight']} in-flight")
        if not args.dry_run:
            response = input("Continue anyway? [y/N]: ")
            if response.lower() != "y":
                logger.info("Aborted")
                return
    
    # Estimate time
    total_ids = args.end - args.start + 1
    # With 7 servers at ~100 IDs/sec each = 700 IDs/sec
    est_seconds = total_ids / 700
    est_minutes = est_seconds / 60
    
    logger.info(f"Range: {args.start}-{args.end} ({total_ids} IDs)")
    logger.info(f"Estimated time with 7 servers: ~{est_minutes:.1f} minutes")
    
    enqueue_ranges(
        start_id=args.start,
        end_id=args.end,
        chunk_size=args.chunk_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    asyncio.run(main())
