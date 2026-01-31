#!/usr/bin/env python3
"""Enqueue RMS slugs to SQS for distributed ingestion.

Reads slugs from S3 or local file and pushes them to SQS in batches.
Each message contains a batch of slugs for a worker to process.

Usage:
    # Enqueue from S3 discovered slugs
    uv run python -m workflows.enqueue_rms_slugs --s3 crawl-data/rms_archive_discovery_20260130.txt
    
    # Enqueue from local JSON file  
    uv run python -m workflows.enqueue_rms_slugs --input /tmp/rms_slugs.json
    
    # Enqueue with custom batch size
    uv run python -m workflows.enqueue_rms_slugs --s3 crawl-data/rms.txt --batch-size 50
"""

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from loguru import logger


SQS_QUEUE_NAME = "sadie-gtm-rms-slug-ingest"
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = "sadie-gtm"


def get_or_create_queue() -> str:
    """Get or create the SQS queue for RMS slug ingestion."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    
    try:
        response = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        return response["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        logger.info(f"Creating queue: {SQS_QUEUE_NAME}")
        response = sqs.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "1800",  # 30 min
                "MessageRetentionPeriod": "86400",  # 1 day
            }
        )
        return response["QueueUrl"]


def load_slugs_from_s3(key: str) -> list[str]:
    """Load slugs from S3 file."""
    s3 = boto3.client("s3")
    
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = obj["Body"].read().decode("utf-8")
    
    slugs = []
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        
        # Extract slug from URL if needed
        match = re.search(r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+)", line, re.IGNORECASE)
        if match:
            slugs.append(match.group(1).upper())
        elif not line.startswith("http"):
            # Assume it's a raw slug
            slugs.append(line.upper())
    
    return list(set(slugs))  # Dedupe


def load_slugs_from_json(path: str) -> list[str]:
    """Load slugs from JSON file."""
    with open(path) as f:
        data = json.load(f)
    
    slugs = []
    for engine_slugs in data.get("engines", {}).values():
        for item in engine_slugs:
            if isinstance(item, dict):
                slugs.append(item.get("slug", "").upper())
            else:
                slugs.append(str(item).upper())
    
    return list(set(slugs))


async def enqueue_slugs(
    slugs: list[str],
    batch_size: int = 50,
    dry_run: bool = False,
) -> int:
    """Enqueue slugs to SQS in batches."""
    if not slugs:
        logger.warning("No slugs to enqueue")
        return 0
    
    queue_url = get_or_create_queue()
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    
    logger.info(f"Enqueueing {len(slugs)} slugs in batches of {batch_size}")
    
    messages_sent = 0
    
    for i in range(0, len(slugs), batch_size):
        batch = slugs[i:i + batch_size]
        
        message = {
            "slugs": batch,
            "source": "archive_discovery",
        }
        
        if dry_run:
            logger.debug(f"Would enqueue batch {i // batch_size + 1}: {len(batch)} slugs")
        else:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message),
            )
        
        messages_sent += 1
    
    logger.info(f"Enqueued {messages_sent} messages ({len(slugs)} total slugs)")
    return messages_sent


async def main():
    parser = argparse.ArgumentParser(description="Enqueue RMS slugs to SQS")
    
    parser.add_argument("--s3", type=str, help="S3 key to load slugs from (e.g., crawl-data/rms.txt)")
    parser.add_argument("--input", type=str, help="Local JSON file to load slugs from")
    parser.add_argument("--batch-size", type=int, default=50, help="Slugs per SQS message (default: 50)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    
    args = parser.parse_args()
    
    if not args.s3 and not args.input:
        parser.error("Must provide either --s3 or --input")
    
    # Load slugs
    if args.s3:
        logger.info(f"Loading slugs from S3: {args.s3}")
        slugs = load_slugs_from_s3(args.s3)
    else:
        logger.info(f"Loading slugs from file: {args.input}")
        slugs = load_slugs_from_json(args.input)
    
    logger.info(f"Loaded {len(slugs)} unique slugs")
    
    # Enqueue
    await enqueue_slugs(slugs, batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
