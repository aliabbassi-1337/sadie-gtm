#!/usr/bin/env python3
"""Consume RMS ID ranges from queue and scan them.

Runs as a continuous service, pulling ID ranges from SQS and scanning them.

Usage:
    uv run python workflows/ingest_rms_consumer.py --concurrency 6
"""

import argparse
import asyncio
import json
import os
import signal
import sys
from pathlib import Path

# Add project root to path for imports when run as script
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from loguru import logger

from services.ingestor.ingestors.rms import RMSIngestor


def get_queue_url() -> str:
    url = os.getenv("SQS_RMS_INGEST_QUEUE_URL")
    if not url:
        raise ValueError("SQS_RMS_INGEST_QUEUE_URL environment variable not set")
    return url


class IngestConsumer:
    """Consumes ID ranges from SQS and scans them."""
    
    def __init__(self, concurrency: int = 6):
        self.concurrency = concurrency
        self.queue_url = get_queue_url()
        self.sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "eu-north-1"))
        self.ingestor = RMSIngestor()
        self._shutdown = False
    
    def request_shutdown(self):
        logger.info("Shutdown requested")
        self._shutdown = True
        self.ingestor.request_shutdown()
    
    async def run(self) -> dict:
        """Run the consumer loop."""
        stats = {"ranges_processed": 0, "hotels_found": 0, "hotels_saved": 0}
        
        logger.info(f"Starting RMS ingest consumer (concurrency={self.concurrency})")
        
        while not self._shutdown:
            # Receive messages (long polling)
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,  # Process one range at a time
                WaitTimeSeconds=20,
                VisibilityTimeout=3600,  # 1 hour to process
            )
            
            messages = response.get("Messages", [])
            if not messages:
                logger.info("Queue empty, waiting...")
                continue
            
            for msg in messages:
                if self._shutdown:
                    break
                
                try:
                    body = json.loads(msg["Body"])
                    start_id = body["start"]
                    end_id = body["end"]
                    
                    logger.info(f"Processing range: {start_id}-{end_id}")
                    
                    result = await self.ingestor.ingest(
                        start_id=start_id,
                        end_id=end_id,
                        concurrency=self.concurrency,
                        dry_run=False,
                    )
                    
                    stats["ranges_processed"] += 1
                    stats["hotels_found"] += result.hotels_found
                    stats["hotels_saved"] += result.hotels_saved
                    
                    logger.success(f"Completed range {start_id}-{end_id}: found={result.hotels_found}, saved={result.hotels_saved}")
                    
                    # Delete message on success
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing range: {e}")
                    # Don't delete - will be retried after visibility timeout
        
        logger.info(f"Consumer stopped: {stats}")
        return stats


async def run(args):
    consumer = IngestConsumer(concurrency=args.concurrency)
    
    # Handle shutdown signals
    def handle_signal(sig, frame):
        consumer.request_shutdown()
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    await consumer.run()


def main():
    parser = argparse.ArgumentParser(description="Consume RMS ID ranges from queue")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent scanners (default: 6)")
    
    args = parser.parse_args()
    
    logger.info(f"Starting RMS ingest consumer (concurrency={args.concurrency})")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
