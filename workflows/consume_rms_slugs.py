#!/usr/bin/env python3
"""Consume RMS slugs from SQS and ingest them.

Runs as a continuous service, polling SQS for slug batches to process.
Designed to run on multiple EC2 instances for distributed ingestion.

Usage:
    uv run python -m workflows.consume_rms_slugs --concurrency 5
"""

import argparse
import asyncio
import json
import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from loguru import logger

from services.ingestor.ingestors.rms import RMSIngestor


SQS_QUEUE_NAME = "sadie-gtm-rms-slug-ingest"
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")


def get_queue_url() -> str:
    """Get the SQS queue URL."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    try:
        response = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        return response["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        raise ValueError(f"Queue {SQS_QUEUE_NAME} does not exist. Run enqueue_rms_slugs.py first.")


class RMSSlugConsumer:
    """Consumes slug batches from SQS and ingests them."""
    
    def __init__(self, concurrency: int = 10, batch_concurrency: int = 3):
        self.concurrency = concurrency  # Concurrent API calls per batch
        self.batch_concurrency = batch_concurrency  # Concurrent batches
        self.queue_url = get_queue_url()
        self.sqs = boto3.client("sqs", region_name=AWS_REGION)
        self._shutdown = False
        
        # Stats
        self.messages_processed = 0
        self.slugs_processed = 0
        self.hotels_found = 0
        self.hotels_saved = 0
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown = True
    
    async def _process_message(self, msg: dict) -> bool:
        """Process a single message. Returns True on success."""
        try:
            body = json.loads(msg["Body"])
            slugs = body.get("slugs", [])
            source = body.get("source", "sqs")
            
            logger.info(f"Processing batch: {len(slugs)} slugs")
            
            # Create a new ingestor per batch to avoid state issues
            ingestor = RMSIngestor()
            
            result = await ingestor.ingest_slugs(
                slugs=slugs,
                source_name=source,
                concurrency=self.concurrency,
                use_api=True,
                dry_run=False,
            )
            
            self.messages_processed += 1
            self.slugs_processed += len(slugs)
            self.hotels_found += result.get("found", 0)
            self.hotels_saved += result.get("saved", 0)
            
            logger.success(
                f"Batch complete: found={result.get('found', 0)}, saved={result.get('saved', 0)} | "
                f"Total: {self.hotels_saved} saved from {self.slugs_processed} slugs"
            )
            
            # Delete message on success
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return False
    
    async def run(self):
        """Run the consumer loop with concurrent batch processing."""
        logger.info(f"Starting RMS slug consumer (api_concurrency={self.concurrency}, batch_concurrency={self.batch_concurrency})")
        logger.info(f"Queue: {self.queue_url}")
        
        semaphore = asyncio.Semaphore(self.batch_concurrency)
        pending_tasks = set()
        
        while not self._shutdown:
            # Long poll for multiple messages
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=min(10, self.batch_concurrency * 2),  # Up to 10 messages
                WaitTimeSeconds=20,
                VisibilityTimeout=1800,  # 30 min to process
            )
            
            messages = response.get("Messages", [])
            
            if not messages:
                # Wait for any pending tasks before continuing
                if pending_tasks:
                    done, pending_tasks = await asyncio.wait(
                        pending_tasks, timeout=1.0, return_when=asyncio.FIRST_COMPLETED
                    )
                else:
                    logger.debug("Queue empty, waiting...")
                continue
            
            for msg in messages:
                if self._shutdown:
                    break
                
                async def process_with_semaphore(m):
                    async with semaphore:
                        return await self._process_message(m)
                
                task = asyncio.create_task(process_with_semaphore(msg))
                pending_tasks.add(task)
                task.add_done_callback(pending_tasks.discard)
            
            # Clean up completed tasks
            if pending_tasks:
                done, pending_tasks = await asyncio.wait(
                    pending_tasks, timeout=0.1, return_when=asyncio.FIRST_COMPLETED
                )
        
        # Wait for remaining tasks on shutdown
        if pending_tasks:
            logger.info(f"Waiting for {len(pending_tasks)} pending tasks...")
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        
        # Final stats
        logger.info("=" * 50)
        logger.info("CONSUMER STOPPED")
        logger.info("=" * 50)
        logger.info(f"Messages processed: {self.messages_processed}")
        logger.info(f"Slugs processed: {self.slugs_processed}")
        logger.info(f"Hotels found: {self.hotels_found}")
        logger.info(f"Hotels saved: {self.hotels_saved}")


async def main():
    parser = argparse.ArgumentParser(description="Consume RMS slugs from SQS")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent API calls per batch (default: 10)")
    parser.add_argument("--batch-concurrency", type=int, default=3, help="Concurrent batches (default: 3)")
    
    args = parser.parse_args()
    
    consumer = RMSSlugConsumer(concurrency=args.concurrency, batch_concurrency=args.batch_concurrency)
    
    # Handle shutdown signals
    def handle_signal(sig, frame):
        consumer.request_shutdown()
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
