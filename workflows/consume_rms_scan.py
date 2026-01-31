#!/usr/bin/env python3
"""Consume RMS ID ranges from SQS and scan them.

Runs as a continuous service, polling SQS for ID ranges to scan.
Designed to run on multiple EC2 instances (7+) for distributed scanning.

Each message contains a start/end range. The scanner uses the fast
OnlineApi method (~100ms per ID) to find valid RMS properties.

Usage:
    # Run scanner with default settings
    uv run python -m workflows.consume_rms_scan
    
    # Custom concurrency and delay
    uv run python -m workflows.consume_rms_scan --concurrency 20 --delay 0.1
    
    # Single message mode (process one chunk and exit)
    uv run python -m workflows.consume_rms_scan --max-messages 1
    
    # Force Brightdata proxy from the start
    uv run python -m workflows.consume_rms_scan --brightdata
    
    # Disable auto-switch to Brightdata (direct only)
    uv run python -m workflows.consume_rms_scan --no-auto-brightdata

By default, auto_brightdata is enabled: starts with direct API calls,
automatically switches to Brightdata proxy after 10 consecutive rate limit errors.

Environment variables for Brightdata:
    BRIGHTDATA_ZONE: Zone name from Brightdata dashboard
    BRIGHTDATA_ZONE_PASSWORD: Zone password
    BRIGHTDATA_CUSTOMER_ID: Customer ID
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

from db.client import init_db, close_db
from lib.rms.scanner import RMSScanner


SQS_QUEUE_NAME = "sadie-gtm-rms-scan"
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
VISIBILITY_TIMEOUT = 1800  # 30 min per chunk


def get_queue_url() -> str:
    """Get the SQS queue URL."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    try:
        response = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
        return response["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        raise ValueError(f"Queue {SQS_QUEUE_NAME} does not exist. Run enqueue_rms_scan.py first.")


BATCH_SIZE = 50  # Batch insert threshold


class RMSScanConsumer:
    """Consumes ID ranges from SQS and scans them."""
    
    def __init__(
        self,
        concurrency: int = 20,
        delay: float = 0.1,
        save_to_db: bool = True,
        use_brightdata: bool = False,
        auto_brightdata: bool = True,  # Auto-switch to Brightdata on rate limiting
    ):
        self.concurrency = concurrency
        self.delay = delay
        self.save_to_db = save_to_db
        self.use_brightdata = use_brightdata
        self.auto_brightdata = auto_brightdata
        self.queue_url = get_queue_url()
        self.sqs = boto3.client("sqs", region_name=AWS_REGION)
        self._shutdown = False
        
        # Stats
        self.messages_processed = 0
        self.ids_scanned = 0
        self.hotels_found = 0
        self.hotels_saved = 0
        
        # DB resources
        self._engine_id = None
        
        # Batch buffer for hotels
        self._hotel_buffer: list = []
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown = True
    
    async def _init_db(self):
        """Initialize database and get RMS booking engine ID."""
        if not self.save_to_db:
            return
        
        await init_db()
        
        from db.client import queries, get_conn
        
        async with get_conn() as conn:
            result = await queries.get_rms_booking_engine_id(conn)
            if result:
                self._engine_id = result["id"]
            else:
                raise ValueError("RMS Cloud booking engine not found in database")
    
    async def _collect_hotel(self, hotel: dict):
        """Collect a found hotel into the buffer, flush when full."""
        if not self.save_to_db:
            return
        
        self._hotel_buffer.append(hotel)
        
        if len(self._hotel_buffer) >= BATCH_SIZE:
            await self._flush_buffer()
    
    async def _flush_buffer(self):
        """Batch insert all hotels in buffer."""
        if not self._hotel_buffer:
            return
        
        try:
            from services.ingestor import repo
            
            # Format: (name, source, external_id, external_id_type, booking_engine_id, booking_url, slug, detection_method)
            records = []
            for hotel in self._hotel_buffer:
                slug = hotel.get("slug", str(hotel.get("id", "")))
                booking_url = hotel.get("url") or f"https://bookings.rmscloud.com/Search/Index/{slug}/90/"
                records.append((
                    hotel.get("name", f"Unknown ({slug})"),
                    "rms_scan",
                    slug,  # external_id
                    "rms_scan",  # external_id_type
                    self._engine_id,
                    booking_url,
                    slug,  # engine_property_id
                    "api",  # detection_method
                ))
            
            saved = await repo.batch_insert_crawled_hotels(records)
            self.hotels_saved += saved
            logger.info(f"Batch inserted {saved} hotels (buffer had {len(self._hotel_buffer)})")
            
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
        finally:
            self._hotel_buffer = []
    
    async def _scan_range(self, start_id: int, end_id: int) -> int:
        """Scan an ID range and return count of hotels found."""
        found = 0
        
        async with RMSScanner(
            concurrency=self.concurrency,
            delay=self.delay,
            use_brightdata=self.use_brightdata,
            auto_brightdata=self.auto_brightdata,
        ) as scanner:
            results = await scanner.scan_range(
                start_id=start_id,
                end_id=end_id,
                on_found=self._collect_hotel if self.save_to_db else None,
            )
            found = len(results)
        
        # Flush remaining hotels in buffer
        await self._flush_buffer()
        
        return found
    
    async def run(self, max_messages: int = 0):
        """Run the consumer loop.
        
        Args:
            max_messages: Stop after this many messages (0 = infinite)
        """
        await self._init_db()
        
        logger.info(f"Starting RMS scan consumer (concurrency={self.concurrency}, delay={self.delay})")
        logger.info(f"Queue: {self.queue_url}")
        
        while not self._shutdown:
            if max_messages > 0 and self.messages_processed >= max_messages:
                logger.info(f"Reached max messages: {max_messages}")
                break
            
            # Long poll for messages
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
            )
            
            messages = response.get("Messages", [])
            
            if not messages:
                logger.debug("Queue empty, waiting...")
                continue
            
            for msg in messages:
                if self._shutdown:
                    break
                
                try:
                    body = json.loads(msg["Body"])
                    start_id = body["start_id"]
                    end_id = body["end_id"]
                    chunk_size = end_id - start_id + 1
                    
                    logger.info(f"Scanning range {start_id}-{end_id} ({chunk_size} IDs)")
                    
                    # Scan the range
                    found = await self._scan_range(start_id, end_id)
                    
                    self.messages_processed += 1
                    self.ids_scanned += chunk_size
                    self.hotels_found += found
                    
                    logger.success(
                        f"Range {start_id}-{end_id} complete: found={found} | "
                        f"Total: {self.hotels_found} found, {self.hotels_saved} saved from {self.ids_scanned} IDs"
                    )
                    
                    # Delete message on success
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing range: {e}")
                    # Don't delete - will be retried after visibility timeout
        
        # Cleanup
        if self.save_to_db:
            await close_db()
        
        # Final stats
        logger.info("=" * 50)
        logger.info("CONSUMER STOPPED")
        logger.info("=" * 50)
        logger.info(f"Messages processed: {self.messages_processed}")
        logger.info(f"IDs scanned: {self.ids_scanned}")
        logger.info(f"Hotels found: {self.hotels_found}")
        logger.info(f"Hotels saved: {self.hotels_saved}")
        if self.ids_scanned > 0:
            logger.info(f"Hit rate: {self.hotels_found / self.ids_scanned * 100:.2f}%")


async def main():
    parser = argparse.ArgumentParser(description="Consume RMS ID ranges from SQS and scan them")
    
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent API calls (default: 20)")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between requests (default: 0.1s)")
    parser.add_argument("--max-messages", type=int, default=0, help="Max messages to process (0=infinite)")
    parser.add_argument("--no-db", action="store_true", help="Don't save to database (dry run)")
    parser.add_argument("--brightdata", action="store_true", help="Force Brightdata proxy from start")
    parser.add_argument("--no-auto-brightdata", action="store_true", help="Disable auto-switch to Brightdata on rate limit")
    
    args = parser.parse_args()
    
    consumer = RMSScanConsumer(
        concurrency=args.concurrency,
        delay=args.delay,
        save_to_db=not args.no_db,
        use_brightdata=args.brightdata,
        auto_brightdata=not args.no_auto_brightdata,
    )
    
    # Handle shutdown signals
    def handle_signal(sig, frame):
        consumer.request_shutdown()
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    await consumer.run(max_messages=args.max_messages)


if __name__ == "__main__":
    asyncio.run(main())
