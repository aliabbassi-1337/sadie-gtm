"""RMS Queue - SQS operations for RMS enrichment."""

import os
from typing import Optional, List

from lib.rms import RMSHotelRecord, QueueStats, QueueMessage
from infra.sqs import (
    send_message,
    receive_messages,
    delete_message,
    get_queue_attributes,
)


class RMSQueue:
    """SQS operations for RMS enrichment."""
    
    def __init__(self):
        self._queue_url: Optional[str] = None
    
    @property
    def queue_url(self) -> str:
        if not self._queue_url:
            self._queue_url = os.getenv("SQS_RMS_ENRICHMENT_QUEUE_URL")
            if not self._queue_url:
                raise ValueError("SQS_RMS_ENRICHMENT_QUEUE_URL environment variable not set")
        return self._queue_url
    
    def get_stats(self) -> QueueStats:
        """Get queue statistics."""
        attrs = get_queue_attributes(self.queue_url)
        return QueueStats(
            pending=int(attrs.get("ApproximateNumberOfMessages", 0)),
            in_flight=int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        )
    
    def enqueue_hotels(self, hotels: List[RMSHotelRecord], batch_size: int = 10) -> int:
        """Enqueue hotels for enrichment."""
        enqueued = 0
        for i in range(0, len(hotels), batch_size):
            batch = hotels[i:i + batch_size]
            message = {"hotels": [{"hotel_id": h.hotel_id, "booking_url": h.booking_url} for h in batch]}
            send_message(self.queue_url, message)
            enqueued += len(batch)
        return enqueued
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        """Receive messages from queue."""
        raw_messages = receive_messages(
            self.queue_url,
            max_messages=min(max_messages, 10),
            visibility_timeout=3600,
            wait_time_seconds=20,
        )
        messages = []
        for msg in raw_messages:
            hotels_data = msg["body"].get("hotels", [])
            hotels = [RMSHotelRecord(hotel_id=h["hotel_id"], booking_url=h["booking_url"]) for h in hotels_data]
            messages.append(QueueMessage(receipt_handle=msg["receipt_handle"], hotels=hotels))
        return messages
    
    def delete_message(self, receipt_handle: str) -> None:
        """Delete a processed message."""
        delete_message(self.queue_url, receipt_handle)


class MockQueue:
    """Mock queue for testing."""
    
    def __init__(self):
        self._messages: List[QueueMessage] = []
        self.pending = 0
        self.in_flight = 0
    
    def get_stats(self) -> QueueStats:
        return QueueStats(pending=self.pending, in_flight=self.in_flight)
    
    def enqueue_hotels(self, hotels: List[RMSHotelRecord], batch_size: int = 10) -> int:
        self.pending += len(hotels)
        return len(hotels)
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        messages = self._messages[:max_messages]
        self._messages = self._messages[max_messages:]
        self.pending -= len(messages)
        self.in_flight += len(messages)
        return messages
    
    def delete_message(self, receipt_handle: str) -> None:
        self.in_flight = max(0, self.in_flight - 1)
