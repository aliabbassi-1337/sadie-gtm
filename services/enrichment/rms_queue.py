"""RMS Queue operations.

Abstracts SQS queue operations for RMS enrichment.
"""

from typing import Optional, List, Dict, Any, Protocol, runtime_checkable, Callable
from pydantic import BaseModel

from loguru import logger

from infra.sqs import (
    send_message,
    receive_messages,
    delete_message,
    get_queue_url,
    get_queue_attributes,
)
from services.enrichment.rms_repo import RMSHotelRecord


QUEUE_NAME = "sadie-gtm-rms-enrichment"
DEFAULT_VISIBILITY_TIMEOUT = 3600  # 1 hour
DEFAULT_WAIT_TIME = 20


class QueueStats(BaseModel):
    """Queue statistics."""
    pending: int
    in_flight: int


class QueueMessage(BaseModel):
    """Message from queue."""
    receipt_handle: str
    hotels: List[RMSHotelRecord]


@runtime_checkable
class IRMSQueue(Protocol):
    """Protocol for RMS queue operations."""
    
    def get_stats(self) -> QueueStats:
        """Get queue statistics."""
        ...
    
    def enqueue_hotels(
        self,
        hotels: List[RMSHotelRecord],
        batch_size: int = 10,
    ) -> int:
        """Enqueue hotels for enrichment. Returns count enqueued."""
        ...
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        """Receive messages from queue."""
        ...
    
    def delete_message(self, receipt_handle: str) -> None:
        """Delete a processed message."""
        ...


class RMSQueue(IRMSQueue):
    """SQS implementation of RMS queue."""
    
    def __init__(
        self,
        queue_name: str = QUEUE_NAME,
        visibility_timeout: int = DEFAULT_VISIBILITY_TIMEOUT,
        wait_time: int = DEFAULT_WAIT_TIME,
    ):
        self.queue_name = queue_name
        self.visibility_timeout = visibility_timeout
        self.wait_time = wait_time
        self._queue_url: Optional[str] = None
    
    @property
    def queue_url(self) -> str:
        if not self._queue_url:
            self._queue_url = get_queue_url(self.queue_name)
        return self._queue_url
    
    def get_stats(self) -> QueueStats:
        """Get queue statistics."""
        attrs = get_queue_attributes(self.queue_url)
        return QueueStats(
            pending=int(attrs.get("ApproximateNumberOfMessages", 0)),
            in_flight=int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        )
    
    def enqueue_hotels(
        self,
        hotels: List[RMSHotelRecord],
        batch_size: int = 10,
    ) -> int:
        """Enqueue hotels for enrichment."""
        enqueued = 0
        
        for i in range(0, len(hotels), batch_size):
            batch = hotels[i:i + batch_size]
            message = {
                "hotels": [
                    {"hotel_id": h.hotel_id, "booking_url": h.booking_url}
                    for h in batch
                ]
            }
            send_message(self.queue_url, message)
            enqueued += len(batch)
            
            if enqueued % 100 == 0:
                logger.info(f"Enqueued {enqueued}/{len(hotels)} hotels")
        
        return enqueued
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        """Receive messages from queue."""
        raw_messages = receive_messages(
            self.queue_url,
            max_messages=min(max_messages, 10),  # SQS limit
            visibility_timeout=self.visibility_timeout,
            wait_time=self.wait_time,
        )
        
        messages = []
        for msg in raw_messages:
            hotels_data = msg["body"].get("hotels", [])
            hotels = [
                RMSHotelRecord(hotel_id=h["hotel_id"], booking_url=h["booking_url"])
                for h in hotels_data
            ]
            messages.append(QueueMessage(
                receipt_handle=msg["receipt_handle"],
                hotels=hotels,
            ))
        
        return messages
    
    def delete_message(self, receipt_handle: str) -> None:
        """Delete a processed message."""
        delete_message(self.queue_url, receipt_handle)


class MockQueue(IRMSQueue):
    """Mock queue for unit testing."""
    
    def __init__(self):
        self._messages: List[QueueMessage] = []
        self._enqueued: List[List[RMSHotelRecord]] = []
        self._deleted: List[str] = []
        self.pending = 0
        self.in_flight = 0
    
    def add_message(self, hotels: List[RMSHotelRecord]) -> str:
        """Add a message to the mock queue."""
        receipt = f"mock-receipt-{len(self._messages)}"
        self._messages.append(QueueMessage(receipt_handle=receipt, hotels=hotels))
        self.pending += 1
        return receipt
    
    def get_stats(self) -> QueueStats:
        return QueueStats(pending=self.pending, in_flight=self.in_flight)
    
    def enqueue_hotels(self, hotels: List[RMSHotelRecord], batch_size: int = 10) -> int:
        self._enqueued.append(hotels)
        self.pending += len(hotels)
        return len(hotels)
    
    def receive_messages(self, max_messages: int = 10) -> List[QueueMessage]:
        messages = self._messages[:max_messages]
        self._messages = self._messages[max_messages:]
        self.pending -= len(messages)
        self.in_flight += len(messages)
        return messages
    
    def delete_message(self, receipt_handle: str) -> None:
        self._deleted.append(receipt_handle)
        self.in_flight = max(0, self.in_flight - 1)
