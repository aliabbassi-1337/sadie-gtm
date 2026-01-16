"""Messages module - Type-safe SQS message handling.

This module provides a clean abstraction for SQS-powered async operations:

1. Define messages as dataclasses with type hints
2. Register handlers with the @handler decorator
3. Send/receive messages through a simple API

Quick Start:
    # Send a scrape message
    from messages import ScrapeCity, send_message

    send_message(ScrapeCity(
        city="miami_beach",
        state="florida",
        radius_km=15.0,
    ))

    # In a consumer, dispatch received messages
    from messages import dispatch

    await dispatch({"_type": "ScrapeCity", "city": "miami_beach", ...})

Message Types:
    - ScrapeCity: Scrape hotels in a city radius
    - ScrapeState: Scrape hotels across a state

Functions:
    - send_message(msg): Send a message to SQS
    - send_messages_batch(msgs): Send multiple messages
    - dispatch(data): Route a message dict to its handler
    - consume(queue): Poll and process messages from a queue

Creating New Messages:
    1. Create a new file in messages/ (e.g., messages/export.py)
    2. Define a dataclass extending Message with queue = "your-queue"
    3. Register a handler with @handler(YourMessage)
    4. Import in __init__.py to auto-register

Example:
    @dataclass
    class ExportRegion(Message):
        queue: ClassVar[str] = "export-queue"
        state: str
        format: str = "xlsx"

    @handler(ExportRegion)
    async def handle_export(msg: ExportRegion):
        await export_to_file(msg.state, msg.format)
"""

# Base classes and decorators
from messages.base import Message, handler, HandlerRegistry

# Dispatcher and sender functions
from messages.handlers import (
    send_message,
    send_messages_batch,
    dispatch,
    consume,
    get_queue_url,
)

# Message types - importing registers their handlers
from messages.scrape import (
    ScrapeCity,
    ScrapeState,
    SCRAPE_QUEUE,
)

__all__ = [
    # Base
    "Message",
    "handler",
    "HandlerRegistry",
    # Functions
    "send_message",
    "send_messages_batch",
    "dispatch",
    "consume",
    "get_queue_url",
    # Scrape messages
    "ScrapeCity",
    "ScrapeState",
    "SCRAPE_QUEUE",
]
