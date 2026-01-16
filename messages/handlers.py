"""Message dispatcher for routing SQS messages to handlers.

This module provides utilities for:
- Dispatching messages to their registered handlers
- Sending messages to SQS queues
- Consuming messages from SQS with automatic handler routing

Example usage:

    # Send a message
    from messages import ScrapeCity, send_message
    await send_message(ScrapeCity(city="miami_beach", state="florida"))

    # Dispatch a received message
    from messages import dispatch
    result = await dispatch(message_dict)

    # Run a consumer loop
    from messages import consume
    await consume("scrape-queue", max_messages=10)
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, List

from loguru import logger

from messages.base import Message, HandlerRegistry
from infra import sqs


def get_queue_url(queue_name: str) -> str:
    """Get SQS queue URL from environment.

    Queue URLs are configured via environment variables:
    - SQS_SCRAPE_QUEUE_URL for 'scrape-queue'
    - SQS_DETECTION_QUEUE_URL for 'detection-queue'
    - etc.
    """
    # Map queue names to env var names
    env_var_map = {
        "scrape-queue": "SQS_SCRAPE_QUEUE_URL",
        "detection-queue": "SQS_DETECTION_QUEUE_URL",
    }

    env_var = env_var_map.get(queue_name, f"SQS_{queue_name.upper().replace('-', '_')}_URL")
    url = os.getenv(env_var)

    if not url:
        raise ValueError(f"Queue URL not configured. Set {env_var} environment variable.")

    return url


def send_message(msg: Message) -> str:
    """Send a message to its configured SQS queue.

    Args:
        msg: Message instance to send

    Returns:
        SQS message ID

    Example:
        msg_id = send_message(ScrapeCity(city="miami_beach", state="florida"))
    """
    queue_url = get_queue_url(msg.queue)
    message_id = sqs.send_message(queue_url, msg.to_dict())
    logger.info(f"Sent {msg.__class__.__name__} to {msg.queue}: {message_id}")
    return message_id


def send_messages_batch(messages: List[Message]) -> int:
    """Send multiple messages to SQS in batches.

    All messages must be for the same queue.

    Args:
        messages: List of Message instances

    Returns:
        Number of messages successfully sent

    Raises:
        ValueError: If messages have different queues
    """
    if not messages:
        return 0

    # Verify all messages go to same queue
    queues = {msg.queue for msg in messages}
    if len(queues) > 1:
        raise ValueError(f"All messages must have same queue, got: {queues}")

    queue_name = messages[0].queue
    queue_url = get_queue_url(queue_name)
    message_dicts = [msg.to_dict() for msg in messages]

    count = sqs.send_messages_batch(queue_url, message_dicts)
    logger.info(f"Sent {count}/{len(messages)} messages to {queue_name}")
    return count


async def dispatch(data: dict) -> Any:
    """Dispatch a message dict to its registered handler.

    This parses the message, finds the handler, and invokes it.

    Args:
        data: Message dict with '_type' field

    Returns:
        Result from handler function

    Raises:
        ValueError: If message type is unknown
    """
    type_name = data.get("_type")
    if not type_name:
        raise ValueError("Message missing '_type' field")

    handler_info = HandlerRegistry.get_handler(type_name)
    if not handler_info:
        raise ValueError(f"No handler registered for message type: {type_name}")

    message_cls, handler_func = handler_info
    msg = message_cls.from_dict(data)

    logger.debug(f"Dispatching {type_name} to handler")
    result = await handler_func(msg)
    logger.debug(f"Handler {type_name} completed")

    return result


async def consume(
    queue_name: str,
    max_messages: int = 0,
    visibility_timeout: int = 7200,
    wait_time_seconds: int = 20,
) -> dict:
    """Consume and process messages from an SQS queue.

    Polls the queue and dispatches messages to their registered handlers.
    Deletes messages after successful processing.

    Args:
        queue_name: Name of the queue (e.g., 'scrape-queue')
        max_messages: Max messages to process (0 = unlimited)
        visibility_timeout: How long messages are hidden after receive (seconds)
        wait_time_seconds: Long polling wait time (seconds)

    Returns:
        Dict with stats: {'processed': int, 'errors': int}
    """
    queue_url = get_queue_url(queue_name)
    logger.info(f"Starting consumer for {queue_name}")

    processed = 0
    errors = 0

    while True:
        # Check limit
        if max_messages > 0 and processed >= max_messages:
            logger.info(f"Reached max messages limit ({max_messages})")
            break

        # Poll for messages
        messages = sqs.receive_messages(
            queue_url=queue_url,
            max_messages=10,
            wait_time_seconds=wait_time_seconds,
            visibility_timeout=visibility_timeout,
        )

        if not messages:
            # Check if queue is empty
            attrs = sqs.get_queue_attributes(queue_url)
            pending = int(attrs.get("ApproximateNumberOfMessages", 0))
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))

            if pending == 0 and in_flight == 0:
                logger.info("Queue empty, exiting")
                break

            continue

        # Process each message
        for msg in messages:
            try:
                await dispatch(msg["body"])
                sqs.delete_message(queue_url, msg["receipt_handle"])
                processed += 1
                logger.info(f"Processed message {msg['message_id']}")

            except Exception as e:
                logger.error(f"Error processing message {msg['message_id']}: {e}")
                errors += 1
                # Don't delete - SQS will retry after visibility timeout

    logger.info(f"Consumer complete: {processed} processed, {errors} errors")
    return {"processed": processed, "errors": errors}
