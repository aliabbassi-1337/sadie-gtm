"""SQS client for message queue operations."""

import json
import os
from typing import List, Dict, Any, Optional

import boto3
from loguru import logger


def get_sqs_client():
    """Get SQS client using environment credentials."""
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "eu-north-1"),
    )


def get_queue_url() -> str:
    """Get the detection queue URL from environment."""
    url = os.getenv("SQS_DETECTION_QUEUE_URL")
    if not url:
        raise ValueError("SQS_DETECTION_QUEUE_URL environment variable not set")
    return url


def send_message(queue_url: str, body: Dict[str, Any]) -> str:
    """Send a single message to SQS.

    Returns message ID.
    """
    client = get_sqs_client()
    response = client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(body),
    )
    return response["MessageId"]


def send_messages_batch(queue_url: str, messages: List[Dict[str, Any]], concurrency: int = 20) -> int:
    """Send multiple messages to SQS in batches of 10, with concurrent API calls.

    Args:
        queue_url: SQS queue URL
        messages: List of message bodies (dicts)
        concurrency: Max concurrent API calls (default 20)

    Returns:
        Number of messages successfully sent.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    if not messages:
        return 0
    
    client = get_sqs_client()

    # Prepare all batches (SQS allows max 10 messages per batch)
    batches = []
    for i in range(0, len(messages), 10):
        batch = messages[i:i + 10]
        entries = [
            {
                "Id": str(idx),
                "MessageBody": json.dumps(msg),
            }
            for idx, msg in enumerate(batch)
        ]
        batches.append(entries)
    
    def send_batch(entries):
        response = client.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries,
        )
        failed = response.get("Failed", [])
        if failed:
            for failure in failed:
                logger.error(f"Failed to send message: {failure}")
        return len(response.get("Successful", []))
    
    # Send batches concurrently
    sent_count = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_batch, batch) for batch in batches]
        for future in as_completed(futures):
            sent_count += future.result()

    return sent_count


def receive_messages(
    queue_url: str,
    max_messages: int = 1,
    wait_time_seconds: int = 20,
    visibility_timeout: int = 7200,  # 2 hours default
) -> List[Dict[str, Any]]:
    """Receive messages from SQS with long polling.

    Args:
        queue_url: SQS queue URL
        max_messages: Max messages to receive (1-10)
        wait_time_seconds: Long polling wait time
        visibility_timeout: How long message is hidden after receive

    Returns:
        List of messages with 'body' (parsed JSON) and 'receipt_handle'.
    """
    client = get_sqs_client()

    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=min(max_messages, 10),
        WaitTimeSeconds=wait_time_seconds,
        VisibilityTimeout=visibility_timeout,
    )

    messages = []
    for msg in response.get("Messages", []):
        messages.append({
            "body": json.loads(msg["Body"]),
            "receipt_handle": msg["ReceiptHandle"],
            "message_id": msg["MessageId"],
        })

    return messages


def delete_message(queue_url: str, receipt_handle: str) -> None:
    """Delete a message from SQS after successful processing."""
    client = get_sqs_client()
    client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )


def delete_messages_batch(queue_url: str, receipt_handles: List[str], concurrency: int = 20) -> int:
    """Delete multiple messages from SQS in batches of 10, with concurrent API calls.
    
    Args:
        queue_url: SQS queue URL
        receipt_handles: List of receipt handles to delete
        concurrency: Max concurrent API calls (default 20)
        
    Returns:
        Number of messages successfully deleted.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    if not receipt_handles:
        return 0
    
    client = get_sqs_client()
    
    # Prepare all batches (SQS allows max 10 messages per batch)
    batches = []
    for i in range(0, len(receipt_handles), 10):
        batch = receipt_handles[i:i + 10]
        entries = [
            {
                "Id": str(idx),
                "ReceiptHandle": handle,
            }
            for idx, handle in enumerate(batch)
        ]
        batches.append(entries)
    
    def delete_batch(entries):
        response = client.delete_message_batch(
            QueueUrl=queue_url,
            Entries=entries,
        )
        failed = response.get("Failed", [])
        if failed:
            for failure in failed:
                logger.debug(f"Failed to delete message: {failure}")
        return len(response.get("Successful", []))
    
    # Delete batches concurrently
    deleted_count = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(delete_batch, batch) for batch in batches]
        for future in as_completed(futures):
            deleted_count += future.result()
    
    return deleted_count


def get_queue_attributes(queue_url: str) -> Dict[str, str]:
    """Get queue attributes like message count."""
    client = get_sqs_client()
    response = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )
    return response.get("Attributes", {})
