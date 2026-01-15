"""Slack client for sending notifications."""

import os
from typing import Optional

import httpx
from loguru import logger


def get_webhook_url(channel: str = "#leads") -> Optional[str]:
    """Get Slack webhook URL from environment.

    Args:
        channel: Channel name (used to select webhook if multiple configured)

    Returns:
        Webhook URL or None if not configured
    """
    # Default webhook
    url = os.getenv("SLACK_WEBHOOK_URL")

    # Channel-specific webhooks (optional)
    if channel == "#leads":
        url = os.getenv("SLACK_LEADS_WEBHOOK_URL", url)

    return url


def send_message(
    text: str,
    channel: str = "#leads",
    webhook_url: Optional[str] = None,
) -> bool:
    """Send a message to Slack.

    Args:
        text: Message text (supports Slack markdown)
        channel: Channel name (for webhook selection)
        webhook_url: Override webhook URL

    Returns:
        True if sent successfully, False otherwise
    """
    url = webhook_url or get_webhook_url(channel)

    if not url:
        logger.warning(f"Slack webhook URL not configured for {channel}")
        return False

    try:
        response = httpx.post(
            url,
            json={"text": text},
            timeout=10.0,
        )

        if response.status_code == 200:
            logger.info(f"Sent Slack message to {channel}")
            return True
        else:
            logger.error(f"Slack API error: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        logger.error(f"Failed to send Slack message: {e}")
        return False


def send_export_notification(
    location: str,
    lead_count: int,
    s3_uri: str,
    channel: str = "#leads",
) -> bool:
    """Send a formatted export notification.

    Args:
        location: City or state name
        lead_count: Number of leads exported
        s3_uri: S3 URI of the exported file
        channel: Slack channel

    Returns:
        True if sent successfully
    """
    message = f"""*Lead Export Complete*
• Location: {location}
• Leads: {lead_count}
• File: `{s3_uri}`"""

    return send_message(message, channel)
