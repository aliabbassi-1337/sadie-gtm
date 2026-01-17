"""Scrape messages for SQS-powered hotel scraping.

Messages:
    - ScrapeRegion: Scrape hotels in a circular region by coordinates

These messages enable async, distributed scraping via SQS.
"""

from typing import ClassVar

from pydantic import Field
from loguru import logger

from messages.base import Message, handler
from db.client import init_db, close_db
from services.leadgen.service import Service
from infra import slack


# Queue name for scrape messages
SCRAPE_QUEUE = "scrape-queue"


class ScrapeRegion(Message):
    """Message to scrape hotels in a circular region.

    Args:
        name: Region name for logging/notifications
        lat: Center latitude
        lng: Center longitude
        radius_km: Radius around center in km (default: 10)
        cell_size_km: Grid cell size in km (default: 2.0)
        notify: Send Slack notification on completion (default: True)
    """

    queue: ClassVar[str] = SCRAPE_QUEUE

    name: str
    lat: float
    lng: float
    radius_km: float = Field(default=10.0, gt=0)
    cell_size_km: float = Field(default=2.0, gt=0)
    notify: bool = True


@handler(ScrapeRegion)
async def handle_scrape_region(msg: ScrapeRegion) -> int:
    """Handle ScrapeRegion message - scrapes hotels in a circular region.

    Returns:
        Number of hotels scraped.
    """
    logger.info(f"Handling ScrapeRegion: {msg.name} ({msg.lat}, {msg.lng}) r={msg.radius_km}km")

    await init_db()
    try:
        service = Service()
        count = await service.scrape_region(msg.lat, msg.lng, msg.radius_km, msg.cell_size_km)

        logger.info(f"ScrapeRegion complete: {count} hotels scraped in {msg.name}")

        if msg.notify and count > 0:
            slack.send_message(
                f"*Scrape Complete*\n"
                f"• Region: {msg.name}\n"
                f"• Radius: {msg.radius_km}km\n"
                f"• Hotels scraped: {count}"
            )

        return count

    except Exception as e:
        logger.error(f"ScrapeRegion failed: {e}")
        if msg.notify:
            slack.send_error("Region Scrape", str(e))
        raise
    finally:
        await close_db()
