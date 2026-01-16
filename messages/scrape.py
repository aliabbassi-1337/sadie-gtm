"""Scrape messages for SQS-powered hotel scraping.

Messages:
    - ScrapeCity: Scrape hotels in a specific city with radius
    - ScrapeState: Scrape hotels across an entire state

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


class ScrapeCity(Message):
    """Message to scrape hotels in a city.

    Args:
        city: City name (e.g., 'miami_beach', 'orlando')
        state: State name (e.g., 'florida')
        country: Country code (default: 'usa')
        radius_km: Radius around city center in km (default: 10)
        cell_size_km: Grid cell size in km (default: 2.0)
        notify: Send Slack notification on completion (default: True)
    """

    queue: ClassVar[str] = SCRAPE_QUEUE

    city: str
    state: str
    country: str = "usa"
    radius_km: float = Field(default=10.0, gt=0)
    cell_size_km: float = Field(default=2.0, gt=0)
    notify: bool = True


class ScrapeState(Message):
    """Message to scrape hotels across an entire state.

    Args:
        state: State name (e.g., 'florida', 'california')
        country: Country code (default: 'usa')
        cell_size_km: Grid cell size in km (default: 2.0)
        notify: Send Slack notification on completion (default: True)
    """

    queue: ClassVar[str] = SCRAPE_QUEUE

    state: str
    country: str = "usa"
    cell_size_km: float = Field(default=2.0, gt=0)
    notify: bool = True


@handler(ScrapeCity)
async def handle_scrape_city(msg: ScrapeCity) -> int:
    """Handle ScrapeCity message - scrapes hotels in a city radius.

    Returns:
        Number of hotels scraped.
    """
    logger.info(f"Handling ScrapeCity: {msg.city}, {msg.state} (r={msg.radius_km}km)")

    await init_db()
    try:
        service = Service()

        # Get city coordinates from service
        from services.leadgen.service import CITY_COORDINATES

        if msg.city not in CITY_COORDINATES:
            raise ValueError(f"Unknown city: {msg.city}. Available: {list(CITY_COORDINATES.keys())}")

        lat, lng = CITY_COORDINATES[msg.city]
        count = await service.scrape_region(lat, lng, msg.radius_km, msg.cell_size_km)

        logger.info(f"ScrapeCity complete: {count} hotels scraped in {msg.city}")

        if msg.notify and count > 0:
            city_display = msg.city.replace("_", " ").title()
            slack.send_message(
                f"*Scrape Complete*\n"
                f"• City: {city_display}, {msg.state.title()}\n"
                f"• Radius: {msg.radius_km}km\n"
                f"• Hotels scraped: {count}"
            )

        return count

    except Exception as e:
        logger.error(f"ScrapeCity failed: {e}")
        if msg.notify:
            slack.send_error("City Scrape", str(e))
        raise
    finally:
        await close_db()


@handler(ScrapeState)
async def handle_scrape_state(msg: ScrapeState) -> int:
    """Handle ScrapeState message - scrapes hotels across a state.

    Returns:
        Number of hotels scraped.
    """
    logger.info(f"Handling ScrapeState: {msg.state} (cell={msg.cell_size_km}km)")

    await init_db()
    try:
        service = Service()
        count = await service.scrape_state(msg.state, msg.cell_size_km)

        logger.info(f"ScrapeState complete: {count} hotels scraped in {msg.state}")

        if msg.notify and count > 0:
            slack.send_message(
                f"*Scrape Complete*\n"
                f"• State: {msg.state.title()}\n"
                f"• Cell size: {msg.cell_size_km}km\n"
                f"• Hotels scraped: {count}"
            )

        return count

    except Exception as e:
        logger.error(f"ScrapeState failed: {e}")
        if msg.notify:
            slack.send_error("State Scrape", str(e))
        raise
    finally:
        await close_db()
