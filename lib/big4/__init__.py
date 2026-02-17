"""BIG4 Holiday Parks - Scraper library.

Scrapes big4.com.au to discover and extract park data.
"""

from lib.big4.models import Big4Park, Big4ScrapeResult
from lib.big4.scraper import Big4Scraper

__all__ = [
    "Big4Park",
    "Big4ScrapeResult",
    "Big4Scraper",
]
