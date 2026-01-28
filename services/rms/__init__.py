"""RMS Cloud booking engine components.

Shared module for RMS scanning, scraping, and data management.
Used by both ingestor and enrichment services.
"""

from services.rms.scanner import (
    RMSScanner,
    IRMSScanner,
    MockScanner,
    ScannedURL,
    RMS_SUBDOMAINS,
)
from services.rms.scraper import (
    RMSScraper,
    IRMSScraper,
    MockScraper,
    ExtractedRMSData,
    decode_cloudflare_email,
    normalize_country,
)
from services.rms.repo import (
    RMSRepo,
    IRMSRepo,
    RMSHotelRecord,
)
from services.rms.queue import (
    RMSQueue,
    IRMSQueue,
    MockQueue,
    QueueStats,
    QueueMessage,
)

__all__ = [
    # Scanner
    "RMSScanner",
    "IRMSScanner",
    "MockScanner",
    "ScannedURL",
    "RMS_SUBDOMAINS",
    # Scraper
    "RMSScraper",
    "IRMSScraper",
    "MockScraper",
    "ExtractedRMSData",
    "decode_cloudflare_email",
    "normalize_country",
    # Repo
    "RMSRepo",
    "IRMSRepo",
    "RMSHotelRecord",
    # Queue
    "RMSQueue",
    "IRMSQueue",
    "MockQueue",
    "QueueStats",
    "QueueMessage",
]
