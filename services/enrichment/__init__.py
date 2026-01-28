"""Enrichment service.

Single service for all hotel enrichment operations:
- Room count enrichment
- Customer proximity calculation
- Website enrichment
- RMS booking page enrichment
"""

from services.enrichment.service import (
    Service,
    IService,
    EnrichResult,
    EnqueueResult,
    ConsumeResult,
)
from services.enrichment.rms_repo import (
    RMSRepo,
    IRMSRepo,
    RMSHotelRecord,
)
from services.enrichment.rms_scanner import (
    RMSScanner,
    IRMSScanner,
    MockScanner,
    ScannedURL,
)
from services.enrichment.rms_scraper import (
    RMSScraper,
    IRMSScraper,
    MockScraper,
    ExtractedRMSData,
)
from services.enrichment.rms_queue import (
    RMSQueue,
    IRMSQueue,
    MockQueue,
    QueueStats,
    QueueMessage,
)

__all__ = [
    # Service
    "Service",
    "IService",
    "EnrichResult",
    "EnqueueResult",
    "ConsumeResult",
    # Repo
    "RMSRepo",
    "IRMSRepo",
    "RMSHotelRecord",
    # Scanner
    "RMSScanner",
    "IRMSScanner",
    "MockScanner",
    "ScannedURL",
    # Scraper
    "RMSScraper",
    "IRMSScraper",
    "MockScraper",
    "ExtractedRMSData",
    # Queue
    "RMSQueue",
    "IRMSQueue",
    "MockQueue",
    "QueueStats",
    "QueueMessage",
]
