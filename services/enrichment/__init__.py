"""Enrichment service.

RMS booking engine discovery and enrichment.

Components:
- Repo: Database operations (rms_repo.py)
- Scanner: Find valid RMS URLs (rms_scanner.py)  
- Scraper: Extract data from pages (rms_scraper.py)
- Queue: SQS operations (rms_queue.py)
- Service: Orchestrates everything (rms_service.py)
"""

from services.enrichment.rms_service import (
    RMSService,
    IRMSService,
    IngestResult,
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
    "RMSService",
    "IRMSService",
    "IngestResult",
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
