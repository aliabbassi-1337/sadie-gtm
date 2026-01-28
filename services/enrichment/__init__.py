"""RMS Enrichment service.

Enriches existing RMS hotels by scraping booking pages.
For ingestion (discovering new hotels), use services.ingestor.ingestors.rms.
"""

from services.enrichment.rms_service import (
    RMSEnrichmentService,
    IRMSEnrichmentService,
    EnrichResult,
    EnqueueResult,
    ConsumeResult,
    # Backward compatibility
    RMSService,
    IRMSService,
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
    # Enrichment Service
    "RMSEnrichmentService",
    "IRMSEnrichmentService",
    "EnrichResult",
    "EnqueueResult",
    "ConsumeResult",
    # Backward compatibility
    "RMSService",
    "IRMSService",
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
