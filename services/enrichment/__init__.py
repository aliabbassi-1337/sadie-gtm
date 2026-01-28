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

# Re-export from services.rms for backward compatibility
from services.rms import (
    RMSRepo,
    IRMSRepo,
    RMSHotelRecord,
    RMSScanner,
    IRMSScanner,
    MockScanner,
    ScannedURL,
    RMSScraper,
    IRMSScraper,
    MockScraper,
    ExtractedRMSData,
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
    # RMS (re-exported from services.rms)
    "RMSRepo",
    "IRMSRepo",
    "RMSHotelRecord",
    "RMSScanner",
    "IRMSScanner",
    "MockScanner",
    "ScannedURL",
    "RMSScraper",
    "IRMSScraper",
    "MockScraper",
    "ExtractedRMSData",
    "RMSQueue",
    "IRMSQueue",
    "MockQueue",
    "QueueStats",
    "QueueMessage",
]
