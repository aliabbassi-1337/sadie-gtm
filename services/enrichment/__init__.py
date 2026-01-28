"""Enrichment service.

Single service for all hotel enrichment operations.
Self-contained - no imports from other services.
"""

from services.enrichment.service import (
    Service,
    IService,
    EnrichResult,
    EnqueueResult,
    ConsumeResult,
    RMSHotelRecord,
    QueueStats,
    MockQueue,
)

__all__ = [
    "Service",
    "IService",
    "EnrichResult",
    "EnqueueResult",
    "ConsumeResult",
    "RMSHotelRecord",
    "QueueStats",
    "MockQueue",
]
