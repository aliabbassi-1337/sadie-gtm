"""RMS shared library.

Shared code for RMS booking engine operations.
Can be imported by any service.
"""

from lib.rms.models import (
    ScannedURL,
    ExtractedRMSData,
    RMSHotelRecord,
    QueueStats,
    QueueMessage,
)
from lib.rms.scanner import RMSScanner, IRMSScanner
from lib.rms.scraper import RMSScraper, IRMSScraper
from lib.rms.helpers import decode_cloudflare_email, normalize_country

__all__ = [
    # Models
    "ScannedURL",
    "ExtractedRMSData",
    "RMSHotelRecord",
    "QueueStats",
    "QueueMessage",
    # Scanner
    "RMSScanner",
    "IRMSScanner",
    # Scraper
    "RMSScraper",
    "IRMSScraper",
    # Helpers
    "decode_cloudflare_email",
    "normalize_country",
]
