"""RMS shared library.

Shared code for RMS booking engine operations.
Models, scanner, scraper, utils only - NO repo/queue (those are service-specific).
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
from lib.rms.utils import decode_cloudflare_email, normalize_country

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
    # Utils
    "decode_cloudflare_email",
    "normalize_country",
]
