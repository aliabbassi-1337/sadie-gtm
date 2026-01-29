"""Constants for the leadgen service.

Universal status values used across all tables:
- NULL = not attempted (for enrichment_status)
- 0 = pending (for hotels.status)
- 1 = success/live
- -1 = failed/error
"""


class HotelStatus:
    """Hotel status values.
    
    Simple 3-value system:
    - PENDING (0): Needs processing
    - LIVE (1): Successfully processed, ready for outreach
    - ERROR (-1): Failed/rejected
    """
    PENDING = 0
    LIVE = 1
    ERROR = -1


class EnrichmentStatus:
    """Enrichment status values.
    
    Simple 3-value system:
    - None/NULL: Not attempted
    - SUCCESS (1): Enrichment succeeded
    - FAILED (-1): Enrichment failed or URL dead
    """
    SUCCESS = 1
    FAILED = -1


# Human-readable labels
HOTEL_STATUS_LABELS = {
    HotelStatus.PENDING: "pending",
    HotelStatus.LIVE: "live",
    HotelStatus.ERROR: "error",
}

ENRICHMENT_STATUS_LABELS = {
    None: "not_attempted",
    EnrichmentStatus.SUCCESS: "success",
    EnrichmentStatus.FAILED: "failed",
}


def get_status_label(status: int) -> str:
    """Get human-readable label for a hotel status."""
    return HOTEL_STATUS_LABELS.get(status, f"unknown_{status}")


# Booking engine URL patterns for crawled data ingestion
BOOKING_ENGINE_URL_PATTERNS = {
    "cloudbeds": "https://hotels.cloudbeds.com/reservation/{slug}",
    "mews": "https://app.mews.com/distributor/{slug}",
    "rms": None,  # RMS URLs are stored as full URLs
    "siteminder": "https://{slug}.siteminder.com",
}

# Booking engine tier (1 = primary target, 2 = secondary)
BOOKING_ENGINE_TIERS = {
    "cloudbeds": 1,
    "mews": 2,
    "rms": 2,
    "siteminder": 2,
}
