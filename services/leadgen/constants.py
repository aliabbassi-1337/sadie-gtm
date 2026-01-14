"""Constants for the leadgen service."""


class HotelStatus:
    """Hotel processing status values."""

    SCRAPED = 0              # Initial state after scraping, pending detection
    DETECTED = 1             # Booking engine found
    ENQUEUED = 10            # Queued for detection processing
    LOCATION_MISMATCH = 98   # Website location doesn't match target region
    NO_BOOKING_ENGINE = 99   # No booking engine found (dead end)


HOTEL_STATUS_LABELS = {
    HotelStatus.SCRAPED: "scraped",
    HotelStatus.DETECTED: "detected",
    HotelStatus.ENQUEUED: "enqueued",
    HotelStatus.LOCATION_MISMATCH: "location_mismatch",
    HotelStatus.NO_BOOKING_ENGINE: "no_booking_engine",
}


def get_status_label(status: int) -> str:
    """Get human-readable label for a hotel status code."""
    return HOTEL_STATUS_LABELS.get(status, f"unknown_{status}")
