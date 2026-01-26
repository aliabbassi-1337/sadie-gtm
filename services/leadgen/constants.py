"""Constants for the leadgen service."""


class HotelStatus:
    """Hotel processing status values.

    Simplified status system:
    - Negative values = rejected (dead ends)
    - 0 = pending (in pipeline, not yet launched)
    - 1 = launched (live lead)

    Detection/enrichment progress is tracked by presence of records in:
    - hotel_booking_engines (detection complete)
    - hotel_room_count (room count enrichment complete)
    - hotel_customer_proximity (proximity enrichment complete)
    """

    # Rejected statuses (negative)
    NON_HOTEL = -4           # Not a hotel (restaurant, store, etc.)
    DUPLICATE = -3           # Duplicate hotel (same placeId, location, or name)
    LOCATION_MISMATCH = -2   # Website location doesn't match target region
    NO_BOOKING_ENGINE = -1   # No booking engine found (dead end)

    # Active statuses
    PENDING = 0              # In pipeline, not yet launched
    LAUNCHED = 1             # Live lead, fully enriched and launched


HOTEL_STATUS_LABELS = {
    HotelStatus.NON_HOTEL: "non_hotel",
    HotelStatus.DUPLICATE: "duplicate",
    HotelStatus.LOCATION_MISMATCH: "location_mismatch",
    HotelStatus.NO_BOOKING_ENGINE: "no_booking_engine",
    HotelStatus.PENDING: "pending",
    HotelStatus.LAUNCHED: "launched",
}


def get_status_label(status: int) -> str:
    """Get human-readable label for a hotel status code."""
    return HOTEL_STATUS_LABELS.get(status, f"unknown_{status}")


# Booking engine URL patterns for crawled data ingestion
# {slug} or {id} will be replaced with the actual value
BOOKING_ENGINE_URL_PATTERNS = {
    "cloudbeds": "https://hotels.cloudbeds.com/reservation/{slug}",
    "mews": "https://app.mews.com/distributor/{slug}",
    "rms": None,  # RMS URLs are stored as full URLs in crawl file
    "siteminder": "https://{slug}.siteminder.com",  # TODO: verify pattern
}

# Booking engine tier (1 = primary target, 2 = secondary, 3 = other)
BOOKING_ENGINE_TIERS = {
    "cloudbeds": 1,
    "mews": 2,
    "rms": 2,
    "siteminder": 2,
}
