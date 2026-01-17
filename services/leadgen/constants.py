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
    DUPLICATE = -3           # Duplicate hotel (same placeId, location, or name)
    LOCATION_MISMATCH = -2   # Website location doesn't match target region
    NO_BOOKING_ENGINE = -1   # No booking engine found (dead end)

    # Active statuses
    PENDING = 0              # In pipeline, not yet launched
    LAUNCHED = 1             # Live lead, fully enriched and launched


HOTEL_STATUS_LABELS = {
    HotelStatus.DUPLICATE: "duplicate",
    HotelStatus.LOCATION_MISMATCH: "location_mismatch",
    HotelStatus.NO_BOOKING_ENGINE: "no_booking_engine",
    HotelStatus.PENDING: "pending",
    HotelStatus.LAUNCHED: "launched",
}


def get_status_label(status: int) -> str:
    """Get human-readable label for a hotel status code."""
    return HOTEL_STATUS_LABELS.get(status, f"unknown_{status}")
