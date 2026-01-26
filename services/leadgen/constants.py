"""Constants for the leadgen service."""


class PipelineStage:
    """Pipeline stages for hotel processing.
    
    Each stage indicates what has been COMPLETED:
    
    INGESTED (0)
        ↓ enrich website
    HAS_WEBSITE (10)
        ↓ enrich location (if needed)
    HAS_LOCATION (20)
        ↓ detect booking engine
    DETECTED (30)
        ↓ enrich room count, proximity
    ENRICHED (40)
        ↓ launch
    LAUNCHED (100)
    
    Negative values are terminal (rejected/dead end):
    -1 to -10: Detection failures
    -11 to -20: Enrichment failures
    -21 to -30: Data quality issues
    """
    
    # Pipeline progress (positive, ordered by completion)
    INGESTED = 0           # Just ingested, needs website
    HAS_WEBSITE = 10       # Website found/enriched
    HAS_LOCATION = 20      # Coordinates found/enriched
    DETECTED = 30          # Booking engine detected
    ENRICHED = 40          # All enrichments complete
    LAUNCHED = 100         # Live lead
    
    # Detection failures (-1 to -10)
    NO_BOOKING_ENGINE = -1       # No booking engine found
    LOCATION_MISMATCH = -2       # Website location doesn't match target
    DETECTION_TIMEOUT = -3       # Detection timed out too many times
    
    # Enrichment failures (-11 to -20)
    ENRICHMENT_FAILED = -11      # Website enrichment permanently failed
    UNENRICHABLE = -12           # No data to enrich with (no name, no coords)
    
    # Data quality issues (-21 to -30)
    DUPLICATE = -21              # Duplicate of another hotel
    NON_HOTEL = -22              # Not actually a hotel
    INVALID_DATA = -23           # Data too broken to process


# Backwards compatibility aliases
class HotelStatus:
    """DEPRECATED: Use PipelineStage instead."""
    NON_HOTEL = PipelineStage.NON_HOTEL
    DUPLICATE = PipelineStage.DUPLICATE
    LOCATION_MISMATCH = PipelineStage.LOCATION_MISMATCH
    NO_BOOKING_ENGINE = PipelineStage.NO_BOOKING_ENGINE
    PENDING = PipelineStage.INGESTED
    LAUNCHED = PipelineStage.LAUNCHED


PIPELINE_STAGE_LABELS = {
    # Progress stages
    PipelineStage.INGESTED: "ingested",
    PipelineStage.HAS_WEBSITE: "has_website",
    PipelineStage.HAS_LOCATION: "has_location",
    PipelineStage.DETECTED: "detected",
    PipelineStage.ENRICHED: "enriched",
    PipelineStage.LAUNCHED: "launched",
    # Detection failures
    PipelineStage.NO_BOOKING_ENGINE: "no_booking_engine",
    PipelineStage.LOCATION_MISMATCH: "location_mismatch",
    PipelineStage.DETECTION_TIMEOUT: "detection_timeout",
    # Enrichment failures
    PipelineStage.ENRICHMENT_FAILED: "enrichment_failed",
    PipelineStage.UNENRICHABLE: "unenrichable",
    # Data quality
    PipelineStage.DUPLICATE: "duplicate",
    PipelineStage.NON_HOTEL: "non_hotel",
    PipelineStage.INVALID_DATA: "invalid_data",
}

# Backwards compat
HOTEL_STATUS_LABELS = PIPELINE_STAGE_LABELS


def get_status_label(status: int) -> str:
    """Get human-readable label for a pipeline stage."""
    return PIPELINE_STAGE_LABELS.get(status, f"unknown_{status}")


def get_stage_label(stage: int) -> str:
    """Get human-readable label for a pipeline stage."""
    return PIPELINE_STAGE_LABELS.get(stage, f"unknown_{stage}")


def is_terminal(stage: int) -> bool:
    """Check if a stage is terminal (hotel won't progress further)."""
    return stage < 0 or stage == PipelineStage.LAUNCHED


def is_actionable(stage: int) -> bool:
    """Check if a hotel at this stage needs action."""
    return 0 <= stage < PipelineStage.LAUNCHED


def next_action(stage: int, has_website: bool = False, has_location: bool = False) -> str:
    """Get the next action needed for a hotel at this stage."""
    if stage < 0:
        return "none (terminal)"
    if stage == PipelineStage.LAUNCHED:
        return "none (launched)"
    if stage == PipelineStage.INGESTED:
        if has_website:
            return "detect_booking_engine"
        return "enrich_website"
    if stage == PipelineStage.HAS_WEBSITE:
        if has_location:
            return "detect_booking_engine"
        return "enrich_location"
    if stage == PipelineStage.HAS_LOCATION:
        return "detect_booking_engine"
    if stage == PipelineStage.DETECTED:
        return "enrich_room_count"
    if stage == PipelineStage.ENRICHED:
        return "launch"
    return "unknown"


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
