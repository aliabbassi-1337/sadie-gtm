"""Data models for deep-link URL generation."""

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional


class DeepLinkConfidence(str, Enum):
    """Confidence that the generated URL will have dates pre-filled."""

    HIGH = "HIGH"  # Verified: params work (SiteMinder, Cloudbeds, Mews)
    LOW = "LOW"  # Base URL correct, date params unverified (RMS)
    NONE = "NONE"  # Unknown engine, returning base URL unchanged


@dataclass
class DeepLinkRequest:
    """Input for deep-link generation."""

    booking_url: str
    checkin: date
    checkout: date
    adults: int = 2
    children: int = 0
    rooms: int = 1
    promo_code: Optional[str] = None
    rate_id: Optional[str] = None  # Engine-specific room/rate ID

    def __post_init__(self):
        if self.checkout <= self.checkin:
            raise ValueError("checkout must be after checkin")
        if self.adults < 1:
            raise ValueError("adults must be >= 1")
        if self.children < 0:
            raise ValueError("children must be >= 0")
        if self.rooms < 1:
            raise ValueError("rooms must be >= 1")


@dataclass
class DeepLinkResult:
    """Output of deep-link generation."""

    url: str
    engine_name: str  # "SiteMinder", "Cloudbeds", etc. or "Unknown"
    confidence: DeepLinkConfidence
    dates_prefilled: bool  # True if URL params include dates
    original_url: str  # The input booking URL
