"""Base class for engine-specific URL builders."""

from abc import ABC, abstractmethod
from typing import Optional

from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult


class EngineBuilder(ABC):
    """Abstract base for building deep-link URLs for a specific booking engine."""

    engine_name: str
    confidence: DeepLinkConfidence

    @abstractmethod
    def build(self, request: DeepLinkRequest) -> DeepLinkResult:
        """Build a deep-link URL with dates/guests pre-filled."""

    @abstractmethod
    def extract_slug(self, url: str) -> Optional[str]:
        """Extract the property identifier from a booking URL."""
