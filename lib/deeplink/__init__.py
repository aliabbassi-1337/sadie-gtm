"""Deep-link URL generator for booking engines.

Tier 1 (URL construction): SiteMinder, Mews — instant, no browser.
Tier 2 (browser automation): Cloudbeds, RMS — Playwright, session-based.
"""

from lib.deeplink.generator import (
    generate_deeplink,
    generate_deeplink_async,
    generate_deeplink_for_hotel,
)
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

__all__ = [
    "generate_deeplink",
    "generate_deeplink_async",
    "generate_deeplink_for_hotel",
    "DeepLinkRequest",
    "DeepLinkResult",
    "DeepLinkConfidence",
]
