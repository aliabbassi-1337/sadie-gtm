"""Deep-link URL generator for booking engines.

Tier 1 (URL construction): SiteMinder, Mews — instant, no browser.
Tier 2 (reverse proxy + autobook): Cloudbeds, RMS — proxy with client-side JS.
"""

from lib.deeplink.generator import (
    generate_deeplink,
    generate_deeplink_for_hotel,
    generate_deeplink_proxy,
)
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

__all__ = [
    "generate_deeplink",
    "generate_deeplink_proxy",
    "generate_deeplink_for_hotel",
    "DeepLinkRequest",
    "DeepLinkResult",
    "DeepLinkConfidence",
]
