"""Deep-link URL generator for booking engines.

Tier 1 (URL construction): SiteMinder, Mews — instant, no browser.
Tier 2 (reverse proxy + autobook): Cloudbeds, RMS — proxy with client-side JS.

Shared library: models and engine builders only.
Business logic lives in services/deeplink/.
API layer lives in api/deeplink/.
"""

from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult

__all__ = [
    "DeepLinkRequest",
    "DeepLinkResult",
    "DeepLinkConfidence",
]
