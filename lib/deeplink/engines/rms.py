"""RMS Cloud deep-link URL builder.

URL format (date params UNVERIFIED):
  https://bookings13.rmscloud.com/Search/Index/13308/90/?arrival=2026-04-10&departure=2026-04-12&adults=2

Base URL is correct but date param names are unverified — confidence is LOW
and dates_prefilled is False.
"""

import re
from typing import Optional
from urllib.parse import urlencode, urlparse

from lib.deeplink.engines.base import EngineBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult


class RmsBuilder(EngineBuilder):
    engine_name = "RMS Cloud"
    confidence = DeepLinkConfidence.LOW

    def extract_slug(self, url: str) -> Optional[str]:
        """Extract RMS ID from URL.

        Regex from lib/rms/scraper.py:extract_rms_id
        Handles numeric IDs and hex slugs across multiple server variants.
        """
        patterns = [
            r"ibe1[234]\.rmscloud\.com/([A-Fa-f0-9]{16})",
            r"bookings\d*\.rmscloud\.com/(?:obookings\d*/)?[Ss]earch/[Ii]ndex/([A-Fa-f0-9]{16})",
            r"ibe1[234]\.rmscloud\.com/(\d+)",
            r"bookings\d*\.rmscloud\.com/(?:obookings\d*/)?[Ss]earch/[Ii]ndex/(\d+)",
            r"rmscloud\.com/.*?/(\d+)/?",
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def build(self, request: DeepLinkRequest) -> DeepLinkResult:
        # RMS: we know the base URL works, but date params are unverified.
        # Return the base URL with best-guess date params appended,
        # but mark dates_prefilled=False and confidence=LOW.
        parsed = urlparse(request.booking_url)
        # Strip existing query params, rebuild with date params
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if not base.endswith("/"):
            base += "/"

        params = {
            "arrival": request.checkin.isoformat(),
            "departure": request.checkout.isoformat(),
            "adults": request.adults,
        }

        url = f"{base}?{urlencode(params)}"

        return DeepLinkResult(
            url=url,
            engine_name=self.engine_name,
            confidence=self.confidence,
            dates_prefilled=False,  # Unverified params
            original_url=request.booking_url,
        )
