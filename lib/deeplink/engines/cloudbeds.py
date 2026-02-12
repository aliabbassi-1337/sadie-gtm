"""Cloudbeds deep-link URL builder.

With room_type_id: skips room selection, goes to checkout:
  https://hotels.cloudbeds.com/reservation/{slug}#checkin=...&checkout=...&adults=2&room_type_id=560388&submit=1

Without room_type_id: lands on search page with dates pre-filled.
"""

import re
from typing import Optional

from lib.deeplink.engines.base import EngineBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult


class CloudbedsBuilder(EngineBuilder):
    engine_name = "Cloudbeds"
    confidence = DeepLinkConfidence.HIGH

    def extract_slug(self, url: str) -> Optional[str]:
        """Extract property code from Cloudbeds URL.

        Regex from lib/cloudbeds/api_client.py:extract_property_code
        Handles malformed URLs with duplicate domain.
        """
        # Fix malformed URLs with duplicate domain
        if "cloudbeds.com/reservation/hotels.cloudbeds.com" in url:
            url = url.replace(
                "hotels.cloudbeds.com/reservation/hotels.cloudbeds.com/reservation/",
                "hotels.cloudbeds.com/reservation/",
            )

        match = re.search(r"/(?:reservation|booking)/([a-zA-Z0-9]{2,10})(?:/|$|\?|#)", url)
        if match:
            code = match.group(1)
            if code.lower() in ("hotels", "www", "booking"):
                return None
            return code
        return None

    def build(self, request: DeepLinkRequest) -> DeepLinkResult:
        slug = self.extract_slug(request.booking_url)
        if not slug:
            return DeepLinkResult(
                url=request.booking_url,
                engine_name=self.engine_name,
                confidence=DeepLinkConfidence.NONE,
                dates_prefilled=False,
                original_url=request.booking_url,
            )

        # Hash params auto-trigger room search with dates pre-filled
        # If rate_id is provided, use it as room_type_id to skip room selection
        parts = [
            f"checkin={request.checkin.isoformat()}",
            f"checkout={request.checkout.isoformat()}",
            f"adults={request.adults}",
        ]
        if request.rate_id:
            parts.append(f"room_type_id={request.rate_id}")
        parts.append("submit=1")
        url = f"https://hotels.cloudbeds.com/reservation/{slug}#{'&'.join(parts)}"

        return DeepLinkResult(
            url=url,
            engine_name=self.engine_name,
            confidence=self.confidence,
            dates_prefilled=True,
            original_url=request.booking_url,
        )
