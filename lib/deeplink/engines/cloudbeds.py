"""Cloudbeds deep-link URL builder.

Checkout URL (query params, goes straight to guest details):
  https://hotels.cloudbeds.com/en/reservation/{slug}/guests?checkin=2026-07-01&checkout=2026-07-05&adults=2&kids=0&currency=usd
"""

import re
from typing import Optional
from urllib.parse import urlencode

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

        params = {
            "checkin": request.checkin.isoformat(),
            "checkout": request.checkout.isoformat(),
            "adults": request.adults,
            "kids": request.children,
            "currency": "usd",
        }

        base = f"https://hotels.cloudbeds.com/en/reservation/{slug}/guests"
        url = f"{base}?{urlencode(params)}"

        return DeepLinkResult(
            url=url,
            engine_name=self.engine_name,
            confidence=self.confidence,
            dates_prefilled=True,
            original_url=request.booking_url,
        )
