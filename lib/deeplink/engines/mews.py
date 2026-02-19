"""Mews deep-link URL builder.

URL format:
  https://app.mews.com/distributor/{uuid}?mewsStart=2026-06-01&mewsEnd=2026-06-03&mewsAdultCount=2&mewsChildCount=0
"""

import re
from typing import Optional
from urllib.parse import urlencode

from lib.deeplink.engines.base import EngineBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult


class MewsBuilder(EngineBuilder):
    engine_name = "Mews"
    confidence = DeepLinkConfidence.HIGH

    def extract_slug(self, url: str) -> Optional[str]:
        """Extract distributor UUID from Mews URL.

        Pattern: /distributor/{uuid} where uuid is 8-4-4-4-12 hex.
        """
        match = re.search(r"/distributor/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", url)
        return match.group(1) if match else None

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
            "mewsStart": request.checkin.isoformat(),
            "mewsEnd": request.checkout.isoformat(),
            "mewsAdultCount": request.adults,
            "mewsChildCount": request.children,
        }
        if request.promo_code:
            params["mewsVoucherCode"] = request.promo_code

        base = f"https://app.mews.com/distributor/{slug}"
        url = f"{base}?{urlencode(params)}"

        return DeepLinkResult(
            url=url,
            engine_name=self.engine_name,
            confidence=self.confidence,
            dates_prefilled=True,
            original_url=request.booking_url,
        )
