"""SiteMinder deep-link URL builder.

Without rate_id (room selection page, dates pre-filled):
  https://direct-book.com/properties/{slug}?checkInDate=2026-03-01&checkOutDate=2026-03-03&items[0][adults]=2&items[0][children]=0

With rate_id (straight to checkout):
  https://direct-book.com/properties/{slug}/book?checkInDate=2026-03-01&checkOutDate=2026-03-03&items[0][adults]=2&items[0][children]=0&items[0][infants]=0&items[0][rateId]=273850&selected=0&step=step1
"""

import re
from typing import Optional
from urllib.parse import urlencode, urlparse

from lib.deeplink.engines.base import EngineBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult


class SiteMinderBuilder(EngineBuilder):
    engine_name = "SiteMinder"
    confidence = DeepLinkConfidence.HIGH

    def extract_slug(self, url: str) -> Optional[str]:
        """Extract channel code from direct-book.com URL.

        Regex from lib/siteminder/api_client.py:extract_channel_code
        """
        parsed = urlparse(url)
        match = re.match(r"^/properties/([^/?]+)", parsed.path)
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
            "checkInDate": request.checkin.isoformat(),
            "checkOutDate": request.checkout.isoformat(),
            "items[0][adults]": request.adults,
            "items[0][children]": request.children,
            "items[0][infants]": 0,
        }

        if request.rate_id:
            # With rate_id: go to /book checkout page
            params["items[0][rateId]"] = request.rate_id
            params["selected"] = 0
            params["step"] = "step1"
            base = f"https://direct-book.com/properties/{slug}/book"
        else:
            base = f"https://direct-book.com/properties/{slug}"

        if request.promo_code:
            params["promoCode"] = request.promo_code

        url = f"{base}?{urlencode(params)}"

        return DeepLinkResult(
            url=url,
            engine_name=self.engine_name,
            confidence=self.confidence,
            dates_prefilled=True,
            original_url=request.booking_url,
        )
