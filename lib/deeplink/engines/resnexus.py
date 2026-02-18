"""ResNexus deep-link URL builder.

URL format:
  https://resnexus.com/resnexus/reservations/book/{GUID}?startdate=MM/DD/YYYY&nights=N&adults=N

Uses startdate (MM/DD/YYYY) + nights (not checkout date).
"""

import re
from typing import Optional

from lib.deeplink.engines.base import EngineBuilder
from lib.deeplink.models import DeepLinkConfidence, DeepLinkRequest, DeepLinkResult


class ResNexusBuilder(EngineBuilder):
    engine_name = "ResNexus"
    confidence = DeepLinkConfidence.HIGH

    def extract_slug(self, url: str) -> Optional[str]:
        """Extract GUID from ResNexus booking URL."""
        match = re.search(
            r"/book/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
            r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
            url,
        )
        return match.group(1) if match else None

    def build(self, request: DeepLinkRequest) -> DeepLinkResult:
        guid = self.extract_slug(request.booking_url)
        if not guid:
            return DeepLinkResult(
                url=request.booking_url,
                engine_name=self.engine_name,
                confidence=DeepLinkConfidence.NONE,
                dates_prefilled=False,
                original_url=request.booking_url,
            )

        nights = (request.checkout - request.checkin).days
        startdate = request.checkin.strftime("%m/%d/%Y")

        url = (
            f"https://resnexus.com/resnexus/reservations/book/{guid}"
            f"?startdate={startdate}"
            f"&nights={nights}"
            f"&adults={request.adults}"
        )

        return DeepLinkResult(
            url=url,
            engine_name=self.engine_name,
            confidence=self.confidence,
            dates_prefilled=True,
            original_url=request.booking_url,
        )
