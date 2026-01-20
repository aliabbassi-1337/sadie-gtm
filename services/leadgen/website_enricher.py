"""
Website Enrichment - Find hotel websites using Serper search.

For hotels from DBPR (or other sources) that don't have websites,
search Google to find their official website.
"""

import asyncio
import re
from dataclasses import dataclass
from typing import List, Optional, Dict
import httpx
from loguru import logger


SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Domains to skip (OTAs, directories, not the hotel's own site)
SKIP_DOMAINS = {
    "booking.com", "expedia.com", "hotels.com", "tripadvisor.com",
    "trivago.com", "kayak.com", "priceline.com", "agoda.com",
    "orbitz.com", "travelocity.com", "hotwire.com",
    "yelp.com", "facebook.com", "instagram.com", "twitter.com",
    "linkedin.com", "youtube.com", "pinterest.com",
    "yellowpages.com", "whitepages.com", "bbb.org",
    "mapquest.com", "google.com", "apple.com",
    "wikipedia.org", "wikidata.org",
    "zomato.com", "opentable.com",
    "airbnb.com", "vrbo.com", "homeaway.com",
}


@dataclass
class EnrichmentResult:
    """Result of website enrichment."""
    name: str
    city: str
    website: Optional[str] = None
    search_query: str = ""
    error: Optional[str] = None


@dataclass
class EnrichmentStats:
    """Stats from enrichment run."""
    total: int = 0
    found: int = 0
    not_found: int = 0
    errors: int = 0
    api_calls: int = 0


class WebsiteEnricher:
    """Find hotel websites using Serper search."""

    def __init__(self, api_key: str, delay_between_requests: float = 0.1):
        self.api_key = api_key
        self.delay = delay_between_requests

    async def find_website(
        self,
        name: str,
        city: str,
        state: str = "FL",
    ) -> EnrichmentResult:
        """
        Search for a hotel's website.

        Args:
            name: Hotel/business name
            city: City name
            state: State code

        Returns:
            EnrichmentResult with website if found
        """
        # Build search query
        query = f'"{name}" {city} {state} official site'

        result = EnrichmentResult(
            name=name,
            city=city,
            search_query=query,
        )

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    SERPER_SEARCH_URL,
                    headers={
                        "X-API-KEY": self.api_key,
                        "Content-Type": "application/json",
                    },
                    json={"q": query, "num": 10},
                )

                if resp.status_code != 200:
                    result.error = f"HTTP {resp.status_code}"
                    return result

                data = resp.json()
                organic = data.get("organic", [])

                # Find first result that's not an OTA/directory
                for item in organic:
                    url = item.get("link", "")
                    domain = self._extract_domain(url)

                    if domain and domain not in SKIP_DOMAINS:
                        # Verify it looks like a hotel site
                        title = item.get("title", "").lower()
                        snippet = item.get("snippet", "").lower()
                        name_lower = name.lower()

                        # Check if result seems related to the hotel
                        name_words = set(name_lower.split())
                        title_words = set(title.split())

                        # At least some overlap in words
                        if name_words & title_words or name_lower[:10] in title:
                            result.website = url
                            return result

                # No suitable website found
                result.error = "no_match"

        except Exception as e:
            result.error = str(e)

        return result

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            # Remove protocol
            if "://" in url:
                url = url.split("://", 1)[1]
            # Get domain
            domain = url.split("/")[0].lower()
            # Remove www
            if domain.startswith("www."):
                domain = domain[4:]
            return domain
        except Exception:
            return None

    async def enrich_batch(
        self,
        hotels: List[Dict],
        name_key: str = "name",
        city_key: str = "city",
        state_key: str = "state",
    ) -> tuple[List[EnrichmentResult], EnrichmentStats]:
        """
        Enrich a batch of hotels with websites.

        Args:
            hotels: List of hotel dicts
            name_key: Key for hotel name in dict
            city_key: Key for city in dict
            state_key: Key for state in dict

        Returns:
            Tuple of (results, stats)
        """
        results = []
        stats = EnrichmentStats(total=len(hotels))

        for i, hotel in enumerate(hotels):
            if (i + 1) % 100 == 0:
                logger.info(f"  Enriching... {i + 1}/{len(hotels)} ({stats.found} found)")

            name = hotel.get(name_key, "")
            city = hotel.get(city_key, "")
            state = hotel.get(state_key, "FL")

            if not name or not city:
                stats.errors += 1
                continue

            result = await self.find_website(name, city, state)
            stats.api_calls += 1

            if result.website:
                stats.found += 1
            elif result.error == "no_match":
                stats.not_found += 1
            else:
                stats.errors += 1

            results.append(result)

            # Rate limit
            await asyncio.sleep(self.delay)

        return results, stats
