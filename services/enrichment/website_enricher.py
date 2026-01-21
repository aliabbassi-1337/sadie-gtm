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
SERPER_PLACES_URL = "https://google.serper.dev/places"

# Domains to skip (OTAs, directories, not the hotel's own site)
SKIP_DOMAINS = {
    # OTAs
    "booking.com", "expedia.com", "hotels.com", "tripadvisor.com",
    "trivago.com", "kayak.com", "priceline.com", "agoda.com",
    "orbitz.com", "travelocity.com", "hotwire.com",
    "airbnb.com", "vrbo.com", "homeaway.com", "momondo.com",
    "hostelworld.com", "decolar.com", "despegar.com", "skyscanner.com",
    # Meta-search / aggregators
    "bluepillow.com", "vio.com", "wowotrip.com", "getaroom.com",
    "hotellook.com", "hotelscombined.com", "roomkey.com",
    # Social media
    "yelp.com", "facebook.com", "instagram.com", "twitter.com",
    "linkedin.com", "youtube.com", "pinterest.com", "tiktok.com",
    # Directories
    "yellowpages.com", "whitepages.com", "bbb.org",
    "mapquest.com", "google.com", "apple.com",
    "wikipedia.org", "wikidata.org",
    "zomato.com", "opentable.com",
    "tripadvisor.ca", "yelp.ca", "b2bhint.com",
    "chamberofcommerce.com", "manta.com", "dnb.com",
    "hotfrog.com", "citysearch.com", "foursquare.com",
    "cylex.us.com", "localdatabase.com", "hotelsone.com",
    "hotelguides.com", "2findlocal.com", "placedirectory.com",
    # Government/license sites
    "myfloridalicense.com", "sunbiz.org", "tallahassee.com", "jacksonville.com",
    "data.tallahassee.com", "data.jacksonville.com", "flcompanyregistry.com",
    # People search (garbage results)
    "spokeo.com", "intelius.com", "socialcatfish.com", "whitepages.com",
    "truepeoplesearch.com", "fastpeoplesearch.com", "beenverified.com",
    "zabasearch.com", "peoplefinders.com", "radaris.com",
    # Chain hotels (not our target)
    "marriott.com", "hilton.com", "ihg.com", "wyndhamhotels.com",
    "choicehotels.com", "hyatt.com", "accor.com", "bestwestern.com",
    "radissonhotels.com",
    # News sites (articles about hotels, not the hotel itself)
    "orlandosentinel.com", "miamiherald.com", "sun-sentinel.com",
    "tampabay.com", "jacksonville.com", "floridatoday.com",
    "news-press.com", "news-journalonline.com", "heraldtribune.com",
    "naplesnews.com", "palmbeachpost.com", "tcpalm.com",
    "bizjournals.com", "businesswire.com", "prnewswire.com",
    "prweb.com", "globenewswire.com", "marketwatch.com",
    "patch.com", "local10.com", "wsvn.com", "wplg.com",
    "nytimes.com", "wsj.com", "usatoday.com", "cnn.com",
    # Real estate / rentals (not short-term accommodation)
    "zillow.com", "apartments.com", "rent.com", "trulia.com",
    "realtor.com", "redfin.com", "hotpads.com", "apartmentfinder.com",
    # Event / wedding venues
    "weddingwire.com", "theknot.com", "eventective.com",
    # Job sites
    "indeed.com", "glassdoor.com", "ziprecruiter.com",
    # Maps
    "maps.google.com", "bing.com", "here.com",
    # Review aggregators
    "oyster.com", "cntraveler.com", "travelandleisure.com",
    "fodors.com", "frommers.com", "lonelyplanet.com",
}

# URL patterns that indicate a bad result (news articles, listings, etc.)
BAD_URL_PATTERNS = [
    "/article/", "/news/", "/story/", "/press-release/",
    "/blog/", "/review/", "/reviews/", "/listing/",
    "/places/", "/map/", "/directory/", "/business/",
    "/profile/", "/company/", "/location/", "/venue/",
    "/event/", "/jobs/", "/careers/", "/apartments/",
    "/rental/", "/rent/", "/sale/", "/buy/",
    "/wiki/", "/about/", "/contact-us/",
    "?hotel=", "?property=", "?listing=",
]


@dataclass
class EnrichmentResult:
    """Result of website enrichment."""
    name: str
    city: str
    website: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
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

    async def find_website_places(
        self,
        name: str,
        address: str,
        city: str,
        state: str = "FL",
    ) -> tuple[Optional[str], Optional[float], Optional[float]]:
        """
        Search for hotel website using Serper Places API.

        Args:
            name: Hotel/business name
            address: Street address
            city: City name
            state: State code

        Returns:
            Tuple of (website, lat, lng) - any can be None
        """
        query = f"{name} {address} {city} {state}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    SERPER_PLACES_URL,
                    headers={
                        "X-API-KEY": self.api_key,
                        "Content-Type": "application/json",
                    },
                    json={"q": query, "num": 5},
                )

                if resp.status_code != 200:
                    return None, None, None

                data = resp.json()
                places = data.get("places", [])

                for place in places:
                    # Extract coordinates
                    lat = place.get("latitude")
                    lng = place.get("longitude")

                    # Extract website
                    website = place.get("website")
                    if website:
                        domain = self._extract_domain(website)
                        # Skip OTAs and directories
                        if domain and domain in SKIP_DOMAINS:
                            website = None  # Skip but keep coords
                        # Skip bad URL patterns
                        elif any(pattern in website.lower() for pattern in BAD_URL_PATTERNS):
                            website = None

                    # Return first place with coords (website optional)
                    if lat and lng:
                        return website, lat, lng

        except Exception:
            pass

        return None, None, None

    async def find_website(
        self,
        name: str,
        city: str,
        state: str = "FL",
        address: Optional[str] = None,
        try_places: bool = True,
    ) -> EnrichmentResult:
        """
        Search for a hotel's website using Places API first, then regular search.

        Args:
            name: Hotel/business name
            city: City name
            state: State code
            address: Street address (optional, improves Places lookup)
            try_places: Whether to try Serper Places API first

        Returns:
            EnrichmentResult with website if found
        """
        result = EnrichmentResult(
            name=name,
            city=city,
            search_query="",
        )

        # Try Serper Places API first if we have an address
        if try_places and address:
            website, lat, lng = await self.find_website_places(name, address, city, state)
            result.lat = lat
            result.lng = lng
            if website:
                result.website = website
                result.search_query = f"places: {name} {address} {city}"
                return result
            # If Places found coords but no website, continue to search

        # Fall back to regular search
        query = f'"{name}" {city} {state} official site'
        result.search_query = query

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
                        # Skip URLs that match bad patterns (news articles, listings, etc.)
                        url_lower = url.lower()
                        if any(pattern in url_lower for pattern in BAD_URL_PATTERNS):
                            continue

                        # Verify it looks like a hotel site
                        title = item.get("title", "").lower()
                        snippet = item.get("snippet", "").lower()
                        name_lower = name.lower()

                        # Skip results that look like news articles
                        news_indicators = ["news", "article", "press release", "announces", "announced", "opening", "opens", "closed", "closing", "sold", "sells", "acquires", "acquired"]
                        if any(ind in title for ind in news_indicators):
                            continue

                        # Skip results that are clearly directories/listings
                        listing_indicators = ["listing", "directory", "business profile", "company profile", "reviews of", "review:", "yelp", "tripadvisor", "best hotels in", "top hotels", "hotels near"]
                        if any(ind in title for ind in listing_indicators):
                            continue

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
        address_key: str = "address",
        try_places: bool = True,
    ) -> tuple[List[EnrichmentResult], EnrichmentStats]:
        """
        Enrich a batch of hotels with websites.

        Args:
            hotels: List of hotel dicts
            name_key: Key for hotel name in dict
            city_key: Key for city in dict
            state_key: Key for state in dict
            address_key: Key for address in dict
            try_places: Whether to try Serper Places API first

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
            address = hotel.get(address_key, "")

            if not name or not city:
                stats.errors += 1
                continue

            result = await self.find_website(name, city, state, address=address, try_places=try_places)
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
