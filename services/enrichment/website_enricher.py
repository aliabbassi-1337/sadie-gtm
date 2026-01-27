"""
Website Enrichment - Find hotel websites using Serper search.

For hotels from DBPR (or other sources) that don't have websites,
search Google to find their official website.
"""

import asyncio
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import httpx
from loguru import logger


SERPER_SEARCH_URL = "https://google.serper.dev/search"
SERPER_PLACES_URL = "https://google.serper.dev/places"

# Business suffixes to remove from hotel names
BUSINESS_SUFFIXES = [
    r"\bLLC\b\.?",
    r"\bL\.L\.C\.?\b",
    r"\bINC\.?\b",
    r"\bINCORPORATED\b",
    r"\bCORP\.?\b",
    r"\bCORPORATION\b",
    r"\bLTD\.?\b",
    r"\bLIMITED\b",
    r"\bLP\b\.?",
    r"\bL\.P\.?\b",
    r"\bLLP\b\.?",
    r"\bL\.L\.P\.?\b",
    r"\bPLC\b\.?",
    r"\bP\.L\.C\.?\b",
    r"\bCO\.?\b",
    r"\bCOMPANY\b",
    r"\bGROUP\b",
    r"\bHOLDINGS?\b",
    r"\bENTERPRISES?\b",
    r"\bPROPERTIES\b",
    r"\bMANAGEMENT\b",
    r"\bSERVICES\b",
    r"\bSOLUTIONS\b",
    r"\bINTERNATIONAL\b",
    r"\bINT'?L\.?\b",
    r"\bUSA\b",
    r"\bU\.S\.A\.?\b",
    r"\bOF FLORIDA\b",
    r"\bFL\b",
    r"\bFLA\.?\b",
]

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
    "myfloridalicense.com", "sunbiz.org", "dos.myflorida.com",
    "flhealthsource.gov", "floridahealthfinder.gov",
    "tallahassee.com", "jacksonville.com",
    "data.tallahassee.com", "data.jacksonville.com", "flcompanyregistry.com",
    "opencorporates.com", "bizapedia.com", "corporationwiki.com",
    # People search (garbage results)
    "spokeo.com", "intelius.com", "socialcatfish.com",
    "truepeoplesearch.com", "fastpeoplesearch.com", "beenverified.com",
    "zabasearch.com", "peoplefinders.com", "radaris.com",
    "publicrecords.com", "instantcheckmate.com", "ussearch.com",
    # Chain hotels (not our target)
    "marriott.com", "hilton.com", "ihg.com", "wyndhamhotels.com",
    "choicehotels.com", "hyatt.com", "accor.com", "bestwestern.com",
    "radissonhotels.com", "motel6.com", "redlion.com", "laquinta.com",
    # News sites
    "orlandosentinel.com", "miamiherald.com", "sun-sentinel.com",
    "tampabay.com", "jacksonville.com", "floridatoday.com",
    "news-press.com", "news-journalonline.com", "heraldtribune.com",
    "naplesnews.com", "palmbeachpost.com", "tcpalm.com",
    "bizjournals.com", "businesswire.com", "prnewswire.com",
    "prweb.com", "globenewswire.com", "marketwatch.com",
    "patch.com", "local10.com", "wsvn.com", "wplg.com",
    "nytimes.com", "wsj.com", "usatoday.com", "cnn.com",
    # Real estate / rentals
    "zillow.com", "apartments.com", "rent.com", "trulia.com",
    "realtor.com", "redfin.com", "hotpads.com", "apartmentfinder.com",
    "loopnet.com", "costar.com", "crexi.com",
    # Event / wedding venues
    "weddingwire.com", "theknot.com", "eventective.com",
    # Job sites
    "indeed.com", "glassdoor.com", "ziprecruiter.com",
    # Maps
    "maps.google.com", "bing.com", "here.com",
    # Review aggregators
    "oyster.com", "cntraveler.com", "travelandleisure.com",
    "fodors.com", "frommers.com", "lonelyplanet.com",
    # Hotel tech / PMS vendors
    "mews.com", "cloudbeds.com", "guestline.com", "opera.com",
    "apaleo.com", "protel.net", "hotelogix.com", "roomracoon.com",
    "littlehotelier.com", "sirvoy.com", "webrezpro.com", "innroad.com",
    "ezee.com", "stayntouch.com", "hoteltechreport.com", "hotel-online.com",
    "hospitalityleaderonline.com", "hospitalitynet.org",
    "hotelmanagement.net", "hotelnewsnow.com", "htrends.com",
    "hotelsmag.com", "hotelbusiness.com", "lodgingmagazine.com",
    "ehotelier.com", "hotelexecutive.com", "phocuswire.com", "skift.com",
    # Job boards
    "startup.jobs", "lever.co", "greenhouse.io", "workable.com",
    # Software comparison sites
    "g2.com", "capterra.com", "softwareadvice.com", "getapp.com",
    # Vacation rental aggregators
    "redawning.com", "vacasa.com", "evolve.com", "turnkeyvr.com",
    # Florida-specific garbage
    "florida-ede.org", "floridastateparks.org", "visitflorida.com",
}

# URL patterns that indicate a bad result
BAD_URL_PATTERNS = [
    "/article/", "/news/", "/story/", "/press-release/",
    "/blog/", "/review/", "/reviews/", "/listing/",
    "/places/", "/map/", "/directory/", "/business/",
    "/profile/", "/company/", "/location/", "/venue/",
    "/event/", "/jobs/", "/careers/", "/apartments/",
    "/rental/", "/rent/", "/sale/", "/buy/",
    "/wiki/", "/about/", "/contact-us/",
    "?hotel=", "?property=", "?listing=",
    "/customers/", "/case-study/", "/resources/", "/events/",
    "/compare/", "/webinar/", "/podcast/",
    "/ebook/", "/whitepaper/", "/report/", "/guide/",
    "/doc/", "/documentation/", "/support/", "/help/",
    "/search?", "/results?", "/find?",
]

# Chain hotel name patterns - skip enrichment for these (waste of API calls)
CHAIN_NAME_PATTERNS = [
    # Marriott brands
    "marriott", "ritz carlton", "westin", "sheraton", "w hotel",
    "st. regis", "st regis", "le meridien", "four points", "aloft",
    "springhill", "residence inn", "fairfield", "courtyard", "ac hotel",
    "moxy", "protea", "element", "towneplace",
    # Hilton brands
    "hilton", "waldorf", "conrad", "canopy", "signia", "curio",
    "doubletree", "tapestry", "embassy suites", "hilton garden",
    "hampton inn", "hampton by", "tru by hilton", "homewood suites",
    "home2 suites", "spark by hilton",
    # IHG brands
    "intercontinental", "kimpton", "regent", "six senses", "vignette",
    "hotel indigo", "crowne plaza", "hualuxe", "even hotel",
    "holiday inn", "avid hotel", "candlewood", "staybridge", "atwell",
    # Hyatt brands
    "hyatt", "park hyatt", "andaz", "thompson hotel", "grand hyatt",
    "hyatt regency", "hyatt place", "hyatt house", "hyatt centric",
    # Wyndham brands
    "wyndham", "dolce", "registry", "ramada", "days inn", "super 8",
    "la quinta", "wingate", "hawthorn", "microtel", "travelodge", "trademark",
    # Choice Hotels
    "comfort inn", "comfort suites", "quality inn", "sleep inn",
    "clarion", "econo lodge", "rodeway", "mainstay", "suburban",
    "ascend", "cambria",
    # Best Western
    "best western", "glo", "surestay", "aiden",
    # Radisson
    "radisson", "park plaza", "park inn", "country inn",
    # Accor
    "sofitel", "pullman", "mgallery", "swissotel", "novotel",
    "mercure", "ibis", "greet", "motel 6",
    # Others
    "extended stay america", "red roof", "motel 6", "studio 6",
]


def is_chain_hotel(name: str) -> bool:
    """Check if hotel name indicates a chain hotel."""
    if not name:
        return False
    name_lower = name.lower()
    return any(chain in name_lower for chain in CHAIN_NAME_PATTERNS)


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
    confidence: str = "none"  # high, medium, low, none
    validated: bool = False


@dataclass
class EnrichmentStats:
    """Stats from enrichment run."""
    total: int = 0
    found: int = 0
    not_found: int = 0
    errors: int = 0
    api_calls: int = 0
    validated: int = 0
    by_confidence: Dict[str, int] = field(default_factory=lambda: {"high": 0, "medium": 0, "low": 0})


def clean_hotel_name(name: str) -> str:
    """Clean hotel name for better search results.

    Removes business suffixes like LLC, INC, CORP, etc.
    Normalizes whitespace and casing.
    """
    if not name:
        return ""

    cleaned = name.strip()

    # Remove business suffixes
    for suffix in BUSINESS_SUFFIXES:
        cleaned = re.sub(suffix, "", cleaned, flags=re.IGNORECASE)

    # Remove trailing punctuation and whitespace
    cleaned = re.sub(r"[,.\-\s]+$", "", cleaned)

    # Normalize multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Title case if all caps
    if cleaned.isupper():
        cleaned = cleaned.title()

    return cleaned.strip()


class WebsiteEnricher:
    """Find hotel websites using Serper search."""

    def __init__(
        self,
        api_key: str,
        delay_between_requests: float = 0.0,
        max_concurrent: int = 50,
        max_retries: int = 3,
        validate_urls: bool = True,
    ):
        self.api_key = api_key
        self.delay = delay_between_requests
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.validate_urls = validate_urls
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0)
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> Optional[httpx.Response]:
        """Make HTTP request with retry logic."""
        client = self._client or httpx.AsyncClient(timeout=30.0)
        close_client = self._client is None

        try:
            for attempt in range(self.max_retries):
                try:
                    if method == "GET":
                        resp = await client.get(url, **kwargs)
                    else:
                        resp = await client.post(url, **kwargs)

                    if resp.status_code == 429:  # Rate limited
                        wait = 2 ** attempt
                        logger.warning(f"Rate limited, waiting {wait}s...")
                        await asyncio.sleep(wait)
                        continue

                    return resp

                except (httpx.TimeoutException, httpx.ConnectError) as e:
                    if attempt < self.max_retries - 1:
                        wait = 2 ** attempt
                        logger.warning(f"Request failed ({e}), retrying in {wait}s...")
                        await asyncio.sleep(wait)
                    else:
                        raise
        finally:
            if close_client:
                await client.aclose()

        return None

    async def _validate_website(self, url: str) -> bool:
        """Check if website actually resolves."""
        if not self.validate_urls:
            return True

        try:
            resp = await self._request_with_retry(
                "GET",
                url,
                follow_redirects=True,
                timeout=10.0,
            )
            return resp is not None and resp.status_code < 400
        except Exception:
            return False

    async def find_website_places(
        self,
        name: str,
        address: str,
        city: str,
        state: str = "FL",
    ) -> tuple[Optional[str], Optional[float], Optional[float], str]:
        """
        Search for hotel website using Serper Places API.

        Returns:
            Tuple of (website, lat, lng, confidence)
        """
        cleaned_name = clean_hotel_name(name)
        query = f"{cleaned_name}, {city}, Florida"

        try:
            resp = await self._request_with_retry(
                "POST",
                SERPER_PLACES_URL,
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 5},
            )

            if not resp or resp.status_code != 200:
                return None, None, None, "none"

            data = resp.json()
            places = data.get("places", [])

            for place in places:
                lat = place.get("latitude")
                lng = place.get("longitude")
                website = place.get("website")

                if website:
                    domain = self._extract_domain(website)
                    if domain and domain in SKIP_DOMAINS:
                        website = None
                    elif any(pattern in website.lower() for pattern in BAD_URL_PATTERNS):
                        website = None

                if lat and lng:
                    confidence = "high" if website else "none"
                    return website, lat, lng, confidence

        except Exception:
            pass

        return None, None, None, "none"

    async def find_by_coordinates(
        self,
        lat: float,
        lon: float,
        category: str = "hotel",
    ) -> Optional[Dict]:
        """
        Search for hotel at specific coordinates using Serper Places API.

        Args:
            lat: Latitude
            lon: Longitude
            category: Search query (hotel, motel, etc.)

        Returns:
            Dict with name, website, phone, rating, cid or None if not found
        """
        try:
            resp = await self._request_with_retry(
                "POST",
                SERPER_PLACES_URL,
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "q": category,
                    "ll": f"@{lat},{lon},17z",
                },
            )

            if not resp or resp.status_code != 200:
                return None

            data = resp.json()
            places = data.get("places", [])

            if not places:
                return None

            # Get closest result (first one since we searched at exact coords)
            best = places[0]

            # Filter website
            website = best.get("website")
            if website:
                domain = self._extract_domain(website)
                if domain and domain in SKIP_DOMAINS:
                    website = None
                elif any(pattern in website.lower() for pattern in BAD_URL_PATTERNS):
                    website = None

            return {
                "name": best.get("title"),
                "website": website,
                "phone": best.get("phoneNumber"),
                "rating": best.get("rating"),
                "cid": best.get("cid"),
                "address": best.get("address"),
            }

        except Exception as e:
            logger.debug(f"Error searching coordinates ({lat}, {lon}): {e}")
            return None

    async def find_by_name(
        self,
        name: str,
    ) -> Optional[Dict]:
        """
        Search for hotel details using Serper Places API by name only.

        Used for geocoding crawl data hotels that have names but no location.
        Returns coordinates, address, phone, and other details.

        Args:
            name: Hotel name to search for

        Returns:
            Dict with name, address, phone, latitude, longitude, website, rating or None
        """
        cleaned_name = clean_hotel_name(name)
        query = f"{cleaned_name} hotel"

        try:
            resp = await self._request_with_retry(
                "POST",
                SERPER_PLACES_URL,
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 5},
            )

            if not resp or resp.status_code != 200:
                return None

            data = resp.json()
            places = data.get("places", [])

            if not places:
                return None

            # Get the best match - first result or one with matching name
            best = None
            cleaned_lower = cleaned_name.lower()
            
            for place in places:
                title = place.get("title", "").lower()
                # Prefer exact or close name match
                if cleaned_lower in title or title in cleaned_lower:
                    best = place
                    break
            
            if not best:
                best = places[0]

            # Filter website
            website = best.get("website")
            if website:
                domain = self._extract_domain(website)
                if domain and domain in SKIP_DOMAINS:
                    website = None
                elif any(pattern in website.lower() for pattern in BAD_URL_PATTERNS):
                    website = None

            return {
                "name": best.get("title"),
                "address": best.get("address"),
                "phone": best.get("phoneNumber"),
                "email": best.get("email"),  # Serper Places may include email for some businesses
                "latitude": best.get("latitude"),
                "longitude": best.get("longitude"),
                "website": website,
                "rating": best.get("rating"),
                "cid": best.get("cid"),
            }

        except Exception as e:
            logger.debug(f"Error searching by name '{name}': {e}")
            return None

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
        """
        result = EnrichmentResult(
            name=name,
            city=city,
            search_query="",
        )

        # Skip chain hotels - they don't have independent websites worth finding
        if is_chain_hotel(name):
            result.error = "chain_hotel"
            return result

        cleaned_name = clean_hotel_name(name)

        # Try Serper Places API first
        if try_places and address:
            website, lat, lng, confidence = await self.find_website_places(
                name, address, city, state
            )
            result.lat = lat
            result.lng = lng
            if website:
                result.website = website
                result.confidence = confidence
                result.search_query = f"places: {cleaned_name}, {city}, Florida"
                return result

        # Fall back to regular search
        query = f'"{cleaned_name}" hotel {city} Florida'
        result.search_query = query

        try:
            resp = await self._request_with_retry(
                "POST",
                SERPER_SEARCH_URL,
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 10},
            )

            if not resp or resp.status_code != 200:
                result.error = f"HTTP {resp.status_code if resp else 'timeout'}"
                return result

            data = resp.json()
            organic = data.get("organic", [])

            name_lower = cleaned_name.lower()
            name_words = set(name_lower.split())

            for item in organic:
                url = item.get("link", "")
                domain = self._extract_domain(url)

                if not domain or domain in SKIP_DOMAINS:
                    continue

                url_lower = url.lower()
                if any(pattern in url_lower for pattern in BAD_URL_PATTERNS):
                    continue

                title = item.get("title", "").lower()

                # Skip news/marketing content
                skip_indicators = [
                    "news", "article", "press release", "announces", "announced",
                    "opening", "opens", "closed", "closing", "sold", "sells",
                    "listing", "directory", "business profile", "reviews of",
                    "case study", "customer story", "guide to", "vs.", "comparison",
                ]
                if any(ind in title for ind in skip_indicators):
                    continue

                # Check name match
                title_words = set(title.split())
                overlap = name_words & title_words

                if len(overlap) >= 2 or name_lower[:10] in title:
                    result.website = url
                    result.confidence = "medium" if len(overlap) >= 3 else "low"
                    return result

            result.error = "no_match"

        except Exception as e:
            result.error = str(e)

        return result

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            if "://" in url:
                url = url.split("://", 1)[1]
            domain = url.split("/")[0].lower()
            if domain.startswith("www."):
                domain = domain[4:]
            return domain
        except Exception:
            return None

    async def _enrich_single(
        self,
        hotel: Dict,
        name_key: str,
        city_key: str,
        state_key: str,
        address_key: str,
        try_places: bool,
    ) -> Optional[EnrichmentResult]:
        """Enrich a single hotel (with semaphore)."""
        async with self._semaphore:
            name = hotel.get(name_key, "")
            city = hotel.get(city_key, "")
            state = hotel.get(state_key, "FL")
            address = hotel.get(address_key, "")

            if not name or not city:
                return None

            result = await self.find_website(
                name, city, state, address=address, try_places=try_places
            )

            # Validate URL if found
            if result.website and self.validate_urls:
                result.validated = await self._validate_website(result.website)
                if not result.validated:
                    logger.debug(f"URL failed validation: {result.website}")

            if self.delay > 0:
                await asyncio.sleep(self.delay)
            return result

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
        Enrich a batch of hotels with websites (parallel processing).
        """
        stats = EnrichmentStats(total=len(hotels))

        # Use context manager if not already in one
        if self._client is None:
            async with self:
                return await self._enrich_batch_internal(
                    hotels, name_key, city_key, state_key, address_key, try_places, stats
                )
        else:
            return await self._enrich_batch_internal(
                hotels, name_key, city_key, state_key, address_key, try_places, stats
            )

    async def _enrich_batch_internal(
        self,
        hotels: List[Dict],
        name_key: str,
        city_key: str,
        state_key: str,
        address_key: str,
        try_places: bool,
        stats: EnrichmentStats,
    ) -> tuple[List[EnrichmentResult], EnrichmentStats]:
        """Internal batch processing."""
        tasks = [
            self._enrich_single(h, name_key, city_key, state_key, address_key, try_places)
            for h in hotels
        ]

        results = []
        completed = 0

        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1

            if completed % 100 == 0:
                logger.info(f"  Enriching... {completed}/{len(hotels)} ({stats.found} found)")

            if result is None:
                stats.errors += 1
                continue

            stats.api_calls += 1

            if result.website:
                stats.found += 1
                stats.by_confidence[result.confidence] = stats.by_confidence.get(result.confidence, 0) + 1
                if result.validated:
                    stats.validated += 1
            elif result.error == "no_match":
                stats.not_found += 1
            else:
                stats.errors += 1

            results.append(result)

        return results, stats
