"""
Reverse Lookup - Find hotels by their booking engine software.

Instead of searching for hotels and detecting their booking engine,
we search for booking engine URLs directly. These are pre-qualified leads.

Supported engines and their URL patterns:
- Cloudbeds: hotels.cloudbeds.com/reservation/{slug}
- Guesty: *.guestybookings.com
- Little Hotelier: *.littlehotelier.com
- WebRezPro: "powered by webrezpro"
- Lodgify: *.lodgify.com
- Hostaway: *.hostaway.com
"""

import re
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from loguru import logger
from pydantic import BaseModel


SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Booking engine dork patterns
# Each tuple: (engine_name, dork_template, url_pattern_regex)
BOOKING_ENGINE_DORKS = [
    # Cloudbeds - Major PMS
    ("cloudbeds", 'site:hotels.cloudbeds.com {location}', r'hotels\.cloudbeds\.com/(?:en/)?reservation/(\w+)'),
    ("cloudbeds", '"powered by cloudbeds" {location} hotel', r'cloudbeds'),
    ("cloudbeds", 'inurl:cloudbeds.com/reservation {location}', r'cloudbeds\.com'),

    # Guesty - Vacation rental focused
    ("guesty", 'site:guestybookings.com {location}', r'guestybookings\.com'),
    ("guesty", 'inurl:guesty {location} hotel', r'guesty'),

    # Little Hotelier - Small hotels
    ("little_hotelier", 'site:littlehotelier.com {location}', r'littlehotelier\.com'),
    ("little_hotelier", 'inurl:littlehotelier {location}', r'littlehotelier'),

    # WebRezPro
    ("webrezpro", '"powered by webrezpro" {location}', r'webrezpro'),
    ("webrezpro", 'inurl:webrezpro {location} hotel', r'webrezpro'),

    # Lodgify - Vacation rentals
    ("lodgify", 'site:lodgify.com {location}', r'lodgify\.com'),
    ("lodgify", 'inurl:lodgify {location} hotel', r'lodgify'),

    # Hostaway
    ("hostaway", 'site:hostaway.com {location}', r'hostaway\.com'),

    # RMS Cloud
    ("rms_cloud", '"powered by rms cloud" {location} hotel', r'rms'),
    ("rms_cloud", 'inurl:rmscloud {location}', r'rmscloud'),

    # innRoad
    ("innroad", '"powered by innroad" {location}', r'innroad'),
    ("innroad", 'inurl:innroad {location} hotel', r'innroad'),

    # ResNexus
    ("resnexus", 'site:resnexus.com {location}', r'resnexus\.com'),
    ("resnexus", 'inurl:resnexus {location}', r'resnexus'),

    # SiteMinder
    ("siteminder", '"powered by siteminder" {location} hotel', r'siteminder'),
    ("siteminder", 'inurl:siteminder {location}', r'siteminder'),

    # Mews
    ("mews", 'inurl:mews.com {location} hotel', r'mews\.com'),
    ("mews", '"powered by mews" {location}', r'mews'),

    # Clock PMS
    ("clock_pms", 'inurl:clock-software {location} hotel', r'clock'),

    # eviivo
    ("eviivo", 'inurl:eviivo {location} hotel', r'eviivo'),
    ("eviivo", '"powered by eviivo" {location}', r'eviivo'),

    # Beds24
    ("beds24", 'inurl:beds24 {location}', r'beds24'),

    # Sirvoy
    ("sirvoy", 'inurl:sirvoy {location} hotel', r'sirvoy'),
    ("sirvoy", '"book now" sirvoy {location}', r'sirvoy'),

    # ThinkReservations
    ("thinkreservations", 'inurl:thinkreservations {location}', r'thinkreservations'),

    # Direct booking patterns (fallback - needs manual engine detection)
    ("unknown", 'intitle:"book direct" "independent hotel" {location}', None),
    ("unknown", 'intitle:"official site" hotel {location} "book now"', None),
]


class ReverseLookupResult(BaseModel):
    """A hotel found via reverse lookup."""
    name: str
    booking_url: str
    booking_engine: str
    website: Optional[str] = None  # Main website (if different from booking URL)
    snippet: Optional[str] = None  # Search result snippet
    source_dork: str  # The dork that found this


class ReverseLookupStats(BaseModel):
    """Stats from a reverse lookup run."""
    dorks_run: int = 0
    api_calls: int = 0
    results_found: int = 0
    unique_results: int = 0
    by_engine: dict = {}


class ReverseLookupService:
    """Find hotels by searching for their booking engine URLs."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._seen_urls: set = set()
        self._stats = ReverseLookupStats()

    async def search_dork(
        self,
        dork: str,
        num_results: int = 100,
    ) -> List[dict]:
        """Run a single Google dork via Serper."""
        self._stats.api_calls += 1

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                SERPER_SEARCH_URL,
                headers={"X-API-KEY": self.api_key, "Content-Type": "application/json"},
                json={"q": dork, "num": num_results},
            )

            if resp.status_code != 200:
                logger.error(f"Serper error {resp.status_code}: {resp.text[:100]}")
                return []

            data = resp.json()
            return data.get("organic", [])

    def _extract_hotel_name(self, title: str, url: str) -> str:
        """Extract hotel name from search result title."""
        # Remove common suffixes
        name = title
        suffixes = [
            " - Book Direct", " | Book Now", " - Official Site",
            " - Reservations", " | Reservations", " - Hotels.com",
            " - Booking.com", " | Booking", " - Cloudbeds",
            ", United States of America", ", USA", ", Florida",
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
            # Also try case-insensitive
            if name.lower().endswith(suffix.lower()):
                name = name[:-len(suffix)]

        # If name is still the full URL, try to extract from URL
        if "cloudbeds.com" in name.lower() or len(name) > 100:
            # Try to get slug from URL
            match = re.search(r'/reservation/(\w+)', url)
            if match:
                slug = match.group(1)
                # Convert slug to name (e.g., "BeachHotel" -> "Beach Hotel")
                name = re.sub(r'([a-z])([A-Z])', r'\1 \2', slug)

        return name.strip()

    def _extract_website_from_booking_url(self, booking_url: str) -> Optional[str]:
        """Try to extract the hotel's main website from booking URL."""
        # Cloudbeds URLs don't give us the main site
        # We'd need to scrape the booking page to find it
        return None

    async def search_location(
        self,
        location: str,
        engines: Optional[List[str]] = None,
        max_results_per_dork: int = 100,
    ) -> Tuple[List[ReverseLookupResult], ReverseLookupStats]:
        """
        Search for hotels in a location using booking engine dorks.

        Args:
            location: Location string (e.g., "Palm Beach Florida", "Miami FL")
            engines: List of engine names to search (None = all)
            max_results_per_dork: Max results per dork query

        Returns:
            Tuple of (results list, stats)
        """
        self._seen_urls = set()
        self._stats = ReverseLookupStats()
        results: List[ReverseLookupResult] = []

        # Filter dorks by engine if specified
        dorks_to_run = BOOKING_ENGINE_DORKS
        if engines:
            engines_lower = [e.lower() for e in engines]
            dorks_to_run = [d for d in BOOKING_ENGINE_DORKS if d[0] in engines_lower]

        logger.info(f"Running {len(dorks_to_run)} dorks for location: {location}")

        for engine_name, dork_template, url_pattern in dorks_to_run:
            dork = dork_template.format(location=location)
            self._stats.dorks_run += 1

            try:
                search_results = await self.search_dork(dork, max_results_per_dork)
                self._stats.results_found += len(search_results)

                for r in search_results:
                    url = r.get("link", "")
                    title = r.get("title", "")
                    snippet = r.get("snippet", "")

                    # Skip if we've seen this URL
                    if url in self._seen_urls:
                        continue
                    self._seen_urls.add(url)

                    # Validate URL matches expected pattern (if pattern provided)
                    if url_pattern and not re.search(url_pattern, url, re.I):
                        continue

                    # Extract hotel name
                    name = self._extract_hotel_name(title, url)
                    if not name or len(name) < 3:
                        continue

                    result = ReverseLookupResult(
                        name=name,
                        booking_url=url,
                        booking_engine=engine_name,
                        snippet=snippet,
                        source_dork=dork,
                    )
                    results.append(result)

                    # Track by engine
                    if engine_name not in self._stats.by_engine:
                        self._stats.by_engine[engine_name] = 0
                    self._stats.by_engine[engine_name] += 1

                logger.debug(f"Dork '{dork[:50]}...' -> {len(search_results)} results")

            except Exception as e:
                logger.error(f"Error running dork '{dork}': {e}")

        self._stats.unique_results = len(results)

        logger.info(
            f"Reverse lookup complete: {self._stats.unique_results} unique results "
            f"from {self._stats.api_calls} API calls"
        )
        for engine, count in sorted(self._stats.by_engine.items(), key=lambda x: -x[1]):
            logger.info(f"  {engine}: {count}")

        return results, self._stats

    async def search_multiple_locations(
        self,
        locations: List[str],
        engines: Optional[List[str]] = None,
        max_results_per_dork: int = 50,
    ) -> Tuple[List[ReverseLookupResult], ReverseLookupStats]:
        """
        Search multiple locations, deduplicating across all.

        Args:
            locations: List of location strings
            engines: List of engine names to search (None = all)
            max_results_per_dork: Max results per dork query

        Returns:
            Combined results and stats
        """
        all_results: List[ReverseLookupResult] = []
        combined_stats = ReverseLookupStats()

        for location in locations:
            results, stats = await self.search_location(
                location=location,
                engines=engines,
                max_results_per_dork=max_results_per_dork,
            )

            # Dedupe against previous locations
            for r in results:
                if r.booking_url not in self._seen_urls:
                    all_results.append(r)

            # Combine stats
            combined_stats.dorks_run += stats.dorks_run
            combined_stats.api_calls += stats.api_calls
            combined_stats.results_found += stats.results_found
            for engine, count in stats.by_engine.items():
                if engine not in combined_stats.by_engine:
                    combined_stats.by_engine[engine] = 0
                combined_stats.by_engine[engine] += count

        combined_stats.unique_results = len(all_results)
        return all_results, combined_stats
