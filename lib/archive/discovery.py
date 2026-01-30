"""Archive slug discovery from Wayback Machine and Common Crawl."""

import asyncio
import re
import logging
from typing import Optional
from urllib.parse import unquote

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class BookingEnginePattern(BaseModel):
    """Pattern configuration for a booking engine."""

    name: str
    # Wayback CDX query URL pattern (use * for wildcard)
    wayback_url_pattern: str
    # Regex to extract slug from URL
    slug_regex: str
    # Common Crawl URL pattern
    commoncrawl_url_pattern: str
    # Whether slug is numeric, hex, or alphanumeric
    slug_type: str = "numeric"


class DiscoveredSlug(BaseModel):
    """A discovered booking engine slug."""

    engine: str
    slug: str
    source_url: str
    archive_source: str  # "wayback" or "commoncrawl"
    timestamp: Optional[str] = None


# Booking engine patterns for archive queries
BOOKING_ENGINE_PATTERNS = [
    BookingEnginePattern(
        name="rms",
        wayback_url_pattern="bookings*.rmscloud.com/Search/Index/*",
        slug_regex=r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+)/",
        commoncrawl_url_pattern="*.rmscloud.com/Search/Index/*",
        slug_type="mixed",  # numeric or hex
    ),
    BookingEnginePattern(
        name="rms_ibe",
        wayback_url_pattern="ibe*.rmscloud.com/*",
        slug_regex=r"ibe\d*\.rmscloud\.com/(\d+)",
        commoncrawl_url_pattern="ibe*.rmscloud.com/*",
        slug_type="numeric",
    ),
    BookingEnginePattern(
        name="cloudbeds",
        wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
        slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
        slug_type="alphanumeric",
    ),
    BookingEnginePattern(
        name="mews",
        wayback_url_pattern="*.mews.com/distributor/*",
        slug_regex=r"/distributor/([a-f0-9-]{36})",
        commoncrawl_url_pattern="*.mews.com/distributor/*",
        slug_type="uuid",
    ),
    BookingEnginePattern(
        name="siteminder",
        wayback_url_pattern="*.siteminder.com/reservations/*",
        slug_regex=r"/reservations/([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="*.siteminder.com/reservations/*",
        slug_type="alphanumeric",
    ),
]


class ArchiveSlugDiscovery(BaseModel):
    """Discover booking engine slugs from web archives."""

    timeout: float = 60.0
    max_results_per_query: int = 10000
    discovered_slugs: dict = Field(default_factory=dict)

    async def discover_all(self) -> dict[str, list[DiscoveredSlug]]:
        """Discover slugs from all sources for all engines."""
        results = {}

        for pattern in BOOKING_ENGINE_PATTERNS:
            logger.info(f"Discovering slugs for {pattern.name}...")
            engine_slugs = []

            # Query Wayback Machine
            wayback_slugs = await self.query_wayback(pattern)
            engine_slugs.extend(wayback_slugs)
            logger.info(f"  Wayback: {len(wayback_slugs)} slugs")

            # Query Common Crawl
            cc_slugs = await self.query_commoncrawl(pattern)
            engine_slugs.extend(cc_slugs)
            logger.info(f"  Common Crawl: {len(cc_slugs)} slugs")

            # Dedupe by slug
            unique_slugs = self._dedupe_slugs(engine_slugs)
            results[pattern.name] = unique_slugs
            logger.info(f"  Total unique: {len(unique_slugs)}")

        return results

    async def query_wayback(
        self, pattern: BookingEnginePattern
    ) -> list[DiscoveredSlug]:
        """Query Wayback Machine CDX API for URLs matching pattern."""
        slugs = []

        # Wayback CDX API endpoint
        cdx_url = "https://web.archive.org/cdx/search/cdx"
        params = {
            "url": pattern.wayback_url_pattern,
            "output": "json",
            "limit": self.max_results_per_query,
            "fl": "original,timestamp",
            "collapse": "urlkey",  # Dedupe by URL
            "filter": "statuscode:200",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(cdx_url, params=params)
                response.raise_for_status()
                data = response.json()

                # First row is headers
                if len(data) > 1:
                    for row in data[1:]:
                        url = row[0]
                        timestamp = row[1] if len(row) > 1 else None
                        slug = self._extract_slug(url, pattern.slug_regex)
                        if slug:
                            slugs.append(
                                DiscoveredSlug(
                                    engine=pattern.name,
                                    slug=slug,
                                    source_url=url,
                                    archive_source="wayback",
                                    timestamp=timestamp,
                                )
                            )
        except httpx.HTTPError as e:
            logger.warning(f"Wayback query failed for {pattern.name}: {e}")
        except Exception as e:
            logger.error(f"Wayback error for {pattern.name}: {e}")

        return slugs

    async def query_commoncrawl(
        self, pattern: BookingEnginePattern
    ) -> list[DiscoveredSlug]:
        """Query Common Crawl Index API for URLs matching pattern."""
        slugs = []

        # Get latest Common Crawl index
        index_url = await self._get_latest_cc_index()
        if not index_url:
            logger.warning("Could not get Common Crawl index")
            return slugs

        params = {
            "url": pattern.commoncrawl_url_pattern,
            "output": "json",
            "limit": self.max_results_per_query,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(index_url, params=params)
                response.raise_for_status()

                # Common Crawl returns NDJSON (newline-delimited)
                for line in response.text.strip().split("\n"):
                    if not line:
                        continue
                    try:
                        import json

                        record = json.loads(line)
                        url = record.get("url", "")
                        timestamp = record.get("timestamp")
                        slug = self._extract_slug(url, pattern.slug_regex)
                        if slug:
                            slugs.append(
                                DiscoveredSlug(
                                    engine=pattern.name,
                                    slug=slug,
                                    source_url=url,
                                    archive_source="commoncrawl",
                                    timestamp=timestamp,
                                )
                            )
                    except Exception:
                        continue
        except httpx.HTTPError as e:
            logger.warning(f"Common Crawl query failed for {pattern.name}: {e}")
        except Exception as e:
            logger.error(f"Common Crawl error for {pattern.name}: {e}")

        return slugs

    async def _get_latest_cc_index(self) -> Optional[str]:
        """Get the URL of the latest Common Crawl index."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    "https://index.commoncrawl.org/collinfo.json"
                )
                response.raise_for_status()
                indexes = response.json()
                if indexes:
                    # Return the latest (first) index
                    return indexes[0].get("cdx-api")
        except Exception as e:
            logger.error(f"Failed to get CC index: {e}")
        return None

    def _extract_slug(self, url: str, regex: str) -> Optional[str]:
        """Extract slug from URL using regex."""
        try:
            url = unquote(url)
            match = re.search(regex, url)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None

    def _dedupe_slugs(self, slugs: list[DiscoveredSlug]) -> list[DiscoveredSlug]:
        """Deduplicate slugs, keeping first occurrence (case-insensitive)."""
        seen = set()
        unique = []
        for slug in slugs:
            # Case-insensitive deduplication
            slug_lower = slug.slug.lower()
            if slug_lower not in seen:
                seen.add(slug_lower)
                unique.append(slug)
        return unique


async def discover_slugs_for_engine(engine_name: str) -> list[DiscoveredSlug]:
    """Discover slugs for a specific engine."""
    discovery = ArchiveSlugDiscovery()
    pattern = next((p for p in BOOKING_ENGINE_PATTERNS if p.name == engine_name), None)
    if not pattern:
        raise ValueError(f"Unknown engine: {engine_name}")

    wayback_slugs = await discovery.query_wayback(pattern)
    cc_slugs = await discovery.query_commoncrawl(pattern)
    all_slugs = wayback_slugs + cc_slugs
    return discovery._dedupe_slugs(all_slugs)
