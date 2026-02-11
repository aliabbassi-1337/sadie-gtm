"""Archive slug discovery from Wayback Machine, Common Crawl, AlienVault OTX, and URLScan.io."""

import asyncio
import json
import os
import re
import logging
from typing import Optional, Set

import httpx
from pydantic import BaseModel, Field
from urllib.parse import unquote

logger = logging.getLogger(__name__)

# Number of historical Common Crawl indexes to query
DEFAULT_CC_INDEX_COUNT = 40  # Query last 40 indexes (~2 years of crawls)


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
    # Domains to query for OSINT sources (AlienVault, URLScan)
    domains: list[str] = Field(default_factory=list)


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
        # Match hex slugs (16 char), numeric slugs, and slugs with format like "13915/90"
        slug_regex=r"/(?:Search|Rates)/Index/([A-Fa-f0-9]{16}|\d+(?:/\d+)?)",
        commoncrawl_url_pattern="*.rmscloud.com/Search/Index/*",
        slug_type="mixed",  # numeric or hex
        domains=["rmscloud.com"],
    ),
    BookingEnginePattern(
        name="rms_rates",
        wayback_url_pattern="bookings*.rmscloud.com/Rates/Index/*",
        slug_regex=r"/Rates/Index/([A-Fa-f0-9]{16}|\d+(?:/\d+)?)",
        commoncrawl_url_pattern="*.rmscloud.com/Rates/Index/*",
        slug_type="mixed",
        domains=["rmscloud.com"],
    ),
    BookingEnginePattern(
        name="rms_ibe",
        wayback_url_pattern="ibe*.rmscloud.com/*",
        slug_regex=r"ibe\d*\.rmscloud\.com/(\d+)",
        commoncrawl_url_pattern="ibe*.rmscloud.com/*",
        slug_type="numeric",
        domains=["rmscloud.com"],
    ),
    BookingEnginePattern(
        name="cloudbeds",
        wayback_url_pattern="hotels.cloudbeds.com/reservation/*",
        slug_regex=r"/reservation/([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="hotels.cloudbeds.com/reservation/*",
        slug_type="alphanumeric",
        domains=["cloudbeds.com"],
    ),
    BookingEnginePattern(
        name="mews",
        wayback_url_pattern="*.mews.com/distributor/*",
        slug_regex=r"/distributor/([a-f0-9-]{36})",
        commoncrawl_url_pattern="*.mews.com/distributor/*",
        slug_type="uuid",
        domains=["mews.com"],
    ),
    BookingEnginePattern(
        name="siteminder",
        wayback_url_pattern="*.siteminder.com/reservations/*",
        slug_regex=r"/reservations/([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="*.siteminder.com/reservations/*",
        slug_type="alphanumeric",
        domains=["siteminder.com"],
    ),
    BookingEnginePattern(
        name="siteminder_directbook",
        wayback_url_pattern="direct-book.com/properties/*",
        slug_regex=r"/properties/([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="*.direct-book.com/properties/*",
        slug_type="alphanumeric",
        domains=["direct-book.com"],
    ),
    # Additional booking engines
    BookingEnginePattern(
        name="bookonlinenow",
        wayback_url_pattern="*.book-onlinenow.net/*",
        slug_regex=r"([a-z0-9_-]+)\.book-onlinenow\.net",
        commoncrawl_url_pattern="*.book-onlinenow.net/*",
        slug_type="alphanumeric",
        domains=["book-onlinenow.net"],
    ),
    BookingEnginePattern(
        name="resnexus",
        wayback_url_pattern="*.resnexus.com/*",
        slug_regex=r"([a-z0-9_-]+)\.resnexus\.com",
        commoncrawl_url_pattern="*.resnexus.com/*",
        slug_type="alphanumeric",
        domains=["resnexus.com"],
    ),
    BookingEnginePattern(
        name="webrez",
        wayback_url_pattern="secure.webrez.com/hotel/*",
        slug_regex=r"/hotel/(\d+)",
        commoncrawl_url_pattern="*.webrez.com/hotel/*",
        slug_type="numeric",
        domains=["webrez.com"],
    ),
    BookingEnginePattern(
        name="thinkreservations",
        wayback_url_pattern="*.thinkreservations.com/book/*",
        slug_regex=r"([a-z0-9_-]+)\.thinkreservations\.com",
        commoncrawl_url_pattern="*.thinkreservations.com/*",
        slug_type="alphanumeric",
        domains=["thinkreservations.com"],
    ),
    BookingEnginePattern(
        name="innroad",
        wayback_url_pattern="*.innroad.com/*",
        slug_regex=r"([a-z0-9_-]+)\.innroad\.com",
        commoncrawl_url_pattern="*.innroad.com/*",
        slug_type="alphanumeric",
        domains=["innroad.com"],
    ),
    BookingEnginePattern(
        name="lodgify",
        wayback_url_pattern="checkout.lodgify.com/*",
        slug_regex=r"/([A-Za-z0-9_-]+)/",
        commoncrawl_url_pattern="*.lodgify.com/*",
        slug_type="alphanumeric",
        domains=["lodgify.com"],
    ),
    BookingEnginePattern(
        name="hostaway",
        wayback_url_pattern="*.hostaway.com/listing/*",
        slug_regex=r"/listing/(\d+)",
        commoncrawl_url_pattern="*.hostaway.com/listing/*",
        slug_type="numeric",
        domains=["hostaway.com"],
    ),
    BookingEnginePattern(
        name="guesty",
        wayback_url_pattern="*.guesty.com/properties/*",
        slug_regex=r"/properties/([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="*.guesty.com/properties/*",
        slug_type="alphanumeric",
        domains=["guesty.com"],
    ),
    BookingEnginePattern(
        name="streamline",
        wayback_url_pattern="*.streamlinevrs.com/*",
        slug_regex=r"([a-z0-9_-]+)\.streamlinevrs\.com",
        commoncrawl_url_pattern="*.streamlinevrs.com/*",
        slug_type="alphanumeric",
        domains=["streamlinevrs.com"],
    ),
    BookingEnginePattern(
        name="escapia",
        wayback_url_pattern="*.escapia.com/property/*",
        slug_regex=r"/property/(\d+)",
        commoncrawl_url_pattern="*.escapia.com/property/*",
        slug_type="numeric",
        domains=["escapia.com"],
    ),
    # ipms247 / Yanolja Cloud Solution (formerly eZee)
    # URL format: live.ipms247.com/booking/book-rooms-{slug}
    BookingEnginePattern(
        name="ipms247",
        wayback_url_pattern="live.ipms247.com/booking/book-rooms-*",
        slug_regex=r"/booking/book-rooms-([A-Za-z0-9_-]+)",
        commoncrawl_url_pattern="live.ipms247.com/booking/book-rooms-*",
        slug_type="alphanumeric",
        domains=["ipms247.com"],
    ),
]


class ArchiveSlugDiscovery(BaseModel):
    """Discover booking engine slugs from web archives."""

    timeout: float = 60.0
    max_results_per_query: int = 50000
    discovered_slugs: dict = Field(default_factory=dict)
    cc_index_count: int = DEFAULT_CC_INDEX_COUNT
    existing_slugs: dict = Field(default_factory=dict)  # engine -> set of existing slugs
    enable_wayback: bool = True
    enable_commoncrawl: bool = True
    enable_alienvault: bool = True
    enable_urlscan: bool = True

    model_config = {"arbitrary_types_allowed": True}

    async def discover_all(
        self,
        existing_slugs: Optional[dict[str, Set[str]]] = None,
    ) -> dict[str, list[DiscoveredSlug]]:
        """
        Discover slugs from all sources for all engines.

        Args:
            existing_slugs: Dict of engine -> set of existing slugs to exclude
        """
        results = {}
        existing = existing_slugs or {}

        for pattern in BOOKING_ENGINE_PATTERNS:
            logger.info(f"Discovering slugs for {pattern.name}...")
            engine_slugs = []
            engine_existing = existing.get(pattern.name, set())

            # Query Wayback Machine
            if self.enable_wayback:
                wayback_slugs = await self.query_wayback(pattern)
                engine_slugs.extend(wayback_slugs)
                logger.info(f"  Wayback: {len(wayback_slugs)} slugs")

            # Query Common Crawl (multiple historical indexes)
            if self.enable_commoncrawl:
                cc_slugs = await self.query_commoncrawl_historical(pattern)
                engine_slugs.extend(cc_slugs)
                logger.info(f"  Common Crawl ({self.cc_index_count} indexes): {len(cc_slugs)} slugs")

            # Query AlienVault OTX
            if self.enable_alienvault:
                av_slugs = await self.query_alienvault(pattern)
                engine_slugs.extend(av_slugs)
                logger.info(f"  AlienVault: {len(av_slugs)} slugs")

            # Query URLScan.io
            if self.enable_urlscan:
                us_slugs = await self.query_urlscan(pattern)
                engine_slugs.extend(us_slugs)
                logger.info(f"  URLScan: {len(us_slugs)} slugs")

            # Dedupe by slug (case-insensitive)
            unique_slugs = self._dedupe_slugs(engine_slugs)
            logger.info(f"  After deduplication: {len(unique_slugs)}")

            # Filter out existing slugs from database
            if engine_existing:
                new_slugs = [
                    s for s in unique_slugs
                    if s.slug.lower() not in engine_existing
                ]
                logger.info(f"  After DB filter: {len(new_slugs)} new (filtered {len(unique_slugs) - len(new_slugs)} existing)")
                unique_slugs = new_slugs

            results[pattern.name] = unique_slugs
            logger.info(f"  Total new: {len(unique_slugs)}")

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

    async def query_alienvault(
        self, pattern: BookingEnginePattern
    ) -> list[DiscoveredSlug]:
        """Query AlienVault OTX passive DNS / URL list for matching URLs."""
        if not pattern.domains:
            return []

        slugs = []
        api_key = os.getenv("ALIENVAULT_OTX_KEY")
        headers = {}
        if api_key:
            headers["X-OTX-API-KEY"] = api_key

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for domain in pattern.domains:
                    page = 1
                    while True:
                        url = (
                            f"https://otx.alienvault.com/api/v1/indicators/domain"
                            f"/{domain}/url_list?limit=500&page={page}"
                        )
                        try:
                            response = await client.get(url, headers=headers)

                            if response.status_code == 429:
                                logger.warning(f"AlienVault rate limited on {domain}, stopping pagination")
                                break

                            response.raise_for_status()
                            data = response.json()

                            url_list = data.get("url_list", [])
                            if not url_list:
                                break

                            for entry in url_list:
                                entry_url = entry.get("url", "")
                                slug = self._extract_slug(entry_url, pattern.slug_regex)
                                if slug:
                                    slugs.append(
                                        DiscoveredSlug(
                                            engine=pattern.name,
                                            slug=slug,
                                            source_url=entry_url,
                                            archive_source="alienvault",
                                        )
                                    )

                            if not data.get("has_next", False):
                                break

                            page += 1
                            await asyncio.sleep(1)

                        except httpx.HTTPStatusError as e:
                            logger.warning(f"AlienVault query failed for {domain} page {page}: {e}")
                            break
                        except httpx.HTTPError as e:
                            logger.warning(f"AlienVault request error for {domain}: {e}")
                            break

        except Exception as e:
            logger.error(f"AlienVault error for {pattern.name}: {e}")

        return slugs

    async def query_urlscan(
        self, pattern: BookingEnginePattern
    ) -> list[DiscoveredSlug]:
        """Query URLScan.io search API for matching URLs."""
        if not pattern.domains:
            return []

        slugs = []
        api_key = os.getenv("URLSCAN_API_KEY")
        headers = {}
        if api_key:
            headers["API-Key"] = api_key

        max_results = 10000

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for domain in pattern.domains:
                    total_fetched = 0
                    search_after = None

                    while total_fetched < max_results:
                        params = {"q": f"domain:{domain}", "size": 100}
                        if search_after:
                            params["search_after"] = search_after

                        try:
                            response = await client.get(
                                "https://urlscan.io/api/v1/search/",
                                params=params,
                                headers=headers,
                            )

                            if response.status_code == 429:
                                retry_after = response.headers.get("Retry-After", "5")
                                try:
                                    wait = int(retry_after)
                                except ValueError:
                                    wait = 5
                                logger.warning(f"URLScan rate limited, waiting {wait}s")
                                await asyncio.sleep(wait)
                                continue

                            response.raise_for_status()
                            data = response.json()

                            results = data.get("results", [])
                            if not results:
                                break

                            for result in results:
                                # Extract from both page.url and task.url
                                for url_field in ("page", "task"):
                                    entry_url = result.get(url_field, {}).get("url", "")
                                    if entry_url:
                                        slug = self._extract_slug(entry_url, pattern.slug_regex)
                                        if slug:
                                            slugs.append(
                                                DiscoveredSlug(
                                                    engine=pattern.name,
                                                    slug=slug,
                                                    source_url=entry_url,
                                                    archive_source="urlscan",
                                                )
                                            )

                            total_fetched += len(results)

                            # Cursor-based pagination via sort field of last result
                            last_sort = results[-1].get("sort")
                            if not last_sort:
                                break
                            search_after = ",".join(str(s) for s in last_sort)

                            await asyncio.sleep(1)

                        except httpx.HTTPStatusError as e:
                            logger.warning(f"URLScan query failed for {domain}: {e}")
                            break
                        except httpx.HTTPError as e:
                            logger.warning(f"URLScan request error for {domain}: {e}")
                            break

        except Exception as e:
            logger.error(f"URLScan error for {pattern.name}: {e}")

        return slugs

    async def query_commoncrawl(
        self, pattern: BookingEnginePattern
    ) -> list[DiscoveredSlug]:
        """Query latest Common Crawl Index API for URLs matching pattern."""
        # For backward compatibility, query just the latest index
        index_url = await self._get_latest_cc_index()
        if not index_url:
            return []
        return await self._query_single_cc_index(pattern, index_url)

    async def query_commoncrawl_historical(
        self, pattern: BookingEnginePattern
    ) -> list[DiscoveredSlug]:
        """Query multiple historical Common Crawl indexes for more coverage."""
        slugs = []
        seen_slugs: Set[str] = set()

        # Get list of all Common Crawl indexes
        indexes = await self._get_cc_indexes()
        if not indexes:
            logger.warning("Could not get Common Crawl indexes")
            return slugs

        # Query the last N indexes
        indexes_to_query = indexes[: self.cc_index_count]
        logger.info(f"  Querying {len(indexes_to_query)} CC indexes...")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for idx in indexes_to_query:
                index_name = idx.get("id", "unknown")
                cdx_api = idx.get("cdx-api")
                if not cdx_api:
                    continue

                try:
                    params = {
                        "url": pattern.commoncrawl_url_pattern,
                        "output": "json",
                        "limit": self.max_results_per_query,
                    }
                    response = await client.get(cdx_api, params=params)

                    if response.status_code != 200:
                        continue

                    count = 0
                    for line in response.text.strip().split("\n"):
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                            url = record.get("url", "")
                            timestamp = record.get("timestamp")
                            slug = self._extract_slug(url, pattern.slug_regex)

                            if slug and slug.lower() not in seen_slugs:
                                seen_slugs.add(slug.lower())
                                slugs.append(
                                    DiscoveredSlug(
                                        engine=pattern.name,
                                        slug=slug,
                                        source_url=url,
                                        archive_source="commoncrawl",
                                        timestamp=timestamp,
                                    )
                                )
                                count += 1
                        except Exception:
                            continue

                    if count > 0:
                        logger.debug(f"    {index_name}: +{count} slugs")

                except httpx.HTTPError as e:
                    logger.debug(f"    {index_name}: error - {e}")
                except Exception as e:
                    logger.debug(f"    {index_name}: error - {e}")

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.3)

        return slugs

    async def _query_single_cc_index(
        self, pattern: BookingEnginePattern, index_url: str
    ) -> list[DiscoveredSlug]:
        """Query a single Common Crawl index."""
        slugs = []
        params = {
            "url": pattern.commoncrawl_url_pattern,
            "output": "json",
            "limit": self.max_results_per_query,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(index_url, params=params)
                response.raise_for_status()

                for line in response.text.strip().split("\n"):
                    if not line:
                        continue
                    try:
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

    async def _get_cc_indexes(self) -> list[dict]:
        """Get list of all Common Crawl indexes."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    "https://index.commoncrawl.org/collinfo.json"
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Failed to get CC indexes: {e}")
        return []

    async def _get_latest_cc_index(self) -> Optional[str]:
        """Get the URL of the latest Common Crawl index."""
        indexes = await self._get_cc_indexes()
        if indexes:
            return indexes[0].get("cdx-api")
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


async def discover_slugs_for_engine(
    engine_name: str,
    existing_slugs: Optional[Set[str]] = None,
    enable_wayback: bool = True,
    enable_commoncrawl: bool = True,
    enable_alienvault: bool = True,
    enable_urlscan: bool = True,
) -> list[DiscoveredSlug]:
    """
    Discover slugs for a specific engine.

    Args:
        engine_name: Name of the booking engine
        existing_slugs: Optional set of existing slugs to exclude (lowercase)
        enable_wayback: Query Wayback Machine
        enable_commoncrawl: Query Common Crawl
        enable_alienvault: Query AlienVault OTX
        enable_urlscan: Query URLScan.io
    """
    discovery = ArchiveSlugDiscovery(
        enable_wayback=enable_wayback,
        enable_commoncrawl=enable_commoncrawl,
        enable_alienvault=enable_alienvault,
        enable_urlscan=enable_urlscan,
    )
    pattern = next((p for p in BOOKING_ENGINE_PATTERNS if p.name == engine_name), None)
    if not pattern:
        raise ValueError(f"Unknown engine: {engine_name}")

    all_slugs = []

    if enable_wayback:
        all_slugs.extend(await discovery.query_wayback(pattern))
    if enable_commoncrawl:
        all_slugs.extend(await discovery.query_commoncrawl_historical(pattern))
    if enable_alienvault:
        all_slugs.extend(await discovery.query_alienvault(pattern))
    if enable_urlscan:
        all_slugs.extend(await discovery.query_urlscan(pattern))

    unique_slugs = discovery._dedupe_slugs(all_slugs)

    # Filter out existing slugs
    if existing_slugs:
        unique_slugs = [
            s for s in unique_slugs
            if s.slug.lower() not in existing_slugs
        ]

    return unique_slugs
