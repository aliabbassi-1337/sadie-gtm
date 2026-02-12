"""
Ingestor Service - Import hotel data from external sources.

Provides a unified interface for ingesting hotel data from various sources:
- Florida DBPR (lodging licenses)
- Texas Comptroller (hotel occupancy tax)
- Generic CSV sources (S3, HTTP, local) via CSVIngestorConfig
- Archive slug discovery (Wayback Machine, Common Crawl)
"""

import asyncio
import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Type

import aioboto3
from loguru import logger
from pydantic import BaseModel

from services.ingestor.base import BaseIngestor
from services.ingestor.models.base import BaseRecord, IngestStats
from services.ingestor.models.dbpr import DBPRLicense, LICENSE_TYPES
from services.ingestor.models.texas import TexasHotel
from services.ingestor.config import CSVIngestorConfig, IngestorConfig
from services.ingestor import registry
from lib.archive.discovery import (
    ArchiveSlugDiscovery,
    DiscoveredSlug,
    BOOKING_ENGINE_PATTERNS,
    discover_slugs_for_engine,
)

# Import ingestors to register them
from services.ingestor.ingestors.dbpr import DBPRIngestor
from services.ingestor.ingestors.texas import TexasIngestor
from services.ingestor.ingestors.generic_csv import GenericCSVIngestor

# Hotel status constants
HOTEL_STATUS_PENDING = 0
HOTEL_STATUS_LAUNCHED = 1
HOTEL_STATUS_NO_BOOKING_ENGINE = -1
HOTEL_STATUS_LOCATION_MISMATCH = -2

# S3 configuration for archive discovery
S3_BUCKET = "sadie-gtm"
S3_PREFIX = "crawl-data/"
S3_REGION = "eu-north-1"

class DiscoveryResult(BaseModel):
    """Result of archive slug discovery."""

    engine: str
    total_slugs: int
    wayback_count: int
    commoncrawl_count: int
    alienvault_count: int = 0
    urlscan_count: int = 0
    virustotal_count: int = 0
    crtsh_count: int = 0
    arquivo_count: int = 0
    github_count: int = 0
    s3_key: Optional[str] = None


class IService(ABC):
    """Ingestor Service Interface - Import hotel data from external sources."""

    @abstractmethod
    async def ingest(
        self,
        source: str,
        filters: Optional[dict] = None,
        **kwargs,
    ) -> Tuple[List[BaseRecord], IngestStats]:
        """
        Generic ingestion method for any registered source.

        Args:
            source: Registered ingestor name (e.g., "dbpr", "texas")
            filters: Filters to apply (counties, states, categories, etc.)
            **kwargs: Additional arguments passed to ingestor constructor

        Returns:
            Tuple of (records, stats)
        """
        pass

    @abstractmethod
    async def ingest_dbpr(
        self,
        counties: Optional[List[str]] = None,
        license_types: Optional[List[str]] = None,
        new_only: bool = False,
    ) -> Tuple[List[DBPRLicense], dict]:
        """
        Ingest Florida DBPR lodging licenses.

        Args:
            counties: Filter to specific counties (e.g., ["Palm Beach", "Miami-Dade"])
            license_types: Filter to specific types (e.g., ["Hotel", "Motel"])
            new_only: Only download new licenses (current fiscal year)

        Returns:
            Tuple of (licenses, stats dict)
        """
        pass

    @abstractmethod
    async def ingest_texas(
        self,
        quarter: Optional[str] = None,
    ) -> Tuple[List[TexasHotel], dict]:
        """
        Ingest Texas hotel occupancy tax data.

        Args:
            quarter: Specific quarter directory (e.g., "HOT 25 Q3"). If None, loads all quarters.

        Returns:
            Tuple of (hotels, stats dict)
        """
        pass

    @abstractmethod
    def get_dbpr_license_types(self) -> dict:
        """Get mapping of DBPR license type codes to names."""
        pass

    @abstractmethod
    def list_sources(self) -> List[str]:
        """List all registered ingestor sources."""
        pass


class Service(IService):
    """Service for ingesting hotel data from external sources."""

    async def ingest(
        self,
        source: str,
        filters: Optional[dict] = None,
        **kwargs,
    ) -> Tuple[List[BaseRecord], IngestStats]:
        """
        Generic ingestion method for any registered source.

        Usage:
            service = Service()

            # DBPR ingestion
            records, stats = await service.ingest("dbpr", new_only=True)

            # Texas ingestion
            records, stats = await service.ingest("texas", quarter="HOT 25 Q3")

            # With filters
            records, stats = await service.ingest(
                "dbpr",
                filters={"counties": ["Palm Beach"], "license_types": ["Hotel"]}
            )
        """
        # Get ingestor class
        ingestor_cls = registry.get_ingestor(source)

        # Create ingestor instance
        ingestor = ingestor_cls(**kwargs)

        # Run ingestion
        records, stats = await ingestor.ingest(filters=filters)

        return records, stats

    async def ingest_dbpr(
        self,
        counties: Optional[List[str]] = None,
        license_types: Optional[List[str]] = None,
        new_only: bool = False,
    ) -> Tuple[List[DBPRLicense], dict]:
        """
        Ingest Florida DBPR lodging licenses.

        This method provides backward compatibility with the old API.
        """
        # Build filters
        filters = {}
        if counties:
            filters["counties"] = counties
        if license_types:
            filters["license_types"] = license_types

        # Create and run ingestor
        ingestor = DBPRIngestor(new_only=new_only)
        records, stats = await ingestor.ingest(
            filters=filters if filters else None,
        )

        return records, stats.to_dict()

    async def ingest_texas(
        self,
        quarter: Optional[str] = None,
    ) -> Tuple[List[TexasHotel], dict]:
        """
        Ingest Texas hotel occupancy tax data.

        This method provides backward compatibility with the old API.
        """
        ingestor = TexasIngestor(quarter=quarter)
        records, stats = await ingestor.ingest()

        return records, stats.to_dict()

    async def ingest_from_config(
        self,
        config: CSVIngestorConfig,
        filters: Optional[dict] = None,
    ) -> Tuple[List[BaseRecord], IngestStats]:
        """
        Ingest data using a CSV configuration.

        This enables zero-code ingestion from new data sources.

        Usage:
            config = CSVIngestorConfig(
                name="new_state",
                external_id_type="new_state_license",
                source_type="s3",
                s3_bucket="my-bucket",
                s3_prefix="data/",
                columns=[...],
                external_id_columns=["LICENSE_NO"],
            )
            records, stats = await service.ingest_from_config(config)
        """
        ingestor = GenericCSVIngestor(config)
        return await ingestor.ingest(filters=filters)

    def get_dbpr_license_types(self) -> dict:
        """Get mapping of DBPR license type codes to names."""
        return LICENSE_TYPES.copy()

    def list_sources(self) -> List[str]:
        """List all registered ingestor sources."""
        return registry.list_ingestors()

    async def discover_archive_slugs(
        self,
        engine: Optional[str] = None,
        s3_upload: bool = False,
        output_path: Optional[str] = None,
        timeout: float = 120.0,
        max_results: int = 50000,
        cc_index_count: int = 12,
        dedupe_from_db: bool = True,
        db_connection=None,
        enable_alienvault: bool = True,
        enable_urlscan: bool = True,
        enable_virustotal: bool = True,
        enable_crtsh: bool = True,
        enable_arquivo: bool = True,
        enable_github: bool = True,
        proxy_url: Optional[str] = None,
    ) -> List[DiscoveryResult]:
        """
        Discover booking engine slugs from web archives.

        Queries Wayback Machine, Common Crawl, AlienVault OTX, URLScan.io,
        and VirusTotal for historical booking URLs, then deduplicates against
        existing slugs in the database.

        Args:
            engine: Specific engine to query (rms, cloudbeds, etc.), or None for all
            s3_upload: Whether to upload results to S3
            output_path: Local file path to save JSON results
            timeout: Request timeout in seconds
            max_results: Max results per query
            cc_index_count: Number of historical Common Crawl indexes to query
            dedupe_from_db: Whether to deduplicate against database
            db_connection: Optional database connection (for testing)
            enable_alienvault: Whether to query AlienVault OTX
            enable_urlscan: Whether to query URLScan.io
            enable_virustotal: Whether to query VirusTotal

        Returns:
            List of DiscoveryResult for each engine
        """
        # Fetch existing slugs from database for deduplication
        existing_slugs = {}
        if dedupe_from_db:
            existing_slugs = await self._fetch_existing_slugs(db_connection)
            total_existing = sum(len(v) for v in existing_slugs.values())
            logger.info(f"Loaded {total_existing} existing slugs from database for deduplication")

        discovery = ArchiveSlugDiscovery(
            timeout=timeout,
            max_results_per_query=max_results,
            cc_index_count=cc_index_count,
            enable_alienvault=enable_alienvault,
            enable_urlscan=enable_urlscan,
            enable_virustotal=enable_virustotal,
            enable_crtsh=enable_crtsh,
            enable_arquivo=enable_arquivo,
            enable_github=enable_github,
            proxy_url=proxy_url,
        )

        if engine and engine != "all":
            engine_existing = {engine: existing_slugs.get(engine, set())}
            slugs = await discover_slugs_for_engine(
                engine,
                engine_existing.get(engine),
                enable_alienvault=enable_alienvault,
                enable_urlscan=enable_urlscan,
                enable_virustotal=enable_virustotal,
                enable_crtsh=enable_crtsh,
                enable_arquivo=enable_arquivo,
                enable_github=enable_github,
                proxy_url=proxy_url,
            )
            results = {engine: slugs}
        else:
            results = await discovery.discover_all(existing_slugs=existing_slugs)

        # Process results
        discovery_results = []
        output_data = {}

        for eng, slugs in results.items():
            wayback_count = sum(1 for s in slugs if s.archive_source == "wayback")
            cc_count = sum(1 for s in slugs if s.archive_source == "commoncrawl")
            av_count = sum(1 for s in slugs if s.archive_source == "alienvault")
            us_count = sum(1 for s in slugs if s.archive_source == "urlscan")
            vt_count = sum(1 for s in slugs if s.archive_source == "virustotal")
            ct_count = sum(1 for s in slugs if s.archive_source == "crtsh")
            arq_count = sum(1 for s in slugs if s.archive_source == "arquivo")
            gh_count = sum(1 for s in slugs if s.archive_source == "github")

            logger.info(
                f"{eng.upper()}: {len(slugs)} NEW slugs "
                f"(wayback: {wayback_count}, cc: {cc_count}, "
                f"alienvault: {av_count}, urlscan: {us_count}, "
                f"virustotal: {vt_count}, crtsh: {ct_count}, "
                f"arquivo: {arq_count}, github: {gh_count})"
            )

            # Prepare output data
            slug_dicts = [
                {
                    "slug": s.slug,
                    "source_url": s.source_url,
                    "archive_source": s.archive_source,
                    "timestamp": s.timestamp,
                }
                for s in slugs
            ]
            output_data[eng] = slug_dicts

            # Upload to S3 if requested
            s3_key = None
            if s3_upload and slug_dicts:
                s3_key = await self._upload_slugs_to_s3(eng, slug_dicts)

            discovery_results.append(
                DiscoveryResult(
                    engine=eng,
                    total_slugs=len(slugs),
                    wayback_count=wayback_count,
                    commoncrawl_count=cc_count,
                    alienvault_count=av_count,
                    urlscan_count=us_count,
                    virustotal_count=vt_count,
                    crtsh_count=ct_count,
                    arquivo_count=arq_count,
                    github_count=gh_count,
                    s3_key=s3_key,
                )
            )

        # Save to local file if requested
        if output_path:
            await self._save_discovery_to_file(output_path, output_data)

        total = sum(r.total_slugs for r in discovery_results)
        logger.info(f"Total NEW slugs discovered: {total}")

        return discovery_results

    async def _fetch_existing_slugs(self, conn=None) -> dict[str, set[str]]:
        """
        Fetch existing slugs from the database for deduplication.

        Returns dict of engine_name -> set of lowercase slugs.
        """
        import os
        import re

        # Engine name to booking_engine_id mapping
        ENGINE_IDS = {
            "rms": 12,
            "rms_rates": 12,  # Same as rms
            "rms_ibe": 12,  # Same as rms
            "cloudbeds": 3,
            "mews": 4,
            "siteminder": 14,
            "siteminder_directbook": 14,  # Same as siteminder
            "siteminder_bookingbutton": 14,  # Same as siteminder
            "siteminder_directonline": 14,  # Same as siteminder
            "ipms247": 22,  # JEHS / iPMS / Yanolja Cloud Solution
        }

        existing: dict[str, set[str]] = {}

        try:
            # Import asyncpg here to avoid circular imports
            import asyncpg

            if conn is None:
                conn = await asyncpg.connect(
                    host=os.getenv("SADIE_DB_HOST"),
                    port=os.getenv("SADIE_DB_PORT"),
                    database=os.getenv("SADIE_DB_NAME"),
                    user=os.getenv("SADIE_DB_USER"),
                    password=os.getenv("SADIE_DB_PASSWORD"),
                    statement_cache_size=0,  # For pgbouncer compatibility
                )
                should_close = True
            else:
                should_close = False

            for engine_name, engine_id in ENGINE_IDS.items():
                if engine_name in existing:
                    continue  # Skip duplicate (rms_ibe uses same as rms)

                rows = await conn.fetch(
                    """
                    SELECT booking_url FROM sadie_gtm.hotel_booking_engines
                    WHERE booking_engine_id = $1 AND booking_url IS NOT NULL
                    """,
                    engine_id,
                )

                slugs = set()
                for row in rows:
                    url = row["booking_url"].lower()
                    slug = self._extract_slug_from_url(url, engine_name)
                    if slug:
                        slugs.add(slug.lower())

                existing[engine_name] = slugs
                logger.debug(f"  {engine_name}: {len(slugs)} existing slugs")

            if should_close:
                await conn.close()

        except Exception as e:
            logger.warning(f"Failed to fetch existing slugs from DB: {e}")

        return existing

    def _extract_slug_from_url(self, url: str, engine: str) -> Optional[str]:
        """Extract slug from a booking URL."""
        import re

        url = url.lower()

        if engine in ("rms", "rms_rates", "rms_ibe"):
            # RMS formats: /search/index/SLUG, /rates/index/SLUG, ibe*.rmscloud.com/SLUG
            if "/search/index/" in url:
                return url.split("/search/index/")[-1].split("/")[0].split("?")[0]
            elif "/rates/index/" in url:
                return url.split("/rates/index/")[-1].split("/")[0].split("?")[0]
            elif "ibe" in url and ".rmscloud.com/" in url:
                return url.split(".rmscloud.com/")[-1].split("/")[0].split("?")[0]
        elif engine == "cloudbeds":
            # Cloudbeds: /reservation/SLUG
            if "/reservation/" in url:
                return url.split("/reservation/")[-1].split("/")[0].split("?")[0]
        elif engine == "mews":
            # Mews: /distributor/UUID
            if "/distributor/" in url:
                return url.split("/distributor/")[-1].split("/")[0].split("?")[0]
        elif engine in ("siteminder", "siteminder_directbook", "siteminder_bookingbutton", "siteminder_directonline"):
            # SiteMinder: /reservations/SLUG or /properties/SLUG
            if "/reservations/" in url:
                return url.split("/reservations/")[-1].split("/")[0].split("?")[0]
            elif "/properties/" in url:
                return url.split("/properties/")[-1].split("/")[0].split("?")[0]
        elif engine == "ipms247":
            # ipms247/Yanolja: /booking/book-rooms-SLUG
            if "/booking/book-rooms-" in url:
                return url.split("/booking/book-rooms-")[-1].split("/")[0].split("?")[0]

        return None

    async def _upload_slugs_to_s3(self, engine: str, slugs: List[dict]) -> str:
        """Upload discovered slugs to S3 as txt file (async).
        
        Saves just the slug/ID, not the full URL path. The CrawlIngestor
        will build the full URL from the slug using URL_PATTERNS.
        """
        if not slugs:
            return ""

        # Just save the raw slugs, not URL paths
        lines = [slug_data["slug"] for slug_data in slugs]

        # Deduplicate (case-insensitive)
        seen = set()
        unique_lines = []
        for line in lines:
            line_lower = line.lower()
            if line_lower not in seen:
                seen.add(line_lower)
                unique_lines.append(line)

        content = "\n".join(sorted(unique_lines))

        # Upload to S3
        timestamp = datetime.utcnow().strftime("%Y%m%d")
        s3_key = f"{S3_PREFIX}{engine}_archive_discovery_{timestamp}.txt"

        session = aioboto3.Session()
        async with session.client("s3", region_name=S3_REGION) as s3:
            await s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/plain",
            )

        logger.info(f"Uploaded {len(unique_lines)} slugs to s3://{S3_BUCKET}/{s3_key}")
        return s3_key

    async def _save_discovery_to_file(self, output_path: str, data: dict) -> None:
        """Save discovery results to local JSON file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        output = {
            "discovered_at": datetime.utcnow().isoformat(),
            "engines": data,
            "total_slugs": sum(len(v) for v in data.values()),
        }

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: path.write_text(json.dumps(output, indent=2)),
        )

        logger.info(f"Results saved to: {output_path}")


# Alias for backward compatibility
IngestorService = Service
