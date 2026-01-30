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

# URL templates for each booking engine
ARCHIVE_URL_TEMPLATES = {
    "rms": "bookings.rmscloud.com/search/index/{slug}",
    "rms_ibe": "ibe.rmscloud.com/{slug}",
    "cloudbeds": "hotels.cloudbeds.com/reservation/{slug}",
    "mews": "app.mews.com/distributor/{slug}",
    "siteminder": "book-directonline.com/properties/{slug}",
}


class DiscoveryResult(BaseModel):
    """Result of archive slug discovery."""
    
    engine: str
    total_slugs: int
    wayback_count: int
    commoncrawl_count: int
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
        timeout: float = 60.0,
        max_results: int = 10000,
    ) -> List[DiscoveryResult]:
        """
        Discover booking engine slugs from web archives.

        Queries Wayback Machine and Common Crawl for historical booking URLs.

        Args:
            engine: Specific engine to query (rms, cloudbeds, etc.), or None for all
            s3_upload: Whether to upload results to S3
            output_path: Local file path to save JSON results
            timeout: Request timeout in seconds
            max_results: Max results per query

        Returns:
            List of DiscoveryResult for each engine
        """
        discovery = ArchiveSlugDiscovery(
            timeout=timeout,
            max_results_per_query=max_results,
        )

        if engine and engine != "all":
            slugs = await discover_slugs_for_engine(engine)
            results = {engine: slugs}
        else:
            results = await discovery.discover_all()

        # Process results
        discovery_results = []
        output_data = {}

        for eng, slugs in results.items():
            wayback_count = sum(1 for s in slugs if s.archive_source == "wayback")
            cc_count = sum(1 for s in slugs if s.archive_source == "commoncrawl")

            logger.info(f"{eng.upper()}: {len(slugs)} slugs (wayback: {wayback_count}, cc: {cc_count})")

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
                    s3_key=s3_key,
                )
            )

        # Save to local file if requested
        if output_path:
            await self._save_discovery_to_file(output_path, output_data)

        total = sum(r.total_slugs for r in discovery_results)
        logger.info(f"Total unique slugs discovered: {total}")

        return discovery_results

    async def _upload_slugs_to_s3(self, engine: str, slugs: List[dict]) -> str:
        """Upload discovered slugs to S3 as txt file (async)."""
        if not slugs:
            return ""

        # Build URL paths
        url_template = ARCHIVE_URL_TEMPLATES.get(engine)
        lines = []

        for slug_data in slugs:
            slug = slug_data["slug"]
            if url_template:
                url_path = url_template.format(slug=slug)
            else:
                url_path = slug_data.get("source_url", slug).replace("https://", "").replace("http://", "")
            lines.append(url_path)

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
