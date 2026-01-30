"""Archive slug discovery service.

Orchestrates discovery from Wayback Machine and Common Crawl,
with S3 upload support.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import aioboto3
from pydantic import BaseModel

from lib.archive.discovery import (
    ArchiveSlugDiscovery,
    DiscoveredSlug,
    BOOKING_ENGINE_PATTERNS,
    discover_slugs_for_engine,
)

logger = logging.getLogger(__name__)


# S3 configuration
S3_BUCKET = "sadie-gtm"
S3_PREFIX = "crawl-data/"
S3_REGION = "eu-north-1"


# URL templates for each engine
URL_TEMPLATES = {
    "rms": "bookings.rmscloud.com/search/index/{slug}",
    "rms_ibe": "ibe.rmscloud.com/{slug}",
    "cloudbeds": "hotels.cloudbeds.com/reservation/{slug}",
    "mews": "app.mews.com/distributor/{slug}",
    "siteminder": "book-directonline.com/properties/{slug}",
}


class DiscoveryResult(BaseModel):
    """Result of archive discovery."""
    
    engine: str
    total_slugs: int
    wayback_count: int
    commoncrawl_count: int
    s3_key: Optional[str] = None


class ArchiveDiscoveryService:
    """Service for discovering slugs from web archives."""

    def __init__(
        self,
        timeout: float = 60.0,
        max_results: int = 10000,
    ):
        self.timeout = timeout
        self.max_results = max_results

    async def discover(
        self,
        engine: Optional[str] = None,
        s3_upload: bool = False,
        output_path: Optional[str] = None,
    ) -> list[DiscoveryResult]:
        """Discover slugs from archives.
        
        Args:
            engine: Specific engine to query, or None for all
            s3_upload: Whether to upload results to S3
            output_path: Local file path to save JSON results
            
        Returns:
            List of DiscoveryResult for each engine
        """
        discovery = ArchiveSlugDiscovery(
            timeout=self.timeout,
            max_results_per_query=self.max_results,
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
                s3_key = await self._save_to_s3(eng, slug_dicts)

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
            await self._save_to_file(output_path, output_data)

        total = sum(r.total_slugs for r in discovery_results)
        logger.info(f"Total unique slugs discovered: {total}")

        return discovery_results

    async def _save_to_s3(self, engine: str, slugs: list[dict]) -> str:
        """Save slugs to S3 as txt file (async)."""
        if not slugs:
            return ""

        # Build URL paths
        url_template = URL_TEMPLATES.get(engine)
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

    async def _save_to_file(self, output_path: str, data: dict) -> None:
        """Save results to local JSON file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        output = {
            "discovered_at": datetime.utcnow().isoformat(),
            "engines": data,
            "total_slugs": sum(len(v) for v in data.values()),
        }

        # Run file I/O in executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: path.write_text(json.dumps(output, indent=2)),
        )

        logger.info(f"Results saved to: {output_path}")
