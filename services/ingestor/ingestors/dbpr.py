"""
Florida DBPR License Ingestor - Download and parse lodging licenses.

The Florida Department of Business and Professional Regulation (DBPR) provides
free CSV downloads of all licensed lodging establishments. This is an authoritative
data source for Florida hotels, motels, condos, and vacation rentals.

Data sources:
- hrlodge{1-7}.csv: Active lodging establishments by district
- newlodg.csv: New lodging establishments (current fiscal year)

Total: ~193,000 lodging licenses across Florida.
"""

import csv
import io
from pathlib import Path
from typing import List, Optional, AsyncIterator, Tuple

from loguru import logger

from services.ingestor.base import BaseIngestor
from services.ingestor.registry import register
from services.ingestor.models.base import IngestStats
from services.ingestor.models.dbpr import DBPRLicense, LICENSE_TYPES, RANK_CODES
from services.ingestor.sources.http import HTTPSource


# Cache directory for downloaded DBPR files
CACHE_DIR = Path(__file__).parent.parent.parent.parent / "data" / "dbpr_cache"

# DBPR data extract URLs
DBPR_BASE_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts"

# District files contain ALL active lodging licenses
DISTRICT_FILES = [f"hrlodge{i}.csv" for i in range(1, 8)]

# New licenses file (current fiscal year only)
NEW_LICENSES_FILE = "newlodg.csv"


@register("dbpr")
class DBPRIngestor(BaseIngestor[DBPRLicense]):
    """
    Download and parse Florida DBPR lodging license data.

    Usage:
        ingestor = DBPRIngestor()
        licenses, stats = await ingestor.ingest()

        # New licenses only
        ingestor = DBPRIngestor(new_only=True)
        licenses, stats = await ingestor.ingest()

        # Filter by county and type
        licenses, stats = await ingestor.ingest(
            filters={"counties": ["Palm Beach"], "categories": ["hotel"]}
        )
    """

    source_name = "dbpr"
    external_id_type = "dbpr_license"

    def __init__(
        self,
        new_only: bool = False,
        use_cache: bool = True,
        timeout: float = 120.0,
    ):
        """
        Initialize DBPR ingestor.

        Args:
            new_only: Only download new licenses (current fiscal year)
            use_cache: Use local cache for downloaded files
            timeout: HTTP timeout in seconds
        """
        self.new_only = new_only
        self.use_cache = use_cache
        self.timeout = timeout

        # Set up HTTP source
        if new_only:
            urls = [f"{DBPR_BASE_URL}/{NEW_LICENSES_FILE}"]
        else:
            urls = [f"{DBPR_BASE_URL}/{f}" for f in DISTRICT_FILES]

        self._source = HTTPSource(
            urls=urls,
            timeout=timeout,
            cache_dir=str(CACHE_DIR),
            use_cache=use_cache,
        )

    async def fetch(self) -> AsyncIterator[Tuple[str, bytes]]:
        """Fetch DBPR CSV files."""
        async for filename, content in self._source.fetch_all():
            yield filename, content

    def parse(self, data: bytes, filename: str = "") -> List[DBPRLicense]:
        """Parse DBPR CSV data into license objects."""
        licenses = []

        # Decode with fallback
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("latin-1")

        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            try:
                license = DBPRLicense.from_csv_row(row)
                if license:
                    licenses.append(license)
            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")
                continue

        return licenses

    def _apply_filters(
        self, records: List[DBPRLicense], filters: dict
    ) -> List[DBPRLicense]:
        """Apply DBPR-specific filters."""
        result = records

        # Filter by county
        if "counties" in filters and filters["counties"]:
            counties_lower = [c.lower() for c in filters["counties"]]
            result = [
                r for r in result if r.county and r.county.lower() in counties_lower
            ]

        # Filter by license type or category
        if "license_types" in filters and filters["license_types"]:
            types_lower = [t.lower() for t in filters["license_types"]]
            result = [
                r
                for r in result
                if r.license_type.lower() in types_lower
                or r.rank.lower() in types_lower
            ]

        # Also support generic category filter
        if "categories" in filters and filters["categories"]:
            categories_lower = [c.lower() for c in filters["categories"]]
            result = [
                r
                for r in result
                if r.category and r.category.lower() in categories_lower
            ]

        return result


def get_license_types() -> dict:
    """Get mapping of DBPR license type codes to names."""
    return LICENSE_TYPES.copy()


def get_rank_codes() -> dict:
    """Get mapping of DBPR rank codes to names."""
    return RANK_CODES.copy()
