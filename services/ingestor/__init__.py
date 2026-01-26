"""
Ingestor Service - Import hotel data from external sources.

Supported sources:
- Florida DBPR (lodging licenses)
- Texas Comptroller (hotel occupancy tax)
- Crawled booking engine URLs (Cloudbeds, Mews, RMS, SiteMinder)
- Generic CSV (configurable for any CSV source via S3, HTTP, or local files)

Usage:
    from services.ingestor import Service, DBPRIngestor, TexasIngestor, CrawlIngestor

    # Via service
    service = Service()
    records, stats = await service.ingest("dbpr")
    records, stats = await service.ingest("texas", quarter="HOT 25 Q3")

    # Direct ingestor usage
    ingestor = DBPRIngestor(new_only=True)
    licenses, stats = await ingestor.ingest(filters={"counties": ["Palm Beach"]})

    # Crawl ingestor - booking engine URLs
    ingestor = CrawlIngestor(engine="cloudbeds", file_path="data/cloudbeds.txt")
    hotels, stats = await ingestor.ingest()

    # Config-driven ingestion (for new data sources)
    config = CSVIngestorConfig(
        name="new_state",
        external_id_type="new_state_license",
        source_type="s3",
        s3_bucket="my-bucket",
        s3_prefix="data/",
        columns=[...],
        external_id_columns=["LICENSE_NO"],
    )
    ingestor = GenericCSVIngestor(config)
    records, stats = await ingestor.ingest()
"""

# Service
from services.ingestor.service import Service, IService

# Base classes
from services.ingestor.base import BaseIngestor
from services.ingestor.models.base import BaseRecord, IngestStats

# Registry
from services.ingestor.registry import register, get_ingestor, list_ingestors

# Models
from services.ingestor.models.crawl import CrawledHotel, URL_PATTERNS
from services.ingestor.models.dbpr import DBPRLicense, LICENSE_TYPES, RANK_CODES, STATUS_CODES
from services.ingestor.models.texas import TexasHotel, COLUMNS

# Config
from services.ingestor.config import CSVIngestorConfig, ColumnMapping, IngestorConfig

# Ingestors
from services.ingestor.ingestors.crawl import CrawlIngestor
from services.ingestor.ingestors.dbpr import DBPRIngestor
from services.ingestor.ingestors.texas import TexasIngestor
from services.ingestor.ingestors.generic_csv import GenericCSVIngestor

# Sources
from services.ingestor.sources import HTTPSource, S3Source, LocalSource

# Logging
from services.ingestor.logging import IngestLogger, capture_ingest_logs

__all__ = [
    # Service
    "Service",
    "IService",
    # Base classes
    "BaseIngestor",
    "BaseRecord",
    "IngestStats",
    # Registry
    "register",
    "get_ingestor",
    "list_ingestors",
    # Models
    "CrawledHotel",
    "DBPRLicense",
    "TexasHotel",
    "URL_PATTERNS",
    "LICENSE_TYPES",
    "RANK_CODES",
    "STATUS_CODES",
    "COLUMNS",
    # Config
    "CSVIngestorConfig",
    "ColumnMapping",
    "IngestorConfig",
    # Ingestors
    "CrawlIngestor",
    "DBPRIngestor",
    "TexasIngestor",
    "GenericCSVIngestor",
    # Sources
    "HTTPSource",
    "S3Source",
    "LocalSource",
    # Logging
    "IngestLogger",
    "capture_ingest_logs",
]
