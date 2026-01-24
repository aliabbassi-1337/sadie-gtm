"""
Ingestor Service - Import hotel data from external sources.

Provides a unified interface for ingesting hotel data from various sources:
- Florida DBPR (lodging licenses)
- Texas Comptroller (hotel occupancy tax)
- Generic CSV sources (S3, HTTP, local)
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Type

from loguru import logger

from services.ingestor.base import BaseIngestor
from services.ingestor.models.base import BaseRecord, IngestStats
from services.ingestor.models.dbpr import DBPRLicense, LICENSE_TYPES
from services.ingestor.models.texas import TexasHotel
from services.ingestor.config import CSVIngestorConfig, IngestorConfig
from services.ingestor import registry

# Import ingestors to register them
from services.ingestor.ingestors.dbpr import DBPRIngestor
from services.ingestor.ingestors.texas import TexasIngestor
from services.ingestor.ingestors.generic_csv import GenericCSVIngestor

# Hotel status constants
HOTEL_STATUS_PENDING = 0
HOTEL_STATUS_LAUNCHED = 1
HOTEL_STATUS_NO_BOOKING_ENGINE = -1
HOTEL_STATUS_LOCATION_MISMATCH = -2


class IService(ABC):
    """Ingestor Service Interface - Import hotel data from external sources."""

    @abstractmethod
    async def ingest(
        self,
        source: str,
        save_to_db: bool = True,
        filters: Optional[dict] = None,
        **kwargs,
    ) -> Tuple[List[BaseRecord], IngestStats]:
        """
        Generic ingestion method for any registered source.

        Args:
            source: Registered ingestor name (e.g., "dbpr", "texas")
            save_to_db: Whether to save to database
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
        save_to_db: bool = True,
    ) -> Tuple[List[DBPRLicense], dict]:
        """
        Ingest Florida DBPR lodging licenses.

        Args:
            counties: Filter to specific counties (e.g., ["Palm Beach", "Miami-Dade"])
            license_types: Filter to specific types (e.g., ["Hotel", "Motel"])
            new_only: Only download new licenses (current fiscal year)
            save_to_db: Whether to save to hotels table

        Returns:
            Tuple of (licenses, stats dict)
        """
        pass

    @abstractmethod
    async def ingest_texas(
        self,
        quarter: Optional[str] = None,
        save_to_db: bool = True,
    ) -> Tuple[List[TexasHotel], dict]:
        """
        Ingest Texas hotel occupancy tax data.

        Args:
            quarter: Specific quarter directory (e.g., "HOT 25 Q3"). If None, loads all quarters.
            save_to_db: Whether to save to hotels table

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
        save_to_db: bool = True,
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
        records, stats = await ingestor.ingest(
            save_to_db=save_to_db,
            filters=filters,
        )

        return records, stats

    async def ingest_dbpr(
        self,
        counties: Optional[List[str]] = None,
        license_types: Optional[List[str]] = None,
        new_only: bool = False,
        save_to_db: bool = True,
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
            save_to_db=save_to_db,
            filters=filters if filters else None,
        )

        return records, stats.to_dict()

    async def ingest_texas(
        self,
        quarter: Optional[str] = None,
        save_to_db: bool = True,
    ) -> Tuple[List[TexasHotel], dict]:
        """
        Ingest Texas hotel occupancy tax data.

        This method provides backward compatibility with the old API.
        """
        ingestor = TexasIngestor(quarter=quarter)
        records, stats = await ingestor.ingest(save_to_db=save_to_db)

        return records, stats.to_dict()

    async def ingest_from_config(
        self,
        config: CSVIngestorConfig,
        save_to_db: bool = True,
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
        return await ingestor.ingest(save_to_db=save_to_db, filters=filters)

    def get_dbpr_license_types(self) -> dict:
        """Get mapping of DBPR license type codes to names."""
        return LICENSE_TYPES.copy()

    def list_sources(self) -> List[str]:
        """List all registered ingestor sources."""
        return registry.list_ingestors()


# Alias for backward compatibility
IngestorService = Service
