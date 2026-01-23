"""
Ingestor Service - Import hotel data from external sources.

Handles ingestion from:
- Florida DBPR (lodging licenses)
- SEC EDGAR (hotel property filings) - TODO
- Other public data sources
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Tuple
from loguru import logger

from services.ingestor.dbpr import DBPRIngestor, DBPRLicense, DBPRIngestStats, LICENSE_TYPES
from services.ingestor.texas import TexasIngestor, TexasHotel, TexasIngestStats
from services.ingestor import repo

# Hotel status constants
HOTEL_STATUS_PENDING = 0
HOTEL_STATUS_LAUNCHED = 1
HOTEL_STATUS_NO_BOOKING_ENGINE = -1
HOTEL_STATUS_LOCATION_MISMATCH = -2


class IService(ABC):
    """Ingestor Service Interface - Import hotel data from external sources."""

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
    def get_dbpr_license_types(self) -> Dict[str, str]:
        """Get mapping of DBPR license type codes to names."""
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


class Service(IService):
    """Service for ingesting hotel data from external sources."""

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
        ingester = DBPRIngestor()

        if new_only:
            licenses, stats = await ingester.download_new_licenses()
        else:
            licenses, stats = await ingester.download_all_licenses()

        # Filter by county if specified
        if counties:
            counties_lower = [c.lower() for c in counties]
            licenses = [
                lic for lic in licenses
                if lic.county.lower() in counties_lower
            ]
            logger.info(f"Filtered to {len(licenses)} licenses in counties: {counties}")

        # Filter by license type if specified
        if license_types:
            types_lower = [t.lower() for t in license_types]
            licenses = [
                lic for lic in licenses
                if lic.license_type.lower() in types_lower or lic.rank.lower() in types_lower
            ]
            logger.info(f"Filtered to {len(licenses)} licenses of types: {license_types}")

        # Save to database
        if save_to_db and licenses:
            saved = await self._save_dbpr_licenses(licenses)
            stats.records_saved = saved
            logger.info(f"Saved {saved} licenses to database")

        return licenses, {
            "files_downloaded": stats.files_downloaded,
            "records_parsed": stats.records_parsed,
            "records_saved": stats.records_saved,
            "duplicates_skipped": stats.duplicates_skipped,
            "errors": stats.errors,
        }

    async def _save_dbpr_licenses(self, licenses: List[DBPRLicense]) -> int:
        """Save DBPR licenses to hotels table."""
        saved = 0

        for i, lic in enumerate(licenses):
            if (i + 1) % 1000 == 0:
                logger.info(f"  Saving... {i + 1}/{len(licenses)}")

            try:
                # Insert (will skip duplicates based on name+city)
                result = await repo.insert_hotel(
                    name=lic.business_name or lic.licensee_name,
                    source=f"dbpr_{lic.license_type.lower().replace(' ', '_').replace('-', '_')}",
                    status=HOTEL_STATUS_PENDING,
                    address=lic.address,
                    city=lic.city,
                    state=lic.state,
                    phone=lic.phone,
                )
                if result:
                    saved += 1

            except Exception as e:
                # Likely duplicate - skip silently
                logger.debug(f"Failed to save {lic.license_number}: {e}")
                continue

        return saved

    def get_dbpr_license_types(self) -> Dict[str, str]:
        """Get mapping of DBPR license type codes to names."""
        return LICENSE_TYPES.copy()

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
        ingestor = TexasIngestor()

        # Load data - either single quarter or all quarters
        if quarter:
            hotels, stats = ingestor.load_quarterly_data(quarter)
            unique_hotels = ingestor.deduplicate_hotels(hotels)
        else:
            unique_hotels, stats = ingestor.load_all_quarters()

        logger.info(f"Loaded {len(unique_hotels)} unique Texas hotels")

        # Save to database
        if save_to_db and unique_hotels:
            saved = await self._save_texas_hotels(unique_hotels)
            stats.records_saved = saved
            logger.info(f"Saved {saved} hotels to database")

        return unique_hotels, {
            "files_processed": stats.files_processed,
            "records_parsed": stats.records_parsed,
            "records_saved": stats.records_saved,
            "duplicates_skipped": stats.duplicates_skipped,
            "errors": stats.errors,
        }

    async def _save_texas_hotels(self, hotels: List[TexasHotel]) -> int:
        """Save Texas hotels to hotels table using batch inserts."""
        logger.info(f"Starting batch insert of {len(hotels)} hotels...")

        BATCH_SIZE = 500
        saved = 0

        for batch_start in range(0, len(hotels), BATCH_SIZE):
            batch = hotels[batch_start:batch_start + BATCH_SIZE]

            # Prepare batch data
            records = [
                (
                    hotel.name,
                    f"texas_hot:{hotel.taxpayer_number}:{hotel.location_number}",
                    HOTEL_STATUS_PENDING,
                    hotel.address,
                    hotel.city,
                    hotel.state,
                    "USA",
                    hotel.phone,
                    "hotel",
                )
                for hotel in batch
            ]

            # Batch insert via repo layer
            batch_saved = await repo.batch_insert_hotels(records)
            saved += batch_saved

            logger.info(f"  Batch {batch_start//BATCH_SIZE + 1}: {batch_start + len(batch)}/{len(hotels)} processed")

        # Batch insert room counts
        logger.info("Inserting room counts...")
        room_records = [
            (hotel.room_count, f"texas_hot:{hotel.taxpayer_number}:{hotel.location_number}", "texas_hot")
            for hotel in hotels if hotel.room_count
        ]

        if room_records:
            await repo.batch_insert_room_counts(room_records)

        logger.info(f"Batch insert complete: {saved} hotels processed")
        return saved
