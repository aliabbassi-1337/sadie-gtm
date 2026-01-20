"""
Ingestor Service - Import hotel data from external sources.

Handles ingestion from:
- Florida DBPR (lodging licenses)
- SEC EDGAR (hotel property filings) - TODO
- Other public data sources
"""

from typing import List, Optional, Dict, Tuple
from loguru import logger

from services.ingestor.dbpr import DBPRIngester, DBPRLicense, DBPRIngestStats, LICENSE_TYPES
from services.ingestor import repo


class IngestorService:
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
        ingester = DBPRIngester()

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
                    status=0,  # Pending
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
