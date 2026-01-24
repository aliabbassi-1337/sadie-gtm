"""
Base ingestor class - Abstract base for all data source ingestors.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Tuple, Optional, AsyncIterator
from loguru import logger

from services.ingestor.models.base import BaseRecord, IngestStats
from services.ingestor import repo

T = TypeVar("T", bound=BaseRecord)


class BaseIngestor(ABC, Generic[T]):
    """
    Abstract base class for all ingestors.

    Subclasses must implement:
    - source_name: Unique identifier for this data source
    - external_id_type: Type of external ID for deduplication
    - fetch(): Stream raw data from source
    - parse(): Parse raw data into domain objects

    Optional overrides:
    - validate(): Custom validation logic
    - transform(): Post-parse transformation
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this data source (e.g., 'dbpr', 'texas_hot')."""
        pass

    @property
    @abstractmethod
    def external_id_type(self) -> str:
        """Type of external ID used for deduplication."""
        pass

    @abstractmethod
    async def fetch(self) -> AsyncIterator[Tuple[str, bytes]]:
        """
        Stream raw data from source.

        Yields tuples of (filename, content) for each file.
        """
        pass

    @abstractmethod
    def parse(self, data: bytes, filename: str = "") -> List[T]:
        """Parse raw data into domain objects."""
        pass

    def validate(self, record: T) -> Optional[T]:
        """
        Validate a record before saving.

        Override for custom validation logic.
        Returns the record if valid, None if invalid.
        """
        if not record.name or not record.external_id:
            return None
        return record

    def transform(self, record: T) -> T:
        """
        Transform a record after parsing.

        Override for custom transformation logic.
        """
        return record

    def deduplicate(self, records: List[T]) -> List[T]:
        """
        Deduplicate records by external_id.

        Override for custom deduplication logic.
        """
        seen = {}
        for record in records:
            if record.external_id not in seen:
                seen[record.external_id] = record
        return list(seen.values())

    async def ingest(
        self,
        save_to_db: bool = True,
        batch_size: int = 500,
        filters: Optional[dict] = None,
    ) -> Tuple[List[T], IngestStats]:
        """
        Full ingestion pipeline.

        Args:
            save_to_db: Whether to persist records to database
            batch_size: Number of records per batch insert
            filters: Optional filters to apply (implementation-specific)

        Returns:
            Tuple of (records, stats)
        """
        stats = IngestStats()
        all_records: List[T] = []

        # Fetch and parse
        async for filename, data in self.fetch():
            stats.files_processed += 1
            logger.info(f"Processing {filename}...")

            try:
                parsed = self.parse(data, filename)
                logger.info(f"  Parsed {len(parsed)} records from {filename}")

                for record in parsed:
                    # Validate
                    validated = self.validate(record)
                    if not validated:
                        continue

                    # Transform
                    transformed = self.transform(validated)
                    all_records.append(transformed)
                    stats.records_parsed += 1

            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                stats.errors += 1

        # Deduplicate
        unique_records = self.deduplicate(all_records)
        stats.duplicates_skipped = stats.records_parsed - len(unique_records)
        logger.info(
            f"Deduplicated: {stats.records_parsed} -> {len(unique_records)} records"
        )

        # Apply filters if provided
        if filters:
            unique_records = self._apply_filters(unique_records, filters)
            logger.info(f"After filtering: {len(unique_records)} records")

        # Save to database
        if save_to_db and unique_records:
            stats.records_saved = await self._batch_save(unique_records, batch_size)
            logger.info(f"Saved {stats.records_saved} records to database")

        return unique_records, stats

    def _apply_filters(self, records: List[T], filters: dict) -> List[T]:
        """Apply filters to records. Override for custom filter logic."""
        result = records

        # Common filters
        if "counties" in filters and filters["counties"]:
            counties_lower = [c.lower() for c in filters["counties"]]
            result = [r for r in result if r.county and r.county.lower() in counties_lower]

        if "states" in filters and filters["states"]:
            states_lower = [s.lower() for s in filters["states"]]
            result = [r for r in result if r.state and r.state.lower() in states_lower]

        if "categories" in filters and filters["categories"]:
            categories_lower = [c.lower() for c in filters["categories"]]
            result = [
                r for r in result if r.category and r.category.lower() in categories_lower
            ]

        return result

    async def _batch_save(self, records: List[T], batch_size: int = 500) -> int:
        """Batch insert records into database."""
        saved = 0

        for batch_start in range(0, len(records), batch_size):
            batch = records[batch_start : batch_start + batch_size]

            # Convert to tuples for batch insert
            tuples = [record.to_db_tuple() for record in batch]

            try:
                batch_saved = await repo.batch_insert_hotels(
                    tuples, external_id_type=self.external_id_type
                )
                saved += batch_saved

                logger.info(
                    f"  Batch {batch_start // batch_size + 1}: "
                    f"{batch_start + len(batch)}/{len(records)} processed"
                )

            except Exception as e:
                logger.error(f"Batch insert failed: {e}")

        # Handle room counts if records have them
        room_records = [
            (r.room_count, r.external_id, self.source_name)
            for r in records
            if r.room_count
        ]

        if room_records:
            logger.info(f"Inserting {len(room_records)} room counts...")
            try:
                await repo.batch_insert_room_counts(
                    room_records, external_id_type=self.external_id_type
                )
            except Exception as e:
                logger.error(f"Room count insert failed: {e}")

        return saved
