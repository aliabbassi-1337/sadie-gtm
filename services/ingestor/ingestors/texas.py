"""
Texas Hotel Tax Ingestor - Parse hotel data from Texas Comptroller files.

The Texas Comptroller of Public Accounts provides quarterly hotel tax receipt data
including hotel names, addresses, and room counts.

Data source: Texas Comptroller Open Records / Hotel Occupancy Tax files
"""

import csv
from pathlib import Path
from typing import List, Optional, AsyncIterator, Tuple

from loguru import logger

from services.ingestor.base import BaseIngestor
from services.ingestor.registry import register
from services.ingestor.models.base import IngestStats
from services.ingestor.models.texas import TexasHotel
from services.ingestor.sources.local import LocalSource


# Cache directory for Texas data files
CACHE_DIR = Path(__file__).parent.parent.parent.parent / "data" / "texas_cache"


@register("texas")
class TexasIngestor(BaseIngestor[TexasHotel]):
    """
    Parse Texas Comptroller hotel tax data.

    Usage:
        # Load all quarters
        ingestor = TexasIngestor()
        hotels, stats = await ingestor.ingest()

        # Load specific quarter
        ingestor = TexasIngestor(quarter="HOT 25 Q3")
        hotels, stats = await ingestor.ingest()
    """

    source_name = "texas_hot"
    external_id_type = "texas_hot"

    def __init__(
        self,
        quarter: Optional[str] = None,
        state_filter: str = "TX",
        encoding: str = "latin-1",
    ):
        """
        Initialize Texas ingestor.

        Args:
            quarter: Specific quarter directory (e.g., "HOT 25 Q3"). If None, loads all quarters.
            state_filter: Only include hotels in this state (default: TX)
            encoding: File encoding (default: latin-1 for Texas files)
        """
        self.quarter = quarter
        self.state_filter = state_filter
        self.encoding = encoding
        self._source: Optional[LocalSource] = None

    async def fetch(self) -> AsyncIterator[Tuple[str, bytes]]:
        """Fetch Texas CSV files from local cache."""
        if self.quarter:
            # Single quarter
            quarter_path = CACHE_DIR / self.quarter
            if not quarter_path.exists():
                logger.error(f"Quarter directory not found: {quarter_path}")
                return

            source = LocalSource(str(quarter_path), encoding=self.encoding)
            async for filepath, content in source.fetch_all("*.csv"):
                yield filepath, content
            async for filepath, content in source.fetch_all("*.CSV"):
                yield filepath, content
        else:
            # All quarters
            if not CACHE_DIR.exists():
                logger.error(f"Cache directory not found: {CACHE_DIR}")
                return

            quarter_dirs = sorted([d for d in CACHE_DIR.iterdir() if d.is_dir()])
            if not quarter_dirs:
                logger.error(f"No quarter directories found in {CACHE_DIR}")
                return

            logger.info(f"Found {len(quarter_dirs)} quarter directories")

            for quarter_dir in quarter_dirs:
                source = LocalSource(str(quarter_dir), encoding=self.encoding)
                async for filepath, content in source.fetch_all("*.csv"):
                    yield filepath, content
                async for filepath, content in source.fetch_all("*.CSV"):
                    yield filepath, content

    def parse(self, data: bytes, filename: str = "") -> List[TexasHotel]:
        """Parse Texas CSV data into hotel objects."""
        hotels = []

        # Decode with encoding
        try:
            text = data.decode(self.encoding)
        except UnicodeDecodeError:
            text = data.decode("latin-1")

        reader = csv.reader(text.splitlines())

        for row in reader:
            try:
                hotel = TexasHotel.from_csv_row(row)
                if hotel:
                    # Apply state filter
                    if self.state_filter and hotel.state != self.state_filter:
                        continue
                    hotels.append(hotel)
            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")
                continue

        return hotels

    def deduplicate(self, records: List[TexasHotel]) -> List[TexasHotel]:
        """
        Deduplicate by taxpayer_number + location_number.

        Keeps the most recent quarter's record.
        """
        seen = {}  # (taxpayer_number, location_number) -> hotel

        for hotel in records:
            key = (hotel.taxpayer_number, hotel.location_number)

            if key not in seen:
                seen[key] = hotel
            else:
                existing = seen[key]
                # Keep most recent quarter
                if hotel.reporting_quarter and existing.reporting_quarter:
                    if hotel.reporting_quarter > existing.reporting_quarter:
                        seen[key] = hotel

        return list(seen.values())

    # =========================================================================
    # Backward compatibility methods for tests and legacy code
    # =========================================================================

    def parse_csv(self, filepath: Path, state_filter: str = "TX") -> List[TexasHotel]:
        """
        Parse a CSV file from a file path (backward compatibility).

        Args:
            filepath: Path to the CSV file
            state_filter: Only include hotels in this state
        """
        old_filter = self.state_filter
        self.state_filter = state_filter
        try:
            content = filepath.read_bytes()
            return self.parse(content, str(filepath))
        finally:
            self.state_filter = old_filter

    def load_quarterly_data(
        self, quarter_dir: str = "HOT 25 Q3"
    ) -> Tuple[List[TexasHotel], IngestStats]:
        """
        Load hotel data from a quarterly file (backward compatibility).

        Returns tuple of (hotels, stats).
        """
        stats = IngestStats()

        # Find the CSV file
        dir_path = CACHE_DIR / quarter_dir
        csv_files = list(dir_path.glob("*.CSV")) + list(dir_path.glob("*.csv"))

        if not csv_files:
            logger.error(f"No CSV files found in {dir_path}")
            return [], stats

        csv_path = csv_files[0]
        logger.info(f"Parsing {csv_path}...")

        hotels = self.parse_csv(csv_path)
        stats.files_processed = 1
        stats.records_parsed = len(hotels)

        logger.info(f"  Parsed {len(hotels)} hotel records")

        return hotels, stats

    def load_all_quarters(self) -> Tuple[List[TexasHotel], IngestStats]:
        """
        Load and merge hotel data from all available quarter directories (backward compatibility).

        Returns tuple of (hotels, stats).
        """
        stats = IngestStats()
        all_hotels = []

        if not CACHE_DIR.exists():
            logger.error(f"Cache directory not found: {CACHE_DIR}")
            return [], stats

        # Find all quarter directories
        quarter_dirs = sorted([d for d in CACHE_DIR.iterdir() if d.is_dir()])

        if not quarter_dirs:
            logger.error(f"No quarter directories found in {CACHE_DIR}")
            return [], stats

        logger.info(f"Found {len(quarter_dirs)} quarter directories")

        for quarter_dir in quarter_dirs:
            hotels, q_stats = self.load_quarterly_data(quarter_dir.name)
            all_hotels.extend(hotels)
            stats.files_processed += q_stats.files_processed
            stats.records_parsed += q_stats.records_parsed

        logger.info(f"Total records from all quarters: {len(all_hotels):,}")

        # Deduplicate across all quarters
        unique_hotels = self.deduplicate(all_hotels)
        logger.info(f"Unique hotels after merge: {len(unique_hotels):,}")

        return unique_hotels, stats

    def deduplicate_hotels(self, hotels: List[TexasHotel]) -> List[TexasHotel]:
        """Alias for deduplicate (backward compatibility)."""
        return self.deduplicate(hotels)
