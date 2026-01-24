"""
Texas Hotel Tax Ingestion - Parse hotel data from Texas Comptroller files.

The Texas Comptroller of Public Accounts provides quarterly hotel tax receipt data
including hotel names, addresses, and room counts.

Data source: Texas Comptroller Open Records / Hotel Occupancy Tax files
"""

import csv
from pathlib import Path
from typing import List, Optional, Dict
from pydantic import BaseModel, Field
from loguru import logger

# Cache directory for Texas data files
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "texas_cache"

# Column indices for the quarterly CSV (no header row)
# Based on "Hotel Quarterly File Record Layout"
COLUMNS = {
    "taxpayer_number": 0,
    "taxpayer_name": 1,
    "taxpayer_address": 2,
    "taxpayer_city": 3,
    "taxpayer_state": 4,
    "taxpayer_zip": 5,
    "taxpayer_county": 6,
    "taxpayer_phone": 7,
    "location_number": 8,
    "location_name": 9,
    "location_address": 10,
    "location_city": 11,
    "location_state": 12,
    "location_zip": 13,
    "location_county": 14,
    "location_phone": 15,
    "unit_capacity": 16,  # Room count!
    "responsibility_begin_date": 17,
    "responsibility_end_date": 18,
    "reporting_quarter": 19,
    "filer_type": 20,
    "total_room_receipts": 21,
    "taxable_receipts": 22,
}


class TexasHotel(BaseModel):
    """A hotel from Texas Comptroller hotel tax data."""
    taxpayer_number: str
    location_number: str

    # Hotel info
    name: str
    address: str
    city: str
    state: str
    zip_code: str
    county: Optional[str] = None
    phone: Optional[str] = None

    # Room count from tax records
    room_count: Optional[int] = None

    # Tax info
    reporting_quarter: Optional[str] = None
    total_receipts: Optional[float] = None

    # Raw data
    raw: Dict = Field(default_factory=dict)


class TexasIngestStats(BaseModel):
    """Stats from a Texas ingestion run."""
    files_processed: int = 0
    records_parsed: int = 0
    records_saved: int = 0
    duplicates_skipped: int = 0
    errors: int = 0


class TexasIngestor:
    """Parse Texas Comptroller hotel tax data."""

    def __init__(self):
        pass

    def parse_csv(self, filepath: Path, state_filter: str = "TX") -> List[TexasHotel]:
        """Parse a Texas hotel tax CSV file into hotel objects.

        Args:
            filepath: Path to CSV file
            state_filter: Only include hotels in this state (default: TX)
        """
        hotels = []

        with open(filepath, 'r', encoding='latin-1') as f:
            reader = csv.reader(f)

            for row in reader:
                try:
                    hotel = self._parse_row(row)
                    if hotel:
                        # Filter by state
                        if state_filter and hotel.state != state_filter:
                            continue
                        # Skip government entities (not actual hotels)
                        name_upper = hotel.name.upper()
                        if name_upper.startswith("COUNTY OF") or name_upper.startswith("CITY OF"):
                            continue
                        # Skip single-room entries (likely not real hotels)
                        if hotel.room_count and hotel.room_count <= 1:
                            continue
                        hotels.append(hotel)
                except Exception as e:
                    logger.debug(f"Failed to parse row: {e}")
                    continue

        return hotels

    def _parse_row(self, row: List[str]) -> Optional[TexasHotel]:
        """Parse a single CSV row into a TexasHotel."""
        if len(row) < 17:
            return None

        def get_col(name: str) -> str:
            idx = COLUMNS.get(name, -1)
            if idx >= 0 and idx < len(row):
                return row[idx].strip().strip('"')
            return ""

        # Get hotel name (location name)
        name = get_col("location_name")
        if not name:
            return None

        # Get address
        address = get_col("location_address")
        city = get_col("location_city")
        state = get_col("location_state") or "TX"
        zip_code = get_col("location_zip")

        if not city:
            return None

        # Get phone - prefer location phone, fall back to taxpayer phone
        phone = get_col("location_phone")
        if not phone or phone.strip() == "":
            phone = get_col("taxpayer_phone")

        # Clean phone
        if phone:
            phone = phone.strip()
            if len(phone) == 10 and phone.isdigit():
                phone = f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"

        # Get room count
        room_count = None
        capacity_str = get_col("unit_capacity")
        if capacity_str:
            try:
                room_count = int(capacity_str)
            except ValueError:
                pass

        # Get county code
        county = get_col("location_county")

        # Get tax info
        quarter = get_col("reporting_quarter")

        total_receipts = None
        receipts_str = get_col("total_room_receipts")
        if receipts_str:
            try:
                total_receipts = float(receipts_str)
            except ValueError:
                pass

        return TexasHotel(
            taxpayer_number=get_col("taxpayer_number"),
            location_number=get_col("location_number"),
            name=name,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            county=county,
            phone=phone if phone else None,
            room_count=room_count,
            reporting_quarter=quarter,
            total_receipts=total_receipts,
            raw=dict(zip(COLUMNS.keys(), row[:len(COLUMNS)])),
        )

    def load_quarterly_data(self, quarter_dir: str = "HOT 25 Q3") -> tuple[List[TexasHotel], TexasIngestStats]:
        """
        Load hotel data from a quarterly file.

        Returns tuple of (hotels, stats).
        """
        stats = TexasIngestStats()

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

    def load_all_quarters(self) -> tuple[List[TexasHotel], TexasIngestStats]:
        """
        Load and merge hotel data from all available quarter directories.

        Finds all directories in CACHE_DIR, loads each one, and deduplicates
        across all quarters (keeping the most recent/best record for each hotel).

        Returns tuple of (hotels, stats).
        """
        stats = TexasIngestStats()
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
        unique_hotels = self.deduplicate_hotels(all_hotels)
        logger.info(f"Unique hotels after merge: {len(unique_hotels):,}")

        return unique_hotels, stats

    def deduplicate_hotels(self, hotels: List[TexasHotel]) -> List[TexasHotel]:
        """
        Deduplicate by taxpayer_number + location_number (official unique key).

        Keeps the most recent quarter's record.
        """
        seen = {}  # (taxpayer_number, location_number) -> hotel

        for hotel in hotels:
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


def hotel_to_db_dict(hotel: TexasHotel) -> Dict:
    """Convert a TexasHotel to a dict for database insertion."""
    return {
        "name": hotel.name,
        "address": hotel.address,
        "city": hotel.city,
        "state": hotel.state,
        "country": "USA",
        "phone": hotel.phone,
        "source": "texas_hot",
        "category": "hotel",  # Texas data is all hotel occupancy tax filers
    }
