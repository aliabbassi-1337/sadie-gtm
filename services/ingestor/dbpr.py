"""
Florida DBPR License Ingestion - Download and parse lodging licenses from MyFloridaLicense.com.

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
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import httpx
from loguru import logger


# DBPR data extract URLs
DBPR_BASE_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts"

# District files contain ALL active lodging licenses
DISTRICT_FILES = [
    f"hrlodge{i}.csv" for i in range(1, 8)
]

# New licenses file (current fiscal year only)
NEW_LICENSES_FILE = "newlodg.csv"


# License type codes and their meanings
LICENSE_TYPES = {
    "2001": "Hotel",
    "2002": "Motel",
    "2003": "Nontransient Apartment",
    "2004": "Transient Apartment",
    "2005": "Rooming House",
    "2006": "Vacation Rental - Condo",
    "2007": "Vacation Rental - Dwelling",
    "2008": "Bed and Breakfast",
    "2009": "Timeshare Project",
    "2010": "Resort Condominium",
    "2011": "Public Lodging - Other",
    "2012": "Resort Dwelling",
}

# Rank codes
RANK_CODES = {
    "HTLL": "Hotel",
    "MOTL": "Motel",
    "NTAP": "Nontransient Apt",
    "TRAP": "Transient Apt",
    "ROOM": "Rooming House",
    "CNDO": "Condo",
    "DWEL": "Dwelling",
    "BNKB": "Bed & Breakfast",
    "TMSH": "Timeshare",
    "RCND": "Resort Condo",
    "OTHR": "Other",
    "RDWL": "Resort Dwelling",
}

# Status codes
STATUS_CODES = {
    "10": "Pending",
    "20": "Current/Active",
    "25": "Conditional",
    "30": "Delinquent",
    "40": "Suspended",
    "50": "Revoked",
    "60": "Voluntary Inactive",
    "70": "Retired",
    "80": "Null and Void",
    "90": "Denied",
}


@dataclass
class DBPRLicense:
    """A lodging license from Florida DBPR."""
    license_number: str
    business_name: str
    licensee_name: str
    license_type: str
    rank: str

    # Location
    address: str
    city: str
    state: str
    zip_code: str
    county: str

    # Contact
    phone: Optional[str] = None

    # Status
    status: str = "Active"
    expiration_date: Optional[str] = None
    last_inspection_date: Optional[str] = None

    # Details
    num_units: Optional[int] = None
    district: Optional[str] = None

    # Raw data
    raw: Dict = field(default_factory=dict)


@dataclass
class DBPRIngestStats:
    """Stats from a DBPR ingestion run."""
    files_downloaded: int = 0
    records_parsed: int = 0
    records_saved: int = 0
    duplicates_skipped: int = 0
    errors: int = 0


class DBPRIngester:
    """Download and parse Florida DBPR lodging license data."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def download_file(self, filename: str) -> str:
        """Download a CSV file from DBPR."""
        url = f"{DBPR_BASE_URL}/{filename}"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                raise Exception(f"Failed to download {filename}: HTTP {resp.status_code}")
            return resp.text

    def parse_csv(self, csv_content: str, is_new_licenses: bool = False) -> List[DBPRLicense]:
        """Parse a DBPR CSV file into license objects."""
        licenses = []
        reader = csv.DictReader(io.StringIO(csv_content))

        for row in reader:
            try:
                license = self._parse_row(row, is_new_licenses)
                if license:
                    licenses.append(license)
            except Exception as e:
                logger.debug(f"Failed to parse row: {e}")
                continue

        return licenses

    def _parse_row(self, row: Dict, is_new_licenses: bool = False) -> Optional[DBPRLicense]:
        """Parse a single CSV row into a DBPRLicense."""
        # Get license number
        license_num = row.get("License Number", "").strip()
        if not license_num:
            return None

        # Get names
        business_name = row.get("Business Name", "").strip()
        licensee_name = row.get("Licensee Name", "").strip()

        # Use business name if available, otherwise licensee name
        name = business_name or licensee_name
        if not name:
            return None

        # Get license type
        type_code = row.get("License Type Code", "").strip()
        license_type = LICENSE_TYPES.get(type_code, "Unknown")

        # Get rank
        rank_code = row.get("Rank Code", "").strip()
        rank = RANK_CODES.get(rank_code, rank_code)

        # Get address - prefer location address over mailing
        address = row.get("Location Street Address", "").strip()
        if not address:
            address = row.get("Mailing Street Address", "").strip()

        addr2 = row.get("Location Address Line 2", "").strip()
        if addr2:
            address = f"{address}, {addr2}"

        city = row.get("Location City", "").strip()
        if not city:
            city = row.get("Mailing City", "").strip()

        state = row.get("Location State Code", "FL").strip()

        zip_code = row.get("Location Zip Code", "").strip()
        if not zip_code:
            zip_code = row.get("Mailing Zip Code", "").strip()
        # Clean zip code (remove +4)
        if zip_code and len(zip_code) > 5:
            zip_code = zip_code[:5]

        county = row.get("Location County", "").strip()
        if not county:
            county = row.get("County", "").strip()

        # Get phone
        phone = row.get("Secondary Phone Number", "").strip()
        if not phone:
            phone = row.get("Primary Phone Number", "").strip()

        # Get status
        status_code = row.get("Primary Status Code", "20").strip()
        status = STATUS_CODES.get(status_code, "Unknown")

        # Get dates
        expiration = row.get("License Expiry Date", "").strip()
        inspection = row.get("Last Inspection Date", "").strip()

        # Get units
        units_str = row.get("Number of Seats or Rental Units", "").strip()
        if not units_str:
            units_str = row.get("Number of Rental Units", "").strip()

        try:
            num_units = int(units_str) if units_str else None
        except ValueError:
            num_units = None

        # Get district
        district = row.get("District", "").strip()

        return DBPRLicense(
            license_number=license_num,
            business_name=business_name,
            licensee_name=licensee_name,
            license_type=license_type,
            rank=rank,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            county=county,
            phone=phone,
            status=status,
            expiration_date=expiration or None,
            last_inspection_date=inspection or None,
            num_units=num_units,
            district=district,
            raw=dict(row),
        )

    async def download_all_licenses(self) -> tuple[List[DBPRLicense], DBPRIngestStats]:
        """
        Download all active lodging licenses from DBPR.

        Returns tuple of (licenses, stats).
        """
        all_licenses = []
        stats = DBPRIngestStats()
        seen_license_nums = set()

        for filename in DISTRICT_FILES:
            logger.info(f"Downloading {filename}...")
            try:
                csv_content = await self.download_file(filename)
                stats.files_downloaded += 1

                licenses = self.parse_csv(csv_content)

                # Deduplicate
                for lic in licenses:
                    if lic.license_number in seen_license_nums:
                        stats.duplicates_skipped += 1
                        continue
                    seen_license_nums.add(lic.license_number)
                    all_licenses.append(lic)
                    stats.records_parsed += 1

                logger.info(f"  Parsed {len(licenses)} licenses from {filename}")

            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")
                stats.errors += 1

        return all_licenses, stats

    async def download_new_licenses(self) -> tuple[List[DBPRLicense], DBPRIngestStats]:
        """
        Download new lodging licenses (current fiscal year).

        Returns tuple of (licenses, stats).
        """
        stats = DBPRIngestStats()

        logger.info(f"Downloading {NEW_LICENSES_FILE}...")
        try:
            csv_content = await self.download_file(NEW_LICENSES_FILE)
            stats.files_downloaded += 1

            licenses = self.parse_csv(csv_content, is_new_licenses=True)
            stats.records_parsed = len(licenses)

            logger.info(f"  Parsed {len(licenses)} new licenses")
            return licenses, stats

        except Exception as e:
            logger.error(f"Error downloading {NEW_LICENSES_FILE}: {e}")
            stats.errors += 1
            return [], stats


def license_to_hotel_dict(lic: DBPRLicense) -> Dict:
    """Convert a DBPR license to a hotel dict for database insertion."""
    return {
        "name": lic.business_name or lic.licensee_name,
        "address": lic.address,
        "city": lic.city,
        "state": lic.state,
        "country": "USA",
        "phone_google": lic.phone,  # Use phone_google field for now
        "source": "dbpr",
        "status": 0,  # Pending detection
        # Store DBPR-specific data in metadata or dedicated columns
        # For now, we'll use what we have
    }


def get_license_type_name(code: str) -> str:
    """Get human-readable license type from code."""
    return LICENSE_TYPES.get(code, "Unknown")


def get_status_name(code: str) -> str:
    """Get human-readable status from code."""
    return STATUS_CODES.get(code, "Unknown")
