"""
Florida DBPR License model.
"""

from typing import Optional
from pydantic import Field

from services.ingestor.models.base import BaseRecord


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


class DBPRLicense(BaseRecord):
    """A lodging license from Florida DBPR."""

    # DBPR-specific fields
    license_number: str
    business_name: Optional[str] = None
    licensee_name: Optional[str] = None
    license_type: str
    rank: str

    # Status
    status: str = "Active"
    expiration_date: Optional[str] = None
    last_inspection_date: Optional[str] = None

    # Details
    num_units: Optional[int] = None
    district: Optional[str] = None

    # Override base fields with defaults
    external_id_type: str = "dbpr_license"
    country: str = "USA"

    @classmethod
    def from_csv_row(cls, row: dict) -> Optional["DBPRLicense"]:
        """
        Parse a DBPR CSV row into a DBPRLicense.

        Supports DBPR extract files (hrlodge*.csv) which have 35 properly aligned columns:
        - License Number: column 28
        - Location County: column 24
        - Primary Status Code: column 29
        - Number of Seats or Rental Units: column 33
        """
        # Get license number
        license_num = row.get("License Number", "").strip()
        if not license_num:
            return None

        # Get names
        business_name = row.get("Business Name", "").strip()
        licensee_name = row.get("Licensee Name", "").strip()
        name = business_name or licensee_name
        if not name:
            return None

        # Get license type
        type_code = row.get("License Type Code", "").strip()
        license_type = LICENSE_TYPES.get(type_code, "Unknown")

        # Get rank
        rank_code = row.get("Rank Code", "").strip()
        rank = RANK_CODES.get(rank_code, rank_code)

        # Get address
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
        if zip_code and len(zip_code) > 5:
            zip_code = zip_code[:5]

        # Get county name
        county = row.get("Location County", "").strip()

        # Get phone
        phone = row.get("Primary Phone Number", "").strip()

        # Get status
        status_code = row.get("Primary Status Code", "20").strip()
        status = STATUS_CODES.get(status_code, "Unknown")

        # Get expiry date
        expiration = row.get("License Expiry Date", "").strip()

        # Get last inspection date
        last_inspection = row.get("Last Inspection Date", "").strip()

        # Get unit count
        units_str = row.get("Number of Seats or Rental Units", "").strip()
        try:
            num_units = int(units_str) if units_str else None
        except ValueError:
            num_units = None

        # Get district
        district = row.get("District", "").strip()

        # Build source name
        source = f"dbpr_{license_type.lower().replace(' ', '_').replace('-', '_')}"
        category = license_type.lower().replace(" ", "_").replace("-", "_")

        return cls(
            external_id=license_num,
            external_id_type="dbpr_license",
            name=name,
            address=address or None,
            city=city or None,
            state=state or None,
            zip_code=zip_code or None,
            county=county or None,
            phone=phone or None,
            category=category,
            source=source,
            room_count=num_units,
            raw=dict(row),
            license_number=license_num,
            business_name=business_name or None,
            licensee_name=licensee_name or None,
            license_type=license_type,
            rank=rank,
            status=status,
            expiration_date=expiration or None,
            last_inspection_date=last_inspection or None,
            num_units=num_units,
            district=district or None,
        )
