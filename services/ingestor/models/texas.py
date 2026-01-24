"""
Texas Hotel model.
"""

from typing import Optional, List
from pydantic import Field, model_validator

from services.ingestor.models.base import BaseRecord


# Column indices for the quarterly CSV (no header row)
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


def format_phone(phone: str) -> Optional[str]:
    """Format a 10-digit phone number."""
    if not phone:
        return None
    phone = phone.strip()
    if len(phone) == 10 and phone.isdigit():
        return f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
    return phone if phone else None


class TexasHotel(BaseRecord):
    """A hotel from Texas Comptroller hotel tax data."""

    # Texas-specific fields
    taxpayer_number: str
    location_number: str

    # Tax info
    reporting_quarter: Optional[str] = None
    total_receipts: Optional[float] = None

    # Override base fields with defaults (make external_id optional for backward compat)
    external_id: str = ""  # Will be computed from taxpayer:location
    external_id_type: str = "texas_hot"
    source: str = "texas_hot"
    category: str = "hotel"
    country: str = "USA"

    @model_validator(mode="after")
    def compute_external_id(self) -> "TexasHotel":
        """Auto-compute external_id from taxpayer_number and location_number."""
        if not self.external_id:
            self.external_id = f"{self.taxpayer_number}:{self.location_number}"
        return self

    @classmethod
    def from_csv_row(cls, row: List[str]) -> Optional["TexasHotel"]:
        """Parse a Texas CSV row into a TexasHotel."""
        if len(row) < 17:
            return None

        def get_col(name: str) -> str:
            idx = COLUMNS.get(name, -1)
            if 0 <= idx < len(row):
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
        phone = format_phone(phone)

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

        # Build external ID
        taxpayer_number = get_col("taxpayer_number")
        location_number = get_col("location_number")
        external_id = f"{taxpayer_number}:{location_number}"

        return cls(
            # Base fields
            external_id=external_id,
            external_id_type="texas_hot",
            name=name,
            address=address or None,
            city=city,
            state=state,
            zip_code=zip_code or None,
            county=county or None,
            phone=phone,
            category="hotel",
            source="texas_hot",
            room_count=room_count,
            raw=dict(zip(COLUMNS.keys(), row[: len(COLUMNS)])),
            # Texas-specific fields
            taxpayer_number=taxpayer_number,
            location_number=location_number,
            reporting_quarter=quarter or None,
            total_receipts=total_receipts,
        )
