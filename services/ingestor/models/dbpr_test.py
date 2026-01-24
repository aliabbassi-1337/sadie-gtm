"""Tests for DBPR license model."""

import pytest
from services.ingestor.models.dbpr import (
    DBPRLicense,
    LICENSE_TYPES,
    RANK_CODES,
    STATUS_CODES,
)


class TestDBPRLicense:
    """Tests for DBPRLicense model."""

    @pytest.mark.no_db
    def test_create_with_required_fields(self):
        """Create license with required fields."""
        license = DBPRLicense(
            external_id="H12345678",
            license_number="H12345678",
            name="Test Hotel",
            license_type="Hotel",
            rank="HTLL",
            source="dbpr_hotel",
        )

        assert license.license_number == "H12345678"
        assert license.name == "Test Hotel"
        assert license.license_type == "Hotel"
        assert license.external_id_type == "dbpr_license"

    @pytest.mark.no_db
    def test_from_csv_row_basic(self):
        """Parse a basic CSV row."""
        row = {
            "License Number": "H12345678",
            "Business Name": "Grand Hotel",
            "Licensee Name": "Hotel Corp",
            "License Type Code": "2001",
            "Rank Code": "HTLL",
            "Location Street Address": "123 Main St",
            "Location City": "Miami",
            "Location State Code": "FL",
            "Location Zip Code": "33101",
            "Location County": "Miami-Dade",
            "Primary Phone Number": "3055551234",
            "Primary Status Code": "20",
        }

        license = DBPRLicense.from_csv_row(row)

        assert license is not None
        assert license.license_number == "H12345678"
        assert license.business_name == "Grand Hotel"
        assert license.name == "Grand Hotel"
        assert license.license_type == "Hotel"
        assert license.rank == "Hotel"
        assert license.address == "123 Main St"
        assert license.city == "Miami"
        assert license.state == "FL"
        assert license.zip_code == "33101"
        assert license.county == "Miami-Dade"
        assert license.phone == "3055551234"
        assert license.status == "Current/Active"
        assert license.external_id == "H12345678"

    @pytest.mark.no_db
    def test_from_csv_row_uses_licensee_name_fallback(self):
        """Use licensee name when business name is empty."""
        row = {
            "License Number": "H12345678",
            "Business Name": "",
            "Licensee Name": "John Smith",
            "License Type Code": "2001",
            "Rank Code": "HTLL",
            "Location City": "Miami",
        }

        license = DBPRLicense.from_csv_row(row)

        assert license is not None
        assert license.name == "John Smith"
        assert license.licensee_name == "John Smith"
        assert license.business_name is None

    @pytest.mark.no_db
    def test_from_csv_row_returns_none_for_missing_license_number(self):
        """Return None when license number is missing."""
        row = {
            "License Number": "",
            "Business Name": "Test Hotel",
            "License Type Code": "2001",
        }

        license = DBPRLicense.from_csv_row(row)
        assert license is None

    @pytest.mark.no_db
    def test_from_csv_row_returns_none_for_missing_name(self):
        """Return None when both names are missing."""
        row = {
            "License Number": "H12345678",
            "Business Name": "",
            "Licensee Name": "",
            "License Type Code": "2001",
        }

        license = DBPRLicense.from_csv_row(row)
        assert license is None

    @pytest.mark.no_db
    def test_from_csv_row_handles_address_line_2(self):
        """Combine address lines when present."""
        row = {
            "License Number": "H12345678",
            "Business Name": "Test Hotel",
            "License Type Code": "2001",
            "Rank Code": "HTLL",
            "Location Street Address": "123 Main St",
            "Location Address Line 2": "Suite 100",
            "Location City": "Miami",
        }

        license = DBPRLicense.from_csv_row(row)

        assert license.address == "123 Main St, Suite 100"

    @pytest.mark.no_db
    def test_from_csv_row_truncates_zip_code(self):
        """Truncate ZIP+4 to 5 digits."""
        row = {
            "License Number": "H12345678",
            "Business Name": "Test Hotel",
            "License Type Code": "2001",
            "Rank Code": "HTLL",
            "Location City": "Miami",
            "Location Zip Code": "33101-1234",
        }

        license = DBPRLicense.from_csv_row(row)

        assert license.zip_code == "33101"

    @pytest.mark.no_db
    def test_from_csv_row_parses_unit_count(self):
        """Parse number of units."""
        row = {
            "License Number": "H12345678",
            "Business Name": "Test Hotel",
            "License Type Code": "2001",
            "Rank Code": "HTLL",
            "Location City": "Miami",
            "Number of Seats or Rental Units": "150",
        }

        license = DBPRLicense.from_csv_row(row)

        assert license.num_units == 150
        assert license.room_count == 150

    @pytest.mark.no_db
    def test_from_csv_row_handles_invalid_unit_count(self):
        """Handle non-numeric unit count."""
        row = {
            "License Number": "H12345678",
            "Business Name": "Test Hotel",
            "License Type Code": "2001",
            "Rank Code": "HTLL",
            "Location City": "Miami",
            "Number of Seats or Rental Units": "N/A",
        }

        license = DBPRLicense.from_csv_row(row)

        assert license.num_units is None


class TestLicenseTypes:
    """Tests for license type constants."""

    @pytest.mark.no_db
    def test_license_types_mapping(self):
        """License types map codes to names."""
        assert LICENSE_TYPES["2001"] == "Hotel"
        assert LICENSE_TYPES["2002"] == "Motel"
        assert LICENSE_TYPES["2008"] == "Bed and Breakfast"

    @pytest.mark.no_db
    def test_rank_codes_mapping(self):
        """Rank codes map to names."""
        assert RANK_CODES["HTLL"] == "Hotel"
        assert RANK_CODES["MOTL"] == "Motel"
        assert RANK_CODES["BNKB"] == "Bed & Breakfast"

    @pytest.mark.no_db
    def test_status_codes_mapping(self):
        """Status codes map to names."""
        assert STATUS_CODES["20"] == "Current/Active"
        assert STATUS_CODES["30"] == "Delinquent"
        assert STATUS_CODES["40"] == "Suspended"
