"""Tests for DBPR ingestor."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

from services.ingestor.ingestors.dbpr import DBPRIngestor, get_license_types, get_rank_codes
from services.ingestor.models.dbpr import LICENSE_TYPES


# DBPR extract file header (35 properly aligned columns)
DBPR_HEADER = '"Board Code","License Type Code","Licensee Name","Rank Code","Modifier Code","Mailing Name","Mailing Street Address","Mailing Address Line 2","Mailing Address Line 3","Mailing City","Mailing State Code","Mailing Zip Code","Primary Phone Number","Mailing County Code","Business Name","Filler","Location Street Address","Location Address Line 2","Location Address Line 3","Location City","Location State Code","Location Zip Code","Location County Code","Location County","Secondary Phone Number","District","Region","License Number","Primary Status Code","Secondary Status Code","License Expiry Date","Last Inspection Date","Number of Seats or Rental Units","Base Risk Level","Secondary Risk Level"'


def make_dbpr_row(
    license_number: str,
    business_name: str,
    license_type_code: str = "2001",
    rank_code: str = "HTLL",
    city: str = "Miami",
    state: str = "FL",
    county: str = "Miami-Dade",
    phone: str = "",
    status_code: str = "20",
    units: str = "10",
) -> str:
    """Create a DBPR CSV row matching extract file format (35 columns)."""
    return f'"200","{license_type_code}","{business_name}","{rank_code}","","{business_name}","","","","{city}","{state}","33101","{phone}","99","{business_name}","","123 Main St","","","{city}","{state}","33101","99","{county}","","D1","","{license_number}","{status_code}","","01/01/2026","","{units}","",""'


class TestDBPRIngestor:
    """Tests for DBPRIngestor."""

    @pytest.mark.no_db
    def test_init_all_licenses(self):
        """Initialize ingestor for all licenses."""
        ingestor = DBPRIngestor(new_only=False)

        assert ingestor.new_only is False
        assert ingestor.source_name == "dbpr"
        assert ingestor.external_id_type == "dbpr_license"

    @pytest.mark.no_db
    def test_init_new_only(self):
        """Initialize ingestor for new licenses only."""
        ingestor = DBPRIngestor(new_only=True)

        assert ingestor.new_only is True

    @pytest.mark.no_db
    def test_parse_csv_content(self):
        """Parse CSV content into licenses."""
        csv_content = (
            DBPR_HEADER + "\n" +
            make_dbpr_row("HTL12345678", "Grand Hotel", phone="3055551234")
        ).encode("utf-8")

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        assert len(licenses) == 1
        license = licenses[0]
        assert license.license_number == "HTL12345678"
        assert license.name == "Grand Hotel"
        assert license.license_type == "Hotel"
        assert license.city == "Miami"
        assert license.phone == "3055551234"

    @pytest.mark.no_db
    def test_parse_handles_encoding(self):
        """Parse handles different encodings."""
        csv_content = (
            DBPR_HEADER + "\n" +
            make_dbpr_row("HTL123", "Café Hotel")
        ).encode("latin-1")

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        assert len(licenses) == 1
        assert "Caf" in licenses[0].name

    @pytest.mark.no_db
    def test_parse_skips_invalid_rows(self):
        """Parse skips rows with missing required fields."""
        csv_content = (
            DBPR_HEADER + "\n" +
            make_dbpr_row("", "Missing License") + "\n" +  # Missing license num
            make_dbpr_row("HTL001", "") + "\n" +  # Missing name
            make_dbpr_row("HTL002", "Valid Hotel")  # Valid
        ).encode("utf-8")

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        # Only valid row should be parsed
        assert len(licenses) == 1
        assert licenses[0].license_number == "HTL002"

    @pytest.mark.no_db
    def test_apply_filters_by_county(self):
        """Apply county filter."""
        csv_content = (
            DBPR_HEADER + "\n" +
            make_dbpr_row("HTL001", "Hotel 1", county="Miami-Dade") + "\n" +
            make_dbpr_row("HTL002", "Hotel 2", county="Orange") + "\n" +
            make_dbpr_row("HTL003", "Hotel 3", county="Broward")
        ).encode("utf-8")

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        filtered = ingestor._apply_filters(
            licenses,
            {"counties": ["Miami-Dade", "Broward"]},
        )

        assert len(filtered) == 2
        counties = {l.county for l in filtered}
        assert "Miami-Dade" in counties
        assert "Broward" in counties

    @pytest.mark.no_db
    def test_apply_filters_by_license_type(self):
        """Apply license type filter."""
        csv_content = (
            DBPR_HEADER + "\n" +
            make_dbpr_row("HTL001", "Hotel 1", license_type_code="2001", rank_code="HTLL") + "\n" +
            make_dbpr_row("MOT002", "Motel 2", license_type_code="2002", rank_code="MOTL") + "\n" +
            make_dbpr_row("BNB003", "B&B 3", license_type_code="2008", rank_code="BNKB")
        ).encode("utf-8")

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        filtered = ingestor._apply_filters(
            licenses,
            {"license_types": ["Hotel", "Motel"]},
        )

        assert len(filtered) == 2
        types = {l.license_type for l in filtered}
        assert "Hotel" in types
        assert "Motel" in types


class TestHelperFunctions:
    """Tests for module helper functions."""

    @pytest.mark.no_db
    def test_get_license_types(self):
        """Get license types returns copy of mapping."""
        types = get_license_types()

        assert types == LICENSE_TYPES
        assert types is not LICENSE_TYPES  # Should be a copy

    @pytest.mark.no_db
    def test_get_rank_codes(self):
        """Get rank codes returns mapping."""
        codes = get_rank_codes()

        assert "HTLL" in codes
        assert codes["HTLL"] == "Hotel"
