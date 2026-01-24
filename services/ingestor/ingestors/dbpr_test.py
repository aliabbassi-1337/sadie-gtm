"""Tests for DBPR ingestor."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

from services.ingestor.ingestors.dbpr import DBPRIngestor, get_license_types, get_rank_codes
from services.ingestor.models.dbpr import LICENSE_TYPES


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
        csv_content = b'''License Number,Business Name,Licensee Name,License Type Code,Rank Code,Location Street Address,Location City,Location State Code,Location Zip Code,Location County,Primary Phone Number,Primary Status Code
H12345678,Grand Hotel,Hotel Corp,2001,HTLL,123 Main St,Miami,FL,33101,Miami-Dade,3055551234,20'''

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        assert len(licenses) == 1
        license = licenses[0]
        assert license.license_number == "H12345678"
        assert license.name == "Grand Hotel"
        assert license.license_type == "Hotel"
        assert license.city == "Miami"

    @pytest.mark.no_db
    def test_parse_handles_encoding(self):
        """Parse handles different encodings."""
        # Latin-1 content
        csv_content = "License Number,Business Name,Licensee Name,License Type Code,Rank Code,Location City\nH123,Café Hotel,Owner,2001,HTLL,Miami".encode("latin-1")

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        assert len(licenses) == 1
        assert "Caf" in licenses[0].name

    @pytest.mark.no_db
    def test_parse_skips_invalid_rows(self):
        """Parse skips rows with missing required fields."""
        csv_content = b'''License Number,Business Name,License Type Code,Rank Code,Location City
,Missing License,2001,HTLL,Miami
H12345678,,2001,HTLL,Miami
H12345679,Valid Hotel,2001,HTLL,Miami'''

        ingestor = DBPRIngestor()
        licenses = ingestor.parse(csv_content)

        # Only valid row should be parsed
        assert len(licenses) == 1
        assert licenses[0].license_number == "H12345679"

    @pytest.mark.no_db
    def test_apply_filters_by_county(self):
        """Apply county filter."""
        csv_content = b'''License Number,Business Name,License Type Code,Rank Code,Location City,Location County
H001,Hotel 1,2001,HTLL,Miami,Miami-Dade
H002,Hotel 2,2001,HTLL,Orlando,Orange
H003,Hotel 3,2001,HTLL,Fort Lauderdale,Broward'''

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
        csv_content = b'''License Number,Business Name,License Type Code,Rank Code,Location City
H001,Hotel 1,2001,HTLL,Miami
H002,Motel 2,2002,MOTL,Miami
H003,B&B 3,2008,BNKB,Miami'''

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
