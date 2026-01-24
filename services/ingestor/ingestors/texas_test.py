"""Tests for Texas ingestor."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from services.ingestor.ingestors.texas import TexasIngestor, CACHE_DIR


class TestTexasIngestor:
    """Tests for TexasIngestor."""

    @pytest.mark.no_db
    def test_init_default_quarter(self):
        """Initialize ingestor with default quarter."""
        ingestor = TexasIngestor()

        assert ingestor.source_name == "texas_hot"
        assert ingestor.external_id_type == "texas_hot"
        assert ingestor.quarter is None

    @pytest.mark.no_db
    def test_init_with_quarter(self):
        """Initialize ingestor with specific quarter."""
        ingestor = TexasIngestor(quarter="HOT 25 Q3")

        assert ingestor.quarter == "HOT 25 Q3"

    @pytest.mark.no_db
    def test_parse_csv_row(self):
        """Parse CSV row with indexed columns (no header)."""
        ingestor = TexasIngestor()

        # Row format follows COLUMNS indices:
        # 0: taxpayer_number, 1: taxpayer_name, 2: taxpayer_address, 3: taxpayer_city,
        # 4: taxpayer_state, 5: taxpayer_zip, 6: taxpayer_county, 7: taxpayer_phone,
        # 8: location_number, 9: location_name, 10: location_address, 11: location_city,
        # 12: location_state, 13: location_zip, 14: location_county, 15: location_phone,
        # 16: unit_capacity, 17: responsibility_begin_date, 18: responsibility_end_date,
        # 19: reporting_quarter, 20: filer_type, 21: total_room_receipts, 22: taxable_receipts

        csv_content = b'''12345678901,Hotel Corp,123 Corp St,Dallas,TX,75201,DALLAS,2145551234,001,Test Hotel,456 Main St,Austin,TX,78701,TRAVIS,5125551234,100,01/01/2025,03/31/2025,HOT 25 Q1,1,50000,45000'''

        hotels = ingestor.parse(csv_content)

        assert len(hotels) == 1
        hotel = hotels[0]
        assert hotel.taxpayer_number == "12345678901"
        assert hotel.location_number == "001"
        assert hotel.name == "Test Hotel"
        assert hotel.city == "Austin"
        assert hotel.state == "TX"
        assert hotel.county == "TRAVIS"
        assert hotel.room_count == 100
        assert hotel.phone == "512-555-1234"

    @pytest.mark.no_db
    def test_parse_handles_encoding(self):
        """Parse handles different encodings."""
        ingestor = TexasIngestor()

        # Latin-1 content
        csv_content = "12345678901,Café Corp,123 Corp St,Dallas,TX,75201,DALLAS,2145551234,001,Café Hotel,456 Main St,Austin,TX,78701,TRAVIS,5125551234,50,,,,,,".encode("latin-1")

        hotels = ingestor.parse(csv_content)

        assert len(hotels) == 1
        assert "Caf" in hotels[0].name

    @pytest.mark.no_db
    def test_parse_skips_out_of_state(self):
        """Parse skips records with out-of-state locations."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,123 Corp St,Dallas,TX,75201,DALLAS,2145551234,001,Texas Hotel,456 Main St,Austin,TX,78701,TRAVIS,5125551234,100,,,,,,
12345678902,Hotel Corp,456 Other St,Austin,TX,78701,TRAVIS,5125551234,002,California Hotel,789 Beach St,Los Angeles,CA,90001,LOS ANGELES,3105551234,50,,,,,,'''

        hotels = ingestor.parse(csv_content)

        assert len(hotels) == 1
        assert hotels[0].state == "TX"

    @pytest.mark.no_db
    def test_parse_formats_phone(self):
        """Parse formats 10-digit phone numbers."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,123 Corp St,Dallas,TX,75201,DALLAS,2145551234,001,Test Hotel,456 Main St,Austin,TX,78701,TRAVIS,5125551234,100,,,,,,'''

        hotels = ingestor.parse(csv_content)

        assert len(hotels) == 1
        assert hotels[0].phone == "512-555-1234"

    @pytest.mark.no_db
    def test_parse_uses_taxpayer_phone_fallback(self):
        """Parse uses taxpayer phone when location phone is missing."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,123 Corp St,Dallas,TX,75201,DALLAS,2145559999,001,Test Hotel,456 Main St,Austin,TX,78701,TRAVIS,,50,,,,,,'''

        hotels = ingestor.parse(csv_content)

        assert len(hotels) == 1
        assert hotels[0].phone == "214-555-9999"

    @pytest.mark.no_db
    def test_deduplicate_keeps_unique(self):
        """Deduplicate keeps unique taxpayer:location combinations."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel A,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,
12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,002,Hotel B,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,
99999999999,Other Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel C,Addr,Dallas,TX,75201,DALLAS,,50,,,,,,'''

        hotels = ingestor.parse(csv_content)
        deduped = ingestor.deduplicate(hotels)

        assert len(deduped) == 3

    @pytest.mark.no_db
    def test_deduplicate_removes_same_location(self):
        """Deduplicate removes duplicate taxpayer:location combinations."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel A v1,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,
12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel A v2,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,'''

        hotels = ingestor.parse(csv_content)
        deduped = ingestor.deduplicate(hotels)

        # Should keep only one (same taxpayer:location)
        assert len(deduped) == 1

    @pytest.mark.no_db
    def test_external_id_format(self):
        """External ID uses taxpayer:location format."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,003,Test Hotel,Addr,Austin,TX,78701,TRAVIS,,50,,,,,,'''

        hotels = ingestor.parse(csv_content)

        assert len(hotels) == 1
        assert hotels[0].external_id == "12345678901:003"


class TestTexasIngestorBackwardCompat:
    """Tests for backward compatibility methods."""

    @pytest.mark.no_db
    def test_parse_csv(self):
        """Parse CSV from file path (backward compatibility)."""
        ingestor = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(
                '12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Test Hotel,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,'
            )

            hotels = ingestor.parse_csv(csv_path)

            assert len(hotels) == 1
            assert hotels[0].name == "Test Hotel"

    @pytest.mark.no_db
    def test_load_quarterly_data(self):
        """Load quarterly data from cache directory."""
        ingestor = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            quarter_dir = cache_dir / "HOT 25 Q1"
            quarter_dir.mkdir()

            (quarter_dir / "data.csv").write_text(
                '12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Test Hotel,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,'
            )

            with patch("services.ingestor.ingestors.texas.CACHE_DIR", cache_dir):
                hotels, stats = ingestor.load_quarterly_data("HOT 25 Q1")

                assert len(hotels) == 1
                assert stats.files_processed == 1
                assert stats.records_parsed == 1

    @pytest.mark.no_db
    def test_load_all_quarters(self):
        """Load all quarters and deduplicate."""
        ingestor = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Create two quarters with overlapping data
            q1_dir = cache_dir / "HOT 25 Q1"
            q1_dir.mkdir()
            (q1_dir / "data.csv").write_text(
                '12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel A,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,'
            )

            q2_dir = cache_dir / "HOT 25 Q2"
            q2_dir.mkdir()
            (q2_dir / "data.csv").write_text(
                '12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel A Updated,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,'
            )

            with patch("services.ingestor.ingestors.texas.CACHE_DIR", cache_dir):
                hotels, stats = ingestor.load_all_quarters()

                # Should be deduplicated to 1
                assert len(hotels) == 1
                assert stats.files_processed == 2

    @pytest.mark.no_db
    def test_deduplicate_hotels_alias(self):
        """deduplicate_hotels is alias for deduplicate."""
        ingestor = TexasIngestor()

        csv_content = b'''12345678901,Hotel Corp,Addr,Dallas,TX,75201,DALLAS,,001,Hotel A,Addr,Austin,TX,78701,TRAVIS,,100,,,,,,'''
        hotels = ingestor.parse(csv_content)

        result = ingestor.deduplicate_hotels(hotels)

        assert result == ingestor.deduplicate(hotels)
