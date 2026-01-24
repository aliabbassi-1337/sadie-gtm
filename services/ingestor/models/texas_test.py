"""Tests for Texas hotel model."""

import pytest
from services.ingestor.models.texas import TexasHotel, format_phone, COLUMNS


class TestTexasHotel:
    """Tests for TexasHotel model."""

    @pytest.mark.no_db
    def test_create_with_required_fields(self):
        """Create Texas hotel with required fields."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="001",
            name="Test Hotel",
        )

        assert hotel.taxpayer_number == "12345678901"
        assert hotel.location_number == "001"
        assert hotel.name == "Test Hotel"

    @pytest.mark.no_db
    def test_auto_computes_external_id(self):
        """External ID is auto-computed from taxpayer and location."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="002",
            name="Test Hotel",
        )

        assert hotel.external_id == "12345678901:002"

    @pytest.mark.no_db
    def test_preserves_provided_external_id(self):
        """Preserves external ID if explicitly provided."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="002",
            name="Test Hotel",
            external_id="custom-id",
        )

        assert hotel.external_id == "custom-id"

    @pytest.mark.no_db
    def test_inherits_from_base_record(self):
        """TexasHotel inherits BaseRecord fields."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="001",
            name="Test Hotel",
            city="Austin",
            state="TX",
            zip_code="78701",
            county="Travis",
            phone="512-555-1234",
            room_count=100,
        )

        assert hotel.city == "Austin"
        assert hotel.state == "TX"
        assert hotel.zip_code == "78701"
        assert hotel.county == "Travis"
        assert hotel.phone == "512-555-1234"
        assert hotel.room_count == 100

    @pytest.mark.no_db
    def test_texas_specific_fields(self):
        """TexasHotel has Texas-specific fields."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="001",
            name="Test Hotel",
            reporting_quarter="HOT 25 Q3",
            total_receipts=50000.00,
        )

        assert hotel.reporting_quarter == "HOT 25 Q3"
        assert hotel.total_receipts == 50000.00

    @pytest.mark.no_db
    def test_optional_fields_default_none(self):
        """Optional fields default to None."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="001",
            name="Test Hotel",
        )

        assert hotel.reporting_quarter is None
        assert hotel.total_receipts is None
        assert hotel.address is None
        assert hotel.phone is None

    @pytest.mark.no_db
    def test_default_values(self):
        """Model has expected default values."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="001",
            name="Test Hotel",
        )

        assert hotel.external_id_type == "texas_hot"
        assert hotel.source == "texas_hot"
        assert hotel.category == "hotel"
        assert hotel.country == "USA"

    @pytest.mark.no_db
    def test_from_csv_row(self):
        """Parse hotel from CSV row."""
        # Build a full row with all columns
        row = [
            "12345678901",  # taxpayer_number
            "Hotel Corp",  # taxpayer_name
            "123 Corp St",  # taxpayer_address
            "Dallas",  # taxpayer_city
            "TX",  # taxpayer_state
            "75201",  # taxpayer_zip
            "DALLAS",  # taxpayer_county
            "2145551234",  # taxpayer_phone
            "001",  # location_number
            "Test Hotel",  # location_name
            "456 Main St",  # location_address
            "Austin",  # location_city
            "TX",  # location_state
            "78701",  # location_zip
            "TRAVIS",  # location_county
            "5125551234",  # location_phone
            "100",  # unit_capacity
            "01/01/2025",  # responsibility_begin_date
            "03/31/2025",  # responsibility_end_date
            "HOT 25 Q1",  # reporting_quarter
            "1",  # filer_type
            "50000",  # total_room_receipts
            "45000",  # taxable_receipts
        ]

        hotel = TexasHotel.from_csv_row(row)

        assert hotel is not None
        assert hotel.taxpayer_number == "12345678901"
        assert hotel.location_number == "001"
        assert hotel.name == "Test Hotel"
        assert hotel.city == "Austin"
        assert hotel.state == "TX"
        assert hotel.phone == "512-555-1234"
        assert hotel.room_count == 100
        assert hotel.reporting_quarter == "HOT 25 Q1"

    @pytest.mark.no_db
    def test_from_csv_row_returns_none_for_short_row(self):
        """from_csv_row returns None for rows with too few columns."""
        row = ["12345", "Hotel Corp"]  # Only 2 columns

        hotel = TexasHotel.from_csv_row(row)

        assert hotel is None

    @pytest.mark.no_db
    def test_from_csv_row_returns_none_without_name(self):
        """from_csv_row returns None when location_name is missing."""
        row = [
            "12345678901", "Corp", "Addr", "Dallas", "TX", "75201", "DALLAS", "",
            "001", "",  # Empty location name
            "Addr", "Austin", "TX", "78701", "TRAVIS", "", "100",
        ]

        hotel = TexasHotel.from_csv_row(row)

        assert hotel is None

    @pytest.mark.no_db
    def test_from_csv_row_returns_none_without_city(self):
        """from_csv_row returns None when location_city is missing."""
        row = [
            "12345678901", "Corp", "Addr", "Dallas", "TX", "75201", "DALLAS", "",
            "001", "Test Hotel", "Addr", "",  # Empty location city
            "TX", "78701", "TRAVIS", "", "100",
        ]

        hotel = TexasHotel.from_csv_row(row)

        assert hotel is None

    @pytest.mark.no_db
    def test_to_db_tuple(self):
        """Convert Texas hotel to database tuple."""
        hotel = TexasHotel(
            taxpayer_number="12345678901",
            location_number="001",
            name="Test Hotel",
            city="Austin",
            state="TX",
        )

        db_tuple = hotel.to_db_tuple()

        assert isinstance(db_tuple, tuple)
        # Should contain name, city, state, etc.
        assert "Test Hotel" in db_tuple
        assert "Austin" in db_tuple
        assert "TX" in db_tuple


class TestFormatPhone:
    """Tests for phone formatting function."""

    @pytest.mark.no_db
    def test_formats_10_digit_number(self):
        """Format 10-digit phone number."""
        assert format_phone("5125551234") == "512-555-1234"

    @pytest.mark.no_db
    def test_returns_none_for_empty(self):
        """Return None for empty phone."""
        assert format_phone("") is None
        assert format_phone("   ") is None

    @pytest.mark.no_db
    def test_returns_original_for_non_10_digit(self):
        """Return original for non-10-digit phone."""
        assert format_phone("555-1234") == "555-1234"
        assert format_phone("12345") == "12345"

    @pytest.mark.no_db
    def test_strips_whitespace(self):
        """Strip whitespace before formatting."""
        assert format_phone("  5125551234  ") == "512-555-1234"


class TestColumns:
    """Tests for column index mapping."""

    @pytest.mark.no_db
    def test_expected_columns_present(self):
        """Column mapping has expected keys."""
        expected = [
            "taxpayer_number",
            "taxpayer_name",
            "location_number",
            "location_name",
            "location_city",
            "location_state",
            "unit_capacity",
        ]

        for col in expected:
            assert col in COLUMNS

    @pytest.mark.no_db
    def test_unit_capacity_index(self):
        """Unit capacity (room count) is at expected index."""
        assert COLUMNS["unit_capacity"] == 16
