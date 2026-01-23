"""Tests for Texas hotel tax data ingestion."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from services.ingestor.texas import TexasIngestor, TexasHotel


# Sample rows from actual Texas HOT data
SAMPLE_ROWS = [
    '10105515737,"TAN 1 ON, INC.                                    ","8504 LANDING WAY CT                     ","FORT WORTH          ","TX","76179",220,"8174754668",00006,"LAKEVIEW MARINA                                   ","6657 PEDEN RD                           ","FT WORTH            ","TX","76179",220,"          ",    2,20200301,        ,2025Q3,60,      9032.17,      9032.17,',
    '10105865405,"JAGADIA GAYATRI, INC.                             ","12111 MURPHY RD                         ","STAFFORD            ","TX","77477",079,"2818796900",00002,"GUEST MOTEL                                       ","12111 MURPHY RD                         ","STAFFORD            ","TX","77477",079,"          ",   30,20100401,        ,2025Q3,50,     42384.30,     42384.30,',
    '10106669541,"INTOWN SUITES WEBSTER, L.P.                       ","980 HAMMOND DR STE 500                  ","ATLANTA             ","GA","30328",000,"7707995000",00001,"INTOWN SUITES WEBSTER                             ","480 W BAY AREA BLVD                     ","WEBSTER             ","TX","77598",101,"          ",  132,20020601,        ,2025Q3,50,    426639.04,     58352.17,',
    '10106748378,"SRUTI HOSPITALITY, INC.                           ","5820 KATY FWY                           ","HOUSTON             ","TX","77007",101,"7138699211",00002,"SRUTI HOSPITALITY INC.                            ","5820 KATY FWY                           ","HOUSTON             ","TX","77007",101,"7138699211",  126,20130418,        ,2025Q3,50,   1025224.23,    999766.72,',
]

# Same taxpayer with multiple locations
MULTI_LOCATION_ROWS = [
    '10106997397,"STERLING RELOCATION, INC.                         ","16649 HOLLISTER ST                      ","HOUSTON             ","TX","77066",101,"7138054358",00002,"2400 MCCUE                                        ","2400 MCCUE                              ","HOUSTON             ","TX","77056",101,"          ",   13,20040701,        ,2025Q3,50,     46826.00,         0.00,',
    '10106997397,"STERLING RELOCATION, INC.                         ","16649 HOLLISTER ST                      ","HOUSTON             ","TX","77066",101,"7138054358",00003,"SAN MONTEGO                                       ","1600 ELDRIDGE PKWY                      ","HOUSTON             ","TX","77077",101,"          ",    3,20050131,        ,2025Q3,50,     76148.09,         0.00,',
    '10106997397,"STERLING RELOCATION, INC.                         ","16649 HOLLISTER ST                      ","HOUSTON             ","TX","77066",101,"7138054358",00004,"FAIRMONT                                          ","2323 LONG BEACH DR                      ","SUGARLAND           ","TX","77478",079,"          ",    5,20050201,        ,2025Q3,50,     62465.60,         0.00,',
    '10106997397,"STERLING RELOCATION, INC.                         ","16649 HOLLISTER ST                      ","HOUSTON             ","TX","77066",101,"7138054358",00008,"DOMINION POSTOAK                                  ","2323 MCCUE RD                           ","HOUSTON             ","TX","77056",101,"          ",    5,20050501,        ,2025Q3,50,     43507.50,         0.00,',
]

# Duplicate rows across quarters (same hotel, different quarters)
DUPLICATE_QUARTER_ROWS = [
    '10105865405,"JAGADIA GAYATRI, INC.                             ","12111 MURPHY RD                         ","STAFFORD            ","TX","77477",079,"2818796900",00002,"GUEST MOTEL                                       ","12111 MURPHY RD                         ","STAFFORD            ","TX","77477",079,"          ",   30,20100401,        ,2025Q2,50,     40000.00,     40000.00,',
    '10105865405,"JAGADIA GAYATRI, INC.                             ","12111 MURPHY RD                         ","STAFFORD            ","TX","77477",079,"2818796900",00002,"GUEST MOTEL                                       ","12111 MURPHY RD                         ","STAFFORD            ","TX","77477",079,"          ",   30,20100401,        ,2025Q3,50,     42384.30,     42384.30,',
]


class TestParsing:
    """Unit tests for CSV parsing."""

    @pytest.mark.no_db
    def test_parse_single_row(self):
        """Parse a single row correctly."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(SAMPLE_ROWS[1])  # GUEST MOTEL

            hotels = ingester.parse_csv(csv_path)

        assert len(hotels) == 1
        hotel = hotels[0]

        assert hotel.taxpayer_number == "10105865405"
        assert hotel.location_number == "00002"
        assert hotel.name == "GUEST MOTEL"
        assert hotel.address == "12111 MURPHY RD"
        assert hotel.city == "STAFFORD"
        assert hotel.state == "TX"
        assert hotel.zip_code == "77477"
        assert hotel.room_count == 30
        assert hotel.reporting_quarter == "2025Q3"
        assert hotel.total_receipts == 42384.30

    @pytest.mark.no_db
    def test_parse_multiple_rows(self):
        """Parse multiple rows correctly."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("\n".join(SAMPLE_ROWS))

            hotels = ingester.parse_csv(csv_path)

        assert len(hotels) == 4

        names = {h.name for h in hotels}
        assert "LAKEVIEW MARINA" in names
        assert "GUEST MOTEL" in names
        assert "INTOWN SUITES WEBSTER" in names
        assert "SRUTI HOSPITALITY INC." in names

    @pytest.mark.no_db
    def test_phone_formatting(self):
        """Phone numbers are formatted correctly."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(SAMPLE_ROWS[3])  # SRUTI has location phone

            hotels = ingester.parse_csv(csv_path)

        assert hotels[0].phone == "713-869-9211"

    @pytest.mark.no_db
    def test_fallback_to_taxpayer_phone(self):
        """Falls back to taxpayer phone when location phone is empty."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(SAMPLE_ROWS[2])  # INTOWN SUITES - no location phone

            hotels = ingester.parse_csv(csv_path)

        assert hotels[0].phone == "770-799-5000"

    @pytest.mark.no_db
    def test_out_of_state_taxpayer(self):
        """Correctly handles out-of-state taxpayer with TX location."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(SAMPLE_ROWS[2])  # INTOWN SUITES - GA taxpayer

            hotels = ingester.parse_csv(csv_path)

        hotel = hotels[0]
        assert hotel.state == "TX"
        assert hotel.city == "WEBSTER"

    @pytest.mark.no_db
    def test_state_filter(self):
        """State filter excludes non-TX locations."""
        ingester = TexasIngestor()

        non_tx_row = SAMPLE_ROWS[2].replace(',"TX","77598",', ',"OK","73301",')

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(non_tx_row)

            hotels = ingester.parse_csv(csv_path, state_filter="TX")

        assert len(hotels) == 0


class TestDeduplication:
    """Unit tests for deduplication logic."""

    @pytest.mark.no_db
    def test_keeps_all_unique_locations(self):
        """Same taxpayer with different locations are all kept."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("\n".join(MULTI_LOCATION_ROWS))

            hotels = ingester.parse_csv(csv_path)
            unique = ingester.deduplicate_hotels(hotels)

        assert len(unique) == 4
        location_numbers = {h.location_number for h in unique}
        assert location_numbers == {"00002", "00003", "00004", "00008"}

    @pytest.mark.no_db
    def test_keeps_most_recent_quarter(self):
        """When same hotel appears in multiple quarters, keep most recent."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("\n".join(DUPLICATE_QUARTER_ROWS))

            hotels = ingester.parse_csv(csv_path)
            unique = ingester.deduplicate_hotels(hotels)

        assert len(unique) == 1
        assert unique[0].reporting_quarter == "2025Q3"
        assert unique[0].total_receipts == 42384.30

    @pytest.mark.no_db
    def test_dedup_by_tax_id_not_name(self):
        """Dedup uses tax ID - same name different tax IDs are kept."""
        ingester = TexasIngestor()

        row1 = '10000000001,"OWNER A","ADDR","CITY","TX","77777",101,"5551234567",00001,"HOLIDAY INN","123 MAIN ST","HOUSTON","TX","77001",101,"",100,20200101,,2025Q3,50,10000.00,10000.00,'
        row2 = '10000000002,"OWNER B","ADDR","CITY","TX","77777",101,"5559876543",00001,"HOLIDAY INN","456 OTHER ST","HOUSTON","TX","77002",101,"",80,20200101,,2025Q3,50,8000.00,8000.00,'

        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text(f"{row1}\n{row2}")

            hotels = ingester.parse_csv(csv_path)
            unique = ingester.deduplicate_hotels(hotels)

        assert len(unique) == 2


class TestLoadQuarters:
    """Tests for loading quarterly data files."""

    @pytest.mark.no_db
    def test_load_quarterly_data(self):
        """Load data from a quarter directory."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            quarter_dir = Path(tmpdir) / "HOT 25 Q3"
            quarter_dir.mkdir(parents=True)
            (quarter_dir / "hotels.CSV").write_text("\n".join(SAMPLE_ROWS))

            import services.ingestor.texas as texas_module
            original_cache = texas_module.CACHE_DIR
            texas_module.CACHE_DIR = Path(tmpdir)

            try:
                hotels, stats = ingester.load_quarterly_data("HOT 25 Q3")
            finally:
                texas_module.CACHE_DIR = original_cache

        assert len(hotels) == 4
        assert stats.files_processed == 1
        assert stats.records_parsed == 4

    @pytest.mark.no_db
    def test_load_all_quarters_merges(self):
        """Load and merge data from multiple quarters."""
        ingester = TexasIngestor()

        with TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            q2_dir = cache_dir / "HOT 25 Q2"
            q2_dir.mkdir(parents=True)
            (q2_dir / "hotels.CSV").write_text(DUPLICATE_QUARTER_ROWS[0])

            q3_dir = cache_dir / "HOT 25 Q3"
            q3_dir.mkdir(parents=True)
            (q3_dir / "hotels.CSV").write_text(DUPLICATE_QUARTER_ROWS[1])

            import services.ingestor.texas as texas_module
            original_cache = texas_module.CACHE_DIR
            texas_module.CACHE_DIR = cache_dir

            try:
                hotels, stats = ingester.load_all_quarters()
            finally:
                texas_module.CACHE_DIR = original_cache

        assert len(hotels) == 1
        assert hotels[0].reporting_quarter == "2025Q3"
        assert stats.files_processed == 2


class TestModel:
    """Tests for the TexasHotel Pydantic model."""

    @pytest.mark.no_db
    def test_model_validation(self):
        """Model validates and stores data correctly."""
        hotel = TexasHotel(
            taxpayer_number="10105865405",
            location_number="00002",
            name="GUEST MOTEL",
            address="12111 MURPHY RD",
            city="STAFFORD",
            state="TX",
            zip_code="77477",
            room_count=30,
            phone="281-879-6900",
        )

        assert hotel.taxpayer_number == "10105865405"
        assert hotel.room_count == 30

    @pytest.mark.no_db
    def test_optional_fields_default_none(self):
        """Optional fields default to None."""
        hotel = TexasHotel(
            taxpayer_number="123",
            location_number="001",
            name="Test Hotel",
            address="123 Main St",
            city="Houston",
            state="TX",
            zip_code="77001",
        )

        assert hotel.phone is None
        assert hotel.room_count is None
        assert hotel.county is None
