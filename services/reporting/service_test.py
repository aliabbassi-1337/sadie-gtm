"""Unit tests for reporting service."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from decimal import Decimal

from services.reporting.service import Service
from db.models.reporting import HotelLead, CityStats, EngineCount, ReportStats


@pytest.mark.no_db
def test_format_proximity_with_data():
    """Test proximity formatting with valid data."""
    service = Service()
    lead = HotelLead(
        id=1,
        hotel_name="Test Hotel",
        nearest_customer_name="Sherry Frontenac Hotel",
        nearest_customer_distance_km=Decimal("3.3"),
    )
    result = service._format_proximity(lead)
    assert result == "Nearest: Sherry Frontenac Hotel (3.3km)"


@pytest.mark.no_db
def test_format_proximity_no_customer():
    """Test proximity formatting when no nearest customer."""
    service = Service()
    lead = HotelLead(
        id=1,
        hotel_name="Test Hotel",
        nearest_customer_name=None,
        nearest_customer_distance_km=None,
    )
    result = service._format_proximity(lead)
    assert result == ""


@pytest.mark.no_db
def test_format_proximity_missing_distance():
    """Test proximity formatting when distance is missing."""
    service = Service()
    lead = HotelLead(
        id=1,
        hotel_name="Test Hotel",
        nearest_customer_name="Some Hotel",
        nearest_customer_distance_km=None,
    )
    result = service._format_proximity(lead)
    assert result == ""


@pytest.mark.no_db
def test_create_workbook_empty_leads():
    """Test creating workbook with no leads."""
    service = Service()
    leads = []
    stats = ReportStats(
        location_name="TestCity",
        stats=CityStats(
            total_scraped=100,
            with_website=80,
            booking_found=50,
            with_phone=90,
            with_email=40,
            tier_1_count=45,
            tier_2_count=5,
        ),
        top_engines=[
            EngineCount(engine_name="SynXis", hotel_count=20),
            EngineCount(engine_name="Cloudbeds", hotel_count=15),
        ],
    )

    workbook = service._create_workbook(leads, stats)

    # Check workbook has 2 sheets
    assert len(workbook.sheetnames) == 2
    assert "Leads" in workbook.sheetnames
    assert "Stats" in workbook.sheetnames

    # Check leads sheet has headers
    leads_sheet = workbook["Leads"]
    assert leads_sheet.cell(row=1, column=1).value == "Hotel"
    assert leads_sheet.cell(row=1, column=2).value == "Booking Engine"
    assert leads_sheet.cell(row=1, column=3).value == "Room Count"
    assert leads_sheet.cell(row=1, column=4).value == "Proximity"


@pytest.mark.no_db
def test_create_workbook_with_leads():
    """Test creating workbook with leads data."""
    service = Service()
    leads = [
        HotelLead(
            id=1,
            hotel_name="Hotel A",
            booking_engine_name="SynXis",
            room_count=50,
            nearest_customer_name="Customer Hotel",
            nearest_customer_distance_km=Decimal("2.5"),
        ),
        HotelLead(
            id=2,
            hotel_name="Hotel B",
            booking_engine_name="Cloudbeds",
            room_count=None,
            nearest_customer_name=None,
            nearest_customer_distance_km=None,
        ),
    ]
    stats = ReportStats(
        location_name="TestCity",
        stats=CityStats(),
        top_engines=[],
    )

    workbook = service._create_workbook(leads, stats)

    leads_sheet = workbook["Leads"]
    # Row 1 is headers, row 2 is first lead
    assert leads_sheet.cell(row=2, column=1).value == "Hotel A"
    assert leads_sheet.cell(row=2, column=2).value == "SynXis"
    assert leads_sheet.cell(row=2, column=3).value == 50
    assert leads_sheet.cell(row=2, column=4).value == "Nearest: Customer Hotel (2.5km)"

    assert leads_sheet.cell(row=3, column=1).value == "Hotel B"
    assert leads_sheet.cell(row=3, column=2).value == "Cloudbeds"
    assert leads_sheet.cell(row=3, column=3).value == ""
    assert leads_sheet.cell(row=3, column=4).value == ""


@pytest.mark.no_db
def test_stats_sheet_content():
    """Test stats sheet has correct sections."""
    service = Service()
    leads = []
    stats = ReportStats(
        location_name="Miami Beach",
        stats=CityStats(
            total_scraped=330,
            with_website=182,
            booking_found=149,
            with_phone=165,
            with_email=84,
            tier_1_count=141,
            tier_2_count=8,
        ),
        top_engines=[
            EngineCount(engine_name="SynXis / TravelClick", hotel_count=70),
            EngineCount(engine_name="Triptease", hotel_count=23),
        ],
    )

    workbook = service._create_workbook(leads, stats)
    stats_sheet = workbook["Stats"]

    # Check title
    assert "MIAMI BEACH" in stats_sheet.cell(row=1, column=1).value

    # Check FUNNEL section
    assert stats_sheet.cell(row=3, column=1).value == "FUNNEL"
    assert stats_sheet.cell(row=4, column=1).value == "Hotels Scraped"
    assert stats_sheet.cell(row=4, column=2).value == 330

    # Check LEAD QUALITY section
    assert "LEAD QUALITY" in stats_sheet.cell(row=3, column=4).value
    assert stats_sheet.cell(row=4, column=4).value == "Tier 1 (Known Engine)"

    # Check CONTACT INFO section
    assert stats_sheet.cell(row=8, column=1).value == "CONTACT INFO"

    # Check TOP ENGINES section
    assert stats_sheet.cell(row=8, column=4).value == "TOP ENGINES"
    assert stats_sheet.cell(row=9, column=4).value == "SynXis / TravelClick"
    assert stats_sheet.cell(row=9, column=5).value == 70


@pytest.mark.no_db
@pytest.mark.asyncio
@patch("services.reporting.service.upload_file")
@patch("services.reporting.service.repo")
async def test_export_city_uploads_to_s3(mock_repo, mock_upload):
    """Test that export_city uploads to correct S3 path."""
    # Setup mocks - use AsyncMock for async functions
    mock_repo.get_leads_for_city = AsyncMock(return_value=[])
    mock_repo.get_city_stats = AsyncMock(return_value=CityStats())
    mock_repo.get_top_engines_for_city = AsyncMock(return_value=[])
    mock_upload.return_value = "s3://sadie-gtm/USA/Florida/Miami.xlsx"

    service = Service()
    result = await service.export_city("Miami", "Florida", "USA")

    # Verify S3 upload was called with correct key
    mock_upload.assert_called_once()
    call_args = mock_upload.call_args
    assert call_args[0][1] == "USA/Florida/Miami.xlsx"
    assert result == "s3://sadie-gtm/USA/Florida/Miami.xlsx"


@pytest.mark.no_db
@pytest.mark.asyncio
@patch("services.reporting.service.upload_file")
@patch("services.reporting.service.repo")
async def test_export_state_uploads_to_s3(mock_repo, mock_upload):
    """Test that export_state uploads to correct S3 path."""
    mock_repo.get_leads_for_state = AsyncMock(return_value=[])
    mock_repo.get_state_stats = AsyncMock(return_value=CityStats())
    mock_repo.get_top_engines_for_state = AsyncMock(return_value=[])
    mock_upload.return_value = "s3://sadie-gtm/USA/Florida/Florida.xlsx"

    service = Service()
    result = await service.export_state("Florida", "USA")

    mock_upload.assert_called_once()
    call_args = mock_upload.call_args
    assert call_args[0][1] == "USA/Florida/Florida.xlsx"
    assert result == "s3://sadie-gtm/USA/Florida/Florida.xlsx"


@pytest.mark.no_db
@pytest.mark.asyncio
@patch("services.reporting.service.upload_file")
@patch("services.reporting.service.repo")
async def test_export_state_with_cities(mock_repo, mock_upload):
    """Test exporting all cities in a state."""
    mock_repo.get_cities_in_state = AsyncMock(return_value=["Miami", "Orlando"])
    mock_repo.get_leads_for_city = AsyncMock(return_value=[])
    mock_repo.get_leads_for_state = AsyncMock(return_value=[])
    mock_repo.get_city_stats = AsyncMock(return_value=CityStats())
    mock_repo.get_state_stats = AsyncMock(return_value=CityStats())
    mock_repo.get_top_engines_for_city = AsyncMock(return_value=[])
    mock_repo.get_top_engines_for_state = AsyncMock(return_value=[])

    mock_upload.side_effect = [
        "s3://sadie-gtm/USA/Florida/Miami.xlsx",
        "s3://sadie-gtm/USA/Florida/Orlando.xlsx",
        "s3://sadie-gtm/USA/Florida/Florida.xlsx",
    ]

    service = Service()
    results = await service.export_state_with_cities("Florida", "USA")

    assert len(results) == 3
    assert mock_upload.call_count == 3
