"""Unit tests for reporting repository."""

import pytest
from decimal import Decimal
from services.reporting.repo import (
    get_leads_for_city,
    get_leads_for_state,
    get_city_stats,
    get_state_stats,
    get_top_engines_for_city,
    get_top_engines_for_state,
    get_cities_in_state,
)
from services.leadgen.repo import insert_hotel, delete_hotel


@pytest.mark.asyncio
async def test_get_leads_for_city_empty():
    """Test getting leads for a city with no data returns empty list."""
    leads = await get_leads_for_city(city="NonExistentCity", state="NonExistentState")
    assert leads == []


@pytest.mark.asyncio
async def test_get_leads_for_city_with_data():
    """Test getting leads for a city with detected hotels."""
    # Insert a test hotel with status=1 (detected)
    hotel_id = await insert_hotel(
        name="Test Reporting Hotel",
        website="https://testreporting.com",
        city="TestCity",
        state="TestState",
        status=1,  # detected
        source="test",
    )

    try:
        leads = await get_leads_for_city(city="TestCity", state="TestState")
        assert len(leads) >= 1
        assert any(lead.hotel_name == "Test Reporting Hotel" for lead in leads)
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_leads_for_state_empty():
    """Test getting leads for a state with no data returns empty list."""
    leads = await get_leads_for_state(state="NonExistentState")
    assert leads == []


@pytest.mark.asyncio
async def test_get_city_stats_empty():
    """Test getting stats for a city with no data."""
    stats = await get_city_stats(city="NonExistentCity", state="NonExistentState")
    assert stats.total_scraped == 0
    assert stats.with_website == 0
    assert stats.booking_found == 0


@pytest.mark.asyncio
async def test_get_city_stats_with_data():
    """Test getting stats for a city with hotels."""
    # Insert test hotels
    hotel_id_1 = await insert_hotel(
        name="Stats Test Hotel 1",
        website="https://statstest1.com",
        city="StatsCity",
        state="StatsState",
        phone_google="+1-555-0001",
        email="test1@hotel.com",
        status=0,
        source="test",
    )
    hotel_id_2 = await insert_hotel(
        name="Stats Test Hotel 2",
        website="https://statstest2.com",
        city="StatsCity",
        state="StatsState",
        phone_google="+1-555-0002",
        status=1,
        source="test",
    )

    try:
        stats = await get_city_stats(city="StatsCity", state="StatsState")
        assert stats.total_scraped >= 2
        assert stats.with_website >= 2
        assert stats.with_phone >= 2
    finally:
        await delete_hotel(hotel_id_1)
        await delete_hotel(hotel_id_2)


@pytest.mark.asyncio
async def test_get_state_stats_empty():
    """Test getting stats for a state with no data."""
    stats = await get_state_stats(state="NonExistentState")
    assert stats.total_scraped == 0


@pytest.mark.asyncio
async def test_get_top_engines_for_city_empty():
    """Test getting top engines for a city with no data."""
    engines = await get_top_engines_for_city(city="NonExistentCity", state="NonExistentState")
    assert engines == []


@pytest.mark.asyncio
async def test_get_top_engines_for_state_empty():
    """Test getting top engines for a state with no data."""
    engines = await get_top_engines_for_state(state="NonExistentState")
    assert engines == []


@pytest.mark.asyncio
async def test_get_cities_in_state_empty():
    """Test getting cities in a state with no data."""
    cities = await get_cities_in_state(state="NonExistentState")
    assert cities == []


@pytest.mark.asyncio
async def test_get_cities_in_state_with_data():
    """Test getting cities in a state with detected hotels."""
    # Insert test hotels in different cities with status=1
    hotel_id_1 = await insert_hotel(
        name="City Test Hotel A",
        website="https://citytest1.com",
        city="CityA",
        state="CitiesTestState",
        status=1,
        source="test",
    )
    hotel_id_2 = await insert_hotel(
        name="City Test Hotel B",
        website="https://citytest2.com",
        city="CityB",
        state="CitiesTestState",
        status=1,
        source="test",
    )

    try:
        cities = await get_cities_in_state(state="CitiesTestState")
        assert "CityA" in cities
        assert "CityB" in cities
    finally:
        await delete_hotel(hotel_id_1)
        await delete_hotel(hotel_id_2)
