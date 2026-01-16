"""Unit tests for leadgen repository."""

import pytest
from services.leadgen.repo import (
    get_hotel_by_id,
    insert_hotel,
    delete_hotel,
    insert_hotels_bulk,
    # Target cities
    get_target_cities_by_state,
    get_target_city,
    insert_target_city,
    delete_target_city,
    count_target_cities_by_state,
)


@pytest.mark.asyncio
async def test_get_hotel_by_id_not_found():
    """Test getting a non-existent hotel returns None."""
    hotel = await get_hotel_by_id(hotel_id=999999)
    assert hotel is None


@pytest.mark.asyncio
async def test_get_hotel_by_id_exists():
    """Test getting an existing hotel returns Hotel model."""
    # Insert test hotel (will update if already exists)
    hotel_id = await insert_hotel(
        name="Test Hotel Miami",
        website="https://testhotel.com",
        phone_google="+1-305-555-0100",
        email="test@hotel.com",
        latitude=25.7617,
        longitude=-80.1918,
        address="123 Test St",
        city="Miami",
        state="Florida",
        rating=4.5,
        review_count=100,
        status=0,
        source="test",
    )

    # Query the inserted hotel
    hotel = await get_hotel_by_id(hotel_id=hotel_id)
    assert hotel is not None
    assert hotel.id == hotel_id
    assert hotel.name == "Test Hotel Miami"
    assert hotel.city == "Miami"
    assert hotel.state == "Florida"
    assert hotel.status == 0

    # Cleanup
    await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_insert_hotels_bulk():
    """Test bulk inserting multiple hotels."""
    hotels = [
        {
            "name": "Bulk Test Hotel 1",
            "website": "https://bulktest1.com",
            "city": "Miami",
            "state": "FL",
            "latitude": 25.76,
            "longitude": -80.19,
            "source": "test_bulk",
        },
        {
            "name": "Bulk Test Hotel 2",
            "website": "https://bulktest2.com",
            "city": "Miami Beach",
            "state": "FL",
            "latitude": 25.79,
            "longitude": -80.13,
            "source": "test_bulk",
        },
    ]

    count = await insert_hotels_bulk(hotels)
    assert count == 2

    # Verify by fetching individually
    h1 = await get_hotel_by_id(hotel_id=1)  # May not exist, just checking no crash

    # Cleanup - need to find the IDs
    # Since we can't query by source easily, insert and get IDs
    id1 = await insert_hotel(name="Bulk Test Hotel 1", website="https://bulktest1.com")
    id2 = await insert_hotel(name="Bulk Test Hotel 2", website="https://bulktest2.com")
    await delete_hotel(id1)
    await delete_hotel(id2)


@pytest.mark.asyncio
async def test_insert_hotels_bulk_empty():
    """Test bulk insert with empty list."""
    count = await insert_hotels_bulk([])
    assert count == 0


# =============================================================================
# TARGET CITIES TESTS
# =============================================================================

@pytest.mark.asyncio
async def test_insert_target_city():
    """Test inserting a target city."""
    city_id = await insert_target_city(
        name="Test City",
        state="TX",
        lat=30.2672,
        lng=-97.7431,
        radius_km=15.0,
        display_name="Test City, Texas, USA",
        source="test",
    )
    assert city_id > 0

    # Verify it was inserted
    city = await get_target_city("Test City", "TX")
    assert city is not None
    assert city["name"] == "Test City"
    assert city["state"] == "TX"
    assert city["lat"] == 30.2672
    assert city["lng"] == -97.7431
    assert city["radius_km"] == 15.0

    # Cleanup
    await delete_target_city("Test City", "TX")


@pytest.mark.asyncio
async def test_get_target_city_not_found():
    """Test getting a non-existent target city returns None."""
    city = await get_target_city("NonexistentCity", "XX")
    assert city is None


@pytest.mark.asyncio
async def test_insert_target_city_upsert():
    """Test that inserting same city twice updates it."""
    # First insert
    await insert_target_city(
        name="Upsert City",
        state="TX",
        lat=30.0,
        lng=-97.0,
        radius_km=10.0,
        source="test",
    )

    # Second insert with different radius
    await insert_target_city(
        name="Upsert City",
        state="TX",
        lat=30.0,
        lng=-97.0,
        radius_km=20.0,  # Different radius
        source="test",
    )

    # Should have updated radius
    city = await get_target_city("Upsert City", "TX")
    assert city["radius_km"] == 20.0

    # Cleanup
    await delete_target_city("Upsert City", "TX")


@pytest.mark.asyncio
async def test_get_target_cities_by_state():
    """Test getting all target cities for a state."""
    # Insert test cities
    await insert_target_city(name="City A", state="ZZ", lat=1.0, lng=1.0, source="test")
    await insert_target_city(name="City B", state="ZZ", lat=2.0, lng=2.0, source="test")
    await insert_target_city(name="City C", state="ZZ", lat=3.0, lng=3.0, source="test")

    # Query
    cities = await get_target_cities_by_state("ZZ", limit=100)
    assert len(cities) == 3
    names = {c["name"] for c in cities}
    assert names == {"City A", "City B", "City C"}

    # Cleanup
    await delete_target_city("City A", "ZZ")
    await delete_target_city("City B", "ZZ")
    await delete_target_city("City C", "ZZ")


@pytest.mark.asyncio
async def test_get_target_cities_by_state_empty():
    """Test getting cities for a state with no cities."""
    cities = await get_target_cities_by_state("YY", limit=100)
    assert cities == []


@pytest.mark.asyncio
async def test_count_target_cities_by_state():
    """Test counting target cities for a state."""
    # Insert test cities
    await insert_target_city(name="Count A", state="WW", lat=1.0, lng=1.0, source="test")
    await insert_target_city(name="Count B", state="WW", lat=2.0, lng=2.0, source="test")

    count = await count_target_cities_by_state("WW")
    assert count == 2

    # Cleanup
    await delete_target_city("Count A", "WW")
    await delete_target_city("Count B", "WW")


@pytest.mark.asyncio
async def test_delete_target_city():
    """Test deleting a target city."""
    # Insert
    await insert_target_city(name="Delete Me", state="VV", lat=1.0, lng=1.0, source="test")

    # Verify exists
    city = await get_target_city("Delete Me", "VV")
    assert city is not None

    # Delete
    await delete_target_city("Delete Me", "VV")

    # Verify deleted
    city = await get_target_city("Delete Me", "VV")
    assert city is None


@pytest.mark.asyncio
async def test_get_target_city_case_insensitive():
    """Test that city lookup is case-insensitive."""
    await insert_target_city(name="Case Test", state="UU", lat=1.0, lng=1.0, source="test")

    # Query with different cases
    city1 = await get_target_city("case test", "uu")
    city2 = await get_target_city("CASE TEST", "UU")
    city3 = await get_target_city("Case Test", "Uu")

    assert city1 is not None
    assert city2 is not None
    assert city3 is not None

    # Cleanup
    await delete_target_city("Case Test", "UU")
