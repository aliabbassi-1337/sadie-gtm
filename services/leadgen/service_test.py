"""Tests for leadgen service layer.

Unit tests run offline with mocked dependencies.
Integration tests marked with @pytest.mark.online hit external APIs.

Run unit tests:
    uv run pytest services/leadgen/service_test.py -v -m "not online"

Run all tests:
    uv run pytest services/leadgen/service_test.py -v
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from services.leadgen.service import Service, CityLocation
from services.leadgen import repo


# =============================================================================
# TARGET CITIES UNIT TESTS
# =============================================================================

class TestGetTargetCities:
    """Tests for get_target_cities method."""

    @pytest.mark.asyncio
    async def test_get_target_cities_returns_city_locations(self):
        """Test that get_target_cities returns CityLocation objects."""
        # Insert test data
        await repo.insert_target_city(
            name="Service Test City",
            state="ST",
            lat=30.0,
            lng=-90.0,
            radius_km=15.0,
            display_name="Service Test City, ST, USA",
            source="test",
        )

        service = Service()
        cities = await service.get_target_cities("ST")

        assert len(cities) >= 1
        city = next(c for c in cities if c.name == "Service Test City")
        assert isinstance(city, CityLocation)
        assert city.lat == 30.0
        assert city.lng == -90.0
        assert city.radius_km == 15.0

        # Cleanup
        await repo.delete_target_city("Service Test City", "ST")

    @pytest.mark.asyncio
    async def test_get_target_cities_empty_state(self):
        """Test that empty state returns empty list."""
        service = Service()
        cities = await service.get_target_cities("NONEXISTENT")
        assert cities == []

    @pytest.mark.asyncio
    async def test_get_target_cities_respects_limit(self):
        """Test that limit parameter works."""
        # Insert multiple cities
        for i in range(5):
            await repo.insert_target_city(
                name=f"Limit Test {i}",
                state="LT",
                lat=float(i),
                lng=float(i),
                source="test",
            )

        service = Service()
        cities = await service.get_target_cities("LT", limit=3)
        assert len(cities) == 3

        # Cleanup
        for i in range(5):
            await repo.delete_target_city(f"Limit Test {i}", "LT")


class TestAddTargetCity:
    """Tests for add_target_city method."""

    @pytest.mark.asyncio
    async def test_add_target_city_with_coordinates(self):
        """Test adding city with explicit coordinates (no API call)."""
        service = Service()
        city = await service.add_target_city(
            name="Manual City",
            state="MC",
            lat=35.0,
            lng=-85.0,
            radius_km=20.0,
        )

        assert city.name == "Manual City"
        assert city.state == "MC"
        assert city.lat == 35.0
        assert city.lng == -85.0
        assert city.radius_km == 20.0

        # Verify in DB
        db_city = await repo.get_target_city("Manual City", "MC")
        assert db_city is not None
        assert db_city["lat"] == 35.0

        # Cleanup
        await repo.delete_target_city("Manual City", "MC")

    @pytest.mark.asyncio
    async def test_add_target_city_returns_existing(self):
        """Test that adding existing city returns it without API call."""
        # First add
        await repo.insert_target_city(
            name="Existing City",
            state="EC",
            lat=40.0,
            lng=-75.0,
            radius_km=12.0,
            source="test",
        )

        service = Service()

        # Mock geocode_city to verify it's not called
        with patch("services.leadgen.service.geocode_city") as mock_geocode:
            city = await service.add_target_city("Existing City", "EC")

            # Should not call API
            mock_geocode.assert_not_called()

            # Should return existing city
            assert city.name == "Existing City"
            assert city.lat == 40.0

        # Cleanup
        await repo.delete_target_city("Existing City", "EC")

    @pytest.mark.asyncio
    async def test_add_target_city_geocodes_when_no_coords(self):
        """Test that city is geocoded when coordinates not provided."""
        service = Service()

        # Mock the geocode function
        mock_city = CityLocation(
            name="Geocoded City",
            state="GC",
            lat=33.0,
            lng=-84.0,
            radius_km=15.0,
            display_name="Geocoded City, GC, USA",
        )

        with patch("services.leadgen.service.geocode_city", new_callable=AsyncMock) as mock_geocode:
            mock_geocode.return_value = mock_city

            city = await service.add_target_city("Geocoded City", "GC")

            # Should call geocode
            mock_geocode.assert_called_once_with("Geocoded City", "GC")

            # Should return geocoded result
            assert city.lat == 33.0
            assert city.lng == -84.0

        # Cleanup
        await repo.delete_target_city("Geocoded City", "GC")


class TestRemoveTargetCity:
    """Tests for remove_target_city method."""

    @pytest.mark.asyncio
    async def test_remove_target_city(self):
        """Test removing a target city."""
        # Add city
        await repo.insert_target_city(
            name="Remove Me",
            state="RM",
            lat=30.0,
            lng=-90.0,
            source="test",
        )

        service = Service()
        await service.remove_target_city("Remove Me", "RM")

        # Verify removed
        city = await repo.get_target_city("Remove Me", "RM")
        assert city is None

    @pytest.mark.asyncio
    async def test_remove_nonexistent_city_no_error(self):
        """Test that removing nonexistent city doesn't raise error."""
        service = Service()
        # Should not raise
        await service.remove_target_city("Nonexistent", "XX")


class TestCountTargetCities:
    """Tests for count_target_cities method."""

    @pytest.mark.asyncio
    async def test_count_target_cities(self):
        """Test counting target cities."""
        # Add cities
        for i in range(3):
            await repo.insert_target_city(
                name=f"Count City {i}",
                state="CC",
                lat=float(i),
                lng=float(i),
                source="test",
            )

        service = Service()
        count = await service.count_target_cities("CC")
        assert count == 3

        # Cleanup
        for i in range(3):
            await repo.delete_target_city(f"Count City {i}", "CC")

    @pytest.mark.asyncio
    async def test_count_target_cities_empty(self):
        """Test counting for empty state."""
        service = Service()
        count = await service.count_target_cities("EMPTY")
        assert count == 0


# =============================================================================
# INTEGRATION TESTS - Hit real Nominatim API
# =============================================================================

@pytest.mark.online
class TestAddTargetCityOnline:
    """Integration tests that hit real Nominatim API."""

    @pytest.mark.asyncio
    async def test_add_target_city_geocodes_real_city(self):
        """Test adding a real city with geocoding."""
        service = Service()

        # Use a unique state to avoid conflicts
        city = await service.add_target_city("Austin", "TX")

        assert city.name == "Austin"
        assert city.state == "TX"
        assert 30.0 < city.lat < 31.0  # Austin's approximate latitude
        assert -98.0 < city.lng < -97.0  # Austin's approximate longitude
        assert city.display_name is not None
        assert "Austin" in city.display_name

        # Cleanup
        await service.remove_target_city("Austin", "TX")

    @pytest.mark.asyncio
    async def test_add_target_city_sets_radius_for_major_city(self):
        """Test that major cities get larger radius."""
        service = Service()

        city = await service.add_target_city("Houston", "TX")

        # Houston is a major metro, should get 25km radius
        assert city.radius_km == 25.0

        # Cleanup
        await service.remove_target_city("Houston", "TX")

    @pytest.mark.asyncio
    async def test_add_target_city_nonexistent_raises(self):
        """Test that nonexistent city raises error."""
        service = Service()

        with pytest.raises(ValueError, match="City not found"):
            await service.add_target_city("ThisCityDoesNotExist99999", "XX")
