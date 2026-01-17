"""Tests for geocoding helper.

Unit tests run offline with mocked responses.
Integration tests marked with @pytest.mark.online hit the real Nominatim API.

Run unit tests:
    uv run pytest services/leadgen/geocoding_test.py -v -m "not online"

Run all tests (including online):
    uv run pytest services/leadgen/geocoding_test.py -v
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from services.leadgen.geocoding import (
    CityLocation,
    geocode_city,
    _suggest_radius_from_importance,
)


# =============================================================================
# UNIT TESTS
# =============================================================================

@pytest.mark.no_db
class TestCityLocation:
    """Unit tests for CityLocation model."""

    def test_city_location_basic(self):
        city = CityLocation(
            name="Miami",
            state="FL",
            lat=25.7617,
            lng=-80.1918,
        )
        assert city.name == "Miami"
        assert city.state == "FL"
        assert city.lat == 25.7617
        assert city.lng == -80.1918
        assert city.radius_km == 12.0  # Default

    def test_city_location_with_radius(self):
        city = CityLocation(
            name="Orlando",
            state="FL",
            lat=28.5383,
            lng=-81.3792,
            radius_km=25.0,
        )
        assert city.radius_km == 25.0

    def test_city_location_optional_fields(self):
        city = CityLocation(
            name="Tampa",
            state="FL",
            lat=27.9506,
            lng=-82.4572,
            display_name="Tampa, Hillsborough County, Florida, United States",
        )
        assert "Tampa" in city.display_name


@pytest.mark.no_db
class TestSuggestRadiusFromImportance:
    """Unit tests for _suggest_radius_from_importance helper."""

    def test_major_metro_importance(self):
        # Major metros have importance >= 0.7
        assert _suggest_radius_from_importance(0.8) == 25.0
        assert _suggest_radius_from_importance(0.75) == 25.0
        assert _suggest_radius_from_importance(0.7) == 25.0

    def test_large_city_importance(self):
        # Large cities have importance 0.5-0.7
        assert _suggest_radius_from_importance(0.6) == 20.0
        assert _suggest_radius_from_importance(0.55) == 20.0
        assert _suggest_radius_from_importance(0.5) == 20.0

    def test_medium_city_importance(self):
        # Medium cities have importance 0.3-0.5
        assert _suggest_radius_from_importance(0.4) == 15.0
        assert _suggest_radius_from_importance(0.35) == 15.0
        assert _suggest_radius_from_importance(0.3) == 15.0

    def test_small_city_importance(self):
        # Small cities have importance < 0.3
        assert _suggest_radius_from_importance(0.2) == 12.0
        assert _suggest_radius_from_importance(0.1) == 12.0
        assert _suggest_radius_from_importance(0.0) == 12.0


@pytest.mark.no_db
class TestGeocodeCityMocked:
    """Unit tests for geocode_city with mocked API."""

    @pytest.mark.asyncio
    async def test_geocode_city_success(self):
        """Test successful geocoding with mocked response."""
        mock_response = MagicMock()
        mock_response.json.return_value = [{
            "lat": "25.7617",
            "lon": "-80.1918",
            "display_name": "Miami, Miami-Dade County, Florida, United States",
            "importance": 0.75,  # Major metro
        }]
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            city = await geocode_city("Miami", "FL")

            assert city.name == "Miami"
            assert city.state == "FL"
            assert city.lat == 25.7617
            assert city.lng == -80.1918
            assert city.display_name == "Miami, Miami-Dade County, Florida, United States"
            assert city.radius_km == 25.0  # High importance = major metro

    @pytest.mark.asyncio
    async def test_geocode_city_not_found(self):
        """Test geocoding when city is not found."""
        mock_response = MagicMock()
        mock_response.json.return_value = []  # Empty result
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            with pytest.raises(ValueError, match="City not found"):
                await geocode_city("NonexistentCity", "XX")

    @pytest.mark.asyncio
    async def test_geocode_city_small_city_radius(self):
        """Test that small cities get default 12km radius."""
        mock_response = MagicMock()
        mock_response.json.return_value = [{
            "lat": "26.3587",
            "lon": "-80.0831",
            "display_name": "Boca Raton, Palm Beach County, Florida, United States",
            "importance": 0.2,  # Small city
        }]
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            city = await geocode_city("Boca Raton", "FL")
            assert city.radius_km == 12.0  # Low importance = small city


# =============================================================================
# INTEGRATION TESTS - Hit real Nominatim API
# =============================================================================

@pytest.mark.online
@pytest.mark.no_db
class TestGeocodeCityOnline:
    """Integration tests that hit the real Nominatim API."""

    @pytest.mark.asyncio
    async def test_geocode_miami(self):
        """Test geocoding Miami against real API."""
        city = await geocode_city("Miami", "FL")

        assert city.name == "Miami"
        assert city.state == "FL"
        assert 25.5 < city.lat < 26.0  # Roughly Miami's latitude
        assert -80.5 < city.lng < -80.0  # Roughly Miami's longitude
        assert "Miami" in city.display_name
        assert "Florida" in city.display_name

    @pytest.mark.asyncio
    async def test_geocode_orlando(self):
        """Test geocoding Orlando against real API."""
        city = await geocode_city("Orlando", "FL")

        assert city.name == "Orlando"
        assert 28.0 < city.lat < 29.0
        assert -82.0 < city.lng < -81.0
        assert "Orlando" in city.display_name

    @pytest.mark.asyncio
    async def test_geocode_miami_vs_miami_beach(self):
        """Verify Miami and Miami Beach return different coordinates."""
        miami = await geocode_city("Miami", "FL")
        miami_beach = await geocode_city("Miami Beach", "FL")

        # They should be different locations
        assert miami.lat != miami_beach.lat
        assert miami.lng != miami_beach.lng

        # Miami Beach is east of Miami (less negative longitude)
        assert miami_beach.lng > miami.lng

    @pytest.mark.asyncio
    async def test_geocode_nonexistent_city(self):
        """Test that nonexistent city raises ValueError."""
        with pytest.raises(ValueError, match="City not found"):
            await geocode_city("ThisCityDoesNotExist12345", "XX")
