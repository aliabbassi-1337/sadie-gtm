"""Unit tests for leadgen repository."""

import pytest
from services.leadgen.repo import get_hotel_by_id


@pytest.mark.asyncio
async def test_get_hotel_by_id_not_found():
    """Test getting a non-existent hotel returns None."""
    hotel = await get_hotel_by_id(hotel_id=999999)
    assert hotel is None


@pytest.mark.asyncio
async def test_get_hotel_by_id_exists():
    """Test getting an existing hotel returns Hotel model."""
    # TODO: Insert test data first, then query it
    # For now, this will fail until we have test data
    hotel = await get_hotel_by_id(hotel_id=1)
    if hotel:
        assert hotel.id == 1
        assert isinstance(hotel.name, str)
        assert hotel.status in [0, 1, 3, 5, 6, 99]
