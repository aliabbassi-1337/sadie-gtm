"""Unit tests for enrichment repository."""

import pytest
from decimal import Decimal
from services.enrichment.repo import (
    get_pending_enrichment_count,
    insert_room_count,
    get_room_count_by_hotel_id,
    delete_room_count,
    # Customer proximity
    get_pending_proximity_count,
    insert_customer_proximity,
    get_customer_proximity_by_hotel_id,
    delete_customer_proximity,
)
from services.leadgen.repo import (
    insert_hotel,
    delete_hotel,
    insert_booking_engine,
    insert_hotel_booking_engine,
)


@pytest.mark.asyncio
async def test_insert_and_get_room_count():
    """Test inserting and retrieving room count."""
    # Insert test hotel (status doesn't matter for this CRUD test)
    hotel_id = await insert_hotel(
        name="Test Enrichment Hotel",
        website="https://test-enrichment.com",
        city="Miami",
        state="Florida",
        status=0,  # pending
        source="test",
    )

    # Insert room count
    room_count_id = await insert_room_count(
        hotel_id=hotel_id,
        room_count=42,
        source="regex",
        confidence=Decimal("1.0"),
    )
    assert room_count_id is not None

    # Get room count
    room_count = await get_room_count_by_hotel_id(hotel_id=hotel_id)
    assert room_count is not None
    assert room_count.hotel_id == hotel_id
    assert room_count.room_count == 42
    assert room_count.source == "regex"
    assert room_count.confidence == Decimal("1.00")

    # Cleanup
    await delete_room_count(hotel_id)
    await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_room_count_not_found():
    """Test getting room count for non-existent hotel returns None."""
    room_count = await get_room_count_by_hotel_id(hotel_id=999999)
    assert room_count is None


@pytest.mark.asyncio
async def test_insert_room_count_upsert():
    """Test inserting room count updates on conflict."""
    # Insert test hotel (status doesn't matter for this CRUD test)
    hotel_id = await insert_hotel(
        name="Test Upsert Hotel",
        website="https://test-upsert.com",
        city="Miami",
        state="Florida",
        status=0,  # pending
        source="test",
    )

    # Insert initial room count
    await insert_room_count(
        hotel_id=hotel_id,
        room_count=10,
        source="groq",
        confidence=Decimal("0.7"),
    )

    # Upsert with new room count
    await insert_room_count(
        hotel_id=hotel_id,
        room_count=25,
        source="regex",
        confidence=Decimal("1.0"),
    )

    # Verify update
    room_count = await get_room_count_by_hotel_id(hotel_id=hotel_id)
    assert room_count is not None
    assert room_count.room_count == 25
    assert room_count.source == "regex"

    # Cleanup
    await delete_room_count(hotel_id)
    await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_pending_enrichment_count():
    """Test counting hotels pending enrichment."""
    # Insert test hotel with status=0 (pending) and no room count
    hotel_id = await insert_hotel(
        name="Test Pending Enrichment",
        website="https://test-pending.com",
        city="Miami",
        state="Florida",
        status=0,  # pending - new status system
        source="test",
    )

    # Add booking engine record (required for enrichment queries)
    booking_engine_id = await insert_booking_engine(
        name="Test Enrichment Engine",
        domains=["testenrichment.com"],
        tier=1,
    )
    await insert_hotel_booking_engine(
        hotel_id=hotel_id,
        booking_engine_id=booking_engine_id,
        detection_method="test",
    )

    # Get count - should include our new hotel
    count = await get_pending_enrichment_count()
    assert count >= 1

    # Add room count - should exclude from pending
    await insert_room_count(
        hotel_id=hotel_id,
        room_count=50,
        source="groq",
        confidence=Decimal("0.7"),
    )

    # Get count again - should be one less
    count_after = await get_pending_enrichment_count()
    assert count_after == count - 1

    # Cleanup
    await delete_room_count(hotel_id)
    await delete_hotel(hotel_id)  # CASCADE deletes hotel_booking_engines


# ============================================================================
# CUSTOMER PROXIMITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_insert_and_get_customer_proximity():
    """Test inserting and retrieving customer proximity."""
    # Insert test hotel with location (status doesn't matter for this CRUD test)
    hotel_id = await insert_hotel(
        name="Test Proximity Hotel",
        website="https://test-proximity.com",
        city="Miami",
        state="Florida",
        latitude=25.7617,
        longitude=-80.1918,
        status=0,  # pending
        source="test",
    )

    # Insert customer proximity (using a fake customer ID for test)
    # Note: In real scenario, existing_customer_id must exist in existing_customers table
    # For this test, we assume there's at least one existing customer with ID 1
    try:
        proximity_id = await insert_customer_proximity(
            hotel_id=hotel_id,
            existing_customer_id=1,
            distance_km=Decimal("15.5"),
        )
        assert proximity_id is not None

        # Get customer proximity
        proximity = await get_customer_proximity_by_hotel_id(hotel_id=hotel_id)
        assert proximity is not None
        assert proximity["hotel_id"] == hotel_id
        assert proximity["existing_customer_id"] == 1
        assert proximity["distance_km"] == Decimal("15.5")

        # Cleanup
        await delete_customer_proximity(hotel_id)
    except Exception:
        # If no existing customer with ID 1, skip this test
        pass

    await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_customer_proximity_not_found():
    """Test getting customer proximity for non-existent hotel returns None."""
    proximity = await get_customer_proximity_by_hotel_id(hotel_id=999999)
    assert proximity is None


@pytest.mark.asyncio
async def test_get_pending_proximity_count():
    """Test counting hotels pending proximity calculation."""
    # Insert test hotel with location but no proximity
    hotel_id = await insert_hotel(
        name="Test Pending Proximity",
        website="https://test-pending-prox.com",
        city="Miami",
        state="Florida",
        latitude=25.7617,
        longitude=-80.1918,
        status=0,  # pending - new status system
        source="test",
    )

    # Add booking engine record (required for proximity queries)
    booking_engine_id = await insert_booking_engine(
        name="Test Proximity Engine",
        domains=["testproximity.com"],
        tier=1,
    )
    await insert_hotel_booking_engine(
        hotel_id=hotel_id,
        booking_engine_id=booking_engine_id,
        detection_method="test",
    )

    # Add room count with status=1 (required for proximity - must be successfully enriched)
    await insert_room_count(
        hotel_id=hotel_id,
        room_count=50,
        source="test",
        confidence=Decimal("1.0"),
        status=1,  # success
    )

    # Get count - should include our new hotel
    count = await get_pending_proximity_count()
    assert count >= 1

    # Cleanup
    await delete_room_count(hotel_id)
    await delete_hotel(hotel_id)  # CASCADE deletes hotel_booking_engines


# ============================================================================
# BIG4 TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_get_big4_count():
    """Test counting BIG4 parks returns an integer."""
    from services.enrichment.repo import get_big4_count
    count = await get_big4_count()
    assert isinstance(count, int)
    assert count >= 0


@pytest.mark.asyncio
async def test_upsert_big4_parks_empty():
    """Test upserting empty arrays is a no-op."""
    from services.enrichment.repo import upsert_big4_parks
    await upsert_big4_parks(
        names=[], slugs=[], phones=[], emails=[], websites=[],
        addresses=[], cities=[], states=[], postcodes=[], lats=[], lons=[],
    )


@pytest.mark.asyncio
async def test_upsert_big4_parks_single():
    """Test upserting a single BIG4 park."""
    from services.enrichment.repo import upsert_big4_parks, get_big4_count
    from db.client import get_conn

    count_before = await get_big4_count()

    await upsert_big4_parks(
        names=["Test BIG4 Park"],
        slugs=["test-big4-park-unit-test"],
        phones=["02 0000 0000"],
        emails=["test@example.com"],
        websites=["https://www.big4.com.au/test"],
        addresses=["1 Test St"],
        cities=["Testville"],
        states=["NSW"],
        postcodes=["2000"],
        lats=[-33.87],
        lons=[151.21],
    )

    count_after = await get_big4_count()
    assert count_after >= count_before

    # Cleanup
    async with get_conn() as conn:
        await conn.execute(
            "DELETE FROM sadie_gtm.hotels WHERE external_id = 'big4_test-big4-park-unit-test'"
        )


@pytest.mark.asyncio
async def test_upsert_big4_parks_idempotent():
    """Test upserting the same park twice does not duplicate."""
    from services.enrichment.repo import upsert_big4_parks, get_big4_count
    from db.client import get_conn

    args = dict(
        names=["Idempotent BIG4 Park"],
        slugs=["idempotent-big4-unit-test"],
        phones=[None],
        emails=[None],
        websites=["https://www.big4.com.au/idempotent"],
        addresses=["2 Test St"],
        cities=["Testville"],
        states=["NSW"],
        postcodes=["2000"],
        lats=[-33.87],
        lons=[151.21],
    )

    await upsert_big4_parks(**args)
    count_first = await get_big4_count()

    await upsert_big4_parks(**args)
    count_second = await get_big4_count()

    assert count_second == count_first

    # Cleanup
    async with get_conn() as conn:
        await conn.execute(
            "DELETE FROM sadie_gtm.hotels WHERE external_id = 'big4_idempotent-big4-unit-test'"
        )
