"""Comprehensive unit tests for the launcher functionality."""

import pytest
from decimal import Decimal
from services.reporting.repo import (
    get_launchable_hotels,
    get_launchable_count,
    launch_hotels,
    launch_ready_hotels,
    get_launched_count,
)
from services.reporting.service import Service
from services.leadgen.repo import (
    insert_hotel,
    delete_hotel,
    insert_booking_engine,
    insert_hotel_booking_engine,
    get_hotel_by_id,
)
from services.enrichment.repo import (
    insert_room_count,
    delete_room_count,
    insert_customer_proximity,
    delete_customer_proximity,
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


async def create_launchable_hotel(
    name: str,
    city: str = "LaunchTestCity",
    state: str = "LaunchTestState",
    existing_customer_id: int = 1,
) -> int:
    """Create a fully enriched hotel ready for launch.

    Creates hotel with:
    - status=0 (pending)
    - Booking engine linked
    - Room count with status=1 (success)
    - Customer proximity

    Returns hotel_id for cleanup.
    """
    # Create hotel with status=0
    hotel_id = await insert_hotel(
        name=name,
        website=f"https://{name.lower().replace(' ', '')}.com",
        city=city,
        state=state,
        latitude=25.7617,
        longitude=-80.1918,
        status=0,  # pending
        source="test",
    )

    # Create/get booking engine and link to hotel
    booking_engine_id = await insert_booking_engine(
        name="Test Booking Engine",
        domains=["testbooking.com"],
        tier=1,
    )
    await insert_hotel_booking_engine(
        hotel_id=hotel_id,
        booking_engine_id=booking_engine_id,
        booking_url=f"https://{name.lower().replace(' ', '')}.com/book",
        detection_method="test",
    )

    # Insert room count with status=1 (success)
    await insert_room_count(
        hotel_id=hotel_id,
        room_count=50,
        source="test",
        confidence=Decimal("1.0"),
        status=1,  # success
    )

    # Insert customer proximity
    await insert_customer_proximity(
        hotel_id=hotel_id,
        existing_customer_id=existing_customer_id,
        distance_km=Decimal("10.5"),
    )

    return hotel_id


async def cleanup_hotel(hotel_id: int) -> None:
    """Clean up all data for a test hotel."""
    await delete_customer_proximity(hotel_id)
    await delete_room_count(hotel_id)
    # hotel_booking_engines is deleted via CASCADE
    await delete_hotel(hotel_id)


# ============================================================================
# REPO TESTS - get_launchable_hotels
# ============================================================================


@pytest.mark.asyncio
async def test_get_launchable_hotels_empty():
    """Test getting launchable hotels when none exist."""
    # Use a unique state that won't have any data
    hotels = await get_launchable_hotels(limit=10)
    # Just verify it returns a list (may have existing data)
    assert isinstance(hotels, list)


@pytest.mark.asyncio
async def test_get_launchable_hotels_with_data():
    """Test getting launchable hotels returns fully enriched hotels."""
    hotel_id = None
    try:
        hotel_id = await create_launchable_hotel(name="Test Launchable Hotel 1")

        hotels = await get_launchable_hotels(limit=100)

        # Find our test hotel
        test_hotel = next((h for h in hotels if h.id == hotel_id), None)
        assert test_hotel is not None, "Test hotel should be in launchable list"
        assert test_hotel.hotel_name == "Test Launchable Hotel 1"
        assert test_hotel.booking_engine_name == "Test Booking Engine"
        assert test_hotel.room_count == 50
        assert test_hotel.nearest_customer_distance_km == Decimal("10.5")

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        if hotel_id:
            await cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_launchable_hotels_excludes_incomplete():
    """Test that hotels without all enrichment data are excluded."""
    # Create hotel without room count
    hotel_id = await insert_hotel(
        name="Test Incomplete Hotel",
        website="https://testincomplete.com",
        city="IncompleteCity",
        state="IncompleteState",
        status=0,
        source="test",
    )

    try:
        # Link booking engine but don't add room count or proximity
        booking_engine_id = await insert_booking_engine(
            name="Incomplete Test Engine",
            domains=["incomplete.com"],
            tier=1,
        )
        await insert_hotel_booking_engine(
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            detection_method="test",
        )

        hotels = await get_launchable_hotels(limit=1000)

        # Our incomplete hotel should NOT be in the list
        test_hotel = next((h for h in hotels if h.id == hotel_id), None)
        assert test_hotel is None, "Incomplete hotel should not be launchable"

    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_launchable_hotels_excludes_launched():
    """Test that already launched hotels (status=1) are excluded."""
    # Create hotel with status=1 (already launched)
    hotel_id = await insert_hotel(
        name="Test Already Launched Hotel",
        website="https://testalreadylaunched.com",
        city="LaunchedCity",
        state="LaunchedState",
        status=1,  # already launched
        source="test",
    )

    try:
        hotels = await get_launchable_hotels(limit=1000)

        # Our launched hotel should NOT be in the list
        test_hotel = next((h for h in hotels if h.id == hotel_id), None)
        assert test_hotel is None, "Already launched hotel should not be launchable"

    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_launchable_hotels_limit():
    """Test that limit parameter is respected."""
    hotels = await get_launchable_hotels(limit=1)
    assert len(hotels) <= 1


# ============================================================================
# REPO TESTS - get_launchable_count
# ============================================================================


@pytest.mark.asyncio
async def test_get_launchable_count():
    """Test counting launchable hotels."""
    count_before = await get_launchable_count()
    assert isinstance(count_before, int)
    assert count_before >= 0

    hotel_id = None
    try:
        hotel_id = await create_launchable_hotel(name="Test Count Hotel")

        count_after = await get_launchable_count()
        assert count_after == count_before + 1

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        if hotel_id:
            await cleanup_hotel(hotel_id)


# ============================================================================
# REPO TESTS - launch_hotels
# ============================================================================


@pytest.mark.asyncio
async def test_launch_hotels_single():
    """Test launching a single hotel."""
    hotel_id = None
    try:
        hotel_id = await create_launchable_hotel(name="Test Launch Single Hotel")

        # Verify hotel is launchable (status=0)
        hotel_before = await get_hotel_by_id(hotel_id)
        assert hotel_before.status == 0

        # Launch the hotel
        await launch_hotels([hotel_id])

        # Verify hotel is now launched (status=1)
        hotel_after = await get_hotel_by_id(hotel_id)
        assert hotel_after.status == 1

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        if hotel_id:
            await cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_launch_hotels_multiple():
    """Test launching multiple hotels at once."""
    hotel_ids = []
    try:
        # Create multiple launchable hotels
        for i in range(3):
            hotel_id = await create_launchable_hotel(name=f"Test Launch Multi Hotel {i}")
            hotel_ids.append(hotel_id)

        # Launch all of them
        await launch_hotels(hotel_ids)

        # Verify all are launched
        for hotel_id in hotel_ids:
            hotel = await get_hotel_by_id(hotel_id)
            assert hotel.status == 1, f"Hotel {hotel_id} should be launched"

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        for hotel_id in hotel_ids:
            await cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_launch_hotels_empty_list():
    """Test launching with empty list does nothing."""
    # Should not raise any error
    await launch_hotels([])


@pytest.mark.asyncio
async def test_launch_hotels_invalid_ids():
    """Test launching with invalid IDs does nothing."""
    # Should not raise any error - just skips invalid IDs
    await launch_hotels([999999, 999998])


@pytest.mark.asyncio
async def test_launch_hotels_ignores_incomplete():
    """Test that launch_hotels ignores hotels without full enrichment."""
    # Create incomplete hotel (no room count or proximity)
    hotel_id = await insert_hotel(
        name="Test Launch Incomplete Hotel",
        website="https://testlaunchincomplete.com",
        city="LaunchIncompleteCity",
        state="LaunchIncompleteState",
        status=0,
        source="test",
    )

    try:
        # Try to launch incomplete hotel
        await launch_hotels([hotel_id])

        # Verify hotel status is unchanged
        hotel = await get_hotel_by_id(hotel_id)
        assert hotel.status == 0, "Incomplete hotel should not be launched"

    finally:
        await delete_hotel(hotel_id)


# ============================================================================
# REPO TESTS - launch_ready_hotels
# ============================================================================


@pytest.mark.asyncio
async def test_launch_ready_hotels():
    """Test launching ready hotels with limit (multi-worker safe)."""
    hotel_ids = []
    try:
        # Create multiple launchable hotels
        for i in range(2):
            hotel_id = await create_launchable_hotel(
                name=f"Test Launch Ready Hotel {i}",
                city="LaunchReadyCity",
                state="LaunchReadyState",
            )
            hotel_ids.append(hotel_id)

        # Launch ready hotels with limit
        launched_ids = await launch_ready_hotels(limit=100)

        # Verify our test hotels are launched
        for hotel_id in hotel_ids:
            hotel = await get_hotel_by_id(hotel_id)
            assert hotel.status == 1, f"Hotel {hotel_id} should be launched"

        # Verify returned IDs include our hotels
        assert all(hid in launched_ids for hid in hotel_ids)

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        for hotel_id in hotel_ids:
            await cleanup_hotel(hotel_id)


# ============================================================================
# REPO TESTS - get_launched_count
# ============================================================================


@pytest.mark.asyncio
async def test_get_launched_count():
    """Test counting launched hotels."""
    count_before = await get_launched_count()
    assert isinstance(count_before, int)
    assert count_before >= 0

    hotel_id = None
    try:
        hotel_id = await create_launchable_hotel(name="Test Launched Count Hotel")

        # Launch the hotel
        await launch_hotels([hotel_id])

        count_after = await get_launched_count()
        assert count_after == count_before + 1

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        if hotel_id:
            await cleanup_hotel(hotel_id)


# ============================================================================
# SERVICE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_service_get_launchable_hotels():
    """Test service layer get_launchable_hotels."""
    service = Service()
    hotels = await service.get_launchable_hotels(limit=10)
    assert isinstance(hotels, list)


@pytest.mark.asyncio
async def test_service_get_launchable_count():
    """Test service layer get_launchable_count."""
    service = Service()
    count = await service.get_launchable_count()
    assert isinstance(count, int)
    assert count >= 0


@pytest.mark.asyncio
async def test_service_launch_hotels():
    """Test service layer launch_hotels."""
    hotel_id = None
    try:
        hotel_id = await create_launchable_hotel(name="Test Service Launch Hotel")

        service = Service()
        count = await service.launch_hotels([hotel_id])
        assert count == 1

        # Verify hotel is launched
        hotel = await get_hotel_by_id(hotel_id)
        assert hotel.status == 1

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        if hotel_id:
            await cleanup_hotel(hotel_id)


@pytest.mark.asyncio
async def test_service_launch_hotels_empty():
    """Test service layer launch_hotels with empty list returns 0."""
    service = Service()
    count = await service.launch_hotels([])
    assert count == 0


@pytest.mark.asyncio
async def test_service_launch_hotels_invalid():
    """Test service layer launch_hotels with invalid IDs returns 0."""
    service = Service()
    count = await service.launch_hotels([999999])
    assert count == 0


@pytest.mark.asyncio
async def test_service_launch_ready():
    """Test service layer launch_ready (multi-worker safe)."""
    service = Service()
    # Just verify it doesn't error - count depends on existing data
    count = await service.launch_ready(limit=100)
    assert isinstance(count, int)
    assert count >= 0


@pytest.mark.asyncio
async def test_service_get_launched_count():
    """Test service layer get_launched_count."""
    service = Service()
    count = await service.get_launched_count()
    assert isinstance(count, int)
    assert count >= 0


# ============================================================================
# STATUS EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_excludes_failed_detection():
    """Test that hotels with failed detection (hbe.status=-1) are excluded."""
    hotel_id = await insert_hotel(
        name="Test Failed Detection Hotel",
        website="https://testfaileddetection.com",
        city="FailedDetectionCity",
        state="FailedDetectionState",
        status=0,
        source="test",
    )

    try:
        # Create booking engine link with status=-1 (failed)
        booking_engine_id = await insert_booking_engine(
            name="Failed Detection Engine",
            domains=["faileddetection.com"],
            tier=1,
        )
        await insert_hotel_booking_engine(
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            detection_method="test",
            status=-1,  # failed detection
        )

        # Even with room count and proximity, should not be launchable
        await insert_room_count(
            hotel_id=hotel_id,
            room_count=50,
            source="test",
            confidence=Decimal("1.0"),
            status=1,
        )

        hotels = await get_launchable_hotels(limit=1000)
        test_hotel = next((h for h in hotels if h.id == hotel_id), None)
        assert test_hotel is None, "Hotel with failed detection should not be launchable"

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        await delete_room_count(hotel_id)
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_excludes_failed_room_count():
    """Test that hotels with failed room count (hrc.status=0) are excluded."""
    hotel_id = await insert_hotel(
        name="Test Failed Room Count Hotel",
        website="https://testfailedroomcount.com",
        city="FailedRoomCountCity",
        state="FailedRoomCountState",
        status=0,
        source="test",
    )

    try:
        # Create successful booking engine link
        booking_engine_id = await insert_booking_engine(
            name="Room Count Test Engine",
            domains=["roomcounttest.com"],
            tier=1,
        )
        await insert_hotel_booking_engine(
            hotel_id=hotel_id,
            booking_engine_id=booking_engine_id,
            detection_method="test",
            status=1,  # success
        )

        # Create room count with status=0 (failed)
        await insert_room_count(
            hotel_id=hotel_id,
            room_count=None,
            source="test",
            confidence=None,
            status=0,  # failed
        )

        hotels = await get_launchable_hotels(limit=1000)
        test_hotel = next((h for h in hotels if h.id == hotel_id), None)
        assert test_hotel is None, "Hotel with failed room count should not be launchable"

    finally:
        await delete_room_count(hotel_id)
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_excludes_rejected_hotels():
    """Test that rejected hotels (status=-1, -2) are excluded."""
    hotel_ids = []

    try:
        # Create hotel with status=-1 (no booking engine)
        hotel_id_no_engine = await insert_hotel(
            name="Test No Engine Hotel",
            website="https://testnoengine.com",
            city="NoEngineCity",
            state="NoEngineState",
            status=-1,  # rejected - no booking engine
            source="test",
        )
        hotel_ids.append(hotel_id_no_engine)

        # Create hotel with status=-2 (location mismatch)
        hotel_id_mismatch = await insert_hotel(
            name="Test Location Mismatch Hotel",
            website="https://testmismatch.com",
            city="MismatchCity",
            state="MismatchState",
            status=-2,  # rejected - location mismatch
            source="test",
        )
        hotel_ids.append(hotel_id_mismatch)

        hotels = await get_launchable_hotels(limit=1000)

        # Neither rejected hotel should be launchable
        for hotel_id in hotel_ids:
            test_hotel = next((h for h in hotels if h.id == hotel_id), None)
            assert test_hotel is None, f"Rejected hotel {hotel_id} should not be launchable"

    finally:
        for hotel_id in hotel_ids:
            await delete_hotel(hotel_id)


# ============================================================================
# INTEGRATION TESTS - Full workflow
# ============================================================================


@pytest.mark.asyncio
async def test_full_launch_workflow():
    """Test the complete launch workflow from start to finish."""
    hotel_id = None
    try:
        service = Service()

        # Get initial counts
        launchable_before = await service.get_launchable_count()
        launched_before = await service.get_launched_count()

        # Create a launchable hotel
        hotel_id = await create_launchable_hotel(name="Test Full Workflow Hotel")

        # Verify it's in launchable count
        launchable_during = await service.get_launchable_count()
        assert launchable_during == launchable_before + 1

        # Preview the hotel
        hotels = await service.get_launchable_hotels(limit=100)
        test_hotel = next((h for h in hotels if h.id == hotel_id), None)
        assert test_hotel is not None
        assert test_hotel.hotel_name == "Test Full Workflow Hotel"

        # Launch the hotel
        launched_count = await service.launch_hotels([hotel_id])
        assert launched_count == 1

        # Verify counts updated
        launchable_after = await service.get_launchable_count()
        launched_after = await service.get_launched_count()

        assert launchable_after == launchable_before
        assert launched_after == launched_before + 1

        # Verify hotel status changed
        hotel = await get_hotel_by_id(hotel_id)
        assert hotel.status == 1

    except Exception as e:
        if "existing_customers" in str(e) or "violates foreign key" in str(e):
            pytest.skip("No existing customers in database for test")
        raise
    finally:
        if hotel_id:
            await cleanup_hotel(hotel_id)
