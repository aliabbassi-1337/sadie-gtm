"""Integration tests for pipeline state machine."""

import pytest
from services.reporting.repo import (
    get_pipeline_summary,
    get_pipeline_by_source,
    get_pipeline_by_source_name,
)
from services.reporting.service import Service
from services.leadgen.repo import delete_hotel
from services.leadgen.constants import PipelineStage
from db.client import get_conn, queries


async def insert_test_hotel(
    name: str,
    city: str = None,
    state: str = None,
    website: str = None,
    status: int = 0,
    source: str = "test",
) -> int:
    """Insert a test hotel directly without ON CONFLICT (for testing only)."""
    async with get_conn() as conn:
        result = await conn.fetchrow(
            """
            INSERT INTO sadie_gtm.hotels (name, city, state, website, status, source, country)
            VALUES ($1, $2, $3, $4, $5, $6, 'USA')
            RETURNING id
            """,
            name, city, state, website, status, source
        )
        return result['id']


@pytest.mark.asyncio
async def test_get_pipeline_summary():
    """Test getting overall pipeline summary."""
    # Create test hotels at different stages
    hotel_ids = []
    
    # Hotel at INGESTED (0)
    hotel_ids.append(await insert_test_hotel(
        name="Pipeline Test 1",
        city="PipelineCity",
        state="PipelineState",
        status=PipelineStage.INGESTED,
        source="pipeline_test",
    ))
    
    # Hotel at HAS_WEBSITE (10)
    hotel_ids.append(await insert_test_hotel(
        name="Pipeline Test 2",
        website="https://pipelinetest2.com",
        city="PipelineCity",
        state="PipelineState",
        status=PipelineStage.HAS_WEBSITE,
        source="pipeline_test",
    ))
    
    # Hotel at DETECTED (30)
    hotel_ids.append(await insert_test_hotel(
        name="Pipeline Test 3",
        website="https://pipelinetest3.com",
        city="PipelineCity",
        state="PipelineState",
        status=PipelineStage.DETECTED,
        source="pipeline_test",
    ))
    
    # Hotel at terminal NO_BOOKING_ENGINE (-1)
    hotel_ids.append(await insert_test_hotel(
        name="Pipeline Test 4",
        website="https://pipelinetest4.com",
        city="PipelineCity",
        state="PipelineState",
        status=PipelineStage.NO_BOOKING_ENGINE,
        source="pipeline_test",
    ))
    
    try:
        summary = await get_pipeline_summary()
        
        # Should return list of (status, count) tuples
        assert isinstance(summary, list)
        assert len(summary) > 0
        
        # Convert to dict for easier lookup
        status_counts = {status: count for status, count in summary}
        
        # Check our test hotels are counted
        assert status_counts.get(PipelineStage.INGESTED, 0) >= 1
        assert status_counts.get(PipelineStage.HAS_WEBSITE, 0) >= 1
        assert status_counts.get(PipelineStage.DETECTED, 0) >= 1
        assert status_counts.get(PipelineStage.NO_BOOKING_ENGINE, 0) >= 1
    finally:
        for hotel_id in hotel_ids:
            await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_pipeline_by_source():
    """Test getting pipeline breakdown by source."""
    # Create test hotels
    hotel_ids = []
    
    hotel_ids.append(await insert_test_hotel(
        name="Source Test 1",
        city="SourceCity",
        state="SourceState",
        status=PipelineStage.INGESTED,
        source="pipeline_source_test",
    ))
    
    hotel_ids.append(await insert_test_hotel(
        name="Source Test 2",
        website="https://sourcetest2.com",
        city="SourceCity",
        state="SourceState",
        status=PipelineStage.HAS_WEBSITE,
        source="pipeline_source_test",
    ))
    
    try:
        sources = await get_pipeline_by_source()
        
        # Should return list of dicts
        assert isinstance(sources, list)
        
        # Find our test source
        test_source = next((s for s in sources if s['source'] == 'pipeline_source_test'), None)
        assert test_source is not None
        
        # Check counts
        assert test_source['ingested'] >= 1
        assert test_source['has_website'] >= 1
        assert test_source['total'] >= 2
    finally:
        for hotel_id in hotel_ids:
            await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_get_pipeline_by_source_name():
    """Test getting pipeline breakdown for a specific source."""
    # Create test hotels
    hotel_ids = []
    
    hotel_ids.append(await insert_test_hotel(
        name="Specific Source 1",
        city="SpecificCity",
        state="SpecificState",
        status=PipelineStage.INGESTED,
        source="pipeline_specific_test",
    ))
    
    hotel_ids.append(await insert_test_hotel(
        name="Specific Source 2",
        website="https://specifictest2.com",
        city="SpecificCity",
        state="SpecificState",
        status=PipelineStage.DETECTED,
        source="pipeline_specific_test",
    ))
    
    try:
        detail = await get_pipeline_by_source_name("pipeline_specific_test")
        
        # Should return list of (status, count) tuples
        assert isinstance(detail, list)
        assert len(detail) >= 2
        
        # Convert to dict
        status_counts = {status: count for status, count in detail}
        
        assert status_counts.get(PipelineStage.INGESTED, 0) >= 1
        assert status_counts.get(PipelineStage.DETECTED, 0) >= 1
    finally:
        for hotel_id in hotel_ids:
            await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_advance_to_has_website():
    """Test advancing a hotel to HAS_WEBSITE stage."""
    # Create hotel at INGESTED with a website
    hotel_id = await insert_test_hotel(
        name="Advance Website Test",
        website="https://advancetest.com",
        city="AdvanceCity",
        state="AdvanceState",
        status=PipelineStage.INGESTED,
        source="pipeline_advance_test",
    )
    
    try:
        # Advance to HAS_WEBSITE
        async with get_conn() as conn:
            await queries.advance_to_has_website(conn, hotel_id=hotel_id)
            
            # Verify the status changed
            result = await conn.fetchrow(
                "SELECT status FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id
            )
            assert result['status'] == PipelineStage.HAS_WEBSITE
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_advance_to_has_website_requires_website():
    """Test that advance_to_has_website only works if hotel has a website."""
    # Create hotel at INGESTED WITHOUT a website
    hotel_id = await insert_test_hotel(
        name="No Website Test",
        city="NoWebsiteCity",
        state="NoWebsiteState",
        status=PipelineStage.INGESTED,
        source="pipeline_advance_test",
    )
    
    try:
        # Try to advance to HAS_WEBSITE - should not change because no website
        async with get_conn() as conn:
            await queries.advance_to_has_website(conn, hotel_id=hotel_id)
            
            # Verify the status did NOT change
            result = await conn.fetchrow(
                "SELECT status FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id
            )
            assert result['status'] == PipelineStage.INGESTED
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_advance_to_detected():
    """Test advancing a hotel to DETECTED stage."""
    # Create hotel at HAS_WEBSITE
    hotel_id = await insert_test_hotel(
        name="Detect Test",
        website="https://detecttest.com",
        city="DetectCity",
        state="DetectState",
        status=PipelineStage.HAS_WEBSITE,
        source="pipeline_advance_test",
    )
    
    try:
        # Advance to DETECTED
        async with get_conn() as conn:
            await queries.advance_to_detected(conn, hotel_id=hotel_id)
            
            # Verify the status changed
            result = await conn.fetchrow(
                "SELECT status FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id
            )
            assert result['status'] == PipelineStage.DETECTED
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_set_terminal_status():
    """Test setting a terminal status."""
    # Create hotel at INGESTED
    hotel_id = await insert_test_hotel(
        name="Terminal Test",
        city="TerminalCity",
        state="TerminalState",
        status=PipelineStage.INGESTED,
        source="pipeline_terminal_test",
    )
    
    try:
        # Set terminal status
        async with get_conn() as conn:
            await queries.set_terminal_status(
                conn, 
                hotel_id=hotel_id, 
                terminal_status=PipelineStage.NON_HOTEL
            )
            
            # Verify the status changed
            result = await conn.fetchrow(
                "SELECT status FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id
            )
            assert result['status'] == PipelineStage.NON_HOTEL
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_service_get_pipeline_summary():
    """Test the service layer get_pipeline_summary method."""
    service = Service()
    
    # Create a test hotel
    hotel_id = await insert_test_hotel(
        name="Service Pipeline Test",
        city="ServiceCity",
        state="ServiceState",
        status=PipelineStage.INGESTED,
        source="pipeline_service_test",
    )
    
    try:
        summary = await service.get_pipeline_summary()
        
        # Should return list of tuples
        assert isinstance(summary, list)
        assert len(summary) > 0
        
        # Each item should be (status, count)
        for item in summary:
            assert len(item) == 2
            assert isinstance(item[0], int)  # status
            assert isinstance(item[1], int)  # count
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_service_get_pipeline_by_source():
    """Test the service layer get_pipeline_by_source method."""
    service = Service()
    
    # Create a test hotel
    hotel_id = await insert_test_hotel(
        name="Service Source Test",
        city="ServiceCity",
        state="ServiceState",
        status=PipelineStage.HAS_WEBSITE,
        source="pipeline_service_source_test",
    )
    
    try:
        sources = await service.get_pipeline_by_source()
        
        # Should return list of dicts
        assert isinstance(sources, list)
        
        # Find our test source
        test_source = next((s for s in sources if s['source'] == 'pipeline_service_source_test'), None)
        assert test_source is not None
        assert test_source['has_website'] >= 1
    finally:
        await delete_hotel(hotel_id)


@pytest.mark.asyncio
async def test_cannot_go_backwards():
    """Test that hotels cannot regress to earlier stages."""
    # Create hotel at DETECTED
    hotel_id = await insert_test_hotel(
        name="No Regress Test",
        website="https://noregresstest.com",
        city="NoRegressCity",
        state="NoRegressState",
        status=PipelineStage.DETECTED,
        source="pipeline_regress_test",
    )
    
    try:
        async with get_conn() as conn:
            # Try to advance to HAS_WEBSITE (10) from DETECTED (30)
            # This should NOT change the status because status < 10 check fails
            await queries.advance_to_has_website(conn, hotel_id=hotel_id)
            
            # Verify the status did NOT change
            result = await conn.fetchrow(
                "SELECT status FROM sadie_gtm.hotels WHERE id = $1",
                hotel_id
            )
            assert result['status'] == PipelineStage.DETECTED
    finally:
        await delete_hotel(hotel_id)
