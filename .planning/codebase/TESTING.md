# Testing Patterns

**Analysis Date:** 2026-01-20

## Test Framework

**Runner:**
- pytest 8.4.2+
- pytest-asyncio 1.2.0+ for async test support
- Config: `pyproject.toml`

**Assertion Library:**
- Built-in pytest `assert`
- No additional assertion libraries

**Run Commands:**
```bash
uv run pytest services/ -v                      # Run all service tests
uv run pytest services/leadgen/service_test.py  # Run specific test file
uv run pytest -m "not online"                   # Skip online tests (hit external APIs)
uv run pytest -m "not integration"              # Skip integration tests
uv run pytest -m "no_db"                        # Run only tests that don't need database
```

## Test File Organization

**Location:**
- Co-located with source: `services/leadgen/service.py` -> `services/leadgen/service_test.py`
- Root conftest: `/conftest.py`

**Naming:**
- Files: `*_test.py` (suffix pattern, not prefix)
- Classes: `TestClassName` (e.g., `TestGetTargetCities`, `TestEngineDetector`)
- Functions: `test_descriptive_name()` (e.g., `test_get_hotel_by_id_not_found()`)

**Structure:**
```
services/
  leadgen/
    service.py
    service_test.py
    repo.py
    repo_test.py
    detector.py
    detector_test.py
    geocoding.py
    geocoding_test.py
  reporting/
    service.py
    service_test.py
    repo.py
    repo_test.py
    launcher_test.py
  enrichment/
    repo.py
    repo_test.py
conftest.py
```

## Test Structure

**Suite Organization:**
```python
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
        # Arrange - Insert test data
        await repo.insert_target_city(
            name="Service Test City",
            state="ST",
            lat=30.0,
            lng=-90.0,
        )

        # Act
        service = Service()
        cities = await service.get_target_cities("ST")

        # Assert
        assert len(cities) >= 1
        city = next(c for c in cities if c.name == "Service Test City")
        assert isinstance(city, CityLocation)
        assert city.lat == 30.0

        # Cleanup
        await repo.delete_target_city("Service Test City", "ST")
```

**Patterns:**
- Group related tests in classes (e.g., `TestGetTargetCities`, `TestAddTargetCity`)
- Use section comment headers to separate test categories
- Each test method has docstring explaining what it tests
- Follow Arrange-Act-Assert pattern (implicit, not commented)

## Test Configuration

**pytest.ini (in pyproject.toml):**
```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
testpaths = ["services", "repositories"]
python_files = "*_test.py"
python_classes = "Test*"
python_functions = "test_*"
addopts = "-v --strict-markers"
markers = [
    "integration: marks tests as integration tests",
    "online: marks tests that hit external APIs",
    "no_db: marks tests that don't need database connection",
]
```

## Fixtures

**Root conftest.py:**
```python
"""Pytest configuration and shared fixtures."""

import pytest
from db.client import init_db, close_db


@pytest.fixture(scope="function", autouse=True)
async def setup_db(request):
    """Initialize database connection pool for tests that need it.

    Tests marked with @pytest.mark.no_db will skip database initialization.
    """
    if "no_db" in [marker.name for marker in request.node.iter_markers()]:
        yield
        return

    await init_db()
    yield
    await close_db()


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "no_db: mark test to skip database setup")
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "online: mark test as online test")
```

**Module-level fixtures (e.g., `detector_test.py`):**
```python
@pytest.fixture(autouse=True)
def setup_engine_patterns():
    """Set up test engine patterns before each test."""
    set_engine_patterns(TEST_ENGINE_PATTERNS)

@pytest.fixture
def detector():
    """Create detector with debug enabled for integration tests."""
    config = DetectionConfig(
        concurrency=2,
        headless=True,
        debug=True,
    )
    return BatchDetector(config)
```

## Mocking

**Framework:** unittest.mock (built-in)

**Patterns:**
```python
from unittest.mock import AsyncMock, patch, MagicMock

@pytest.mark.asyncio
async def test_geocode_city_success(self):
    """Test successful geocoding with mocked response."""
    mock_response = MagicMock()
    mock_response.json.return_value = [{
        "lat": "25.7617",
        "lon": "-80.1918",
        "display_name": "Miami, Florida, United States",
        "importance": 0.75,
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
        mock_instance.get.assert_called_once()
```

**Mocking async functions:**
```python
@pytest.mark.no_db
@pytest.mark.asyncio
@patch("services.reporting.service.upload_file")
@patch("services.reporting.service.repo")
async def test_export_city_uploads_to_s3(mock_repo, mock_upload):
    """Test that export_city uploads to correct S3 path."""
    # Setup mocks - use AsyncMock for async functions
    mock_repo.get_leads_for_city = AsyncMock(return_value=[])
    mock_repo.get_city_stats = AsyncMock(return_value=CityStats())
    mock_upload.return_value = "s3://sadie-gtm/USA/Florida/Miami.xlsx"

    service = Service()
    result = await service.export_city("Miami", "Florida", "USA")

    mock_upload.assert_called_once()
    assert result == "s3://sadie-gtm/USA/Florida/Miami.xlsx"
```

**What to Mock:**
- External HTTP APIs (httpx, requests)
- S3 uploads (`infra.s3.upload_file`)
- External services (Slack, SQS)
- Geocoding APIs (Nominatim)

**What NOT to Mock:**
- Database queries (use real database with test data)
- Internal service/repo calls (test actual behavior)
- Pydantic model validation

## Test Data Management

**Test Data Pattern:**
- Insert test data at start of test
- Clean up test data at end of test
- Use unique identifiers to avoid conflicts (e.g., unique state codes `"ST"`, `"XX"`)

```python
@pytest.mark.asyncio
async def test_get_target_cities_respects_limit(self):
    """Test that limit parameter works."""
    # Insert multiple cities with unique state
    for i in range(5):
        await repo.insert_target_city(
            name=f"Limit Test {i}",
            state="LT",  # Unique state code
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
```

**Helper Functions for Complex Setup:**
```python
async def create_launchable_hotel(
    name: str,
    city: str = "LaunchTestCity",
    state: str = "LaunchTestState",
    existing_customer_id: int = 1,
) -> int:
    """Create a fully enriched hotel ready for launch."""
    hotel_id = await insert_hotel(name=name, ...)
    booking_engine_id = await insert_booking_engine(...)
    await insert_hotel_booking_engine(hotel_id=hotel_id, ...)
    await insert_room_count(hotel_id=hotel_id, ...)
    await insert_customer_proximity(hotel_id=hotel_id, ...)
    return hotel_id


async def cleanup_hotel(hotel_id: int) -> None:
    """Clean up all data for a test hotel."""
    await delete_customer_proximity(hotel_id)
    await delete_room_count(hotel_id)
    await delete_hotel(hotel_id)
```

## Test Markers

**Custom Markers:**
```python
@pytest.mark.no_db
def test_format_proximity_with_data():
    """Test proximity formatting with valid data (no database needed)."""
    ...

@pytest.mark.online
@pytest.mark.asyncio
async def test_geocode_miami(self):
    """Test geocoding Miami against real API."""
    ...

@pytest.mark.integration
@pytest.mark.asyncio
async def test_detect_single_hotel_siteminder(detector):
    """Test detection of SiteMinder engine (hits real website)."""
    ...
```

**Running with markers:**
```bash
uv run pytest -m "not online"              # Skip tests hitting external APIs
uv run pytest -m "not integration"         # Skip integration tests
uv run pytest -m "no_db"                   # Only run tests without DB
uv run pytest -m "online and integration"  # Run only online integration tests
```

## Coverage

**Requirements:** None enforced

**View Coverage:**
```bash
# Not currently configured - would need pytest-cov
uv run pytest --cov=services --cov-report=html
```

## Test Types

**Unit Tests:**
- Scope: Single function/method in isolation
- Mocking: External dependencies mocked
- Marker: None (default)
- Location: Co-located `*_test.py` files

**Integration Tests:**
- Scope: Multiple components working together
- Mocking: External services may be mocked, database is real
- Marker: `@pytest.mark.integration`
- Location: Co-located, often in dedicated section within test file

**Online Tests (E2E with External Services):**
- Scope: Full workflow hitting real external APIs
- Mocking: None - uses real services
- Marker: `@pytest.mark.online`
- Location: Co-located, clearly separated in test file

## Common Patterns

**Async Testing:**
```python
@pytest.mark.asyncio
async def test_insert_and_get_room_count():
    """Test inserting and retrieving room count."""
    hotel_id = await insert_hotel(name="Test Hotel", ...)
    room_count_id = await insert_room_count(hotel_id=hotel_id, room_count=42, ...)

    assert room_count_id is not None

    room_count = await get_room_count_by_hotel_id(hotel_id=hotel_id)
    assert room_count.room_count == 42

    # Cleanup
    await delete_room_count(hotel_id)
    await delete_hotel(hotel_id)
```

**Error Testing:**
```python
@pytest.mark.asyncio
async def test_add_target_city_nonexistent_raises(self):
    """Test that nonexistent city raises error."""
    service = Service()

    with pytest.raises(ValueError, match="City not found"):
        await service.add_target_city("ThisCityDoesNotExist99999", "XX")
```

**Assertion Patterns:**
```python
# Existence check
assert hotel is not None

# Type check
assert isinstance(city, CityLocation)

# Collection check
assert len(cities) >= 1
assert "CityA" in cities

# Value check
assert city.lat == 30.0
assert city.radius_km == 15.0

# Range check (for real APIs with variable results)
assert 25.5 < city.lat < 26.0
assert 30.0 < city.lat < 31.0

# Pattern match with pytest.raises
with pytest.raises(ValueError, match="City not found"):
    await geocode_city("Nonexistent", "XX")
```

**Skipping Tests:**
```python
except Exception as e:
    if "existing_customers" in str(e) or "violates foreign key" in str(e):
        pytest.skip("No existing customers in database for test")
    raise
```

---

*Testing analysis: 2026-01-20*
