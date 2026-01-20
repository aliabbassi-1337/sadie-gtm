# Coding Conventions

**Analysis Date:** 2026-01-20

## Naming Patterns

**Files:**
- snake_case for all Python files: `service.py`, `repo_test.py`, `grid_scraper.py`
- Test files use `_test.py` suffix (not `test_` prefix): `service_test.py`, `repo_test.py`
- Module directories are singular nouns: `services/leadgen/`, `services/enrichment/`

**Functions:**
- snake_case for all functions: `get_hotel_by_id()`, `insert_target_city()`
- Async functions use same naming, no special prefix
- Private/internal functions prefixed with underscore: `_save_detection_result()`, `_create_workbook()`

**Variables:**
- snake_case for local variables and parameters: `hotel_id`, `cell_size_km`
- UPPER_SNAKE_CASE for module-level constants: `BATCH_SIZE`, `SKIP_JUNK_DOMAINS`

**Types/Classes:**
- PascalCase for classes and Pydantic models: `Hotel`, `DetectionResult`, `CityLocation`
- Interface classes prefixed with `I`: `IService`
- Status constants in classes: `HotelStatus.PENDING`, `HotelStatus.LAUNCHED`

**Database:**
- SQL uses snake_case for tables and columns
- Schema prefix for all tables: `sadie_gtm.hotels`, `sadie_gtm.booking_engines`

## Code Style

**Formatting:**
- No explicit formatter configured (no .prettierrc, ruff.toml, black.toml found)
- Implicit 4-space indentation following Python conventions
- Single quotes preferred for strings in most files
- Max line length not enforced but generally kept under 100 characters

**Linting:**
- No linting tools configured (no .eslintrc, flake8, pylint configs)
- Type hints used extensively with Pydantic models
- Optional typing used consistently: `Optional[str]`, `List[int]`

## Import Organization

**Order:**
1. Standard library imports (`import os`, `import asyncio`)
2. Third-party imports (`from loguru import logger`, `from pydantic import BaseModel`)
3. Local imports (`from services.leadgen import repo`, `from db.client import get_conn`)

**Path Aliases:**
- No path aliases configured
- Relative imports within packages: `from services.leadgen import repo`
- Direct imports from project root: `from db.client import init_db`

**Example from `services/leadgen/service.py`:**
```python
import math
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional

from loguru import logger
from pydantic import BaseModel

from services.leadgen import repo
from services.leadgen.constants import HotelStatus
from services.leadgen.detector import BatchDetector, DetectionConfig
```

## Error Handling

**Patterns:**
- Use try/except at service layer, log with loguru
- Return None for not-found cases (not exceptions): `get_hotel_by_id()` returns `Optional[Hotel]`
- Raise `ValueError` for invalid input: `raise ValueError("City not found")`
- Log errors but continue processing in batch operations

**Example from `services/leadgen/service.py`:**
```python
async def _save_detection_result(self, result: DetectionResult) -> None:
    """Save detection result to database."""
    try:
        if result.error:
            error_type = result.error.split(":")[0].strip()
            await repo.insert_detection_error(...)
        # ... handle different cases
    except Exception as e:
        logger.error(f"Error saving detection result for hotel {result.hotel_id}: {e}")
```

## Logging

**Framework:** loguru

**Patterns:**
- Import logger at module level: `from loguru import logger`
- Use f-strings for message formatting: `logger.info(f"Processing {len(hotels)} hotels")`
- Log levels: `logger.info()` for progress, `logger.warning()` for recoverable issues, `logger.error()` for failures
- Separator lines for workflow milestones: `logger.info("=" * 60)`

**Example:**
```python
logger.info(f"Starting region scrape: center=({center_lat}, {center_lng}), radius={radius_km}km")
logger.warning(f"Region {region.name} has no valid bounds, skipping")
logger.error(f"Error saving detection result for hotel {result.hotel_id}: {e}")
```

## Comments

**When to Comment:**
- Module-level docstrings describing purpose and usage examples
- Class docstrings explaining responsibility
- Method docstrings with Args/Returns when signature is not self-documenting
- Inline comments for complex business logic or non-obvious behavior

**Docstring Format:**
```python
"""Short description.

Longer description if needed.

Args:
    name: City name (e.g., "Miami")
    state: State code (e.g., "FL")

Returns:
    CityLocation with coordinates

Raises:
    ValueError: If city not found in geocoding API
"""
```

**Section Comments:**
- Use comment headers to separate logical sections in longer files:
```python
# =============================================================================
# TARGET CITIES
# =============================================================================
```

## Function Design

**Size:** Functions generally kept under 50 lines. Longer functions split into private helpers.

**Parameters:**
- Required parameters first, optional with defaults last
- Use keyword arguments for optional params: `limit: int = 100`
- Group related parameters: `center_lat: float, center_lng: float`

**Return Values:**
- Single values or tuples for multiple returns: `Tuple[int, int]`
- Return `Optional[T]` for failable lookups, not exceptions
- Return counts (int) from batch operations

## Module Design

**Exports:**
- Explicit `__all__` list for public API:
```python
__all__ = ["IService", "Service", "ScrapeEstimate", "CityLocation", "ScrapeRegion"]
```

**Barrel Files:**
- Used in `services/` packages: `services/leadgen/__init__.py`
- Used in `db/models/__init__.py` to export all models

## Architecture Patterns

**Service Layer:**
- Abstract interface class `IService` defines contract
- Concrete `Service` class implements interface
- Services use repository modules for database access

**Repository Pattern:**
- Each service has a `repo.py` module
- Functions map 1:1 to SQL queries
- Use `get_conn()` context manager for connections

**Example structure:**
```python
# services/leadgen/service.py
class IService(ABC):
    @abstractmethod
    async def scrape_region(self, ...) -> int:
        pass

class Service(IService):
    async def scrape_region(self, ...) -> int:
        # Implementation calls repo functions
        return await repo.insert_hotels_bulk(hotel_dicts)

# services/leadgen/repo.py
async def insert_hotels_bulk(hotels: List[dict]) -> int:
    async with get_conn() as conn:
        # SQL execution
```

**Pydantic Models:**
- Use `model_config = ConfigDict(from_attributes=True)` for ORM compatibility
- Define at `db/models/` for database entities
- Define at service level for domain-specific types (e.g., `DetectionResult`)

## Async Patterns

**Database Access:**
- All repository functions are async
- Use `async with get_conn() as conn:` pattern
- Connection pool managed globally in `db/client.py`

**Batch Processing:**
- Use callbacks for incremental saves: `on_batch_complete=save_batch`
- Use semaphores for concurrency control in parallel operations
- Close database connections in finally blocks

**Example:**
```python
async def scrape_regions(self, state: str, ...) -> List[ScrapedHotel]:
    async def save_batch(batch_hotels):
        nonlocal total_saved
        count = await self._save_hotels(batch_hotels, source=source)
        total_saved += count

    hotels, stats = await scraper._scrape_bounds(..., on_batch_complete=save_batch)
```

## Configuration

**Environment Variables:**
- Loaded via python-dotenv: `load_dotenv()`
- Access via `os.getenv("VAR_NAME")`
- Database config: `SADIE_DB_HOST`, `SADIE_DB_PORT`, etc.

**Config Objects:**
- Use frozen Pydantic models for configuration:
```python
class DetectionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    timeout_page_load: int = 15000
    concurrency: int = 5
```

---

*Convention analysis: 2026-01-20*
