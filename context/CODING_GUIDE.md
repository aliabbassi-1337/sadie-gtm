# Coding Guide for Sadie GTM

## Architecture: 3-Layer Pattern

```
Service Layer (service.py)     → Business logic, orchestrates repo calls
     ↓
Repository Layer (repo.py)     → Data access functions
     ↓
SQL Queries (queries/*.sql)    → Raw SQL (aiosql format)
```

**3 Services:** leadgen (scraping + detection) | enrichment (room counts + proximity) | reporting (exports + uploads)

### Service Layer Rules

- **You CAN** create new files, classes, and abstractions inside `/services/{service_name}/`
- **ONLY service functions** (defined in `service.py`) can call the repo layer
- **ONLY service functions** can be exported via the service interface (used by workflows)
- **Other classes/modules** in the service folder are internal helpers - they cannot call repo or be exported

Example:
```
services/leadgen/
├── service.py          # ✅ Calls repo, exports via IService interface
├── repo.py             # Data access only
├── helpers.py          # ❌ Cannot call repo
└── processors.py       # ❌ Cannot call repo
```

## Adding a New Feature

### 1. Write SQL Query

**File:** `/db/queries/hotels.sql`

```sql
-- name: get_hotels_by_city
SELECT id, name, website, city, state, status
FROM hotels
WHERE city = :city AND state = :state
ORDER BY name;
```

**Query naming:**
- `get_*^` = SELECT single row
- `get_*` = SELECT multiple rows
- `insert_*<!` = INSERT RETURNING
- `update_*!` / `delete_*!` = UPDATE/DELETE

**Always use `:param_name` syntax** (NOT `$1`, `$2`)

### 2. Create Repo Function

**File:** `/services/leadgen/repo.py`

```python
from typing import List
from db.client import queries, get_conn
from db.models.hotel import Hotel

async def get_hotels_by_city(city: str, state: str) -> List[Hotel]:
    """Get all hotels in a city."""
    async with get_conn() as conn:
        results = await queries.get_hotels_by_city(conn, city=city, state=state)
        return [Hotel.model_validate(dict(row)) for row in results]
```

**Key patterns:**
- Use `async with get_conn()` for DB access
- Named parameters: `city=city` (matches SQL `:city`)
- Convert to dict: `dict(row)` before Pydantic validation
- Return Pydantic models, NOT raw records

### 3. Write Tests

**Create a new test file with `_test.py` suffix** (e.g., `/services/leadgen/repo_test.py`)

```python
import pytest
from services.leadgen.repo import get_hotels_by_city, insert_hotel, delete_hotel

@pytest.mark.asyncio
async def test_get_hotels_by_city():
    """Test getting hotels by city."""
    # Insert test data
    hotel_id = await insert_hotel(
        name="Test Hotel",
        website="https://test.com",
        city="Miami",
        state="Florida",
        status=0,
        source="test",
    )

    # Test
    hotels = await get_hotels_by_city(city="Miami", state="Florida")
    assert len(hotels) >= 1
    assert any(h.name == "Test Hotel" for h in hotels)

    # Cleanup
    await delete_hotel(hotel_id)
```

**Testing rules:**
- Create files with `_test.py` suffix (pytest will auto-discover them)
- Use `@pytest.mark.asyncio` for async tests
- Insert test data with repo functions (has `ON CONFLICT DO UPDATE`)
- Always cleanup with `delete_*()`
- Run tests: `uv run pytest -v`

### 4. Add to Service (Optional)

**File:** `/services/leadgen/service.py`

```python
from services.leadgen import repo

class Service(IService):
    async def get_pending_detection_count(self) -> int:
        """Count hotels waiting for detection (status=0)."""
        hotels = await repo.get_hotels_by_status(status=0)
        return len(hotels)
```

## Database

**Schema:** 7 tables (hotels, booking_engines, existing_customers, hotel_booking_engines, hotel_room_count, hotel_customer_proximity, jobs)

**PostGIS location handling:**

```sql
-- Insert: ST_Point(longitude, latitude)
INSERT INTO hotels (name, location)
VALUES (:name, ST_Point(:longitude, :latitude)::geography)

-- Query: ST_X/ST_Y to get coordinates
SELECT ST_Y(location::geometry) AS latitude,
       ST_X(location::geometry) AS longitude
FROM hotels

-- Distance in km
SELECT ST_Distance(h.location, c.location) / 1000 AS distance_km
FROM hotels h, existing_customers c
```

## Python Conventions (Python 3.9)

```python
# ✅ Use Optional (Python 3.9)
from typing import Optional, List
def get_hotel() -> Optional[Hotel]:
    pass

# ❌ Don't use | syntax (Python 3.10+)
def get_hotel() -> Hotel | None:  # Wrong!
    pass
```

**Pydantic models:** Match DB schema exactly, use `Optional[Type]` for nullable fields

```python
from pydantic import BaseModel, ConfigDict

class Hotel(BaseModel):
    id: int
    name: str
    website: Optional[str] = None
    status: int = 0

    model_config = ConfigDict(from_attributes=True)
```

## Common Mistakes

❌ **Positional params in SQL:** `WHERE id = $1` → ✅ `WHERE id = :hotel_id`

❌ **Raw Record to Pydantic:** `Hotel.model_validate(result)` → ✅ `Hotel.model_validate(dict(result))`

❌ **Creating new pools:** Use `get_conn()` context manager, not `asyncpg.create_pool()`

❌ **Forgetting cleanup:** Always `await delete_hotel(hotel_id)` in tests

## Quick Commands

```bash
uv run pytest           # Run all tests
uv run pytest -v        # Verbose
uv run pytest -k "name" # Run specific test
```
