# Local Development Setup

## Prerequisites

- OrbStack (or Docker Desktop)
- Python 3.11+
- uv

## Setup Local Database

1. **Start PostgreSQL with PostGIS:**
```bash
docker compose up -d
```

This will:
- Start PostgreSQL 17 with PostGIS 3.5
- Create database `sadie_gtm`
- Expose on `localhost:5432`

2. **Apply schema:**
```bash
docker exec -i sadie-gtm-local-db psql -U sadie -d sadie_gtm < db/schema.sql
```

See `db/README_MIGRATIONS.md` for managing schema changes.

3. **Check database is running:**
```bash
docker compose ps
```

4. **View logs:**
```bash
docker compose logs -f postgres
```

5. **Stop database:**
```bash
docker compose down
```

6. **Reset database (delete all data and schema):**
```bash
docker compose down -v
docker compose up -d
docker exec -i sadie-gtm-local-db psql -U sadie -d sadie_gtm < db/schema.sql
```

## Environment Setup

1. **Copy environment file:**
```bash
cp .env.example .env
```

The default `.env` is already configured for local development.

## Running Tests

**Run all tests:**
```bash
uv run pytest
```

**Run specific test file:**
```bash
uv run pytest repositories/hotel_repo_test.py
```

**Run with verbose output:**
```bash
uv run pytest -v
```

**Run with output capture disabled (see print statements):**
```bash
uv run pytest -s
```

## Database Access

**Connect with psql:**
```bash
docker exec -it sadie-gtm-local-db psql -U sadie -d sadie_gtm
```

**Useful psql commands:**
```sql
-- Set search path
SET search_path TO sadie_gtm;

-- List tables
\dt

-- Describe table
\d hotels

-- Run query
SELECT COUNT(*) FROM hotels;

-- Quit
\q
```
