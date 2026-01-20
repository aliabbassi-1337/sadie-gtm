# Codebase Structure

**Analysis Date:** 2026-01-20

## Directory Layout

```
sadie_gtm/
├── db/                     # Database layer
│   ├── client.py           # Connection pool management
│   ├── schema.sql          # Full database schema
│   ├── queries/            # aiosql SQL files
│   ├── models/             # Pydantic data models
│   ├── migrations/         # Schema migrations
│   └── ingest.py           # Data ingestion utilities
├── services/               # Business logic layer
│   ├── leadgen/            # Scraping + detection service
│   ├── enrichment/         # Room count + proximity service
│   └── reporting/          # Export + launcher service
├── workflows/              # CLI entry points
├── infra/                  # AWS + Slack integrations
│   ├── sqs.py              # SQS message queue client
│   ├── s3.py               # S3 file upload
│   ├── slack.py            # Slack notifications
│   └── ec2/                # EC2 deployment scripts
├── scripts/                # Utility scripts (legacy/one-off)
│   ├── pipeline/           # Pipeline utilities
│   ├── scrapers/           # Scraper scripts
│   ├── enrichers/          # Enrichment scripts
│   └── utils/              # General utilities
├── context/                # Project documentation
│   ├── docs/               # Reference documentation
│   └── tasks/              # Task tracking
├── archive/                # Archived/legacy code and data
├── .planning/              # GSD planning docs
├── main.py                 # Main entry point
├── conftest.py             # Pytest fixtures
├── pyproject.toml          # Project configuration
├── workflows.yaml          # Workflow definitions reference
└── docker-compose.yml      # Local development database
```

## Directory Purposes

**`db/`:**
- Purpose: All database-related code
- Contains: Connection management, SQL queries, Pydantic models
- Key files: `client.py` (pool management), `schema.sql` (full schema)

**`db/queries/`:**
- Purpose: SQL queries loaded via aiosql
- Contains: One `.sql` file per domain (hotels, booking_engines, etc.)
- Key files: `hotels.sql`, `hotel_booking_engines.sql`, `scrape_regions.sql`

**`db/models/`:**
- Purpose: Pydantic models for database records
- Contains: One model per table/entity
- Key files: `hotel.py`, `booking_engine.py`, `reporting.py`

**`services/leadgen/`:**
- Purpose: Hotel scraping and booking engine detection
- Contains: Service class, repository, detector, grid scraper
- Key files: `service.py` (public API), `detector.py` (Playwright detection), `grid_scraper.py` (Serper API)

**`services/enrichment/`:**
- Purpose: Data enrichment (room counts, customer proximity)
- Contains: Service class, repository, enricher modules
- Key files: `service.py`, `room_count_enricher.py`, `customer_proximity.py`

**`services/reporting/`:**
- Purpose: Excel export and hotel launching
- Contains: Service class, repository
- Key files: `service.py` (export + launcher methods), `repo.py`

**`workflows/`:**
- Purpose: CLI scripts for pipeline execution
- Contains: One script per workflow step
- Key files: `scrape_bbox.py`, `detection_consumer.py`, `enrichment.py`, `launcher.py`, `export.py`

**`infra/`:**
- Purpose: External service clients
- Contains: AWS (SQS, S3) and Slack integrations
- Key files: `sqs.py`, `s3.py`, `slack.py`

**`scripts/`:**
- Purpose: Utility scripts, one-off tasks, legacy code
- Contains: Various helper scripts
- Note: Newer code should go in `workflows/` or `services/`

## Key File Locations

**Entry Points:**
- `main.py`: Legacy workflow runner
- `workflows/*.py`: Direct execution workflows

**Configuration:**
- `pyproject.toml`: Dependencies, pytest config
- `docker-compose.yml`: Local PostgreSQL/PostGIS
- `.env`: Environment variables (not committed)
- `.env.example`: Template for environment variables

**Core Logic:**
- `services/leadgen/service.py`: Main lead generation service
- `services/leadgen/detector.py`: Booking engine detection (1750 lines)
- `services/leadgen/grid_scraper.py`: Geographic grid scraping
- `services/enrichment/service.py`: Enrichment orchestration
- `services/reporting/service.py`: Export and launcher logic

**Database:**
- `db/schema.sql`: Complete database schema
- `db/client.py`: asyncpg connection pool
- `db/queries/*.sql`: All SQL queries

**Testing:**
- `conftest.py`: Pytest fixtures (DB connection)
- `services/**/test_*.py`: Service tests (co-located)

## Naming Conventions

**Files:**
- `snake_case.py`: All Python files
- `service.py`: Main service implementation
- `repo.py`: Repository (database access)
- `*_test.py`: Test files (co-located with source)

**Directories:**
- `lowercase`: All directories
- Service directories: `services/{domain}/`

**Classes:**
- `PascalCase`: All classes
- `IService`: Interface classes prefixed with `I`
- `*Result`, `*Config`: Suffixes for data classes

**Functions:**
- `snake_case`: All functions
- `async def`: All database and I/O operations
- Private: `_underscore_prefix`

**SQL Files:**
- `table_name.sql`: Queries for single table
- Query names in aiosql: `-- name: get_hotel_by_id`

## Where to Add New Code

**New Feature (business logic):**
- Service method: `services/{domain}/service.py`
- Database queries: `db/queries/{table}.sql`
- Repository function: `services/{domain}/repo.py`
- Tests: `services/{domain}/*_test.py`

**New Workflow (CLI script):**
- Workflow script: `workflows/{name}.py`
- Follow pattern: argparse, init_db, try/finally close_db, slack notifications

**New Database Table:**
- Schema: `db/schema.sql` (append)
- Migration: `db/migrations/{date}_{name}.sql`
- Model: `db/models/{table}.py`
- Queries: `db/queries/{table}.sql`

**New External Integration:**
- Client: `infra/{service}.py`
- Environment variable: `.env.example`

**Utilities/Helpers:**
- Shared utilities: `services/{domain}/` (keep close to usage)
- One-off scripts: `scripts/utils/`

## Special Directories

**`archive/`:**
- Purpose: Legacy code and historical data
- Generated: No
- Committed: Yes (but could be .gitignored)

**`.planning/`:**
- Purpose: GSD planning documents
- Generated: By AI tools
- Committed: Yes

**`.venv/`:**
- Purpose: Python virtual environment
- Generated: Yes (by uv)
- Committed: No

**`context/`:**
- Purpose: Project documentation and task tracking
- Generated: No
- Committed: Yes

---

*Structure analysis: 2026-01-20*
