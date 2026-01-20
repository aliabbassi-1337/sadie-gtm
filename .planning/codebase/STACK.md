# Technology Stack

**Analysis Date:** 2026-01-20

## Languages

**Primary:**
- Python 3.9+ - All application code, workflows, services

**Secondary:**
- SQL - Database queries via aiosql (raw SQL files in `db/queries/`)
- YAML - Workflow definitions (`workflows.yaml`)

## Runtime

**Environment:**
- Python 3.9.6 (system Python on macOS)
- Virtual environment: `.venv/` (managed by uv)

**Package Manager:**
- uv - Modern Python package manager
- Lockfile: `uv.lock` (present, 242KB)
- Legacy: `requirements.txt` (minimal, 5 deps)

**Project Definition:**
- `pyproject.toml` - PEP 621 project metadata

## Frameworks

**Core:**
- Pydantic 2.12.5+ - Data models, validation, settings
- asyncio - Async runtime for all services
- httpx 0.25.0+ - Async HTTP client

**Web Scraping:**
- Playwright 1.40.0+ - Browser automation for booking engine detection

**Database:**
- asyncpg 0.31.0+ - Async PostgreSQL driver
- aiosql 13.4 - SQL file loading (raw queries, no ORM)
- psycopg2-binary 2.9.11+ - Sync PostgreSQL driver (for migra)

**Testing:**
- pytest 8.4.2+ - Test runner
- pytest-asyncio 1.2.0+ - Async test support

**Build/Dev:**
- migra 3.0+ - PostgreSQL schema diffing

## Key Dependencies

**Critical:**
- `asyncpg` - Database connection pooling, async queries
- `playwright` - Website scraping for booking engine detection
- `httpx` - API calls to Serper, Groq, Nominatim
- `pydantic` - All data models across services

**Infrastructure:**
- `boto3` 1.42.27+ - AWS SDK (SQS, S3)
- `pyyaml` 6.0.3+ - YAML workflow config parsing

**Data Processing:**
- `openpyxl` 3.1.0+ - Excel report generation
- `loguru` 0.7.0+ - Structured logging

**Environment:**
- `python-dotenv` 1.0.0+ - .env file loading

## Configuration

**Environment:**
- `.env` - All secrets and configuration
- `.env.example` - Template with all required variables
- Environment loaded via `python-dotenv` at module import

**Key Environment Variables:**
```bash
# Database
SADIE_DB_HOST=localhost
SADIE_DB_PORT=5432
SADIE_DB_NAME=sadie_gtm
SADIE_DB_USER=sadie
SADIE_DB_PASSWORD=<password>

# AWS
AWS_REGION=eu-north-1
SQS_DETECTION_QUEUE_URL=https://sqs...
S3_BUCKET_NAME=sadie-gtm

# APIs
SERPER_API_KEY=<key>           # Google Maps scraping
ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY=<key>  # LLM enrichment

# Notifications
SLACK_WEBHOOK_URL=<url>
SLACK_LEADS_WEBHOOK_URL=<url>
```

**Build:**
- `pyproject.toml` - Dependencies and pytest config
- `docker-compose.yml` - Local PostgreSQL with PostGIS

## Platform Requirements

**Development:**
- Python 3.9+
- Docker (for local PostgreSQL)
- Playwright browsers (`playwright install`)
- uv package manager

**Production:**
- EC2 instances (detection/enrichment workers)
- Supabase PostgreSQL (production database)
- AWS SQS (job queue)
- AWS S3 (report storage)

## Commands

**Run Commands:**
```bash
# Install dependencies
uv sync

# Run workflow
uv run python -m workflows.scrape_polygon --state FL

# Run tests
uv run pytest

# Start local database
docker-compose up -d

# Install Playwright browsers
uv run playwright install
```

**Test Commands:**
```bash
uv run pytest                           # Run all tests
uv run pytest -m "not integration"      # Skip integration tests
uv run pytest -m "not online"           # Skip external API tests
uv run pytest services/leadgen/         # Run specific service tests
```

---

*Stack analysis: 2026-01-20*
