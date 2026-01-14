<p align="center">
  <img src="graphics/sadie-gtm-logo.svg" width="300" alt="Sadie GTM Logo">
</p>

# Sadie GTM Pipeline

Automated lead generation pipeline for hotel booking engine detection at scale.

## Pipeline Overview

Hotels flow through a status-based pipeline:

```
Scrape (0) → Detect (1) → Enrich (3) → Live (5) → Export (6)
                ↓
         No Engine (99)
```

**Services:**
- **leadgen** - Scrape hotels + detect booking engines
- **enrichment** - Add room counts + customer proximity
- **reporting** - Excel exports + OneDrive uploads

## Local Development

### Prerequisites

- Python 3.9+
- Docker (for PostgreSQL + PostGIS)
- [uv](https://github.com/astral-sh/uv) package manager

### Setup

```bash
# Start local database
docker compose up -d

# Apply schema
docker exec -i sadie-gtm-local-db psql -U sadie -d sadie_gtm < db/schema.sql

# Install dependencies
uv sync

# Install Playwright browsers
uv run playwright install chromium
```

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Database (local)
SADIE_DB_HOST=localhost
SADIE_DB_PORT=5432
SADIE_DB_NAME=sadie_gtm
SADIE_DB_USER=sadie
SADIE_DB_PASSWORD=sadie_dev_password

# Serper API (for scraping)
SERPER_API_KEY=your_serper_api_key
```

### Run Tests

```bash
uv run pytest -v
```

## Usage

### Scrape Hotels

```bash
# Estimate cost first
uv run python workflows/scrape_region.py --center-lat 25.79 --center-lng -80.13 --radius-km 5 --estimate

# Run scrape
uv run python workflows/scrape_region.py --center-lat 25.79 --center-lng -80.13 --radius-km 5

# Or scrape entire state
uv run python workflows/scrape_region.py --state florida --estimate
```

### Detect Booking Engines

```python
from services.leadgen import service

svc = service.Service()
results = await svc.detect_booking_engines(limit=100)
```

## Architecture

```
services/
├── leadgen/           # Scraping + detection
│   ├── service.py     # Business logic (exported interface)
│   ├── repo.py        # Database access
│   ├── grid_scraper.py
│   └── detector.py
├── enrichment/        # Room counts + proximity
└── reporting/         # Exports + uploads

db/
├── schema.sql         # Database schema
├── queries/           # SQL queries (aiosql)
└── models/            # Pydantic models
```

See [context/CODING_GUIDE.md](context/CODING_GUIDE.md) for development patterns.
