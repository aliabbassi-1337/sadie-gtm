# Pipeline State Machine & Crawl Data Ingestion

## What We Built

### 1. Pipeline State Machine

Replaced boolean hotel status with granular processing stages:

| Stage | Value | What It Means |
|-------|-------|---------------|
| INGESTED | 0 | Raw data from state sources (DBPR, Texas) |
| HAS_WEBSITE | 10 | Website found/validated |
| HAS_LOCATION | 20 | Geocoded with lat/lng |
| DETECTED | 30 | Booking engine identified |
| ENRICHED | 40 | Room count + proximity enriched |
| LAUNCHED | 100 | Ready for sales outreach |

**Why?** 
- Clear visibility into where hotels are stuck
- Easier to debug and prioritize
- Workflows can target specific stages

### 2. Common Crawl Integration

**Problem:** Finding hotels that use specific booking engines (Cloudbeds, Mews, etc.)

**Solution:** 
- Query Common Crawl archives (billions of web pages indexed)
- Extract hotel names and websites from archived booking pages
- ~21k Cloudbeds hotels identified

**Data flow:**
```
Crawl slugs → CDX API lookup → S3 archive fetch → Extract hotel info → Database
```

### 3. Orchestrated Pipeline

Single command to run everything:
```bash
uv run python -m workflows.crawl_pipeline
```

**Steps:**
1. Download crawl files from S3
2. Ingest into database (with deduplication)
3. Export Excel reports to S3

**Features:**
- Checkpoint/resume on failure
- Progress tracking per engine
- Incremental DB saves

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Cloudbeds slugs | ~21,000 |
| Mews slugs | TBD |
| RMS slugs | TBD |
| Siteminder slugs | TBD |

---

## Challenges & Solutions

### Rate Limiting
- Common Crawl CDX API returns 503 when hit too fast
- Solution: Serial requests with 0.5s delay
- Tradeoff: Slower but reliable

### Deduplication
Multi-tier matching to avoid duplicates:
1. Match by booking URL (most reliable)
2. Match by exact hotel name
3. Fuzzy match (PostgreSQL trigram similarity)
4. Insert as new if no match

### Missing Data
- Crawl data has: name, booking URL, sometimes city
- Missing: full address, state, phone, email
- Next step: Geocoding enrichment (Nominatim or Serper)

---

## What's Next

1. **Run full ingestion** on EC2 (~75 min for Cloudbeds)
2. **Geocoding enrichment** - add location data
3. **Prefect orchestration** - proper workflow management (deferred)
4. **More engines** - expand to other booking systems

---

## PRs

| PR | Title | Status |
|----|-------|--------|
| #65 | Pipeline State Machine | Merged |
| #56 | Cloudbeds Reverse Lookup | Merged |
| #68 | CDX Rate Limiting | Merged |
| #69 | CDX Rate Limiting v2 | Merged |
| #70 | Serial CDX Requests | Merged |

---

## Demo Commands

```bash
# Run the pipeline
uv run python -m workflows.crawl_pipeline

# Check progress
uv run python -m workflows.crawl_pipeline --status

# Query results
psql -c "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source LIKE '%commoncrawl%';"

# Export to Excel
uv run python -m workflows.export_crawl --all
```

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Crawl Data    │────▶│   Ingestion      │────▶│    Database     │
│   (S3 slugs)    │     │   Service        │     │    (Postgres)   │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │  Common Crawl    │
                        │  Archives (S3)   │
                        └──────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │  Hotel Info      │
                        │  (name, website) │
                        └──────────────────┘
```
