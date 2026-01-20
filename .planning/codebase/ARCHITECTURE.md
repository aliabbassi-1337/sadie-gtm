# Architecture

**Analysis Date:** 2026-01-20

## Pattern Overview

**Overall:** Service-Repository-Workflow pattern with message queue orchestration

**Key Characteristics:**
- Services encapsulate business logic behind abstract interfaces (IService)
- Repositories handle all database access through aiosql queries
- Workflows are CLI entry points that orchestrate service calls
- SQS message queues decouple local scraping from EC2-based detection
- Status-based pipeline tracks hotels through processing stages

## Layers

**Workflows Layer:**
- Purpose: CLI entry points and orchestration scripts
- Location: `workflows/`
- Contains: Argument parsing, database init/cleanup, service invocation
- Depends on: Services, DB client, Infra (slack, sqs)
- Used by: Command line, cron jobs, systemd services

**Services Layer:**
- Purpose: Business logic encapsulation
- Location: `services/`
- Contains: Service classes with IService interfaces, domain models, specialized processors
- Depends on: Repositories, external APIs (Serper, Groq)
- Used by: Workflows

**Repository Layer:**
- Purpose: Database access abstraction
- Location: `services/*/repo.py`
- Contains: Async functions wrapping aiosql queries
- Depends on: DB client, aiosql queries
- Used by: Services

**Database Layer:**
- Purpose: PostgreSQL/PostGIS connection management and SQL queries
- Location: `db/`
- Contains: Connection pool (`client.py`), SQL queries (`queries/`), Pydantic models (`models/`)
- Depends on: asyncpg, aiosql
- Used by: Repositories

**Infrastructure Layer:**
- Purpose: External service integrations (AWS, Slack)
- Location: `infra/`
- Contains: SQS client, S3 upload, Slack notifications
- Depends on: boto3, environment variables
- Used by: Workflows, Services

## Data Flow

**Lead Generation Pipeline:**

```
1. Ingest Regions (workflows/ingest_regions.py)
   → Fetch city polygons from OSM
   → Store in scrape_regions table

2. Scrape Hotels (workflows/scrape_polygon.py, scrape_bbox.py)
   → GridScraper queries Serper API
   → Batch insert to hotels table (status=0)

3. Enqueue Detection (workflows/enqueue_detection.py)
   → Query pending hotels (status=0, no booking_engines record)
   → Send batches to SQS

4. Detection Consumer (workflows/detection_consumer.py) [EC2]
   → Poll SQS for batches
   → BatchDetector visits websites with Playwright
   → Insert to hotel_booking_engines or update status to -1

5. Enrichment (workflows/enrichment.py) [EC2]
   → Room count: Groq LLM extraction → hotel_room_count
   → Proximity: PostGIS nearest customer → hotel_customer_proximity

6. Launcher (workflows/launcher.py) [EC2]
   → Find hotels with all enrichments complete
   → Update status to 1 (launched)

7. Export (workflows/export.py)
   → Generate Excel reports
   → Upload to S3
```

**Hotel Status Values:**
- `-3`: duplicate (same placeId/location/name)
- `-2`: location_mismatch (rejected)
- `-1`: no_booking_engine (rejected)
- `0`: pending (in pipeline)
- `1`: launched (live lead)

**State Management:**
- Hotels tracked via integer `status` field
- Detection completion tracked by presence of `hotel_booking_engines` record
- Enrichment tracked by presence of `hotel_room_count` and `hotel_customer_proximity` records
- Multi-worker safety via `FOR UPDATE SKIP LOCKED` in launcher

## Key Abstractions

**Service Interfaces:**
- Purpose: Define contracts for business operations
- Examples: `services/leadgen/service.py::IService`, `services/enrichment/service.py::IService`
- Pattern: Abstract base class with concrete implementation

**Detection Pipeline:**
- Purpose: Visit websites and identify booking engines
- Examples: `services/leadgen/detector.py::BatchDetector`, `services/leadgen/detector.py::HotelProcessor`
- Pattern: Batch processing with Playwright browser reuse, HTTP pre-checks, semaphore-controlled concurrency

**Grid Scraper:**
- Purpose: Systematic geographic coverage of hotel searches
- Examples: `services/leadgen/grid_scraper.py::GridScraper`
- Pattern: Adaptive grid subdivision based on result density

**Pydantic Models:**
- Purpose: Type-safe data transfer objects
- Examples: `db/models/hotel.py::Hotel`, `services/leadgen/detector.py::DetectionResult`
- Pattern: Pydantic BaseModel with `model_validate(dict(row))` for DB conversion

## Entry Points

**Main Workflow Entry:**
- Location: `main.py`
- Triggers: `python main.py <workflow_name>`
- Responsibilities: Initialize DB pool, route to workflow, cleanup

**Direct Workflow Execution:**
- Location: `workflows/*.py`
- Triggers: `uv run python -m workflows.<name>` or `uv run python workflows/<name>.py`
- Responsibilities: Argument parsing, service instantiation, execution

**EC2 Systemd Service:**
- Location: `workflows/detection_consumer.py`
- Triggers: systemd unit on EC2 boot
- Responsibilities: Continuous SQS polling, graceful shutdown on SIGTERM

**EC2 Cron Jobs:**
- Location: `workflows/enrichment.py`, `workflows/launcher.py`
- Triggers: Cron schedule (every 2 minutes)
- Responsibilities: Batch processing of pending records

## Error Handling

**Strategy:** Exception propagation with per-record error capture

**Patterns:**
- Detection errors logged to `detection_errors` table with error_type classification
- Non-retriable errors create `hotel_booking_engines` record with status=-1 to prevent retry loops
- Workflow-level exceptions trigger Slack notifications via `infra/slack.py`
- SQS messages not deleted on failure (automatic retry via visibility timeout)

## Cross-Cutting Concerns

**Logging:**
- Framework: loguru
- Pattern: Module-level logger, debug mode controlled by config flag

**Validation:**
- Input validation via Pydantic models
- Hotel filtering: skip chains, junk domains, non-hotel businesses (detector.py lists)

**Authentication:**
- Database: Environment variables (`SADIE_DB_*`)
- APIs: Environment variables (`SERPER_API_KEY`, `ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY`)
- AWS: boto3 default credential chain

**Notifications:**
- Slack integration via `infra/slack.py`
- Used for workflow completion, errors, and pipeline status

---

*Architecture analysis: 2026-01-20*
