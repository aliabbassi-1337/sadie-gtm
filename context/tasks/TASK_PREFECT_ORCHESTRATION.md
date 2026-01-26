# Task: Prefect Orchestration

## Goal
Add Prefect to orchestrate crawl data ingestion with proper tracking, retries, and observability.

## Status
**Deferred** - Shipping with simple checkpoint-based script for now. Revisit when running pipelines regularly.

## Why Prefect
- Automatic retries on failure
- Visual dashboard to track progress
- Per-engine task tracking
- Resume capability built-in
- Easy to add new engines later

## Current Solution (Shipped)
Using `workflows/crawl_pipeline.py` with JSON checkpointing:
```bash
uv run python -m workflows.crawl_pipeline           # Run full pipeline
uv run python -m workflows.crawl_pipeline --resume  # Resume if failed
uv run python -m workflows.crawl_pipeline --status  # Check progress
```

Progress tracked in: `data/crawl_pipeline_state.json`

## Future Architecture

```
flows/
├── crawl_pipeline.py     # Prefect flow: download → ingest → export
└── __init__.py
```

### Prefect Flow Design

```python
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

@task(retries=2, retry_delay_seconds=60)
async def download_engine(engine: str, s3_file: str) -> Path:
    """Download crawl file from S3."""
    ...

@task(retries=1)
async def ingest_engine(engine: str, file_path: Path) -> dict:
    """Ingest crawl data for one engine."""
    service = Service()
    return await service.ingest_crawled_urls(
        file_path=str(file_path),
        booking_engine=engine,
        source_tag="commoncrawl",
    )

@task
async def export_engine(engine: str) -> str:
    """Export engine data to Excel."""
    service = ReportingService()
    s3_uri, count = await service.export_by_booking_engine(engine)
    return s3_uri

@flow(name="crawl-pipeline", task_runner=ConcurrentTaskRunner())
async def crawl_pipeline(engines: list[str] = None):
    """Full crawl pipeline: download -> ingest -> export."""
    engines = engines or ["cloudbeds", "mews", "rms", "siteminder"]
    
    # Download all files (parallel)
    files = {}
    for engine in engines:
        files[engine] = await download_engine(engine, ENGINE_FILES[engine])
    
    # Ingest all engines (parallel)
    stats = {}
    for engine in engines:
        stats[engine] = await ingest_engine(engine, files[engine])
    
    # Export all engines (parallel)
    exports = {}
    for engine in engines:
        exports[engine] = await export_engine(engine)
    
    return {"stats": stats, "exports": exports}
```

## Implementation Steps (When Ready)

1. Add `prefect>=3.0.0` to pyproject.toml
2. Create `flows/crawl_pipeline.py` with download, ingest, export tasks
3. Delete `workflows/crawl_pipeline.py` (JSON checkpoint version)
4. Test flow locally with one engine

## Running Prefect

```bash
# Install
uv add prefect

# Local run (with dashboard at http://localhost:4200)
prefect server start  # Terminal 1
uv run python -m flows.crawl_pipeline  # Terminal 2

# Or just run directly (no dashboard)
uv run python -m flows.crawl_pipeline
```

## Adding New Engines

Just add to `ENGINE_FILES`:
```python
ENGINE_FILES = {
    "cloudbeds": "cloudbeds_deduped.txt",
    "mews": "mews.txt",
    "rms": "rms.txt",
    "siteminder": "siteminder.txt",
    "new_engine": "new_engine.txt",  # Just add here
}
```

## What Stays the Same

- `services/ingestor/` - BaseIngestor pattern unchanged (for DBPR, Texas)
- `services/leadgen/service.py` - `ingest_crawled_urls` unchanged
- `services/reporting/service.py` - `export_by_booking_engine` unchanged
- `workflows/ingest_crawl.py` - Keep for manual one-off runs

## Decision Log

- **2026-01-25**: Deferred Prefect integration. Shipping with JSON checkpoint script to avoid complexity. Will revisit when running pipelines regularly.
