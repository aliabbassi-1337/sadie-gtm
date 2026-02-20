# Architecture Patterns: Multi-Source Enrichment DAG

**Project:** Sadie GTM Owner Enrichment
**Researched:** 2026-02-20
**Mode:** Ecosystem (brownfield, existing Service-Repository-Workflow)

---

## Executive Summary

This document addresses four architectural concerns for scaling the existing enrichment pipeline from its current "waterfall per hotel" model to a full DAG-based multi-source enrichment system handling 100K+ records. The existing architecture is solid -- Service-Repository-Workflow with SQS, Fargate, PostgreSQL, unnest() batch patterns, bitmask layer tracking, and atomic claiming. The recommendations below extend rather than replace what exists.

The core architectural shift: move from "one workflow enriches one record through N layers" to "N independent sources feed data into a convergence layer that maintains a golden record per hotel." This is the difference between a waterfall and a DAG.

---

## 1. DAG/Pipeline Architecture

### Current State: Waterfall

The existing `owner_enricher.py` runs a waterfall for each hotel:

```
Hotel record
  -> Phase 1 (parallel): CT, RDAP, WHOIS, DNS, Website, Reviews, Gov, ABN/ASIC
  -> Phase 2 (sequential): Email verification (needs contacts + DNS from Phase 1)
  -> Persist: batch_persist_results (5 unnest queries)
```

This works well for single-hotel enrichment where all layers need the same input (a hotel + domain). But it breaks down when:
- Sources have different entry points (Common Crawl starts with URLs, Google Maps starts with places, gov data starts with license records)
- Sources have wildly different throughput (DNS resolves in 100ms; WHOIS scraping takes 5s; CC index queries take 30s)
- Some sources produce hotels themselves (CC, Google Maps) rather than enriching existing ones

### Recommended: Hub-and-Spoke DAG

Instead of a monolithic waterfall per hotel, structure as independent source pipelines converging on a shared hotel record.

```
                 +------------------+
                 |  DISCOVERY TIER  |  (produces hotel stubs)
                 +------------------+
                       |
     +--------+--------+--------+--------+
     |        |        |        |        |
  Google    Common   Gov Data  Direct   Crawl
  Maps      Crawl    (DBPR,    URLs     Data
  Scrape    Index    Texas...)  Import   (RMS, etc.)
     |        |        |        |        |
     v        v        v        v        v
  +----------------------------------------------+
  |         CONVERGENCE / ENTITY RESOLUTION       |
  |  (deduplicate, merge, create golden record)   |
  +----------------------------------------------+
                       |
                       v
              +-----------------+
              | ENRICHMENT TIER |  (enriches existing records)
              +-----------------+
                       |
     +--------+--------+--------+--------+--------+
     |        |        |        |        |        |
   RDAP    WHOIS    DNS      Website  Reviews   Email
   Lookup  History  Intel    Scrape   Mining    Verify
     |        |        |        |        |        |
     v        v        v        v        v        v
  +----------------------------------------------+
  |           GOLDEN RECORD MERGE                 |
  |  (field-level priority, confidence scoring)   |
  +----------------------------------------------+
                       |
                       v
              +-----------------+
              |  ACTION TIER    |
              +-----------------+
                       |
           +-----------+-----------+
           |           |           |
        Export      Lead Score   Outbound
        (Excel/S3)  (ranking)   (future)
```

### Three-Tier Decomposition

**Tier 1: Discovery (produces hotel stubs)**
- Each source runs independently on its own schedule
- Output: minimal hotel record (name, maybe address, maybe URL, source reference)
- Triggers entity resolution on output
- Examples: Google Maps scraping, Common Crawl URL extraction, gov data ingestion, direct CSV import

**Tier 2: Enrichment (fills in fields on existing records)**
- Each enrichment layer runs independently
- Input: hotel_id + whatever that layer needs (domain for DNS, name+city for gov, website for scraping)
- Output: partial record update (just the fields that layer can provide)
- Tracked via bitmask (already implemented in `layers_completed`)
- Examples: RDAP, WHOIS, DNS, Website scrape, Review mining, Email verify, ABN/ASIC

**Tier 3: Action (consumes enriched records)**
- Triggered when enrichment reaches sufficient completeness
- Input: fully or partially enriched hotel record
- Output: business artifacts (exports, scores, notifications)
- Examples: Excel export, lead scoring, Slack notification, CRM push

### Layer Dependencies (The Actual DAG)

Not all layers are independent. The DAG edges represent real data dependencies:

```
Discovery sources   -->  Entity Resolution  -->  Hotel Record Created
                                                       |
                          +----------------------------+
                          |
             +------ Parallel (no deps) ------+
             |            |           |        |
           RDAP        DNS         Gov      CT Certs
             |            |        Data        |
             v            v                    v
          WHOIS      Email Provider       Org Name
         (fallback   (needed for email    (cross-ref
          if RDAP     verification)        with WHOIS)
          privacy)        |
             |            |
             +------+-----+
                    |
                    v
              Website Scrape  (can run anytime, but benefits
                    |          from knowing org name)
                    v
              Email Verify    (REQUIRES: contacts from above
                               + email_provider from DNS)
                    |
                    v
              Lead Score      (REQUIRES: contacts + emails
                               + booking engine + room count)
```

**Key dependency insight:** Email verification MUST run after both contact discovery and DNS analysis. Everything else can run in parallel. The current code already handles this correctly with its Phase 1 (parallel) / Phase 2 (email verify) split. This pattern should be preserved in the DAG.

### Partial Results and Incremental Enrichment

The bitmask approach already handles this well. Extend it:

```python
# Current bitmask (already in owner_models.py)
LAYER_RDAP = 1          # 0b000000001
LAYER_WHOIS_HISTORY = 2 # 0b000000010
LAYER_DNS = 4           # 0b000000100
LAYER_WEBSITE = 8       # 0b000001000
LAYER_REVIEWS = 16      # 0b000010000
LAYER_EMAIL_VERIFY = 32 # 0b000100000
LAYER_GOV_DATA = 64     # 0b001000000
LAYER_CT_CERTS = 128    # 0b010000000
LAYER_ABN_ASIC = 256    # 0b100000000

# Proposed additions for new sources
LAYER_CC_HARVEST = 512   # 0b0000001000000000  Common Crawl HTML
LAYER_CC_CONTACTS = 1024 # 0b0000010000000000  CC contact extraction
LAYER_GMAPS_ENRICH = 2048  # Google Maps details
LAYER_LEAD_SCORE = 4096    # Composite scoring
```

**Re-enrichment query pattern** (already supported by existing SQL):

```sql
-- Find hotels missing a specific layer
SELECT h.id, h.name, h.website
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
WHERE h.website IS NOT NULL
  AND (hoe.hotel_id IS NULL OR hoe.layers_completed & :layer_mask = 0)
ORDER BY h.id
LIMIT :limit;
```

This is already implemented in `get_hotels_pending_owner_enrichment_by_layer`. Adding new layers only requires adding new bitmask constants.

### Idempotent Re-enrichment

The existing pattern is already idempotent because of the `ON CONFLICT ... DO UPDATE` in both `insert_decision_maker` and `update_enrichment_status`. Key properties to maintain:

1. **Additive fields use COALESCE:** New data only overwrites NULL/empty, never clobbers existing data
2. **Bitmask uses OR:** `layers_completed | :new_layers` accumulates, never resets
3. **Sources array uses DISTINCT:** `array_agg(DISTINCT s)` prevents duplicate source tags
4. **Confidence uses GREATEST:** Higher confidence always wins
5. **Timestamps always update:** `updated_at = NOW()` tracks freshness

**Recommendation:** This pattern is correct and should not change. The only addition needed is a `force_refresh` flag that, when set, bypasses the bitmask check and re-runs a layer even if already completed. Useful for periodic data freshness passes.

---

## 2. Scale Patterns (100K+ Records)

### Batch vs Streaming: Use Batch, With Event Triggers

**Recommendation: Batch processing with event-driven triggers.** Not streaming.

Rationale:
- Data changes slowly (hotel ownership changes monthly, not per-second)
- External APIs have rate limits that make streaming wasteful (RDAP: ~2 req/sec; Serper: 100/month on free tier)
- The existing SQS + Fargate architecture is batch-native
- PostgreSQL unnest() batch patterns are already optimized
- No real-time consumer of enrichment data (sales team checks weekly, not per-minute)

The "event" triggers are:
- New hotels ingested (discovery tier output) -> enqueue for enrichment
- Enrichment layer completed -> check if downstream layers can now run
- All enrichment complete -> trigger export / notification
- Scheduled re-enrichment -> monthly pass over stale records

### Worker Pool Management

**Current pattern (good):**
```python
sem = asyncio.Semaphore(concurrency)
async with httpx.AsyncClient(limits=pool_limits) as client:
    async def process_one(hotel):
        async with sem:
            result = await enrich_single_hotel(client, hotel, ...)
        # Buffer + flush outside semaphore
        if persist:
            async with flush_lock:
                pending_buffer.append(result)
            if should_flush:
                await _flush()
    tasks = [process_one(h) for h in hotels]
    results = await asyncio.gather(*tasks, return_exceptions=True)
```

**Recommended improvements:**

#### A. Per-Source Rate Limiters (not just global concurrency)

Different external services have different rate limits. A single semaphore treats all work equally, but RDAP allows 2 req/sec while DNS allows 50 req/sec.

```python
from dataclasses import dataclass
from asyncio import Semaphore
import time

@dataclass
class SourceLimiter:
    """Per-source rate limiter with semaphore + token bucket."""
    name: str
    max_concurrent: int          # semaphore size
    max_per_second: float        # token bucket rate
    _sem: Semaphore = None
    _tokens: float = 0
    _last_refill: float = 0

    def __post_init__(self):
        self._sem = Semaphore(self.max_concurrent)
        self._tokens = self.max_concurrent
        self._last_refill = time.monotonic()

    async def acquire(self):
        await self._sem.acquire()
        # Token bucket: wait if we're sending too fast
        now = time.monotonic()
        self._tokens += (now - self._last_refill) * self.max_per_second
        self._tokens = min(self._tokens, self.max_concurrent)
        self._last_refill = now
        if self._tokens < 1:
            wait = (1 - self._tokens) / self.max_per_second
            await asyncio.sleep(wait)
            self._tokens = 0
        else:
            self._tokens -= 1

    def release(self):
        self._sem.release()

# Usage:
LIMITERS = {
    "rdap": SourceLimiter("rdap", max_concurrent=3, max_per_second=2),
    "dns": SourceLimiter("dns", max_concurrent=20, max_per_second=50),
    "whois_wayback": SourceLimiter("whois_wayback", max_concurrent=5, max_per_second=3),
    "website": SourceLimiter("website", max_concurrent=10, max_per_second=10),
    "serper": SourceLimiter("serper", max_concurrent=2, max_per_second=0.5),
    "crt_sh": SourceLimiter("crt_sh", max_concurrent=2, max_per_second=1),
}
```

#### B. Back-Pressure via Bounded Queue

The current pattern launches all tasks immediately via `asyncio.gather`. For 10K+ records, this creates 10K+ coroutines holding memory. Better: bounded producer-consumer.

```python
async def enrich_batch_with_backpressure(
    hotels: list[dict],
    concurrency: int = 10,
    buffer_size: int = 50,
) -> list[OwnerEnrichmentResult]:
    """Producer-consumer with bounded queue for back-pressure."""
    work_queue = asyncio.Queue(maxsize=buffer_size)
    results = []
    results_lock = asyncio.Lock()

    async def producer():
        for hotel in hotels:
            await work_queue.put(hotel)  # blocks when queue full
        for _ in range(concurrency):
            await work_queue.put(None)   # poison pill

    async def worker():
        while True:
            hotel = await work_queue.get()
            if hotel is None:
                break
            try:
                result = await enrich_single_hotel(client, hotel, ...)
                async with results_lock:
                    results.append(result)
                    if len(results) % flush_interval == 0:
                        await _flush(results[-flush_interval:])
            except Exception as e:
                logger.error(f"Worker error: {e}")
            finally:
                work_queue.task_done()

    async with httpx.AsyncClient(...) as client:
        producer_task = asyncio.create_task(producer())
        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await producer_task
        await asyncio.gather(*workers)

    return results
```

**When to adopt:** When batch sizes exceed ~1000 hotels. Below that, the current `asyncio.gather` approach is fine.

#### C. Adaptive Concurrency

React to external service health by adjusting concurrency dynamically:

```python
class AdaptiveLimiter:
    """Adjusts concurrency based on error rates."""
    def __init__(self, initial: int = 10, min_c: int = 1, max_c: int = 50):
        self.current = initial
        self.min_c = min_c
        self.max_c = max_c
        self._sem = asyncio.Semaphore(initial)
        self._success = 0
        self._failure = 0
        self._window_start = time.monotonic()

    def record_success(self):
        self._success += 1
        self._maybe_adjust()

    def record_failure(self):
        self._failure += 1
        self._maybe_adjust()

    def _maybe_adjust(self):
        total = self._success + self._failure
        if total < 20:
            return
        error_rate = self._failure / total
        if error_rate > 0.3:
            # Back off: halve concurrency
            self.current = max(self.min_c, self.current // 2)
        elif error_rate < 0.05 and self.current < self.max_c:
            # Speed up: add 2
            self.current = min(self.max_c, self.current + 2)
        # Reset window
        self._success = 0
        self._failure = 0
```

### Database Write Optimization

**Current pattern is already near-optimal.** The `batch_persist_results` function does 5 unnest queries per flush (every 20 hotels), which is excellent. Specific recommendations:

1. **Increase flush interval for large batches:** At 100K hotels with 5 concurrent workers, the DB gets hit every ~4 seconds. Increase `FLUSH_INTERVAL` from 20 to 50 for runs > 1000 hotels.

2. **Connection pooling awareness:** The current `get_conn()` context manager should be wrapping asyncpg pool connections. Verify the pool size is at least `concurrency + 2` (for flush writes happening concurrent with reads).

3. **Consider COPY for discovery tier:** When ingesting 100K+ records from Common Crawl or CSV import, `COPY ... FROM STDIN` is 5-10x faster than `INSERT ... UNNEST`. Use `COPY` for bulk discovery, keep `INSERT ... UNNEST` for enrichment upserts.

4. **Partial index for pending work:**
```sql
-- Already exists for DM lookups:
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dm_people_partial
ON sadie_gtm.hotel_decision_makers (hotel_id)
WHERE full_name LIKE '% %';

-- Add for enrichment work queue:
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enrichment_pending
ON sadie_gtm.hotel_owner_enrichment (hotel_id)
WHERE status != 1;  -- not complete
```

---

## 3. Data Convergence (Multi-Source -> Golden Record)

### Entity Resolution Strategy

Hotels from different sources must be matched to avoid duplicate records. The existing `deduplicate_unified.py` handles this with a three-stage approach:

1. **External ID match** (100% accurate): Same external_id + external_id_type
2. **RMS Client ID match** (100% accurate): Extracted from booking URLs
3. **Name + City + Engine match** (high accuracy): Normalized fuzzy matching within same engine

**Recommendation: Add a fourth stage for cross-source matching.**

```
Stage 4: Cross-Source Entity Resolution
  Input: Hotels from different sources (CC, Google Maps, gov, direct)
  Matching signals (weighted):
    - Domain match (hotel website)     weight: 0.9 (strongest signal)
    - Phone match                      weight: 0.8
    - Name + City exact                weight: 0.7
    - Name fuzzy + City exact          weight: 0.5
    - Address + City                   weight: 0.6
    - Google Place ID match            weight: 1.0 (definitive)

  Threshold: combined score >= 0.8 -> auto-merge
             combined score 0.5-0.8 -> flag for review
             combined score < 0.5 -> treat as separate
```

**Implementation approach:**

```sql
-- Cross-source matching candidates
-- Find potential matches for newly ingested hotels
WITH new_hotels AS (
    SELECT id, name, website, city, state, phone_google, google_place_id
    FROM sadie_gtm.hotels
    WHERE created_at > NOW() - INTERVAL '1 day'
      AND status >= 0
)
SELECT n.id AS new_id, e.id AS existing_id,
    CASE
        WHEN n.google_place_id = e.google_place_id THEN 1.0
        WHEN _extract_domain(n.website) = _extract_domain(e.website)
             AND _extract_domain(n.website) IS NOT NULL THEN 0.9
        WHEN n.phone_google = e.phone_google
             AND n.phone_google IS NOT NULL THEN 0.8
        WHEN LOWER(n.name) = LOWER(e.name)
             AND LOWER(n.city) = LOWER(e.city) THEN 0.7
        ELSE 0.0
    END AS match_score
FROM new_hotels n
JOIN sadie_gtm.hotels e ON e.id != n.id AND e.status >= 0
WHERE (match_score) >= 0.5
ORDER BY match_score DESC;
```

### Source Priority and Conflict Resolution

When multiple sources provide the same field, use a priority hierarchy:

```python
# Source priority for field-level merging (higher = preferred)
SOURCE_PRIORITY = {
    # Contact info
    "email": {
        "email_verify_o365": 100,    # Verified via O365 = highest trust
        "email_verify_smtp": 90,     # Verified via SMTP
        "gov_registry": 80,          # Government records
        "website_scrape": 70,        # Hotel's own website
        "rdap": 60,                  # RDAP registrant
        "whois_history": 50,         # Wayback WHOIS
        "review_response": 40,       # Google review responses
        "llm_extract": 35,           # LLM extraction from text
        "cc_harvest": 30,            # Common Crawl HTML
        "email_pattern_guess": 20,   # Pattern-guessed, unverified
    },
    # Name
    "full_name": {
        "gov_registry": 90,
        "asic_director": 85,
        "website_scrape": 80,
        "rdap": 70,
        "whois_history": 60,
        "review_response": 50,
        "llm_extract": 45,
    },
    # Phone
    "phone": {
        "gov_registry": 90,
        "website_scrape": 80,
        "google_maps": 75,
        "cc_harvest": 50,
    },
}
```

**Golden record merge logic** (extends existing `ON CONFLICT` pattern):

```sql
-- Current pattern (already good for additive merge):
ON CONFLICT (hotel_id, full_name, title) DO UPDATE
SET email = COALESCE(NULLIF(EXCLUDED.email, ''), hotel_decision_makers.email),
    email_verified = EXCLUDED.email_verified OR hotel_decision_makers.email_verified,
    sources = (SELECT array_agg(DISTINCT s) FROM unnest(
        array_cat(hotel_decision_makers.sources, EXCLUDED.sources)) s),
    confidence = GREATEST(EXCLUDED.confidence, hotel_decision_makers.confidence)
```

**Recommended enhancement -- priority-aware merge:**

```sql
-- Only overwrite email if new source has higher priority
ON CONFLICT (hotel_id, full_name, title) DO UPDATE
SET email = CASE
    WHEN EXCLUDED.confidence > hotel_decision_makers.confidence
         AND EXCLUDED.email IS NOT NULL AND EXCLUDED.email != ''
    THEN EXCLUDED.email
    ELSE COALESCE(NULLIF(hotel_decision_makers.email, ''), EXCLUDED.email)
    END,
    -- ...rest of merge logic
```

In practice, the confidence field already serves as a proxy for source priority. Encode source priority into confidence scores at the layer level rather than complicating the SQL merge logic. This is what the existing code does -- RDAP gets `confidence=0.7`, gov data gets `confidence=0.9`, etc.

### Confidence Scoring Model

```python
# Base confidence by source (already partially implemented)
BASE_CONFIDENCE = {
    "gov_registry": 0.95,    # Government = highest
    "asic_director": 0.90,   # Corporate regulator
    "abn_lookup": 0.85,      # Tax registry
    "rdap": 0.75,            # Domain registry (but may be privacy-masked)
    "website_scrape": 0.70,  # Hotel's own site
    "whois_history": 0.65,   # Historical WHOIS (may be stale)
    "review_response": 0.55, # Google reviews (name only, often first name)
    "llm_extract": 0.50,     # LLM extraction (can hallucinate)
    "cc_harvest": 0.45,      # Common Crawl HTML (may be stale)
}

# Modifiers
CONFIDENCE_MODIFIERS = {
    "email_verified_o365": +0.20,   # Verified email
    "email_verified_smtp": +0.15,   # SMTP verified
    "multiple_sources": +0.10,      # Corroborated by 2+ sources
    "stale_data": -0.15,           # Data > 12 months old
    "first_name_only": -0.30,      # No surname
    "generic_title": -0.10,        # "Manager" vs "General Manager, Hilton Downtown"
}
```

### Audit Trail / Data Provenance

**Current:** The `sources` array on `hotel_decision_makers` tracks which sources contributed. The `raw_source_url` field tracks the original URL. This is a minimal provenance trail.

**Recommended addition -- field-level provenance table:**

```sql
CREATE TABLE sadie_gtm.field_provenance (
    id BIGSERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL REFERENCES sadie_gtm.hotels(id),
    dm_id INTEGER REFERENCES sadie_gtm.hotel_decision_makers(id),
    field_name TEXT NOT NULL,        -- 'email', 'full_name', 'phone', etc.
    field_value TEXT,
    source TEXT NOT NULL,            -- 'rdap', 'website_scrape', etc.
    confidence FLOAT NOT NULL,
    source_url TEXT,                 -- Where the data came from
    extracted_at TIMESTAMPTZ DEFAULT NOW(),
    superseded_at TIMESTAMPTZ,       -- When a higher-confidence value replaced this
    superseded_by BIGINT             -- FK to the row that replaced this
);

-- Index for quick lookups
CREATE INDEX idx_provenance_hotel ON sadie_gtm.field_provenance (hotel_id, field_name);
CREATE INDEX idx_provenance_dm ON sadie_gtm.field_provenance (dm_id, field_name);
```

**When to build:** This is a "nice to have" for debugging and quality auditing. Do NOT build this in the first pass. Build it when:
- You need to answer "why does this record have this email?"
- You need to compare source quality across the pipeline
- You need to roll back bad enrichment runs

For now, the `sources` array + `raw_source_url` + `layers_completed` bitmask provides enough traceability.

---

## 4. Orchestration Patterns

### Recommended: Hybrid Event-Driven + Scheduled

**Do not adopt a workflow orchestrator (Airflow, Prefect, Dagster).** The existing architecture is simpler and sufficient. Here is why:

1. The pipeline has ~10 stages, not 100. The complexity does not justify an orchestrator.
2. The existing SQS + consumer pattern already provides decoupled, retryable execution.
3. Adding Airflow to a Fargate + SQS setup adds infrastructure cost and operational burden.
4. The team is one person (the founder). An orchestrator is overhead.

Instead, formalize the existing patterns into a consistent orchestration model:

```
+------------------+     +------------------+     +------------------+
|  SCHEDULED       |     |  EVENT-DRIVEN    |     |  ON-DEMAND       |
|  (cron/ECS task) |     |  (SQS trigger)   |     |  (CLI)           |
+------------------+     +------------------+     +------------------+
|                  |     |                  |     |                  |
| - Re-enrichment  |     | - New hotel      |     | - Backfill       |
|   (weekly/monthly|     |   ingested ->    |     | - Debug single   |
|   stale data)    |     |   enqueue for    |     |   hotel          |
|                  |     |   enrichment     |     | - Force re-run   |
| - CC index scan  |     |                  |     |   specific layer |
|   (weekly)       |     | - Enrichment     |     |                  |
|                  |     |   complete ->    |     | - Export          |
| - Stale claim    |     |   trigger next   |     |                  |
|   recovery       |     |   stage          |     |                  |
+------------------+     +------------------+     +------------------+
         |                       |                       |
         v                       v                       v
    +--------------------------------------------------+
    |                  SQS QUEUES                        |
    |  - owner-enrichment (existing)                    |
    |  - contact-enrichment (new)                       |
    |  - discovery-ingestion (new)                      |
    |  - enrichment-complete (new, for chaining)        |
    +--------------------------------------------------+
         |                       |
         v                       v
    +--------------------------------------------------+
    |              FARGATE WORKERS / LOCAL              |
    |  - Consume from queue                             |
    |  - Process batch                                  |
    |  - Persist results                                |
    |  - Optionally enqueue downstream work             |
    +--------------------------------------------------+
```

### Pipeline Chaining Pattern

When one stage completes, it can trigger the next. Use a lightweight "completion event" pattern:

```python
async def on_enrichment_complete(hotel_id: int, layers_completed: int):
    """Check if downstream stages can now run."""
    # Email verify requires contacts + DNS
    NEEDS_EMAIL_VERIFY = LAYER_DNS | LAYER_WEBSITE  # at minimum
    if (layers_completed & NEEDS_EMAIL_VERIFY) == NEEDS_EMAIL_VERIFY:
        if not (layers_completed & LAYER_EMAIL_VERIFY):
            await enqueue_for_layer(hotel_id, LAYER_EMAIL_VERIFY)

    # Lead scoring requires contacts + email + booking engine
    ALL_ENRICHMENT = (
        LAYER_RDAP | LAYER_DNS | LAYER_WEBSITE |
        LAYER_EMAIL_VERIFY | LAYER_GOV_DATA
    )
    if (layers_completed & ALL_ENRICHMENT) == ALL_ENRICHMENT:
        await enqueue_for_scoring(hotel_id)
```

This is explicit and debuggable. No hidden orchestration framework magic.

### Failure Handling and Retry Strategy

**Current pattern (mostly good):**
- SQS visibility timeout (30 min) acts as automatic retry for crashed workers
- Messages not deleted on failure -> SQS retries
- Exceptions logged but don't crash the batch

**Recommended enhancements:**

#### A. Dead Letter Queue (DLQ)

```
SQS Main Queue  -->  Consumer  -->  Success: delete message
                                -->  Failure: return to queue (visibility timeout)
                                -->  Max retries (3): move to DLQ
                                                         |
                                                         v
SQS Dead Letter Queue  -->  Alert (Slack)
                        -->  Manual review / requeue
```

AWS SQS supports DLQ natively. Configure:
- `maxReceiveCount: 3` (move to DLQ after 3 attempts)
- Alert on DLQ depth > 0

#### B. Circuit Breaker for External Services

When an external service is down, stop sending requests instead of burning through retries:

```python
class CircuitBreaker:
    """Track failures per external service. Open circuit on threshold."""
    def __init__(self, name: str, threshold: int = 5, reset_after: float = 300):
        self.name = name
        self.threshold = threshold
        self.reset_after = reset_after
        self.failures = 0
        self.last_failure = 0
        self.state = "closed"  # closed = healthy, open = broken

    def record_failure(self):
        self.failures += 1
        self.last_failure = time.monotonic()
        if self.failures >= self.threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker OPEN for {self.name} "
                          f"({self.failures} failures)")

    def record_success(self):
        self.failures = 0
        self.state = "closed"

    def is_open(self) -> bool:
        if self.state == "open":
            if time.monotonic() - self.last_failure > self.reset_after:
                self.state = "half-open"  # allow one test request
                return False
            return True
        return False

# Usage in layer runner:
BREAKERS = {
    "rdap": CircuitBreaker("rdap.org"),
    "crt_sh": CircuitBreaker("crt.sh"),
    "wayback": CircuitBreaker("web.archive.org"),
}

async def _run_rdap_with_breaker(client, tag, domain, cf_proxy):
    if BREAKERS["rdap"].is_open():
        logger.info(f"{tag} [RDAP] Skipped — circuit breaker open")
        return [], None
    try:
        result = await _run_rdap(client, tag, domain, cf_proxy)
        BREAKERS["rdap"].record_success()
        return result
    except (httpx.ConnectTimeout, httpx.ReadTimeout) as e:
        BREAKERS["rdap"].record_failure()
        raise
```

#### C. Error Classification

Not all errors deserve retries:

```python
class ErrorClass:
    TRANSIENT = "transient"        # Retry: timeout, 429, 503
    PERMANENT = "permanent"        # Don't retry: 404, invalid domain
    RATE_LIMITED = "rate_limited"   # Retry with backoff: 429
    DATA_ERROR = "data_error"      # Don't retry: parse failure, bad input

def classify_error(e: Exception, source: str) -> str:
    if isinstance(e, httpx.TimeoutException):
        return ErrorClass.TRANSIENT
    if isinstance(e, httpx.HTTPStatusError):
        if e.response.status_code == 429:
            return ErrorClass.RATE_LIMITED
        if e.response.status_code in (400, 404, 410):
            return ErrorClass.PERMANENT
        if e.response.status_code >= 500:
            return ErrorClass.TRANSIENT
    if isinstance(e, (ValueError, KeyError, json.JSONDecodeError)):
        return ErrorClass.DATA_ERROR
    return ErrorClass.TRANSIENT  # default to retry
```

### Monitoring and Observability

**Current:** loguru logging + Slack notifications + `pipeline_status.py` CLI tool.

**Recommended additions (in priority order):**

1. **Structured logging with metrics** (LOW effort, HIGH value):
```python
# Add to each layer completion:
logger.info(
    "layer_complete",
    extra={
        "hotel_id": hotel_id,
        "layer": "rdap",
        "success": True,
        "duration_ms": int(elapsed * 1000),
        "contacts_found": len(dms),
    }
)
```

2. **Pipeline health dashboard query** (LOW effort, HIGH value):
```sql
-- Run periodically, post to Slack
SELECT
    COUNT(*) FILTER (WHERE hoe.status = 1) AS complete,
    COUNT(*) FILTER (WHERE hoe.status = 0) AS in_progress,
    COUNT(*) FILTER (WHERE hoe.status IS NULL) AS not_started,
    COUNT(*) FILTER (WHERE hoe.last_attempt < NOW() - INTERVAL '1 hour'
                      AND hoe.status = 0) AS stale_claims,
    -- Layer coverage
    COUNT(*) FILTER (WHERE hoe.layers_completed & 1 > 0) AS has_rdap,
    COUNT(*) FILTER (WHERE hoe.layers_completed & 4 > 0) AS has_dns,
    COUNT(*) FILTER (WHERE hoe.layers_completed & 8 > 0) AS has_website,
    COUNT(*) FILTER (WHERE hoe.layers_completed & 32 > 0) AS has_email_verify,
    -- Quality metrics
    COUNT(DISTINCT hdm.hotel_id) AS hotels_with_contacts,
    COUNT(hdm.id) FILTER (WHERE hdm.email_verified) AS verified_emails,
    AVG(hdm.confidence) AS avg_confidence
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
LEFT JOIN sadie_gtm.hotel_decision_makers hdm ON h.id = hdm.hotel_id
WHERE h.website IS NOT NULL;
```

3. **CloudWatch metrics for Fargate** (MEDIUM effort): Publish custom metrics for queue depth, processing rate, error rate.

4. **Tracing** (HIGH effort, defer): OpenTelemetry spans across SQS -> consumer -> enrichment layers. Only if debugging becomes a pain.

### Pipeline State Management

**Current:** Status tracked via:
- `hotels.status` (integer: -3 to 1)
- `hotel_owner_enrichment.status` + `layers_completed` bitmask
- `hotel_booking_engines.enrichment_status`
- Presence/absence of records in junction tables

**This is fine.** Do not introduce a separate pipeline state machine or workflow table. The existing pattern of "presence of record = step complete" combined with bitmask for fine-grained layer tracking is simple and correct.

**One recommendation:** Add a `pipeline_stage` computed/virtual column or view for easy querying:

```sql
CREATE OR REPLACE VIEW sadie_gtm.hotel_pipeline_status AS
SELECT
    h.id,
    h.name,
    h.status,
    CASE
        WHEN h.status = -3 THEN 'duplicate'
        WHEN h.status = -2 THEN 'location_mismatch'
        WHEN h.status = -1 THEN 'no_booking_engine'
        WHEN hoe.status IS NULL THEN 'pending_enrichment'
        WHEN hoe.status = 0 THEN 'enriching'
        WHEN hoe.status = 1 AND hdm.dm_count = 0 THEN 'enriched_no_contacts'
        WHEN hoe.status = 1 AND hdm.verified_count = 0 THEN 'enriched_unverified'
        WHEN hoe.status = 1 AND hdm.verified_count > 0 THEN 'enriched_verified'
        WHEN h.status = 1 THEN 'launched'
        ELSE 'unknown'
    END AS pipeline_stage,
    hoe.layers_completed,
    hdm.dm_count,
    hdm.verified_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_owner_enrichment hoe ON h.id = hoe.hotel_id
LEFT JOIN LATERAL (
    SELECT hotel_id,
           COUNT(*) AS dm_count,
           COUNT(*) FILTER (WHERE email_verified) AS verified_count
    FROM sadie_gtm.hotel_decision_makers
    WHERE hotel_id = h.id
    GROUP BY hotel_id
) hdm ON true;
```

---

## Component Boundaries

### Existing Components (Keep As-Is)

| Component | Location | Responsibility | Communicates With |
|-----------|----------|----------------|-------------------|
| Workflows | `workflows/` | CLI entry points, argument parsing | Services, Infra |
| Enrichment Service | `services/enrichment/` | Business logic, orchestration | Repo, External APIs |
| Enrichment Repo | `services/enrichment/repo.py` | DB access, batch operations | PostgreSQL |
| Owner Enricher | `services/enrichment/owner_enricher.py` | Waterfall orchestration | Layer modules |
| Layer Modules | `lib/owner_discovery/` | Individual source logic | External APIs |
| SQS Client | `infra/sqs.py` | Queue operations | AWS SQS |
| Proxy Pool | `lib/proxy.py` | IP rotation | CF Worker, BrightData |
| DB Client | `db/client.py` | Connection pool, aiosql | PostgreSQL |

### Proposed New Components

| Component | Location | Responsibility | Communicates With |
|-----------|----------|----------------|-------------------|
| Source Limiters | `lib/rate_limiting.py` | Per-source rate limits + circuit breakers | Layer modules |
| Entity Resolver | `services/enrichment/entity_resolver.py` | Cross-source deduplication | Repo |
| Pipeline Events | `services/enrichment/pipeline_events.py` | Stage completion triggers | SQS, Repo |
| Metrics Collector | `lib/metrics.py` | Structured logging + pipeline stats | Logger, Slack |

### Data Flow Direction

```
INBOUND (left to right):
  External Sources -> Layer Modules -> Owner Enricher -> Repo -> PostgreSQL

OUTBOUND (right to left):
  PostgreSQL -> Repo -> Service -> Export/Notification -> S3/Slack

ORCHESTRATION (top to bottom):
  Workflow (CLI/Cron) -> Service -> SQS -> Consumer -> Service (loop)
```

---

## Suggested Build Order

Based on dependencies and value delivery:

### Phase 1: Harden What Exists
1. Per-source rate limiters (prevents external service bans)
2. Circuit breakers for external services (prevents wasted retries)
3. DLQ configuration on existing SQS queues (captures persistent failures)
4. Pipeline health dashboard query + Slack alert

**Rationale:** These protect the existing system before adding scale. No schema changes, no new queues, minimal code.

### Phase 2: Scale the Enrichment Tier
5. Back-pressure via bounded producer-consumer queue
6. Adaptive concurrency per source
7. Flush interval tuning for large batches
8. Partial index for pending enrichment work

**Rationale:** The enrichment tier is the bottleneck. These changes let you run 10K+ hotel batches safely.

### Phase 3: Multi-Source Discovery
9. Common Crawl URL discovery pipeline (independent source)
10. Cross-source entity resolution (Stage 4 matching)
11. Pipeline chaining (enrichment-complete events)
12. New bitmask constants for new layers

**Rationale:** New data sources only add value after the enrichment tier can handle the volume.

### Phase 4: Golden Record Quality
13. Source priority hierarchy for field-level merge
14. Confidence scoring model (base + modifiers)
15. Field-level provenance table (optional, for debugging)
16. Data quality metrics and reporting

**Rationale:** Quality improvements compound over time. Build after the pipeline can run end-to-end.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: Workflow Orchestrator Adoption
**What:** Introducing Airflow/Prefect/Dagster for a ~10-stage pipeline run by one person.
**Why bad:** Adds infrastructure, learning curve, and operational burden disproportionate to complexity. The existing SQS + consumer + CLI model is simpler and sufficient.
**Instead:** Formalize the existing patterns with explicit pipeline chaining functions and health checks.

### Anti-Pattern 2: Streaming Architecture
**What:** Replacing batch SQS processing with Kafka/Kinesis streaming.
**Why bad:** Hotel data changes monthly, not per-second. Rate-limited external APIs cannot sustain streaming throughput. PostgreSQL is batch-optimized.
**Instead:** Keep batch processing. Use event triggers (SQS messages) for inter-stage coordination.

### Anti-Pattern 3: Monolithic Enrichment Function
**What:** Keeping all enrichment layers in a single function that grows indefinitely.
**Why bad:** The current `enrich_single_hotel` is already 200+ lines with 9 layer dispatches. Adding CC, Google Maps, and lead scoring will make it unmanageable.
**Instead:** Each layer should be a standalone module in `lib/owner_discovery/`. The orchestrator dispatches but does not contain layer logic. (The current code mostly follows this pattern already.)

### Anti-Pattern 4: Global Concurrency for All Sources
**What:** Single `asyncio.Semaphore(concurrency)` controlling access to all external services.
**Why bad:** RDAP allows 2 req/sec while DNS allows 50/sec. A global semaphore either starves fast sources or overwhelms slow ones.
**Instead:** Per-source rate limiters (described above).

### Anti-Pattern 5: Optimistic All-At-Once Processing
**What:** Launching 100K coroutines via `asyncio.gather` and hoping the system handles it.
**Why bad:** Memory pressure from 100K+ pending coroutines. No back-pressure. If the DB flush fails, results accumulate in memory forever.
**Instead:** Bounded producer-consumer with explicit queue sizes and periodic flush.

---

## Sources

- [Data Pipeline Architecture: 5 Design Patterns (Dagster)](https://dagster.io/guides/data-pipeline-architecture-5-design-patterns-with-examples)
- [Waterfall Enrichment (ZoomInfo)](https://pipeline.zoominfo.com/operations/waterfall-enrichment)
- [GTM Studio Waterfall Enrichment (ZoomInfo)](https://pipeline.zoominfo.com/operations/gtm-studio-waterfall-enrichment)
- [Waterfall Enrichment: Golden-Record Data (Cargo)](https://www.getcargo.ai/blog/waterfall-enrichment-the-secret-sauce-to-maximize-enrichment-coverage)
- [How to Make Data Pipelines Idempotent (Start Data Engineering)](https://www.startdataengineering.com/post/why-how-idempotent-data-pipeline/)
- [Idempotent Pipelines: Build Once, Run Safely (Data Lakehouse Hub)](https://datalakehousehub.com/blog/2026-02-de-best-practices-04-idempotent-pipelines/)
- [Entity Resolution Guide (People Data Labs)](https://www.peopledatalabs.com/data-lab/datafication/entity-resolution-guide)
- [Data Deduplication and Entity Resolution (Things Solver)](https://thingsolver.com/blog/data-deduplication-and-entity-resolution/)
- [Entity Resolution for Multiple Sources (Springer)](https://link.springer.com/chapter/10.1007/978-3-031-26438-2_40)
- [Boosting Postgres INSERT Performance with UNNEST (Timescale)](https://www.tigerdata.com/blog/boosting-postgres-insert-performance)
- [Dead Letter Queue Patterns (OneUptime)](https://oneuptime.com/blog/post/2026-02-09-dead-letter-queue-patterns/view)
- [Circuit Breaker Patterns (OneUptime)](https://oneuptime.com/blog/post/2026-02-02-circuit-breaker-patterns/view)
- [Event-Driven vs Scheduled Data Pipelines (Prefect)](https://www.prefect.io/blog/event-driven-versus-scheduled-data-pipelines)
- [Rate Limiting Strategies for Serverless (AWS)](https://aws.amazon.com/blogs/architecture/rate-limiting-strategies-for-serverless-applications/)
- [Limiting Concurrency in Python asyncio (death.andgravity)](https://death.andgravity.com/limit-concurrency)
- [aiolimiter: Asyncio Rate Limiter](https://github.com/mjpieters/aiolimiter)
- [Async Pressure (Armin Ronacher)](https://lucumr.pocoo.org/2020/1/1/async-pressure/)
- [Data Lineage and Provenance (PromptCloud)](https://www.promptcloud.com/blog/data-lineage-and-provenance/)

**Confidence:** HIGH for architecture patterns and scale recommendations (based on existing codebase analysis + established industry patterns). MEDIUM for specific threshold values (confidence scores, rate limits) which should be tuned empirically.
