# Technology Stack

**Project:** Sadie GTM Owner Enrichment Platform
**Researched:** 2026-02-20
**Mode:** Ecosystem (brownfield -- extending existing Python/asyncio/SQS/Fargate stack)

---

## Executive Summary

This is a brownfield project with a working pipeline. The stack recommendations focus on what to ADD, what to REPLACE, and what to KEEP. The existing asyncio + httpx + asyncpg + SQS + Fargate core is sound and should not be disrupted. The biggest wins come from (1) adding workflow orchestration for observability and retry management, (2) switching LLM extraction from Azure OpenAI GPT-3.5-turbo to cheaper/faster alternatives, and (3) formalizing the data quality layer.

---

## 1. Workflow Orchestration

### Recommendation: Prefect 3 (with existing SQS preserved)

| Attribute | Value |
|-----------|-------|
| Library | `prefect >= 3.6` |
| Confidence | **HIGH** |
| Role | Orchestration layer OVER existing SQS consumers, not replacing them |

### Why Prefect 3

**Fits the existing architecture:**
- Native asyncio support via AnyIO. Your existing `asyncio.gather()` patterns in `owner_enricher.py` and `enrich_contacts.py` work directly inside Prefect flows and tasks. Zero rewrite of core logic.
- `@flow` and `@task` decorators wrap existing functions. The enrichment waterfall (`enrich_single_hotel`) becomes a Prefect task; `enrich_batch` becomes a flow.
- Built-in retry with exponential backoff replaces your manual retry logic (e.g., the 429 retry in `llm_extract_contacts`).
- Global concurrency limits and rate limiting built-in -- directly applicable to your LLM calls and SMTP verification.
- Prefect has an `prefect-aws` integration package with ECS, S3, and SQS support. Your SQS consumers can be triggered by Prefect automations.

**What you gain:**
- **Observability dashboard** (Prefect UI/Cloud) showing per-hotel enrichment status, layer completion rates, failure reasons -- currently you rely on Loguru logs.
- **Automatic retries** with configurable backoff per layer (RDAP flaky? 3 retries. LLM throttled? exponential backoff with 429 detection).
- **Flow-level state** -- know which batch of 500 hotels is at what stage without querying the DB.
- **Scheduling** -- trigger nightly re-enrichment of stale records, weekly full-pipeline runs.
- **Caching** -- per-domain results can be cached across runs (e.g., DNS intel, RDAP results don't change daily).

**Deployment model:**
- Self-hosted Prefect server (lightweight: single container + SQLite or Postgres) or Prefect Cloud free tier (10k task runs/month).
- Workers run on existing Fargate tasks. No new infrastructure.
- Prefect client library is ~50MB, minimal dependency footprint.

### Why NOT Luigi

| Issue | Details | Confidence |
|-------|---------|------------|
| Abandoned by creator | Spotify itself migrated from Luigi to Flyte. Luigi is in maintenance mode. | HIGH |
| No DAG concept | Luigi uses target-based dependencies, not DAGs. Complex multi-layer waterfall with conditional branching (skip WHOIS if RDAP found registrant) is awkward. | HIGH |
| No async support | Luigi tasks are synchronous. Your entire codebase is async. You'd need `asyncio.run()` wrappers everywhere. | HIGH |
| No built-in observability | Basic web UI shows task status, but no metrics, no timelines, no concurrency visualization. | HIGH |
| Scaling ceiling | Luigi docs explicitly state: "not meant to scale beyond tens of thousands of jobs." You're targeting 100K+ records. | HIGH |
| No cloud integration | No native SQS, Fargate, or S3 support. | HIGH |

**Verdict:** Luigi is a non-starter for this project. Do not use it.

### Why NOT Dagster

| Attribute | Assessment |
|-----------|------------|
| Asset-centric model | Dagster's core abstraction is "software-defined assets" -- data artifacts as first-class objects. Your pipeline is TASK-centric (run layers on hotels), not asset-centric (produce datasets). Forcing hotel enrichment into asset definitions adds conceptual overhead. |
| Async support | Recently added (2025), but the executor model wraps async inside sync executors. Less native than Prefect's AnyIO-based approach. |
| Operational overhead | Dagster requires dagster-webserver + dagster-daemon + metadata DB. Heavier than Prefect's single-server model. |
| Good for | Analytics/ML pipelines where you care about data lineage and asset freshness. Not ideal for "run this function on 100K records" enrichment patterns. |

**Verdict:** Dagster is excellent for analytics pipelines but is a poor fit for task-based enrichment workflows. MEDIUM confidence (Dagster is evolving fast).

### Why NOT Temporal

| Attribute | Assessment |
|-----------|------------|
| Language-agnostic overkill | Temporal's durable workflow replay requires deterministic workflow code. Your Python waterfall with `asyncio.gather()` is non-deterministic by nature (race conditions in parallel layers). You'd need to restructure. |
| Operational complexity | Self-hosted Temporal is a multi-service system (server + persistence + optional Elasticsearch). Far heavier than needed. |
| Good for | Long-running business processes (months), polyglot microservices, human-in-the-loop workflows. None of which apply here. |

**Verdict:** Temporal is for microservices orchestration, not data enrichment. Do not use.

### Why NOT Airflow

| Attribute | Assessment |
|-----------|------------|
| DAG paradigm mismatch | Airflow DAGs are defined statically. Your waterfall has dynamic branching (skip layers based on results). Airflow 2.x dynamic tasks exist but are clunky. |
| No async | Airflow executors are synchronous. CeleryExecutor or KubernetesExecutor add massive infrastructure overhead. |
| Operational weight | Requires scheduler + webserver + metadata DB + message broker (Redis/RabbitMQ). Enormous footprint for your use case. |

**Verdict:** Airflow is the Java of orchestrators -- powerful but heavy. Do not use for this project.

### Why NOT "Just keep custom SQS"

Your current SQS consumer pattern works but has gaps:
- No retry management beyond SQS visibility timeout
- No observability beyond log parsing
- No flow-level state (which batch is where?)
- No scheduling (manual CLI invocation)
- No concurrency limits across workers

**Recommendation:** Keep SQS as the message transport. Add Prefect as the orchestration layer that TRIGGERS and MONITORS SQS-based work. Prefect flows enqueue to SQS and poll for completion, or Prefect tasks directly run the enrichment logic (replacing the SQS consumer loop).

### Migration Path

```
Phase 1: Wrap existing functions with @flow/@task decorators
Phase 2: Add retry policies and concurrency limits
Phase 3: Replace manual CLI with Prefect deployments + schedules
Phase 4: Add Prefect Cloud for team observability (optional)
```

---

## 2. LLM Extraction at Scale

### Current State

You have TWO LLM integrations:
1. **Azure OpenAI GPT-3.5-turbo** -- in `website_scraper.py` for extracting owner/GM from scraped HTML
2. **Amazon Bedrock Nova Micro** -- in `enrich_contacts.py` for extracting emails/phones from page text

### Recommendation: Standardize on GPT-4o-mini via OpenAI Batch API

| Attribute | Value |
|-----------|-------|
| Model | `gpt-4o-mini` |
| API | OpenAI Batch API (50% discount) |
| Confidence | **HIGH** |

### Why GPT-4o-mini replaces GPT-3.5-turbo

| Factor | GPT-3.5-turbo | GPT-4o-mini |
|--------|---------------|-------------|
| Input price | $0.50/M tokens | $0.15/M tokens (70% cheaper) |
| Output price | $1.50/M tokens | $0.60/M tokens (60% cheaper) |
| Batch input | $0.25/M | $0.075/M (70% cheaper) |
| Batch output | $0.75/M | $0.30/M (60% cheaper) |
| Structured output | No native support | Native JSON mode + function calling |
| Extraction quality | Adequate | "Significantly better" per OpenAI benchmarks |
| Context window | 16K tokens | 128K tokens |

**At 100K records:**
- Assume ~1000 input tokens + ~150 output tokens per record
- GPT-3.5-turbo: (100K * 1000 * $0.50/M) + (100K * 150 * $1.50/M) = $50 + $22.50 = **$72.50**
- GPT-4o-mini standard: (100K * 1000 * $0.15/M) + (100K * 150 * $0.60/M) = $15 + $9 = **$24**
- GPT-4o-mini batch: (100K * 1000 * $0.075/M) + (100K * 150 * $0.30/M) = $7.50 + $4.50 = **$12**

**Result: 83% cost reduction from current GPT-3.5-turbo to GPT-4o-mini batch.**

### Batch API Strategy

The OpenAI Batch API accepts JSONL files of requests, processes them within 24 hours, and returns results at 50% off. For enrichment pipelines where latency is acceptable:

```python
# Use openbatch library for structured Pydantic output
from openbatch import BatchProcessor

processor = BatchProcessor(model="gpt-4o-mini")
results = processor.process(requests, output_schema=OwnerExtraction)
```

**Library:** `openbatch` (Python library for simplified batch processing with Pydantic schemas)
**Confidence:** MEDIUM (library is relatively new, but the underlying Batch API is stable)

### Keep Amazon Bedrock Nova Micro as fallback

| Attribute | Value |
|-----------|-------|
| Model | `eu.amazon.nova-micro-v1:0` |
| On-demand input | $0.035/1K tokens |
| On-demand output | $0.14/1K tokens |
| Batch input | ~$0.0175/1K tokens (50% off) |
| Batch output | ~$0.07/1K tokens (50% off) |
| Role | Fallback when OpenAI is unavailable; already integrated |

Nova Micro is already working in `enrich_contacts.py`. Keep it as a secondary provider. It's roughly comparable in cost to GPT-4o-mini batch but with less structured output support.

### Why NOT Groq/Llama

| Factor | Assessment |
|--------|------------|
| Price | Llama 3.1 8B on Groq: $0.05/$0.08 per M tokens. Cheapest option. Batch: $0.025/$0.04. |
| Speed | 840 tokens/sec on Groq. Fastest inference available. |
| Quality | For structured extraction from messy HTML, Llama 3.1 8B underperforms GPT-4o-mini significantly. The 70B variant ($0.59/$0.79) approaches GPT-4o-mini quality but at 4x the cost. |
| Structured output | JSON mode supported, but hallucination rate on extraction tasks is higher than GPT-4o-mini. |
| Recommendation | Consider Groq Llama 3.1 8B ONLY for simple pattern matching (e.g., "extract emails from text") where quality requirements are lower. Not for nuanced owner/title extraction. |

**Confidence:** MEDIUM (Groq quality assessment based on general benchmarks, not domain-specific testing)

### Why NOT local models

Running local LLMs (Ollama, llama.cpp) would save API costs but:
- Requires GPU infrastructure (not available on Fargate)
- Inference speed on CPU is 10-50x slower than cloud APIs
- Deployment and model management complexity
- Not justified at 100K records/run (total LLM cost is ~$12 with batch API)

**Verdict:** Cloud APIs are the right call. $12/run for 100K records doesn't justify GPU infrastructure.

### Recommended LLM Architecture

```
Primary:   GPT-4o-mini via OpenAI Batch API (non-urgent bulk)
Realtime:  GPT-4o-mini via OpenAI standard API (single-hotel enrichment)
Fallback:  Amazon Bedrock Nova Micro (if OpenAI is down)
Future:    Groq Llama 3.1 8B for simple extraction tasks (email regex validation)
```

---

## 3. Web Scraping at Scale

### Current State

You have a sophisticated multi-layer scraping stack:
- **httpx** -- direct HTTP fetches for hotel websites (500 concurrent connections via aiohttp)
- **crawl4ai** -- Playwright-based headless browser for JS-heavy sites
- **Common Crawl** -- CC Index API queries + WARC record fetching
- **CF Worker proxy** -- Cloudflare Worker for IP rotation ($5/mo)
- **BrightData** -- Residential/datacenter proxy rotation
- **Serper API** -- Google search for contact pages

### Recommendation: Keep current stack, add targeted improvements

| Component | Action | Confidence |
|-----------|--------|------------|
| httpx + aiohttp | KEEP. Working well at 500 concurrent connections. | HIGH |
| crawl4ai | KEEP. Good for JS-heavy sites. Pin version. | HIGH |
| Common Crawl | ENHANCE. Add index caching and smarter filtering. | HIGH |
| CF Worker proxy | KEEP. Excellent cost/performance ratio. | HIGH |
| BrightData | KEEP for residential needs. Consider reducing DC proxy usage. | MEDIUM |

### Common Crawl Improvements

Your current CC integration (`enrich_contacts.py` lines 415-530) is good but can be improved:

1. **Index caching:** Cache CC Index responses in PostgreSQL. The index for a domain doesn't change until the next crawl (every ~2 months). You're re-querying for every batch run.

2. **Multiple index search:** You already search 3 indexes (Dec 2024, Oct 2024, Aug 2024). Good. As of 2025, CC increased the truncation threshold from 1MB to 5MB, so newer indexes have more complete pages.

3. **FastWARC for parsing:** Consider `fastwarc` library for WARC parsing if you need to process raw WARC files. Currently you do manual `gzip.decompress()` + byte splitting which works fine for individual records but is fragile.

4. **Web Data Commons Hotel dataset:** The 3.19GB pre-extracted schema.org dataset mentioned in your project memory is a goldmine. It contains ~2M hotel URLs with pre-extracted structured data. Ingesting this would give you a massive head start on hotel owner data.

### Anti-blocking Strategy

Your current approach is solid but add:

| Strategy | Current | Add |
|----------|---------|-----|
| User-Agent rotation | Single static UA | Rotate 5-10 common browser UAs |
| Request timing | `asyncio.Semaphore(500)` (blast) | Add per-domain rate limiting (2 req/sec/domain) |
| Header fingerprinting | Basic Accept/Language | Add Sec-CH-UA, Sec-Fetch-* headers |
| Cookie handling | None | Add session cookies for multi-page crawls |
| Retry on 403/429 | None in httpx fetcher | Add exponential backoff with proxy rotation |

### Proxy Architecture

```
Tier 1 (Free):       CF Worker proxy ($5/mo, 10M requests)
                      Best for: RDAP, crt.sh, Wayback Machine, gov APIs

Tier 2 (Datacenter):  BrightData DC ($0.60/GB)
                      Best for: Hotel websites that block CF IPs

Tier 3 (Residential): BrightData Residential ($8-12/GB)
                      Best for: Google, TripAdvisor, highly-protected sites
                      ONLY when Tier 1+2 fail
```

### Why NOT Scrapy

Scrapy is the most popular Python scraping framework, but:
- Your pipeline is asyncio-native; Scrapy uses Twisted (incompatible event loop)
- You already have a working httpx + crawl4ai stack
- Scrapy is better for discovery crawls (follow links, extract data from hundreds of pages). Your use case is targeted page fetching (known URLs).

### Why NOT Firecrawl / Apify

- Firecrawl: hosted scraping-as-a-service. Good for one-off scrapes, expensive at scale. Self-hosted version "still isn't production-ready" per community reports.
- Apify: Similar. Per-page pricing doesn't make sense when you're fetching 100K+ pages.

---

## 4. Data Quality

### Email Verification

#### Current State
Your `email_discovery.py` has built-in O365 GetCredentialType and SMTP verification. This is solid for pattern-guessed emails.

#### Recommendation: Keep DIY verification, add NeverBounce for bulk validation

| Service | Use Case | Price | Confidence |
|---------|----------|-------|------------|
| DIY O365/SMTP | Real-time single email verification | Free | HIGH (already working) |
| NeverBounce API | Bulk validation of scraped emails | $0.003/email ($300 per 100K) | MEDIUM |

**Why NeverBounce over ZeroBounce:**
- Cheaper ($0.003 vs $0.004/email)
- Faster bulk processing (100K in ~45 min)
- Simple REST API for programmatic use
- NeverBounce focuses on deliverability verification; ZeroBounce adds data enrichment features you don't need

**When to use external verification:**
- After initial enrichment run, before outreach
- For emails found via scraping (not pattern-guessed with O365 verification)
- Batch job: export emails, verify, update DB

**Library:** Direct HTTP API calls (NeverBounce REST API is simple enough)
**Confidence:** MEDIUM (haven't verified current API stability)

### Deduplication

#### Current State
Your `_deduplicate()` function in `owner_enricher.py` does exact-match dedup on (name, title). The `enrich_contacts.py` has `ENTITY_RE_STR` for filtering entity names vs people.

#### Recommendation: Add Splink for fuzzy matching

| Library | Version | Purpose | Confidence |
|---------|---------|---------|------------|
| `splink >= 4.0` | Latest | Probabilistic record linkage | HIGH |

**Why Splink:**
- Handles fuzzy name matching ("John Smith" vs "J. Smith" vs "Jonathan Smith")
- Scales to millions of records (7M records in 2 minutes with DuckDB backend)
- Probabilistic model gives confidence scores, not just binary match/no-match
- Python-native, runs on DuckDB (no Spark needed at your scale)
- Award-winning (2025 Civil Service Innovation Award, OpenUK 2025)

**Use cases in your pipeline:**
1. **Cross-source dedup:** Same person found via RDAP, website scraping, and Google reviews with slightly different name spellings
2. **Cross-hotel dedup:** Same owner managing multiple hotels (common in hotel groups)
3. **Entity resolution:** "Big4 Holiday Parks Pty Ltd" vs "BIG4 HOLIDAY PARKS PTY. LTD."

**Implementation sketch:**
```python
import splink
from splink import DuckDBAPI, Linker, SettingsCreator

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.JaroWinklerAtThresholds("full_name", [0.9, 0.8]),
        cl.ExactMatch("hotel_id"),
        cl.LevenshteinAtThresholds("email", [1, 2]),
    ],
)
linker = Linker(df, settings, db_api=DuckDBAPI())
```

### Data Freshness/Staleness

#### Current State
Your `hotel_owner_enrichment` table has `enriched_at` timestamp and `layers_completed` bitmask. No automatic staleness detection.

#### Recommendation: Add staleness windows with re-enrichment triggers

| Data Type | Freshness Window | Re-enrich Trigger |
|-----------|-----------------|-------------------|
| RDAP/WHOIS | 90 days | Domain registration changes |
| DNS (MX/SPF) | 30 days | Email provider switch |
| Website content | 60 days | Page content changed |
| Email verified | 30 days | Re-verify deliverability |
| Gov data (DBPR) | 180 days | License renewal cycle |
| CT Certificates | 90 days | New cert issuance |

**Implementation:**
```sql
-- Flag stale records
SELECT hotel_id FROM sadie_gtm.hotel_owner_enrichment
WHERE enriched_at < NOW() - INTERVAL '90 days'
  AND layers_completed & 1 = 1  -- has RDAP
  AND error IS NULL;
```

This should be a Prefect scheduled flow that runs nightly, identifies stale records, and enqueues them for re-enrichment.

---

## 5. Existing Stack -- KEEP

These components are working well. Do not replace.

| Component | Version | Purpose | Status |
|-----------|---------|---------|--------|
| Python | 3.9+ | Runtime | KEEP |
| asyncio + httpx | 0.25+ | Async HTTP client | KEEP |
| asyncpg | 0.31+ | Async PostgreSQL driver | KEEP |
| aiosql | 13.4 | SQL query management | KEEP |
| Pydantic | 2.12+ | Data validation/models | KEEP |
| Playwright | 1.40+ | Headless browser (via crawl4ai) | KEEP |
| crawl4ai | 0.7.4+ | AI-powered crawling | KEEP |
| BeautifulSoup4 | 4.14+ | HTML parsing | KEEP |
| Loguru | 0.7+ | Structured logging | KEEP |
| boto3/aioboto3 | Latest | AWS SDK (SQS, Bedrock) | KEEP |
| FastAPI | 0.115+ | API layer | KEEP |
| dnspython | 2.6+ | DNS queries | KEEP |
| PostgreSQL/Aurora | Latest | Primary database | KEEP |
| SQS | N/A | Message queue | KEEP |
| Fargate | N/A | Container runtime | KEEP |

---

## 6. Recommended Additions

### New Dependencies

```toml
# pyproject.toml additions
[project]
dependencies = [
    # ... existing ...
    "prefect>=3.6",           # Workflow orchestration
    "prefect-aws>=0.5",       # AWS integration (SQS, ECS)
    "splink>=4.0",            # Probabilistic dedup
    "openbatch>=0.2",         # OpenAI Batch API helper (optional)
]
```

### Environment Variables (new)

```bash
# Prefect
PREFECT_API_URL=http://localhost:4200/api   # or Prefect Cloud URL
PREFECT_API_KEY=pnu_...                      # Prefect Cloud only

# OpenAI (replace Azure OpenAI)
OPENAI_API_KEY=sk-...                        # For GPT-4o-mini

# NeverBounce (optional, for bulk verification)
NEVERBOUNCE_API_KEY=...
```

---

## 7. What NOT to Use

| Technology | Why Not | Confidence |
|------------|---------|------------|
| Luigi | Abandoned by Spotify, no async, no DAGs, doesn't scale | HIGH |
| Airflow | Massive operational overhead, no async support | HIGH |
| Temporal | Overkill for data enrichment, requires deterministic code | HIGH |
| Scrapy | Twisted event loop incompatible with asyncio | HIGH |
| Celery | Heavy message broker dependency (Redis/RabbitMQ), when you already have SQS | HIGH |
| GPT-3.5-turbo | 70% more expensive than GPT-4o-mini with worse extraction quality | HIGH |
| Local LLMs | GPU infra not justified at $12/run for 100K records | HIGH |
| Firecrawl/Apify | Per-page pricing doesn't scale to 100K+ | MEDIUM |
| Dagster | Asset-centric model is a poor fit for task-based enrichment | MEDIUM |

---

## Confidence Assessment

| Area | Confidence | Reason |
|------|------------|--------|
| Prefect 3 recommendation | HIGH | Verified async support, AnyIO architecture, AWS integration. Multiple sources confirm fit. |
| Luigi rejection | HIGH | Spotify abandoned it. Docs confirm scaling limits. No async. |
| GPT-4o-mini pricing | HIGH | Verified via multiple sources including OpenAI pricing page. |
| Batch API savings | HIGH | 50% discount is well-documented by OpenAI and Groq. |
| Splink for dedup | HIGH | Well-documented, actively maintained, proven at scale (7M records). |
| NeverBounce recommendation | MEDIUM | Pricing verified but API integration not personally tested. |
| Groq/Llama quality for extraction | MEDIUM | Based on general benchmarks, not domain-specific testing with hotel data. |
| Amazon Nova Micro pricing | MEDIUM | Exact batch pricing could not be fully verified from fetched page. |
| crawl4ai stability | MEDIUM | Relatively new library (0.7.x), API may change. Pin version. |

---

## Sources

- [Prefect Documentation - Run Work Concurrently](https://docs.prefect.io/v3/how-to-guides/workflows/run-work-concurrently)
- [Prefect Documentation - Install](https://docs.prefect.io/v3/get-started/install)
- [Prefect AWS Integration](https://docs.prefect.io/integrations/prefect-aws/index)
- [Luigi Design and Limitations](https://luigi.readthedocs.io/en/stable/design_and_limitations.html)
- [Spotify: Why We Switched from Luigi](https://engineering.atspotify.com/2022/03/why-we-switched-our-data-orchestration-service)
- [Dagster: When Sync Isn't Enough (Async)](https://dagster.io/blog/when-sync-isnt-enough)
- [Temporal vs Prefect Comparison](https://codilime.com/blog/built-same-pipeline-twice-temporal-prefect-comparison/)
- [OpenAI GPT-4o-mini Announcement](https://openai.com/index/gpt-4o-mini-advancing-cost-efficient-intelligence/)
- [OpenAI Batch API Guide](https://developers.openai.com/api/docs/guides/batch)
- [OpenAI Cost Optimization](https://platform.openai.com/docs/guides/cost-optimization)
- [Groq Pricing](https://groq.com/pricing)
- [AWS Bedrock Pricing](https://aws.amazon.com/bedrock/pricing/)
- [Amazon Nova Pricing](https://aws.amazon.com/nova/pricing/)
- [Splink GitHub](https://github.com/moj-analytical-services/splink)
- [Common Crawl Get Started](https://commoncrawl.org/get-started)
- [Common Crawl WARC Format](https://commoncrawl.org/blog/navigating-the-warc-file-format)
- [NeverBounce vs ZeroBounce Comparison](https://mailfloss.com/neverbounce-vs-zerobounce/)
- [Hunter.io Email Verification Guide](https://hunter.io/email-verification-guide/best-email-verifiers/)
- [openbatch Library](https://www.daniel-gomm.com/blog/2025/openbatch/)
- [Workflow Orchestration Platforms Comparison 2025](https://procycons.com/en/blogs/workflow-orchestration-platforms-comparison-2025/)
