---
phase: 07-cc-hotel-domain-sweep
verified: 2026-02-21T14:17:56Z
status: passed
score: 5/5 must-haves verified
---

# Phase 7: CC Hotel Domain Sweep Verification Report

**Phase Goal:** Owner names, titles, and roles are extracted from Common Crawl cached HTML for the majority of hotel domains, with results incrementally persisted and accessible via CLI.
**Verified:** 2026-02-21T14:17:56Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Running `discover_owners --source cc --limit 100 --apply` queries CC indexes, fetches WARC HTML, extracts owners via Nova Micro, and persists to hotel_decision_makers | VERIFIED | `discover_owners_cc()` at line 647 orchestrates load -> harvest -> extract -> persist. `cc_harvest_owner_pages()` at line 280 queries 3 CC indexes via `_proxy_batch()`. `extract_owners_from_page()` at line 596 runs three-tier extraction. `repo.batch_persist_results()` called at line 724 for DB writes. CLI at line 891 dispatches to pipeline. |
| 2 | Results flush to the database every N hotels (not all-or-nothing) -- a crash at hotel 900 preserves the first 900 | VERIFIED | `FLUSH_INTERVAL = 20` at line 128. Flush check at lines 768-773: `should_flush = len(pending_buffer) >= FLUSH_INTERVAL` followed by `await _flush()`. `_flush()` at line 712 calls `repo.batch_persist_results(to_flush)` and resets buffer. Final flush at line 778 handles remainder. Buffer restore on error at line 730-731. |
| 3 | CC index queries target /about, /team, /contact, /management, /staff pages across multiple CC indexes in parallel via CF Worker /batch | VERIFIED | `CC_INDEXES` at lines 94-98 contains 3 indexes. `OWNER_PATHS` at lines 109-116 contains 26 path keywords including about, team, contact, management, staff, leadership, ownership, etc. `cc_harvest_owner_pages()` line 301-309 builds per-index batches and fires all 3 concurrently via `asyncio.gather(*[_proxy_batch(session, batch) for batch in per_index_batches])`. `_proxy_batch()` at line 165 sends to CF Worker `/batch` endpoint. |
| 4 | LLM extraction produces structured output: person name, title/role, organizational relationship (owner vs. GM vs. management company) | VERIFIED | `llm_extract_owners()` at line 521 prompts Nova Micro with explicit JSON format: `[{"name":"First Last","title":"Their Title","role":"owner|general_manager|director|manager|other"}]`. Response parsed at lines 554-560. `llm_results_to_decision_makers()` at line 573 converts to DecisionMaker objects with entity name filtering via `ENTITY_RE_STR`. Three-tier strategy at lines 596-629: JSON-LD (0.9 confidence) -> regex (0.7) -> LLM (0.65). |
| 5 | CLI supports --dry-run, --audit, --source, --limit, --apply, -v | VERIFIED | `main()` at line 891 with argparse: `--source` (required, line 897), `--audit` (line 901), `--apply` (line 903), `--dry-run` (line 905), `--limit` (line 907), `-v/--verbose` (line 909). `--dry-run` handled at lines 677-685 (shows counts, no CC fetch). `--audit` dispatches to `audit()` at line 936. Default mode (no --apply) runs full pipeline but skips DB writes (line 719-721). |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `workflows/discover_owners.py` | Complete CC owner discovery pipeline with CLI | VERIFIED (945 lines) | All infrastructure (Plan 01), extraction (Plan 02), and CLI (Plan 03) present. Module-level import of `DecisionMaker`, `OwnerEnrichmentResult`, `LAYER_WEBSITE` from `owner_models.py`. Lazy import of `repo` inside pipeline function. |
| `services/enrichment/owner_models.py` | DecisionMaker, OwnerEnrichmentResult, LAYER_WEBSITE | VERIFIED (79 lines) | Pre-existing artifact. `DecisionMaker` is a Pydantic BaseModel with fields: full_name, title, email, email_verified, phone, sources, confidence, raw_source_url. `OwnerEnrichmentResult` has hotel_id, domain, decision_makers, layers_completed, and `found_any` property. `LAYER_WEBSITE = 8`. |
| `services/enrichment/repo.py` | `batch_persist_results()` function | VERIFIED (line 1733) | Pre-existing artifact. Function accepts `list[OwnerEnrichmentResult]`, performs 5 bulk SQL queries via unnest for WHOIS cache, DNS cache, cert cache, decision makers, and enrichment status. Returns DM count. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `discover_owners.py` | CF Worker `/batch` | `_proxy_batch()` sending CC Index queries | WIRED | `_proxy_batch()` at line 165 constructs `batch_url = f"{CF_WORKER_URL}/batch"` (line 177), sends POST with JSON body. Called from `cc_harvest_owner_pages()` at lines 308-309 for CC Index queries and line 360 for WARC fetches. |
| `discover_owners.py` | `data.commoncrawl.org` | WARC range requests through CF Worker `/batch` | WIRED | `CC_WARC_BASE = "https://data.commoncrawl.org/"` at line 99. WARC URL construction at line 354: `f"{CC_WARC_BASE}{filename}"`. Range header at line 355. Sent via `_proxy_batch()` at line 360. |
| `discover_owners.py` | Bedrock (Nova Micro) | `boto3.converse` API | WIRED | `_get_bedrock()` at line 510 creates `boto3.client('bedrock-runtime')`. `llm_extract_owners()` at line 546 calls `bedrock.converse(modelId=BEDROCK_MODEL_ID, ...)` via `asyncio.to_thread`. `BEDROCK_MODEL_ID = 'eu.amazon.nova-micro-v1:0'` at line 86. Semaphore(30) at line 518 for throttle protection. |
| `discover_owners.py` | `owner_models.py` | Import of DecisionMaker, OwnerEnrichmentResult, LAYER_WEBSITE | WIRED | Line 38: `from services.enrichment.owner_models import DecisionMaker, OwnerEnrichmentResult, LAYER_WEBSITE`. DecisionMaker constructed at lines 454, 496, 586. OwnerEnrichmentResult constructed at line 759. |
| `discover_owners.py` | `repo.py` | `batch_persist_results()` for incremental flush | WIRED | Line 654: `from services.enrichment import repo`. Line 724: `count = await repo.batch_persist_results(to_flush)`. Function exists at repo.py line 1733 as `async def batch_persist_results(results: list) -> int:`. |
| `discover_owners.py` CLI | `discover_owners_cc()` pipeline | `main()` dispatches to pipeline | WIRED | Line 938: `stats = await discover_owners_cc(args, cfg)`. Line 936: `await audit(args, cfg)` for --audit mode. Line 944: `asyncio.run(main())` in `__main__` block. |
| `discover_owners.py` audit | `hotel_decision_makers` table | SQL count queries | WIRED | `audit()` at line 798 runs 6 SQL queries against `sadie_gtm.hotel_decision_makers` and `sadie_gtm.hotel_owner_enrichment` tables with proper JOIN and WHERE clauses. Verbose mode at line 872 queries CC-specific sources (`cc_website_llm`, `cc_website_regex`, `cc_website_jsonld`). |

### Requirements Coverage

| Requirement | Status | Evidence |
|-------------|--------|----------|
| CC-01: Query CC index in batch for all hotel domains, targeting /about, /team, /contact, /management, /staff pages across multiple CC indexes | SATISFIED | `cc_harvest_owner_pages()` queries 3 CC_INDEXES for all domains via CF Worker /batch. `OWNER_PATHS` contains 26 keywords including all required paths. Per-index concurrent batching at lines 301-309. |
| CC-02: Pull WARC HTML from CC for matched URLs via CF Worker /batch, decompress WARC records, extract clean HTML | SATISFIED | WARC fetch at lines 346-360 via `_proxy_batch(session, warc_requests, chunk_size=200)`. Decompression at lines 376-388: base64 decode, gzip decompress, split by `\r\n\r\n`, take part[2], decode with entry encoding. Min 100 chars filter at line 387. |
| CC-03: Run Nova Micro LLM extraction on CC HTML to extract owner names, titles, roles, and organizational relationships | SATISFIED | `llm_extract_owners()` at line 521 uses Bedrock Nova Micro with structured prompt requesting name, title, role. Three-tier strategy ensures LLM only fires when JSON-LD and regex found nothing (cost optimization). Entity name filter prevents company names. |
| PIPE-03: Incremental persistence, flush every N hotels, CLI matching enrich_contacts pattern | SATISFIED | `FLUSH_INTERVAL = 20`. Incremental flush at lines 768-773 using `repo.batch_persist_results()`. CLI has all required flags: --source, --limit, --apply, --audit, --dry-run, -v. Pattern matches enrich_contacts.py exactly. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None | - | - | - | No TODO, FIXME, placeholder, or stub patterns found in discover_owners.py |

The empty returns (`return []`, `return {}`) at lines 175, 195, 203, 290, 343, 569, 570, 623 are all legitimate edge-case handling (no data found, empty input, too-short text), not stubs.

### Human Verification Required

### 1. End-to-end CC Pipeline Run

**Test:** Run `uv run python3 workflows/discover_owners.py --source big4 --limit 5 --apply -v`
**Expected:** CC harvest runs (index queries + WARC fetches), pages extracted, owners found via JSON-LD/regex/LLM, results flushed to DB with "Flushed N hotels" messages. Re-run `--audit` should show cc_website_* sources.
**Why human:** Requires live database, CF Worker proxy, and Bedrock API access. Structural verification cannot confirm network I/O succeeds.

### 2. Dry-Run Output Correctness

**Test:** Run `uv run python3 workflows/discover_owners.py --source big4 --dry-run`
**Expected:** Shows hotel count, unique domain count, CC index count, total CC queries (domains x 3 indexes). No network I/O occurs.
**Why human:** Requires live database connection to load hotel counts.

### 3. Audit Command Output

**Test:** Run `uv run python3 workflows/discover_owners.py --source big4 --audit -v`
**Expected:** Shows coverage stats (hotels total, with website, enriched, with DM found) and source breakdown. Verbose mode shows recent CC-discovered DMs.
**Why human:** Requires live database with existing data to verify output formatting.

### 4. Incremental Flush Resilience

**Test:** Run a larger batch (`--limit 50 --apply -v`) and observe multiple "Flushed" log messages appearing during processing (not just at the end).
**Expected:** Multiple flush messages at intervals of ~20 hotels, confirming crash resilience.
**Why human:** Requires live execution to observe flush timing behavior.

### Gaps Summary

No gaps found. All 5 observable truths are verified at the structural level. All required artifacts exist, are substantive (945 lines for the main file), and are properly wired. All 4 key links (CF Worker /batch, WARC fetch, Bedrock Nova Micro, repo.batch_persist_results) are connected with real implementations, not stubs. All 4 mapped requirements (CC-01, CC-02, CC-03, PIPE-03) are satisfied by the codebase.

The code closely follows the established patterns from `enrich_contacts.py` (CC harvest, _proxy_batch, _clean_text_for_llm) and `lib/owner_discovery/website_scraper.py` (JSON-LD extraction, regex patterns), adapted with CC-specific source tags and owner-focused URL filtering. The three-tier extraction strategy (JSON-LD -> regex -> LLM) is correctly implemented with early returns to minimize LLM cost. The incremental persistence pattern with FLUSH_INTERVAL=20 and buffer restore on error provides crash resilience.

---

_Verified: 2026-02-21T14:17:56Z_
_Verifier: Claude (gsd-verifier)_
