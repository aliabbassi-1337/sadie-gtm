---
phase: 07-cc-hotel-domain-sweep
plan: 02
subsystem: owner-discovery
tags: [json-ld, regex-extraction, llm-extraction, nova-micro, bedrock, incremental-persistence, pipeline-orchestration]
dependency-graph:
  requires: [07-01]
  provides: [owner-extraction-pipeline, three-tier-extraction, incremental-persistence]
  affects: [07-03]
tech-stack:
  added: [boto3-bedrock-runtime]
  patterns: [three-tier-extraction, incremental-flush, bedrock-converse-api]
key-files:
  created: []
  modified:
    - workflows/discover_owners.py
decisions:
  - id: D-0702-01
    description: "Three-tier extraction: JSON-LD (0.9) -> regex (0.7) -> LLM (0.65) with early return"
    rationale: "Structured data is free and highest confidence; LLM only fires when needed (cost savings)"
  - id: D-0702-02
    description: "Bedrock Semaphore(30) for Nova Micro concurrency"
    rationale: "Bedrock throttles above ~30 concurrent requests; semaphore prevents 429 storms"
  - id: D-0702-03
    description: "Entity name regex filter on LLM results"
    rationale: "LLM sometimes returns company/trust names as person names; ENTITY_RE_STR filters these"
  - id: D-0702-04
    description: "Incremental flush every 20 hotels via batch_persist_results()"
    rationale: "Crash at hotel 900 preserves first 880; matches FLUSH_INTERVAL constant from Plan 01"
metrics:
  duration: "~3 minutes"
  completed: "2026-02-21"
---

# Phase 7 Plan 02: LLM Extraction + Pipeline Orchestration Summary

Three-tier owner extraction (JSON-LD -> regex -> LLM via Bedrock Nova Micro) and main pipeline orchestration with incremental persistence every 20 hotels using batch_persist_results().

## What Was Built

### Task 1: Three-Tier Extraction Functions (94318ab)
Added extraction layer to `workflows/discover_owners.py`:

- **DECISION_MAKER_TITLES**: 16 title keywords shared by JSON-LD and regex (owner, co-owner, proprietor, founder, general manager, etc.)
- **NAME_TITLE_PATTERNS**: 2 regex patterns matching "Name, Title" and "Title: Name" formats
- **`extract_json_ld_persons(html)`**: Parses `<script type="application/ld+json">` blocks, recursively extracts Person entities with decision-maker titles. Source: `cc_website_jsonld`, confidence: 0.9.
- **`_extract_persons_from_jsonld(data, results)`**: Recursive helper checking nested employee/member/founder keys.
- **`extract_name_title_regex(text)`**: Applies NAME_TITLE_PATTERNS, validates 2-4 word capitalized names. Source: `cc_website_regex`, confidence: 0.7.
- **`_get_bedrock()`**: Lazy boto3 bedrock-runtime client initialization.
- **`llm_extract_owners(text, hotel_name)`**: Bedrock Nova Micro converse API, semaphore(30), 3 retries with exponential backoff on 429, JSON cleanup (strip markdown fences, extract array).
- **`llm_results_to_decision_makers(results, source_url)`**: Converts LLM dicts to DecisionMaker objects, filters first-name-only and entity names via ENTITY_RE_STR. Source: `cc_website_llm`, confidence: 0.65.
- **`extract_owners_from_page(html, url, hotel_name)`**: Combined orchestrator -- tries JSON-LD first, then regex, then LLM only if both found nothing. Skips LLM for pages with <50 chars of text, truncates to 20K chars for Nova Micro context.

### Task 2: Pipeline Orchestration (9ddcf92)
Added main pipeline and persistence wiring:

- **`group_pages_by_domain(pages)`**: Groups `{url: html}` dict by domain for hotel matching.
- **`discover_owners_cc(args, cfg)`**: Full pipeline function:
  1. Loads hotels via `load_hotels_for_cc_sweep()` (from Plan 01)
  2. Handles `--dry-run` (shows counts, returns early)
  3. Extracts domains via `extract_hotel_domains()` (from Plan 01)
  4. Harvests CC pages via `cc_harvest_owner_pages()` (from Plan 01)
  5. Groups pages by domain, extracts owners per domain
  6. Creates `OwnerEnrichmentResult` per hotel with `layers_completed=LAYER_WEBSITE`
  7. Incremental flush every `FLUSH_INTERVAL` (20) hotels via `repo.batch_persist_results()`
  8. Respects `--apply` flag (no DB writes without it)
  9. Tracks detailed stats: `jsonld_hits`, `regex_hits`, `llm_calls`, `owners_saved`, `hotels_with_owners`

## Architecture

```
              load_hotels_for_cc_sweep()
                        |
              extract_hotel_domains()
                        |
              cc_harvest_owner_pages()     [Plan 01]
                        |
              group_pages_by_domain()
                        |
        +---------------+----------------+
        |               |                |
  JSON-LD (0.9)   Regex (0.7)    LLM (0.65)    [Plan 02]
        |               |                |
        +-------+-------+--------+------+
                |                 |
         DecisionMaker    ENTITY_RE_STR filter
                |
    OwnerEnrichmentResult(layers=LAYER_WEBSITE)
                |
    repo.batch_persist_results()  [every 20 hotels]
```

## Key Patterns Reused

| Pattern | Source | Adaptation |
|---------|--------|------------|
| JSON-LD extraction | website_scraper.py L113-161 | Changed source tag to `cc_website_jsonld` |
| Regex name+title | website_scraper.py L164-190 | Changed source tag to `cc_website_regex` |
| JSON cleanup (markdown fences) | enrich_contacts.py L1404-1410 | Identical pattern for LLM response parsing |
| Bedrock converse API | enrich_contacts.py pattern | Same boto3 converse call, Nova Micro model |
| batch_persist_results() | services/enrichment/repo.py L1733-1929 | Called directly, no adaptation needed |

## Deviations from Plan

None -- plan executed exactly as written.

## Verification Results

All 7 verification checks passed:
1. `import workflows.discover_owners` succeeds
2. All extraction functions importable: `extract_json_ld_persons`, `extract_name_title_regex`, `llm_extract_owners`, `extract_owners_from_page`
3. Pipeline functions importable: `discover_owners_cc`, `group_pages_by_domain`
4. `llm_extract_owners` uses `asyncio.Semaphore(30)` for Bedrock concurrency
5. `extract_owners_from_page` tries JSON-LD first, then regex, then LLM (in that order)
6. `discover_owners_cc` calls `repo.batch_persist_results()` for incremental flush
7. `FLUSH_INTERVAL` is 20

## Success Criteria Met

- [x] Three-tier extraction: JSON-LD (0.9) -> regex (0.7) -> LLM (0.65)
- [x] LLM only fires when structured extraction found nothing (cost savings)
- [x] Entity name filter prevents company names from becoming DecisionMakers
- [x] Incremental persistence flushes every 20 hotels via batch_persist_results()
- [x] Pipeline orchestrates: load -> harvest -> extract -> persist
- [x] --dry-run shows count without processing, --apply controls DB writes

## Next Phase Readiness

Plan 07-03 can proceed immediately. The pipeline (`discover_owners_cc`) is complete and needs only a CLI entrypoint (argparse) to be user-callable. The file is currently 793 lines.

Functions available for 07-03:
- `discover_owners_cc(args, cfg)` -- the main pipeline, ready to wire to argparse
- `SOURCE_CONFIGS` -- for --source argument
- All stats tracking built in -- ready for --audit reporting
