# State: Sadie GTM Owner Enrichment

**Last updated:** 2026-02-21 14:10 UTC

---

## Project Reference

**Core value:** Turn raw hotel data into actionable sales leads with verified owner/decision-maker contact info at 100K+ scale.

**Current focus:** Build batch-first CC-driven owner discovery pipeline, starting with CC Hotel Domain Sweep.

**Active branch:** feat/generic-enrich-contacts

---

## Current Position

**Milestone:** v2 -- Batch-First Owner Discovery
**Phase:** 7 -- CC Hotel Domain Sweep
**Plan:** 07-02 complete (LLM Extraction + Pipeline Orchestration)
**Status:** In progress
**Last activity:** 2026-02-21 -- Completed 07-02-PLAN.md (extraction + pipeline)

```
[##........] 20% (2/? plans in Phase 7)
```

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Plans completed | 2 |
| Plans total | TBD (Phase 7 in progress) |
| Phases completed | 0 / 6 |
| Requirements completed | 0 / 13 |

---

## Accumulated Context

### Key Decisions

| Decision | Rationale | Date |
|----------|-----------|------|
| v2 supersedes v1 | v1 scope was too broad (6 phases, 13 reqs); v2 focuses on one thing: batch-first owner discovery | 2026-02-21 |
| Batch-first over per-hotel waterfall | Contact enrichment proved batch CC sweep + concurrent processing is dramatically faster and cheaper | 2026-02-21 |
| CC as primary data source (~80%) | CC has most hotel pages cached; free vs Serper per-query costs | 2026-02-21 |
| Serper becomes optional fallback | CC + direct crawl should handle ~80% of owner discovery without paid API | 2026-02-21 |
| AWS Nova Micro for LLM extraction | Proven in contact enrichment; cheap, fast, good at structured extraction | 2026-02-21 |
| Defer Prefect adoption | DIY SQS chaining sufficient; revisit when scheduling needs arise | 2026-02-21 |
| DAG orchestration deferred to v3 | Build the improved owner discovery first, then wire it into automated DAG | 2026-02-21 |
| PIPE-03 bundled with Phase 7 | Incremental persistence and CLI entrypoint are foundational -- CC sweep needs them to be useful | 2026-02-21 |
| Phase 10 parallelizable | Batch RDAP/DNS/WHOIS has no dependency on CC results; can run alongside Phases 8-9 | 2026-02-21 |
| OWNER_PATHS is a 26-keyword superset of CONTACT_PATHS | Owner discovery needs about/team/management plus hotel-specific paths (our-hotel, proprietor) | 2026-02-21 |
| Homepage returns True from _is_owner_url | Many small hotel homepages contain owner info directly on the main page | 2026-02-21 |
| Three-tier extraction: JSON-LD (0.9) -> regex (0.7) -> LLM (0.65) | Structured data is free and highest confidence; LLM only when needed for cost savings | 2026-02-21 |
| Bedrock Semaphore(30) for Nova Micro | Bedrock throttles above ~30 concurrent requests; prevents 429 storms | 2026-02-21 |
| Incremental flush every 20 hotels | Crash at hotel 900 preserves first 880; uses existing batch_persist_results() | 2026-02-21 |

### Technical Notes

- Contact enrichment is substantially complete (Big4: 649/649 emails, 480/649 phones)
- Contact enrichment patterns to reuse: CC bulk sweep, CF Worker /batch, Nova Micro extraction, aiohttp concurrent fetch
- Current owner discovery is per-hotel 9-layer waterfall (slow at scale)
- CF Worker proxy operational ($5/mo for 10M requests)
- CC Index querying works across 3 indexes in parallel
- enrich_contacts.py CLI pattern (--source, --limit, --apply, --audit, --dry-run) is the template for discover_owners CLI
- discover_owners.py now has full pipeline: harvest -> extract -> persist (793 lines)
- Three-tier extraction verified: JSON-LD, regex, LLM with correct confidence scores and source tags

### Blockers

None currently.

---

## Session Continuity

**Last session:** 2026-02-21 14:10 UTC
**Stopped at:** Completed 07-02-PLAN.md
**Resume file:** None

**What just happened:** Completed Plan 07-02 (LLM Extraction + Pipeline Orchestration). Added three-tier owner extraction (JSON-LD -> regex -> LLM via Bedrock Nova Micro) and the main `discover_owners_cc()` pipeline function with incremental persistence every 20 hotels via `repo.batch_persist_results()`. The file is now 793 lines and the pipeline is functionally complete -- just needs a CLI entrypoint.

**What happens next:** Execute Plan 07-03 (CLI entrypoint with argparse + audit mode).

**Key files:**
- `workflows/discover_owners.py` -- CC harvest + extraction + pipeline (793 lines)
- `.planning/phases/07-cc-hotel-domain-sweep/07-02-SUMMARY.md` -- Plan 02 summary
- `.planning/phases/07-cc-hotel-domain-sweep/07-01-SUMMARY.md` -- Plan 01 summary
- `.planning/ROADMAP.md` -- v2 roadmap with 6 phases (7-12)
- `.planning/REQUIREMENTS.md` -- traceability updated with phase assignments
- `.planning/STATE.md` -- this file
- `.planning/PROJECT.md` -- project context
- `.planning/config.json` -- depth=comprehensive, mode=yolo, parallelization=enabled
