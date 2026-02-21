# State: Sadie GTM Owner Enrichment

**Last updated:** 2026-02-21

---

## Project Reference

**Core value:** Turn raw hotel data into actionable sales leads with verified owner/decision-maker contact info at 100K+ scale.

**Current focus:** Rearchitect owner discovery to batch-first CC-driven approach.

**Active branch:** feat/generic-enrich-contacts

---

## Current Position

**Milestone:** v2 -- Batch-First Owner Discovery
**Phase:** Defining requirements
**Plan:** Not yet planned
**Status:** Initializing

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Plans completed | 0 |
| Plans total | TBD (not yet planned) |
| Phases completed | 0 / TBD |
| Requirements completed | 0 / TBD |

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

### Technical Notes

- Contact enrichment is substantially complete (Big4: 649/649 emails, 480/649 phones)
- Contact enrichment patterns to reuse: CC bulk sweep, CF Worker /batch, Nova Micro extraction, aiohttp concurrent fetch
- Current owner discovery is per-hotel 9-layer waterfall (slow at scale)
- CF Worker proxy operational ($5/mo for 10M requests)
- CC Index querying works across 3 indexes in parallel

### Blockers

None currently.

---

## Session Continuity

**What just happened:** Started v2 milestone — Batch-First Owner Discovery. Archived v1 (partially complete, contact enrichment done). Skipped research (domain well-understood from v1 research + contact enrichment experience).

**What happens next:** Define v2 requirements, then create roadmap.

**Key files:**
- `.planning/PROJECT.md` -- updated with v2 milestone
- `.planning/MILESTONES.md` -- v1 archived
- `.planning/REQUIREMENTS.md` -- to be created for v2
- `.planning/ROADMAP.md` -- to be created for v2
- `.planning/config.json` -- depth=comprehensive, mode=yolo, parallelization=enabled
