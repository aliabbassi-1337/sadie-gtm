# State: Sadie GTM Owner Enrichment

**Last updated:** 2026-02-21

---

## Project Reference

**Core value:** Turn raw hotel data into actionable sales leads with verified owner/decision-maker contact info at 100K+ scale.

**Current focus:** Build batch-first CC-driven owner discovery pipeline, starting with CC Hotel Domain Sweep.

**Active branch:** feat/generic-enrich-contacts

---

## Current Position

**Milestone:** v2 -- Batch-First Owner Discovery
**Phase:** 7 -- CC Hotel Domain Sweep
**Plan:** Not yet planned
**Status:** Roadmap complete, awaiting phase planning

```
[..........] 0% (0/6 phases)
```

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Plans completed | 0 |
| Plans total | TBD (not yet planned) |
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

### Technical Notes

- Contact enrichment is substantially complete (Big4: 649/649 emails, 480/649 phones)
- Contact enrichment patterns to reuse: CC bulk sweep, CF Worker /batch, Nova Micro extraction, aiohttp concurrent fetch
- Current owner discovery is per-hotel 9-layer waterfall (slow at scale)
- CF Worker proxy operational ($5/mo for 10M requests)
- CC Index querying works across 3 indexes in parallel
- enrich_contacts.py CLI pattern (--source, --limit, --apply, --audit, --dry-run) is the template for discover_owners CLI

### Blockers

None currently.

---

## Session Continuity

**What just happened:** Created v2 roadmap with 6 phases (7-12). CC Hotel Domain Sweep is Phase 7 with CC-01, CC-02, CC-03, PIPE-03 bundled together. All 13 requirements mapped.

**What happens next:** Plan Phase 7 (CC Hotel Domain Sweep) -- decompose into executable plans covering CC index query, WARC fetch, LLM extraction, incremental persistence, and CLI entrypoint.

**Key files:**
- `.planning/ROADMAP.md` -- v2 roadmap with 6 phases (7-12)
- `.planning/REQUIREMENTS.md` -- traceability updated with phase assignments
- `.planning/STATE.md` -- this file, current position at Phase 7
- `.planning/PROJECT.md` -- project context
- `.planning/MILESTONES.md` -- v1 archived
- `.planning/config.json` -- depth=comprehensive, mode=yolo, parallelization=enabled
