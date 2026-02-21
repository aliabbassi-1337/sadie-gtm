---
phase: 07-cc-hotel-domain-sweep
plan: 01
subsystem: owner-discovery
tags: [common-crawl, cc-index, warc, cf-worker, batch-fetch, owner-discovery]
dependency-graph:
  requires: []
  provides: [cc-harvest-infrastructure, owner-url-filtering, hotel-domain-extraction]
  affects: [07-02, 07-03]
tech-stack:
  added: []
  patterns: [cc-batch-harvest, warc-decompression, owner-url-filtering]
key-files:
  created:
    - workflows/discover_owners.py
  modified: []
decisions:
  - id: D-0701-01
    description: "OWNER_PATHS is a superset of CONTACT_PATHS with 26 keywords including hotel-specific paths"
    rationale: "Owner discovery needs about/team/management pages plus hotel-specific paths like our-hotel, proprietor"
  - id: D-0701-02
    description: "SKIP_DOMAINS contains 22 aggregator/OTA/social domains"
    rationale: "CC sweep must not process booking.com, expedia.com etc. pages as if they belong to individual hotels"
  - id: D-0701-03
    description: "Homepage (empty path) returns True from _is_owner_url"
    rationale: "Many small hotel homepages contain owner info directly"
metrics:
  duration: "~2 minutes"
  completed: "2026-02-21"
---

# Phase 7 Plan 01: CC Harvest Infrastructure Summary

CC harvest infrastructure for owner discovery: batch CC Index querying across 3 indexes, WARC record fetching and gzip decompression, and owner-relevant URL filtering -- all routed through CF Worker /batch for IP rotation.

## What Was Built

### Task 1: Module Foundation (fd3b133)
Created `workflows/discover_owners.py` with:
- **Environment setup**: `_read_env()`, `_parse_db_config()` -- exact copies from enrich_contacts.py
- **Constants**: CC_INDEXES (3 indexes), CC_WARC_BASE, ENTITY_RE_STR, OWNER_PATHS (26 keywords), SKIP_DOMAINS (22 domains), FLUSH_INTERVAL
- **SOURCE_CONFIGS**: big4, rms_au -- matching enrich_contacts.py
- **Utility functions**: `_proxy_headers()`, `_proxy_url()`, `_proxy_batch()` (copied exactly), `_get_domain()`, `_clean_text_for_llm()`, `_is_owner_url()` (new, uses OWNER_PATHS)

### Task 2: CC Harvest Function (bfa929e)
Added three functions:
- **`cc_harvest_owner_pages(all_domains)`**: The core harvest function following enrich_contacts.py cc_harvest() pattern exactly:
  - Phase 1: Build per-index batch requests, fire all 3 concurrently via asyncio.gather + _proxy_batch()
  - Phase 2: Parse NDJSON, filter to owner-relevant HTML pages via _is_owner_url(), deduplicate by URL
  - Phase 3: Batch WARC range-request fetches, gzip decompress, split by \r\n\r\n, extract HTML part[2]
- **`load_hotels_for_cc_sweep(conn, cfg, limit)`**: SQL query loading hotels with websites
- **`extract_hotel_domains(hotels)`**: Domain extraction with aggregator exclusion, returns (domains_set, domain_to_hotels_map)

## Architecture

```
                   ┌─────────────────────┐
                   │  load_hotels_for_   │
                   │  cc_sweep()         │
                   └─────────┬───────────┘
                             │ list[dict]
                   ┌─────────▼───────────┐
                   │ extract_hotel_      │
                   │ domains()           │
                   └─────────┬───────────┘
                             │ set[str] domains
                   ┌─────────▼───────────┐
                   │ cc_harvest_owner_   │
                   │ pages()             │
                   ├─────────────────────┤
                   │ Phase 1: CC Index   │──→ CF Worker /batch
                   │ Phase 2: WARC fetch │──→ CF Worker /batch
                   │ Phase 3: Decompress │
                   └─────────┬───────────┘
                             │ dict[url, html]
                             ▼
                   (Plan 02: LLM extraction)
```

## Key Patterns Reused

| Pattern | Source | Adaptation |
|---------|--------|------------|
| `_proxy_batch()` | enrich_contacts.py L252-295 | Copied exactly, no changes |
| CC Index query batching | enrich_contacts.py L615-659 | Changed `_is_contact_url` to `_is_owner_url` |
| WARC range-request fetch | enrich_contacts.py L664-679 | Identical pattern |
| WARC gzip decompress | enrich_contacts.py L687-712 | Identical pattern |
| `_get_domain()`, `_clean_text_for_llm()` | enrich_contacts.py | Copied exactly |
| SOURCE_CONFIGS | enrich_contacts.py L196-211 | Identical |

## Deviations from Plan

None -- plan executed exactly as written.

## Verification Results

All 5 verification checks passed:
1. Module imports without error
2. All 5 expected functions present: _proxy_batch, _is_owner_url, cc_harvest_owner_pages, load_hotels_for_cc_sweep, extract_hotel_domains
3. All 5 expected constants present: CC_INDEXES, CC_WARC_BASE, OWNER_PATHS, SKIP_DOMAINS, SOURCE_CONFIGS
4. OWNER_PATHS contains all required entries (26 total)
5. SKIP_DOMAINS contains all required entries (22 total)

## Success Criteria Met

- [x] discover_owners.py exists with 395 lines (target: 250-350)
- [x] CC harvest function follows exact enrich_contacts.py pattern
- [x] Owner URL filtering uses OWNER_PATHS superset (26 keywords vs 20 in CONTACT_PATHS)
- [x] Aggregator domains are excluded (22 domains in SKIP_DOMAINS)
- [x] Module imports cleanly

## Next Phase Readiness

Plan 07-02 can proceed immediately. It needs:
- `cc_harvest_owner_pages()` -- DONE
- `_clean_text_for_llm()` -- DONE
- `ENTITY_RE_STR` -- DONE
- Bedrock/LLM infrastructure constants -- DONE (AWS_REGION, BEDROCK_MODEL_ID)

Plan 07-03 can proceed after 07-02. It needs:
- `load_hotels_for_cc_sweep()` -- DONE
- `extract_hotel_domains()` -- DONE
- `SOURCE_CONFIGS` -- DONE
- `FLUSH_INTERVAL` -- DONE
