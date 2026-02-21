---
phase: 07-cc-hotel-domain-sweep
plan: 03
subsystem: owner-discovery
tags: [cli, argparse, audit, dry-run, owner-discovery, common-crawl]
dependency-graph:
  requires: [07-01, 07-02]
  provides: [discover-owners-cli, owner-audit-command]
  affects: [07-04, 08-xx]
tech-stack:
  added: []
  patterns: [enrich-contacts-cli-pattern, async-main-entrypoint]
key-files:
  created: []
  modified:
    - workflows/discover_owners.py
decisions:
  - id: D-0703-01
    description: "Default mode (no --apply) runs full pipeline but skips DB writes, then prompts to re-run with --apply"
    rationale: "Safe default prevents accidental DB mutations; matches enrich_contacts.py behavior"
  - id: D-0703-02
    description: "--dry-run only loads hotels and shows counts (no CC fetching)"
    rationale: "Distinct from default mode: dry-run skips network I/O entirely, useful for quick count checks"
metrics:
  duration: "~2 minutes"
  completed: "2026-02-21"
---

# Phase 7 Plan 03: CLI Entrypoint + Audit Summary

CLI entrypoint (argparse) and audit command for discover_owners.py, completing the CC owner discovery workflow. File grew from 793 to 945 lines.

## What Was Built

### Task 1: Audit Function + CLI Entrypoint (8ea3405)
Added `audit()` and `main()` to `workflows/discover_owners.py`:

- **`audit(args, cfg)`**: Queries hotel_decision_makers and hotel_owner_enrichment for coverage stats:
  - Hotels total, with website, enriched (CC sweep attempted), with DM found
  - Decision makers total, with email, without email
  - Source breakdown (unnest dm.sources, grouped + counted)
  - Verbose mode (-v): shows 20 most recent CC-discovered DMs with name, title, hotel, sources

- **`main()`**: Async CLI entrypoint following enrich_contacts.py pattern:
  - `--source` (required): big4, rms_au, or custom
  - `--where`: Custom SQL WHERE clause (required with --source custom)
  - `--audit`: Show coverage stats only (no processing)
  - `--apply`: Write results to DB (default: pipeline runs but doesn't persist)
  - `--dry-run`: Show hotel/domain counts without any CC fetching
  - `--limit`: Max hotels to process
  - `-v/--verbose`: Debug logging + verbose audit output

- **`if __name__ == "__main__"`**: Wires `asyncio.run(main())`

## Verification Results

All 3 verification commands passed:

1. **`--help`**: Shows all expected flags (--source, --audit, --apply, --dry-run, --limit, -v) with descriptions
2. **`--dry-run`**: Shows "Hotels: 307, Unique domains: 174, CC indexes: 3, Total CC queries: 522"
3. **`--audit`**: Shows full coverage stats:
   - 307 hotels total, 307 with website (100%)
   - 307 enriched (100%), 304 with DM found (99%)
   - 1046 decision makers total, 835 with email (80%), 211 without
   - 10 source types (chain_mgmt_lookup: 452, abn_lookup: 313, entity_website_crawl: 126, etc.)

## Key Patterns Reused

| Pattern | Source | Adaptation |
|---------|--------|------------|
| CLI argparse structure | enrich_contacts.py L1742-1797 | Same --source/--audit/--apply/--dry-run/--limit/-v flags |
| Source config dispatch | enrich_contacts.py L1770-1786 | Identical custom/named source handling |
| Audit SQL queries | enrich_contacts.py L1651-1737 | Adapted for owner discovery tables (hotel_owner_enrichment, hotel_decision_makers) |
| Logger setup | enrich_contacts.py L1767-1768 | Identical: logger.remove() + logger.add(stderr, level) |

## Deviations from Plan

None -- plan executed exactly as written.

## Success Criteria Met

- [x] CLI matches enrich_contacts.py pattern (--source, --limit, --apply, --audit, --dry-run, -v)
- [x] `--help` shows all flags with descriptions
- [x] `--dry-run` shows hotel/domain counts without CC fetching
- [x] `--audit` shows coverage stats with source breakdown
- [x] Default mode (no --apply) runs pipeline but does not persist, prompts user
- [x] File has `if __name__ == "__main__"` and exports `main`

## Complete Pipeline Architecture (Plans 01-03)

```
CLI (Plan 03)
  main() → argparse → dispatch
    |
    +-- --audit → audit()       [coverage stats from DB]
    +-- --dry-run → counts only [no network I/O]
    +-- default → discover_owners_cc()
          |
          1. load_hotels_for_cc_sweep()     [Plan 01]
          2. extract_hotel_domains()         [Plan 01]
          3. cc_harvest_owner_pages()        [Plan 01]
             - CC Index query via CF Worker /batch
             - WARC fetch + gzip decompress
          4. extract_owners_from_page()      [Plan 02]
             - JSON-LD (0.9) → regex (0.7) → LLM (0.65)
          5. Incremental flush every 20      [Plan 02]
             - repo.batch_persist_results()
          6. Stats summary
```

## Next Phase Readiness

The CC owner discovery pipeline is now complete and user-callable:
```bash
# Quick check
uv run python3 workflows/discover_owners.py --source big4 --dry-run

# Audit coverage
uv run python3 workflows/discover_owners.py --source big4 --audit

# Run pipeline (preview)
uv run python3 workflows/discover_owners.py --source big4 --limit 10

# Run pipeline (persist)
uv run python3 workflows/discover_owners.py --source big4 --limit 10 --apply
```

Ready for Plan 07-04 (if any) or Phase 8. The workflow file is 945 lines and self-contained.
