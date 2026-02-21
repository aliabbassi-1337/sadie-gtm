# Roadmap: Sadie GTM Owner Enrichment

**Version:** v2 -- Batch-First Owner Discovery
**Created:** 2026-02-21
**Depth:** Comprehensive
**Phases:** 6 (Phase 7-12, continuing from v1)
**Coverage:** 13/13 v2 requirements mapped

---

## Overview

Rebuild owner discovery from per-hotel sequential waterfall to batch-first CC-driven pipeline. CC bulk sweep discovers owner names for ~80% of hotel domains (free, cached HTML). Live crawling fills the ~20% gap. Batch RDAP/WHOIS/DNS adds structured domain intelligence. Email verification turns discovered names into actionable contacts. Waterfall orchestration ties the stages into a single pipeline invocation.

---

## Phase 7: CC Hotel Domain Sweep

**Goal:** Owner names, titles, and roles are extracted from Common Crawl cached HTML for the majority of hotel domains, with results incrementally persisted and accessible via CLI.

**Dependencies:** None (builds on existing CC infrastructure from contact enrichment)

**Requirements:** CC-01, CC-02, CC-03, PIPE-03

**Plans:** 3 plans

Plans:
- [ ] 07-01-PLAN.md -- CC harvest infrastructure (index query, WARC fetch, URL filtering)
- [ ] 07-02-PLAN.md -- Owner extraction (JSON-LD + regex + LLM) and incremental persistence
- [ ] 07-03-PLAN.md -- CLI entrypoint, audit command, and end-to-end verification

**Success Criteria:**
1. Running `discover_owners --source cc --limit 100 --apply` queries CC indexes for hotel domains, fetches WARC HTML for matches, extracts owner/manager names via Nova Micro, and persists results to `hotel_decision_makers`
2. Results flush to the database every N hotels (not all-or-nothing) -- a crash at hotel 900 of 1000 preserves the first 900
3. CC index queries target /about, /team, /contact, /management, /staff pages across multiple CC indexes in parallel via CF Worker /batch
4. LLM extraction produces structured output: person name, title/role, organizational relationship (owner vs. GM vs. management company)
5. CLI supports --dry-run (shows what would be processed), --audit (shows coverage stats), --source (selects data source), --limit (caps batch size)

---

## Phase 8: CC Third-Party Sources

**Goal:** Owner/manager identity is discovered from review site responses and business directory listings cached in Common Crawl, supplementing direct hotel domain extraction.

**Dependencies:** Phase 7 (CC infrastructure, persistence layer, CLI)

**Requirements:** CC-04, CC-05

**Success Criteria:**
1. CC index queries find TripAdvisor and Google cached pages for hotel properties, and LLM extraction identifies owner/manager names from management responses to reviews
2. CC index queries find BBB, Yelp, and local chamber of commerce pages listing hotel owner/operator information, with extracted names persisted as decision maker candidates
3. Third-party source results merge with Phase 7 hotel-domain results -- same person discovered from both hotel website and TripAdvisor shows as one record with multiple source attributions, not a duplicate

---

## Phase 9: Live Crawl Gap-Fill

**Goal:** Hotel domains not found in Common Crawl (~20%) are live-crawled to extract the same owner/manager information, ensuring no hotel is skipped due to CC coverage gaps.

**Dependencies:** Phase 7 (CC sweep identifies which domains have gaps)

**Requirements:** CRAWL-01, CRAWL-02

**Success Criteria:**
1. Running `discover_owners --source crawl` fetches /about, /team, /management, /staff pages via CF Worker proxy for all hotel domains that had zero CC hits, with the same LLM extraction and persistence as Phase 7
2. JS-heavy hotel sites that return empty/minimal content via httpx are automatically retried with crawl4ai headless browser, producing rendered HTML for LLM extraction
3. After CC sweep (Phase 7) + live crawl gap-fill, at least 90% of hotel domains with active websites have been processed for owner discovery

---

## Phase 10: Batch Structured Data

**Goal:** Domain registration and DNS records for all hotel domains are queried in batch, providing WHOIS registrant names, DNS infrastructure patterns, and email routing intelligence that supplement web-based owner discovery.

**Dependencies:** None (can run in parallel with Phases 8 and 9)

**Requirements:** DATA-01, DATA-02, DATA-03

**Success Criteria:**
1. Batch RDAP queries across all hotel domains complete in a single pipeline run (not one-at-a-time), with registrant names cached in `domain_whois_cache` and surfaced as decision maker candidates when they differ from known management companies
2. Batch DNS queries (MX, SOA, SPF, DMARC) across all hotel domains complete in a single run, with results cached in `domain_dns_cache` -- MX records inform email verification strategy (Google Workspace vs. O365 vs. self-hosted vs. catch-all)
3. WHOIS queries use live query with Wayback Machine fallback for pre-GDPR historical data, extracting registrant/admin contact names that may predate WHOIS privacy adoption
4. All structured data results integrate with the CLI (--source rdap, --source dns, --source whois) and use the same incremental persistence pattern from Phase 7

---

## Phase 11: Email Discovery and Verification

**Goal:** Every discovered owner/manager name is paired with a verified email address through pattern guessing, MX-aware verification, and O365/SMTP probing.

**Dependencies:** Phases 7-10 (owner names must be discovered before email can be guessed)

**Requirements:** PIPE-02

**Success Criteria:**
1. For each discovered owner name + hotel domain, email pattern candidates are generated (first.last@, f.last@, first@, etc.) and verified against the domain's MX infrastructure
2. Batch MX detection determines verification strategy per domain: O365 autodiscover for Microsoft-hosted, SMTP RCPT TO for self-hosted, skip for catch-all domains
3. Verified emails persist to `hotel_decision_makers` with source attribution showing which discovery method found the name and which verification method confirmed the email
4. Email discovery runs as `discover_owners --source email` and processes all decision makers with names but no verified email

---

## Phase 12: Waterfall Orchestration

**Goal:** A single command runs the entire batch-first owner discovery pipeline end-to-end: CC sweep, third-party CC, live crawl gap-fill, structured data, email verification -- each stage processing all hotels before the next begins.

**Dependencies:** Phases 7-11 (all individual stages must work independently before orchestration)

**Requirements:** PIPE-01

**Success Criteria:**
1. Running `discover_owners --source all --apply` executes the full waterfall: CC hotel domains first (cheapest, highest coverage), then CC third-party sources, then live crawl for gaps, then RDAP/WHOIS/DNS, then email verification -- each stage completes for ALL hotels before the next stage starts
2. Progress is observable: each stage reports hotels processed, owners found, and coverage delta (how many new owners this stage added beyond previous stages)
3. The pipeline can be resumed from any stage -- if CC sweep completed but crawl failed, re-running skips CC and starts from crawl
4. Running `discover_owners --audit` after a full pipeline run shows end-to-end coverage: total hotels, hotels with at least one owner name, hotels with verified email, coverage by source (CC vs. crawl vs. RDAP vs. WHOIS)

---

## Progress

| Phase | Name | Requirements | Status |
|-------|------|-------------|--------|
| 7 | CC Hotel Domain Sweep | CC-01, CC-02, CC-03, PIPE-03 | Complete |
| 8 | CC Third-Party Sources | CC-04, CC-05 | Not Started |
| 9 | Live Crawl Gap-Fill | CRAWL-01, CRAWL-02 | Not Started |
| 10 | Batch Structured Data | DATA-01, DATA-02, DATA-03 | Not Started |
| 11 | Email Discovery and Verification | PIPE-02 | Not Started |
| 12 | Waterfall Orchestration | PIPE-01 | Not Started |

---

## Dependency Graph

```
Phase 7 (CC Hotel Sweep) ----> Phase 8 (CC Third-Party)
                          \---> Phase 9 (Live Crawl Gap-Fill)
                          \------------------------------------> Phase 11 (Email Discovery)
Phase 10 (Batch Structured Data) -----------------------------> Phase 11
Phase 8 + Phase 9 + Phase 10 + Phase 11 ---------------------> Phase 12 (Waterfall Orchestration)
```

**Parallelization:** Phase 10 (Batch Structured Data) can run in parallel with Phases 8 and 9. Phase 7 must complete before 8, 9, or 11 can start. Phase 11 needs at least Phases 7+10 complete (owner names + MX data). Phase 12 needs all prior phases working independently before wiring them together.

---

## Coverage Map

| Requirement | Phase | Verified |
|-------------|-------|----------|
| CC-01 | Phase 7 | Yes |
| CC-02 | Phase 7 | Yes |
| CC-03 | Phase 7 | Yes |
| CC-04 | Phase 8 | Yes |
| CC-05 | Phase 8 | Yes |
| CRAWL-01 | Phase 9 | Yes |
| CRAWL-02 | Phase 9 | Yes |
| DATA-01 | Phase 10 | Yes |
| DATA-02 | Phase 10 | Yes |
| DATA-03 | Phase 10 | Yes |
| PIPE-01 | Phase 12 | Yes |
| PIPE-02 | Phase 11 | Yes |
| PIPE-03 | Phase 7 | Yes |

**Mapped: 13/13** -- all v2 requirements covered, no orphans, no duplicates.
