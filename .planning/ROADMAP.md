# Roadmap: Sadie GTM Owner Enrichment

**Version:** v1
**Created:** 2026-02-21
**Depth:** Comprehensive
**Phases:** 6
**Coverage:** 13/13 v1 requirements mapped

---

## Overview

Complete the Owner/Decision Maker pipeline end-to-end: finish contact enrichment (in-progress), expand government data sources early for maximum data coverage, harden resilience, add Common Crawl at scale, unify multi-source ingestion, then wire it all into a fully automated DAG that processes 100K+ hotels without manual orchestration. Data IN before polishing -- every phase adds either new data or the plumbing to handle it reliably.

---

## Phase 1: Contact Enrichment Pipeline

**Goal:** Existing decision makers missing email/phone get enriched automatically through CC harvest, email pattern guessing, and SMTP/O365 verification.

**Dependencies:** None (in-progress on feat/generic-enrich-contacts branch)

**Requirements:** OWNER-02

**Success Criteria:**
1. Running `enrich_contacts` on a batch of DMs with missing emails produces verified email addresses via pattern guessing + SMTP/O365 check
2. CC HTML harvest finds contact pages for hotel domains and extracts email/phone from them
3. Pipeline runs end-to-end without manual intervention between harvest, pattern guessing, and verification stages
4. Enrichment results persist to `hotel_decision_makers` with source attribution (which method found each contact)

**Research Flags:** None -- implementation is in-progress.

---

## Phase 2: Government Data Expansion

**Goal:** State government hotel/lodging records from at least 2 new states provide verified ownership data that supplements and validates the enrichment waterfall.

**Dependencies:** None (independent data source; can parallelize with Phase 1 and Phase 3)

**Requirements:** GOV-01, GOV-02, GOV-03

**Success Criteria:**
1. At least 2 additional state sources (Texas and/or New York recommended) are ingested alongside existing Florida DBPR data
2. Government records are normalized to the existing hotel schema -- matching against known hotels by name, address, and PostGIS proximity
3. Licensee/owner names from state records appear as decision maker candidates in `hotel_decision_makers` with `source='gov_[state]'`
4. Government-sourced ownership data is cross-referenced with waterfall-discovered owners to increase confidence scores

**Research Flags:** Each state has a unique data format and access method. Per-state research needed before implementation. Texas TDLR and New York DOS are the recommended targets.

---

## Phase 3: Pipeline Resilience

**Goal:** External service failures are detected, isolated, and recoverable -- the pipeline degrades gracefully instead of failing silently or wasting requests on down services.

**Dependencies:** None (applies to existing infrastructure; can parallelize with Phase 1 and Phase 2)

**Requirements:** RESIL-01, RESIL-02, RESIL-03

**Success Criteria:**
1. When an external service (RDAP, DNS, website scraping) returns errors for N consecutive requests, the circuit breaker trips and stops sending requests until a probe succeeds
2. SQS messages that fail processing after 3 attempts land in a Dead Letter Queue where they can be inspected and replayed
3. `hotel_owner_enrichment.layers_failed` bitmask tracks which enrichment layers failed (not just completed), and a selective retry command re-runs only failed layers for a hotel
4. `workflows/enrich_owners.py status` shows per-layer health (success rate, failure rate, circuit breaker state)

**Research Flags:** Circuit breaker thresholds (failure count, recovery probe interval) need tuning under real load.

---

## Phase 4: Common Crawl Pipeline

**Goal:** Common Crawl becomes a production data source that discovers new hotel URLs at scale and extracts structured owner/contact data from cached HTML -- all without hitting live websites.

**Dependencies:** Phase 3 (resilience patterns protect CC processing at scale)

**Requirements:** CC-01, CC-02, CC-03

**Success Criteria:**
1. CC index queries return hotel URLs for a target geographic region, with results cached locally to avoid redundant index hits
2. WARC HTML extraction pipeline pulls cached page content from CC for discovered URLs and runs LLM-based structured extraction (owner names, emails, phone numbers, about-page content)
3. CC-discovered hotels that do not exist in the database are ingested as new hotel records (discovery use case)
4. CC-extracted contact pages for existing hotels feed into the enrichment pipeline as an additional data source (enrichment use case)

**Research Flags:** CC data staleness (crawl dates vary) -- need confidence weighting by crawl recency. AWS Nova Micro extraction quality on CC HTML needs validation.

---

## Phase 5: Multi-Source Convergence

**Goal:** Hotels discovered or enriched from any source (Google Maps, Common Crawl, government records, direct URLs, CSV import) converge to a single canonical hotel record with merged enrichment data.

**Dependencies:** Phase 2 (government data as data source), Phase 4 (CC as data source)

**Requirements:** OWNER-03

**Success Criteria:**
1. A hotel discovered via CC that already exists from Google Maps is matched and merged (not duplicated) using domain, name+city, or PostGIS proximity matching
2. A hotel with government ownership data and waterfall-discovered contacts shows both sources on the same record with independent confidence scores
3. Running the convergence process on the full database produces zero net-new duplicates (verified by a dedup audit query)

**Research Flags:** Entity resolution matching thresholds need empirical tuning. Research recommends SQL-based matching for hotel records and Splink for fuzzy DM dedup.

---

## Phase 6: Automated DAG

**Goal:** The entire pipeline -- from hotel discovery through owner enrichment through contact verification -- runs as a fully automated DAG where completing one stage triggers the next, processing 100K+ hotels without manual stage-kicking.

**Dependencies:** Phase 1 (contact enrichment), Phase 3 (resilience), Phase 5 (multi-source convergence)

**Requirements:** OWNER-01, OWNER-04

**Success Criteria:**
1. A single CLI command (or SQS message) kicks off the full pipeline: hotel discovery -> owner enrichment waterfall -> contact enrichment -> verification, with each stage auto-triggering the next
2. 100K+ hotels process through the DAG without manual intervention between stages (no "run step A, wait, run step B")
3. Pipeline progress is observable: a status command shows how many hotels are at each stage, throughput rates, and estimated completion time
4. Partial failures in one stage do not block the pipeline -- failed hotels are captured (DLQ/failed bitmask) while successful ones flow to the next stage

**Research Flags:** Whether SQS chaining + health checks are sufficient or Prefect adoption is warranted should be evaluated after Phase 3 resilience patterns are in place. Research synthesis recommends deferring Prefect.

---

## Progress

| Phase | Name | Requirements | Status |
|-------|------|-------------|--------|
| 1 | Contact Enrichment Pipeline | OWNER-02 | Not Started |
| 2 | Government Data Expansion | GOV-01, GOV-02, GOV-03 | Not Started |
| 3 | Pipeline Resilience | RESIL-01, RESIL-02, RESIL-03 | Not Started |
| 4 | Common Crawl Pipeline | CC-01, CC-02, CC-03 | Not Started |
| 5 | Multi-Source Convergence | OWNER-03 | Not Started |
| 6 | Automated DAG | OWNER-01, OWNER-04 | Not Started |

---

## Dependency Graph

```
Phase 1 (Contact Enrichment) -----------------------> Phase 6 (Automated DAG)
Phase 2 (Government Data) -----> Phase 5 (Convergence) ---> Phase 6
Phase 3 (Pipeline Resilience) -> Phase 4 (Common Crawl) -> Phase 5
                            \---------------------------> Phase 6
```

**Parallelization:** Phases 1, 2, and 3 can all run in parallel (no dependencies between them). Phase 4 starts after Phase 3. Phase 5 starts after Phase 2 and Phase 4. Phase 6 starts after Phase 1, Phase 3, and Phase 5.

---

## Coverage Map

| Requirement | Phase | Verified |
|-------------|-------|----------|
| OWNER-01 | Phase 6 | Yes |
| OWNER-02 | Phase 1 | Yes |
| OWNER-03 | Phase 5 | Yes |
| OWNER-04 | Phase 6 | Yes |
| CC-01 | Phase 4 | Yes |
| CC-02 | Phase 4 | Yes |
| CC-03 | Phase 4 | Yes |
| GOV-01 | Phase 2 | Yes |
| GOV-02 | Phase 2 | Yes |
| GOV-03 | Phase 2 | Yes |
| RESIL-01 | Phase 3 | Yes |
| RESIL-02 | Phase 3 | Yes |
| RESIL-03 | Phase 3 | Yes |

**Mapped: 13/13** -- all v1 requirements covered, no orphans, no duplicates.
