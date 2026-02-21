# Requirements: Sadie GTM Owner Enrichment

**Version:** v2 -- Batch-First Owner Discovery
**Last updated:** 2026-02-21
**Total v2 requirements:** 13

---

## v2 Requirements

### CC Bulk Sweep

- [ ] **CC-01**: Query CC index in batch for all hotel domains (reuse CF Worker /batch pattern from contact enrichment), targeting /about, /team, /contact, /management, /staff pages across multiple CC indexes
- [ ] **CC-02**: Pull WARC HTML from CC for matched URLs via CF Worker /batch endpoint, decompress WARC records, extract clean HTML
- [ ] **CC-03**: Run Nova Micro LLM extraction on CC HTML to extract owner names, titles, roles, and organizational relationships as structured data
- [ ] **CC-04**: Search CC for hotel review site pages (TripAdvisor, Google cached pages) to find owner/manager responses that reveal identity
- [ ] **CC-05**: Search CC for business directory pages (BBB, Yelp, local chambers of commerce) that list hotel owner/operator information

### Live Crawl Gap-Fill

- [ ] **CRAWL-01**: aiohttp concurrent live crawling via CF Worker proxy for hotel domains not found in CC (~20%), targeting /about, /team, /management, /staff pages
- [ ] **CRAWL-02**: crawl4ai headless browser fallback for JS-heavy hotel sites that aiohttp cannot render, with same page targeting and LLM extraction

### Batch Structured Data

- [ ] **DATA-01**: Batch RDAP queries across all hotel domains simultaneously (not per-hotel sequential), with results cached in domain_whois_cache
- [ ] **DATA-02**: Batch DNS intelligence (MX, SOA, SPF, DMARC) across all hotel domains simultaneously, with results cached in domain_dns_cache
- [ ] **DATA-03**: Batch WHOIS (live query + Wayback Machine fallback for pre-GDPR data) across all hotel domains simultaneously

### Pipeline Architecture

- [ ] **PIPE-01**: Batch-level waterfall orchestration -- CC sweep first (cheapest, highest coverage), then live crawl gap-fill, then RDAP/WHOIS/DNS, then email verification -- each stage processes ALL hotels before the next stage begins
- [ ] **PIPE-02**: Email pattern guessing + batch MX detection + O365 autodiscover + SMTP verification for all discovered owner names, with source attribution persisted to hotel_decision_makers
- [ ] **PIPE-03**: Incremental persistence -- flush enrichment results to database every N hotels (not all-or-nothing), with CLI entrypoint matching enrich_contacts pattern (--source, --limit, --apply, --audit, --dry-run)

---

## Deferred (v3+)

### DAG Orchestration
- Automated chaining: owner discovery -> contact enrichment -> normalization/dedup
- SQS-based stage triggering without manual orchestration

### Government Data Expansion
- Texas TDLR, New York DOS, additional state sources
- Government record normalization and entity matching

### Pipeline Resilience
- Circuit breakers for external services
- SQS Dead Letter Queue for persistent failures
- layers_failed bitmask for selective retry

### Multi-Source Convergence
- Cross-source entity resolution (CC + Google Maps + gov records)
- Deduplication via Splink + PostGIS proximity

### Data Quality
- Management company / web agency blocklist
- Multi-probe catch-all domain detection
- Lead quality scoring (0-100 composite)

---

## Out of Scope

- Non-hospitality verticals -- hotels first
- Agentic outbound -- focus on data pipeline
- User-facing UI -- CLI + SQL sufficient
- Real-time processing -- batch is appropriate
- LinkedIn scraping -- ToS risk
- Paid bulk data (ZoomInfo, Apollo) -- budget, poor hospitality coverage
- Workflow orchestrator (Prefect/Dagster) -- defer until scheduling needs arise

---

## Traceability

| REQ-ID | Phase | Status |
|--------|-------|--------|
| CC-01 | Phase 7 | Pending |
| CC-02 | Phase 7 | Pending |
| CC-03 | Phase 7 | Pending |
| CC-04 | Phase 8 | Pending |
| CC-05 | Phase 8 | Pending |
| CRAWL-01 | Phase 9 | Pending |
| CRAWL-02 | Phase 9 | Pending |
| DATA-01 | Phase 10 | Pending |
| DATA-02 | Phase 10 | Pending |
| DATA-03 | Phase 10 | Pending |
| PIPE-01 | Phase 12 | Pending |
| PIPE-02 | Phase 11 | Pending |
| PIPE-03 | Phase 7 | Pending |
