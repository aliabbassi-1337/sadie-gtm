# Requirements: Sadie GTM Owner Enrichment

**Version:** v1
**Last updated:** 2026-02-21
**Total v1 requirements:** 13

---

## v1 Requirements

### Owner/Decision Maker Pipeline

- [ ] **OWNER-01**: Complete the owner discovery waterfall as a fully automated DAG — hotel URL in, verified owner/DM contact info out, no manual stage-kicking
- [ ] **OWNER-02**: Contact enrichment pipeline runs end-to-end for existing DMs missing email/phone (CC harvest, email pattern guessing, SMTP/O365 verification)
- [ ] **OWNER-03**: Multiple data entry points (CC, gov data, direct URLs) all converge to the same enriched hotel record
- [ ] **OWNER-04**: Pipeline can process 100K+ hotels without manual orchestration between stages

### Common Crawl

- [ ] **CC-01**: Improved CC Index querying — targeted hotel URL discovery at scale with CC index caching
- [ ] **CC-02**: HTML extraction from CC WARC data with LLM-based structured data extraction (owner names, contact info, about page content)
- [ ] **CC-03**: CC as a data source for both hotel discovery (new hotels) and contact page content (enrichment)

### Government Data Expansion

- [ ] **GOV-01**: Expand government record ingestion beyond Florida DBPR to at least 2 additional states (Texas, New York recommended)
- [ ] **GOV-02**: Normalize government data to match existing hotel schema for entity matching
- [ ] **GOV-03**: Extract ownership/licensee information from state records as decision maker candidates

### Pipeline Resilience

- [ ] **RESIL-01**: Circuit breakers for external services — detect service degradation and stop sending requests until recovery
- [ ] **RESIL-02**: SQS Dead Letter Queue — capture persistent failures after N retries (maxReceiveCount=3), surface failed records for investigation
- [ ] **RESIL-03**: `layers_failed` bitmask on `hotel_owner_enrichment` — track which enrichment layers failed (not just completed) to enable selective retry

---

## v2 Requirements (Deferred)

### Data Quality
- False positive contact filtering (management company/web agency blocklist, WHOIS registrant cross-check)
- Multi-probe catch-all domain detection for email verification
- Per-source rate limiters (different limits for RDAP, DNS, WHOIS, website scraping)
- Bounded concurrency (replace unbounded asyncio.gather for 10K+ batches)
- Incremental persistence for contact enrichment (flush every N targets)

### Sales-Ready Features
- Lead quality score (composite 0-100 per hotel)
- Chain vs independent classification (brand reference list + fuzzy matching)
- Revenue estimation (room count * market ADR * occupancy benchmarks)

### Data Hygiene
- Entity resolution / cross-source deduplication (Splink + PostGIS proximity)
- Stale data detection + automatic re-enrichment scheduling
- Compliance foundation (suppression table, opt-out tracking, consent basis)

### Tech Detection
- Better booking engine detection (scrape booking pages directly)
- Broader tech stack detection (PMS, phone system, channel manager)

### Differentiators
- Intent signals (job posting monitoring via JobSpy)
- Google Maps deep enrichment (star rating, review count, owner response rate)
- Google Maps ingestion at scale
- Competitive intelligence / vendor displacement queries

---

## Out of Scope

- Non-hospitality verticals — hotels first, other verticals later
- Agentic outbound automation — focus on data pipeline
- AI agent workflows — current focus is batch LLM extraction
- User-facing UI dashboard — CLI + SQL + exports sufficient
- Real-time processing — batch is appropriate for this use case
- LinkedIn scraping — ToS risk, poor ROI
- OTA data scraping (Booking.com, Expedia) — legal risk, not useful for lead qualification
- Paid bulk data (ZoomInfo, Apollo, Clearbit) — $15K-60K/yr, poor for hospitality-specific intel
- Workflow orchestrator adoption (Prefect/Dagster) — defer until scheduling needs arise

---

## Traceability

| REQ-ID | Phase | Status |
|--------|-------|--------|
| OWNER-01 | — | Pending |
| OWNER-02 | — | Pending |
| OWNER-03 | — | Pending |
| OWNER-04 | — | Pending |
| CC-01 | — | Pending |
| CC-02 | — | Pending |
| CC-03 | — | Pending |
| GOV-01 | — | Pending |
| GOV-02 | — | Pending |
| GOV-03 | — | Pending |
| RESIL-01 | — | Pending |
| RESIL-02 | — | Pending |
| RESIL-03 | — | Pending |
