# Feature Landscape

**Domain:** GTM hotel lead enrichment and sales intelligence
**Researched:** 2026-02-21
**Existing pipeline context:** Brownfield -- hotel scraping, booking engine detection, 9-layer owner discovery waterfall, contact enrichment already built.

---

## What Already Exists (Pipeline Inventory)

Before recommending new features, here is what the pipeline already does, based on codebase review. Every recommendation below is scoped relative to this baseline.

| Capability | Module | Status |
|---|---|---|
| Hotel scraping (Google Maps, Common Crawl, gov records) | `workflows/scraper.py`, `workflows/ingest_*.py` | Production |
| Booking engine detection (Playwright, URL fingerprinting) | `services/leadgen/detector.py` | Production |
| Room count enrichment (LLM extraction from booking pages) | `services/enrichment/room_count_enricher.py` | Production |
| Customer proximity scoring (PostGIS) | `services/enrichment/customer_proximity.py` | Production |
| Owner discovery (9-layer waterfall: RDAP, WHOIS, DNS, Website, Reviews, Gov, ABN/ASIC, CT Certs, Email) | `services/enrichment/owner_enricher.py` | Production |
| Contact enrichment (CC harvest, email pattern guessing, SMTP/O365 verify) | `workflows/enrich_contacts.py` | In progress |
| Domain intel caching (WHOIS, DNS, CT, ABN) | `lib/owner_discovery/*.py` | Production |
| Pipeline status + export | `workflows/pipeline_status.py`, `workflows/export.py` | Production |
| CF Worker proxy for IP rotation | `infra/cf-worker-proxy/` | Production |
| SQS-based distributed processing | `workflows/*_consumer.py`, `workflows/*_enqueue.py` | Production |

---

## Table Stakes

Features the pipeline needs to be considered complete for GTM sales use. Missing any of these means the sales team will supplement with manual research or a competing tool.

### TS-1: Data Quality Scoring

**What:** Assign a confidence/completeness score to each hotel lead so the sales team knows which leads are actionable vs. need more work.

**Why Expected:** Every GTM platform (ZoomInfo, Apollo, Clay) shows data quality indicators. Without scoring, the sales team wastes time on leads with wrong emails or stale contacts. The existing `DecisionMaker.confidence` field (0.0-1.0) is per-person but there is no hotel-level "lead score" that rolls up completeness across all enrichment dimensions.

**What to Build:**
- Hotel-level composite score (0-100) based on: has verified email (weighted high), has phone, has decision maker name, has room count, has booking engine detected, has location, data freshness
- Per-field "how we got this" provenance (already partially tracked via `sources` list on DecisionMaker and `layers_completed` bitmask)
- Dashboard query: "show me all hotels with score >= 70 in Florida"

**Complexity:** Low-Medium. This is a SQL view or materialized view over existing tables. No new data collection, just aggregation of what already exists.

**Dependencies:** None (all underlying data already collected).

**Implementation hint:** A `lead_quality_score` materialized view joining `hotels`, `hotel_decision_makers`, `hotel_booking_engines`, `hotel_room_count`. Refresh on each enrichment run.

---

### TS-2: Chain vs. Independent Classification

**What:** Classify each hotel as independent, franchise, chain-managed, or management-company-operated. Include the parent brand/management company name when applicable.

**Why Expected:** Sadie's sales team needs to know if they're pitching to a GM who can make buying decisions (independent) vs. someone who needs corporate approval (franchise/chain). This fundamentally changes the sales motion and is the first question any hospitality sales rep asks.

**What to Build:**
- Chain brand detection from hotel name fuzzy matching against a reference list (Marriott, Hilton, IHG, Wyndham, Choice, Best Western, Hyatt, Accor + sub-brands)
- Management company identification from WHOIS/website scraping (already partially captured as `registrant_org`)
- Flag: `ownership_type` enum: `independent`, `franchise`, `chain_managed`, `management_company`, `unknown`
- Store parent company/brand when known

**Complexity:** Medium. The brand reference list exists publicly (Wikipedia, STR chain scales, Amadeus GitHub dataset). Name matching is fuzzy but tractable. Management company detection requires connecting WHOIS `registrant_org` to known management companies.

**Dependencies:** Hotel name data (exists), WHOIS/website data (exists via owner discovery).

**Data sources (free):**
- Amadeus hotel chains dataset on GitHub (MIT license, comprehensive)
- Wikipedia list of chained-brand hotels
- STR chain scale classifications (public PDF)
- Hotel name pattern matching (e.g., "Fairfield Inn" -> Marriott)

---

### TS-3: Broader Tech Stack Detection (Beyond Booking Engine)

**What:** Detect not just the booking engine but the full technology stack: PMS, channel manager, CRM, revenue management system, phone system, WiFi provider, payment processor.

**Why Expected:** Sadie sells a voice AI product. Knowing the hotel's phone system (RingCentral, Mitel, Avaya, legacy PBX) and existing tech sophistication (cloud PMS vs. on-prem Opera) directly determines product fit and objection handling. The pipeline already detects booking engines via Playwright. Extending to other technologies uses the same infrastructure.

**What to Build:**
- Technology fingerprint library (regex patterns for JS variables, script URLs, meta tags, network requests) modeled on Wappalyzer's open-source pattern format
- Priority categories for hospitality:
  - PMS: Opera, Mews, Cloudbeds, RoomRaccoon, WebRezPro, Little Hotelier, RMS Cloud
  - Channel Manager: SiteMinder, Booking.com Channel Manager, STAAH
  - CRM: Revinate, Cendyn, Salesforce
  - Revenue Management: IDeaS, Duetto, Atomize
  - Phone/VoIP: RingCentral, Dialpad, 8x8, Mitel, Avaya (meta tags, script loads)
  - Chat/Messaging: Asksuite, Quicktext, HiJiffy (widget scripts detectable)
  - WiFi: Ruckus, UniFi (captive portal detection)
- Extend existing `hotel_booking_engines` table to a more general `hotel_tech_stack` table
- Leverage existing Playwright infrastructure in `detector.py`

**Complexity:** Medium-High. The Playwright infrastructure exists, but building and maintaining a hospitality-specific fingerprint library requires ongoing curation. Start with 20-30 most common technologies.

**Dependencies:** Existing Playwright detector, hotel websites.

**Build vs. Buy:**
- Wappalyzer API: $50/mo for 5K lookups. Good for validation, too expensive at 100K+ scale.
- BuiltWith API: Similar pricing constraints.
- DIY fingerprinting: Free, uses existing Playwright infrastructure, full control. Recommended path. Use Wappalyzer's open-source pattern format as a starting point, add hospitality-specific patterns.

---

### TS-4: Deduplication and Entity Resolution

**What:** Merge hotel records that refer to the same physical property but entered via different sources (Google Maps, Common Crawl, DBPR, direct scraping).

**Why Expected:** The pipeline has multiple data entry points (`ingest_csv.py`, `ingest_dbpr.py`, `ingest_rms.py`, `scraper.py`, `commoncrawl_enum.py`). Without deduplication, the sales team gets duplicate leads, which is the fastest way to erode trust in the data. A `deduplicate_unified.py` workflow already exists, suggesting this is a known problem.

**What to Build:**
- Probabilistic entity resolution using: name similarity (Jaro-Winkler or similar), address normalization + matching, phone number matching, domain/website matching, lat/lng proximity (PostGIS `ST_DWithin`)
- Merge strategy: keep richest record as primary, link others as aliases
- Cross-source confidence: hotel found in 3 sources is higher confidence than 1

**Complexity:** Medium. PostGIS proximity matching is cheap. Name/address fuzzy matching is well-understood. The hard part is deciding merge policy when records conflict.

**Dependencies:** PostGIS (exists), hotel records from multiple sources (exist).

---

### TS-5: Stale Data Detection and Re-enrichment

**What:** Track data freshness per field and automatically re-enrich hotels whose data is older than a threshold (e.g., 90 days for contact info, 180 days for tech stack).

**Why Expected:** Hotel GMs turn over every 2-3 years. Management companies change. Email addresses go stale. A pipeline that enriches once and never refreshes degrades rapidly. The `hotel_owner_enrichment` table tracks `layers_completed` but not per-field timestamps for data age.

**What to Build:**
- Per-field `last_verified_at` timestamps on critical data (email, phone, decision maker name, booking engine)
- Configurable staleness thresholds by field type
- Re-enrichment queue: hotels whose data has aged past threshold get re-queued
- Diff tracking: detect when data changes on re-enrichment (new GM vs. same GM)

**Complexity:** Low-Medium. Schema additions + a periodic job that queries for stale records and re-queues them through existing enrichment workflows.

**Dependencies:** Existing enrichment workflows (all exist), schema additions.

---

## Differentiators

Features that give the pipeline competitive advantage over manual research or generic tools like ZoomInfo/Apollo (which don't specialize in hospitality).

### D-1: Intent Signals (Renovation, Hiring, Expansion)

**What:** Monitor external signals that indicate a hotel is likely to buy new technology: renovation permits, GM job postings, expansion announcements, management company changes.

**Value Proposition:** Intent signals let the sales team prioritize outreach to hotels that are *currently* making buying decisions, rather than cold-calling static lists. This is the single highest-leverage feature for sales conversion.

**Signal Sources:**

| Signal | Source | Cost | Detection Method | Value |
|---|---|---|---|---|
| Renovation/construction permits | Shovels.ai API (180M permits, 85% US coverage) | Paid API | Match hotel address to permit records | Hotel renovating = likely upgrading tech stack |
| Job postings (GM, IT, Revenue Manager) | Indeed/LinkedIn via JobSpy OSS library | Free (scraping) | Monitor hotel name + location on job boards | New GM = new vendor decisions |
| Management company changes | State business registry filings | Free | Periodic re-check of state records | New management = tech stack review |
| Google review volume changes | Serper/Google Places | Low cost | Track review count over time | Declining reviews = pain point |
| Website changes | Wayback Machine diff or periodic re-scrape | Free | Compare page hashes over time | New website = tech modernization |
| Social media hiring posts | Google search | Low cost | Serper search for "[hotel name] hiring" | |

**Complexity:** High. Each signal source requires its own ingestion pipeline. Start with just job posting monitoring (free, highest signal-to-noise ratio) and renovation permits (Shovels API if budget allows).

**Dependencies:** Hotel name + location (exist). New tables for signal tracking.

**Recommended phasing:**
1. Job posting monitoring via JobSpy (free, Python library, 1-2 days)
2. Website change detection via periodic re-scrape hashing (free, 1 day)
3. Renovation permits via Shovels API (paid, budget-dependent)

---

### D-2: Revenue Estimation

**What:** Estimate annual room revenue for each hotel using: room count (already collected), location-based ADR benchmarks, and estimated occupancy rates.

**Value Proposition:** Revenue estimation lets the sales team prioritize high-value hotels. A 200-room hotel at $150 ADR is a very different prospect than a 20-room motel at $60 ADR. ZoomInfo/Apollo do not have hospitality-specific revenue estimates.

**What to Build:**
- ADR lookup table by market/submarket (sourced from STR public press releases, market reports)
- Occupancy rate estimates by market type (urban, suburban, resort, highway)
- Formula: `estimated_annual_revenue = room_count * ADR * occupancy_rate * 365`
- Revenue tier classification: micro (<$500K), small ($500K-2M), medium ($2M-10M), large ($10M+)

**Complexity:** Low. Room count already exists. ADR benchmarks are public. This is arithmetic on existing data plus a reference table.

**Dependencies:** Room count enrichment (exists), market/location data (exists).

---

### D-3: Competitive Intelligence (Vendor Displacement)

**What:** Track which technology vendors each hotel uses, specifically to enable "vendor displacement" sales campaigns: "Hotels using [competitor X] that would benefit from switching to Sadie."

**Value Proposition:** Instead of generic outreach, the sales team can say "I see you're using [old phone system]. Hotels like yours have switched to Sadie for [specific benefit]." This is the #1 most effective outbound sales approach.

**What to Build:**
- Extends TS-3 (tech stack detection) with vendor-specific targeting queries
- "Find all hotels in Texas using RingCentral" or "Find all hotels NOT using any VoIP system"
- Competitor presence/absence as a lead scoring input
- Track tech stack changes over time (hotel switched from X to Y)

**Complexity:** Low (given TS-3 is built). This is query/reporting on top of tech stack data.

**Dependencies:** TS-3 (tech stack detection).

---

### D-4: Google Maps Deep Enrichment

**What:** Go beyond basic Google Maps scraping to extract: star rating, review count, review sentiment, owner response rate, photo count, category tags, popular times, question/answer content.

**Value Proposition:** Google Maps is the richest free source of hotel intelligence. Review sentiment and owner response rate are proxies for operational sophistication. Hotels with low ratings and no owner responses are more likely pain points Sadie can solve. The pipeline already uses Serper for Google review mining; this extends that capability.

**What to Build:**
- Structured extraction from Google Maps/Places data:
  - Star rating + review count (trending up or down)
  - Owner response rate (how many reviews the owner replies to)
  - Category tags (hotel, motel, resort, B&B, boutique)
  - Photo count (proxy for marketing investment)
  - Claimed/unclaimed business listing status
- Periodic re-scrape to track changes
- Use Serper Places API (already integrated) or Outscraper/Apify for deeper data

**Complexity:** Medium. Serper Places API is already integrated in `website_enricher.py`. Extending to richer data extraction is incremental. The harder part is rate-limited scraping at 100K+ scale.

**Dependencies:** Serper API (exists), hotel name + location (exist).

---

### D-5: Government Record Expansion

**What:** Expand government data ingestion beyond Florida DBPR to other states and data types: business licenses, liquor licenses, health inspection scores, tax assessor property records.

**Value Proposition:** Government records are free, authoritative, and contain ownership information that is not privacy-protected. The Florida DBPR integration proves the pattern works. Expanding to other states (Texas, California, New York) dramatically increases coverage. Health inspection scores and liquor licenses confirm active operation.

**Key State Sources:**

| State | Source | Data Available | Difficulty |
|---|---|---|---|
| Texas | TDLR (lodging licenses) | Owner name, address, license status | Medium (HTML scraping) |
| California | CalOSHA + CDTFA | Tax permits, hotel occupancy tax | Medium (PDF/CSV) |
| New York | DOS Business Entity Search | Business owner, registered agent | Low (searchable) |
| Nevada | NV Business Search | Owner, registered agent | Low (searchable) |
| Hawaii | DCCA Business Search | Owner, registered agent | Low (searchable) |
| All US | State SOS (Secretary of State) | Business entity, registered agent, officers | Medium (varies by state) |

**Complexity:** Medium per state. Each state has different data format, access method, and rate limits. But the pattern is the same: scrape/download, normalize, match to hotel records.

**Dependencies:** Hotel name + location for matching (exist). `ingest_dbpr.py` as template (exists).

---

### D-6: Email Deliverability Verification

**What:** Add real-time email deliverability verification beyond SMTP RCPT TO, using bounce prediction, mailbox-full detection, and disposable email filtering.

**Value Proposition:** The existing email verification (`email_discovery.py`) uses SMTP RCPT TO and O365 GetCredentialType. SMTP verification is increasingly unreliable as servers block it. Adding a secondary verification method (e.g., Reacher self-hosted, or MillionVerifier API for batch) would improve email accuracy.

**What to Build:**
- Self-hosted Reacher (open-source email verifier) as a verification backend
- Batch verification for all discovered emails before marking as verified
- Categorize: deliverable, risky, undeliverable, catch-all, disposable
- Feed verification results back into lead quality score

**Complexity:** Low-Medium. Reacher is open-source and can run in Docker. Integration is an HTTP API call.

**Dependencies:** Email discovery (exists). Docker/infrastructure capacity.

---

## Anti-Features

Features to deliberately NOT build. Common mistakes in this domain that would waste time or create risk.

### AF-1: DO NOT Build a LinkedIn Scraper

**Why avoid:** LinkedIn scraping violates their Terms of Service. Even though HiQ v. LinkedIn created legal ambiguity, LinkedIn aggressively detects and blocks scrapers, bans accounts, and has sent cease-and-desist letters. The risk-reward ratio is terrible for a small team.

**What to do instead:** Use LinkedIn data indirectly:
- Job postings (public, no login required via JobSpy)
- Company page data via Google search cache
- Proxycurl API for occasional targeted lookups (paid, compliant)
- Decision maker name from other sources, then manually verify on LinkedIn

---

### AF-2: DO NOT Build Real-Time Processing

**Why avoid:** The pipeline processes 100K+ hotels in batch. Real-time enrichment (webhook-triggered, on-demand) adds massive complexity (event sourcing, exactly-once processing, real-time data freshness) with no proportional value. The sales team works on weekly/monthly cadences, not real-time.

**What to do instead:** Keep batch processing. Run enrichment nightly/weekly. Provide a "refresh this hotel" CLI command for ad-hoc needs.

---

### AF-3: DO NOT Pay for Bulk Data from ZoomInfo/Apollo/Clearbit

**Why avoid:** These platforms charge $15K-60K/year. Their data is generic B2B (company size, revenue, employee count) and poor for hospitality-specific intelligence (room count, booking engine, ownership structure). The pipeline's free/cheap sources (RDAP, DNS, CC, gov records, Google reviews) provide more relevant hospitality data.

**What to do instead:** Continue the free-first approach. Use paid APIs (Serper, Shovels) only for high-signal data that cannot be obtained freely.

---

### AF-4: DO NOT Build a UI Dashboard (Yet)

**Why avoid:** The operator is the founder/engineer. CLI + SQL queries + exports are sufficient. Building a web UI is a multi-week effort that serves vanity over utility at this stage.

**What to do instead:** Improve CLI reporting (`pipeline_status.py`), add more SQL views for common queries, export to Google Sheets or Excel for the sales team.

---

### AF-5: DO NOT Build Outbound Email Automation

**Why avoid:** Mixing data enrichment with email sending creates deliverability risk. If the enrichment pipeline's domain gets blacklisted for sending, it breaks email verification (SMTP RCPT TO). Outbound automation is also a separate product category (Reply.io, Instantly, etc.) with significant deliverability engineering.

**What to do instead:** Export enriched leads to the team's existing outbound tool (HubSpot, Reply.io, etc.). Keep enrichment and outbound as separate systems.

---

### AF-6: DO NOT Scrape OTA Data (Booking.com, Expedia Prices)

**Why avoid:** OTAs aggressively block scraping, have strong legal teams, and their pricing data is not useful for lead qualification. Room pricing changes hourly and is not a stable attribute for GTM targeting.

**What to do instead:** Use room count + market ADR benchmarks for revenue estimation (D-2). Much simpler, more stable, and no legal risk.

---

## Feature Dependencies

```
TS-1 (Lead Quality Score)
  depends on: nothing (aggregates existing data)
  unlocks: better prioritization for all downstream features

TS-2 (Chain vs Independent)
  depends on: hotel name data (exists)
  unlocks: sales motion targeting, D-3 (competitive intel)

TS-3 (Broader Tech Stack)
  depends on: existing Playwright detector
  unlocks: D-3 (competitive intel / vendor displacement)

TS-4 (Deduplication)
  depends on: PostGIS (exists)
  unlocks: cleaner data for everything else

TS-5 (Stale Data Detection)
  depends on: schema additions
  unlocks: ongoing data quality maintenance

D-1 (Intent Signals)
  depends on: hotel name + location (exist)
  unlocks: timing-based outreach prioritization

D-2 (Revenue Estimation)
  depends on: room count enrichment (exists)
  unlocks: value-based lead prioritization

D-3 (Competitive Intel)
  depends on: TS-3 (tech stack detection)
  unlocks: vendor displacement campaigns

D-4 (Google Maps Deep)
  depends on: Serper API (exists)
  unlocks: operational health scoring

D-5 (Gov Record Expansion)
  depends on: ingest_dbpr.py pattern (exists)
  unlocks: more ownership data in more states

D-6 (Email Verification)
  depends on: email discovery (exists)
  unlocks: higher email deliverability
```

---

## MVP Recommendation (Next Phase)

Based on impact-to-effort ratio and existing infrastructure, prioritize in this order:

### Immediate (Low effort, high impact)

1. **TS-1: Lead Quality Score** -- SQL view, no new data collection, instantly useful for sales prioritization. 1-2 days.
2. **TS-2: Chain vs Independent** -- Reference list + fuzzy name matching. Fundamentally changes how sales approaches each lead. 2-3 days.
3. **D-2: Revenue Estimation** -- Arithmetic on existing room count data + public ADR benchmarks. 1-2 days.

### Near-term (Medium effort, high impact)

4. **TS-4: Deduplication** -- Critical as more data sources are added. PostGIS + name matching. 3-5 days.
5. **TS-5: Stale Data Detection** -- Schema additions + periodic re-enrichment queue. Prevents data rot. 2-3 days.
6. **D-4: Google Maps Deep Enrichment** -- Extends existing Serper integration. Star rating, review count, response rate. 2-3 days.

### Strategic (Higher effort, high differentiation)

7. **TS-3: Broader Tech Stack Detection** -- Requires hospitality fingerprint library curation. High value for vendor displacement selling. 1-2 weeks.
8. **D-1: Intent Signals (Job Postings)** -- Start with JobSpy for free job board monitoring. Highest sales conversion impact. 3-5 days.
9. **D-5: Gov Record Expansion** -- One state at a time. Texas and New York first. 3-5 days per state.
10. **D-3: Competitive Intel** -- Builds on TS-3. Query layer + reporting. 2-3 days after TS-3.

### Defer

- D-6 (Email Verification with Reacher) -- Current SMTP/O365 approach works. Upgrade when scale demands it.
- D-1 (Renovation Permits via Shovels) -- Paid API, defer until budget allocated.

---

## Sources

### Verified (HIGH confidence)
- Existing codebase review: `services/enrichment/`, `lib/owner_discovery/`, `workflows/`, `db/queries/`
- Amadeus hotel chains dataset: https://github.com/amadeus4dev/data-collection/blob/master/data/hotelchains.md
- Wappalyzer open-source technology fingerprints: https://github.com/AliasIO/wappalyzer (MIT license, pattern files in `src/technologies/`)
- Florida DBPR public lodging records: https://www2.myfloridalicense.com/hotels-restaurants/lodging-public-records/
- JobSpy (open-source job scraper): https://github.com/speedyapply/JobSpy

### Verified (MEDIUM confidence)
- Shovels.ai building permit API: https://www.shovels.ai/api (180M permits, 85% US coverage -- pricing not verified)
- Lodging Econometrics hotel ownership database: https://lodgingeconometrics.com/hotel-ownership-management-groups/ (commercial, pricing unknown)
- STR chain scale classifications: https://str.com/data-insights/resources/glossary
- Wappalyzer API pricing: https://www.wappalyzer.com/api/ ($50/mo for 5K lookups -- may have changed)
- Waterfall enrichment best practices: https://fullenrich.com/blog/waterfall-enrichment

### Context (LOW confidence -- from web search, not verified)
- BuiltWith vs Wappalyzer comparison: https://www.crft.studio/blog/crft-lookup-vs-builtwith-vs-wappalyzer
- Hotel Tech Report tech stack guide: https://hoteltechreport.com/news/hotel-operations-tech-stack
- Intent signals overview: https://www.demandbase.com/faq/intent-signals/
- Hiring signals for buyer intent: https://www.hubspot.com/startups/tech-stacks/sales-csx/identify-buyer-intent-hiring-signals-hubspot-clay
