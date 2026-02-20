# Sadie GTM Owner Enrichment

## What This Is

GTM engineering platform that feeds qualified hotel leads to Sadie's sales team. Sadie is a live voice AI product serving the hospitality vertical. This codebase finds hotels, identifies their owners and decision makers, enriches contact information, and prepares leads for outbound. Hotels first, other verticals later.

## Core Value

Turn raw hotel data from any source (web scraping, Common Crawl, government records, Google Maps) into actionable sales leads with verified owner/decision-maker contact information — at scale (100K+ hotels), fully automated.

## Problem

Sadie's sales team needs to know WHO owns/manages each hotel, HOW to reach them, and WHAT tech stack (booking engine, PMS) they're running. This information is scattered across WHOIS records, DNS, hotel websites, government databases, review platforms, and more. Manual research doesn't scale. The platform automates this entire discovery and enrichment process.

## Who It's For

- **Primary:** Sadie's sales/GTM team — they consume enriched leads for outbound
- **Operator:** Engineering (currently the founder) — runs and maintains pipelines

## Constraints

- **Budget-conscious:** Prefer free data sources (RDAP, DNS, CC, gov records) over paid APIs
- **Infrastructure:** AWS (Fargate, SQS, S3, Lambda), Supabase/PostgreSQL+PostGIS, CF Worker proxy
- **Stack locked:** Python 3.9+, asyncio+httpx+asyncpg, aiosql (no ORM), Pydantic, uv
- **Scale target:** 100K+ hotels, batch processing with multi-worker concurrency
- **LLM usage:** Small/cheap models (GPT-3.5-turbo, Groq) for extraction, not reasoning

## Requirements

### Validated

- Hotel scraping from Google Maps via Serper grid search — existing
- Geographic region management (OSM polygons, city grids) — existing
- Booking engine detection via Playwright browser automation — existing
- Room count enrichment via LLM extraction — existing
- Customer proximity scoring via PostGIS — existing
- Hotel status pipeline (pending → detected → enriched → launched) — existing
- Excel export and S3 upload — existing
- SQS-based distributed job processing — existing
- Fargate/EC2 worker deployment — existing
- Slack notifications on pipeline events — existing
- Owner discovery via 9-layer waterfall (RDAP, WHOIS, DNS, Website, Reviews, Gov, ABN/ASIC, CT Certs, Email Verify) — existing
- Domain intelligence caching (WHOIS, DNS, CT certs, ABN) — existing
- Multi-worker atomic claiming with stale claim recovery — existing
- CF Worker proxy for IP rotation — existing
- Common Crawl index querying — existing
- Contact enrichment for existing decision makers (CC harvest, email pattern guessing, SMTP/O365 verification) — in progress

### Active

- [ ] Complete the Owner/Decision Makers DAG end-to-end
- [ ] Improve data quality (reduce bad emails, wrong owners, stale data)
- [ ] Improve coverage (find more leads, fill in missing info)
- [ ] Reduce manual orchestration (pipeline stages should chain automatically)
- [ ] Harden rate limit handling and anti-blocking resilience
- [ ] Better leverage Common Crawl for hotel URL discovery and HTML extraction
- [ ] LLM-at-scale extraction: scrape CC → get HTML → extract structured data with small models
- [ ] Vendor/booking-engine detection improvement (scrape booking engine pages directly)
- [ ] Multiple data entry points converging to same enriched record (CC, Google Maps, gov data, direct URLs)

### Out of Scope

- Non-hospitality verticals — hotels first, others later
- Agentic outbound — least important right now
- Autonomous AI agents — future state, current focus is batch LLM extraction
- User-facing UI — this is an engineering/CLI tool
- Real-time processing — batch is fine

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Hotels-first vertical strategy | Sadie is live in hospitality, perfect the pipeline before generalizing | Active |
| Free data sources preferred | RDAP, DNS, CC, gov records over paid APIs (budget) | Active |
| LLM extraction over rule-based | Small models (GPT-3.5/Groq) scale better than hand-written parsers | Active |
| Batch over real-time | 100K+ records, batch processing is simpler and cheaper | Active |
| No ORM (aiosql + raw SQL) | Performance at scale, full SQL control, unnest() batch patterns | Locked |
| CF Worker proxy over paid proxy | $5/mo for 10M requests vs expensive residential proxy | Active |

## Architecture (Existing)

```
Workflows (CLI) → Services (business logic) → Repositories (DB) → PostgreSQL/PostGIS
                                             → External APIs (Serper, Groq, RDAP, DNS)
                                             → Message Queues (SQS)
                                             → Infrastructure (S3, Slack, CF Worker)
```

**Key patterns:** Service-Repository-Workflow, SQS message queue orchestration, bitmask layer tracking, atomic multi-worker claiming (FOR UPDATE SKIP LOCKED), unnest() batch persistence.

**59 workflows, 5 service layers, 11 data integrations.**

---
*Last updated: 2026-02-20 after initialization*
