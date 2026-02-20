# Domain Pitfalls: GTM Hotel Owner Enrichment at Scale

**Domain:** Hotel lead generation, owner discovery, contact enrichment, web scraping
**Researched:** 2026-02-20
**Project context:** Brownfield system at 100K+ hotels with 9-layer waterfall enrichment pipeline
**Confidence:** HIGH (grounded in actual codebase analysis + industry research)

---

## 1. Data Quality Pitfalls

---

### C1: False Positive Contacts -- Finding the Wrong Person

**Severity:** CRITICAL

**What goes wrong:** The pipeline attributes a contact to the wrong hotel. A name scraped from a WHOIS record, website, or review belongs to a management company employee, a web developer, a domain reseller, or a previous owner -- not the current hotel decision maker. The sales team then reaches out to the wrong person, destroying credibility.

**Why it happens in this codebase:**

- **WHOIS/RDAP registrant != hotel owner.** The code in `rdap_client.py` and `whois_history.py` treats registrant data as the hotel owner, but the registrant is whoever bought the domain. For small hotels this is often a web agency, hosting company, or domain reseller. The registrant "Jane Doe at Digital Solutions Pty Ltd" is tagged as a decision maker for a hotel she built a website for.

- **Website scraping false matches.** `website_scraper.py` uses `NAME_TITLE_PATTERNS` regex to match "Name, Title" patterns. These patterns match staff photographers listed on `/team` pages, web designers in footers ("Built by John Smith, Director at Agency X"), and historical owners mentioned in `/our-story` narratives.

- **LLM hallucination at confidence 0.6.** The `_llm_extract_owner` function sets extracted contacts at confidence 0.6. GPT-3.5-turbo completion models may invent plausible names when asked to extract from text that contains none. A hallucinated "Michael Turner, General Manager" with no corroborating data gets stored as a real contact.

- **Google review response attribution.** The `extract_from_google_reviews` regex matches "Response from [Name], Owner" but this person may be a social media manager or the response may be from the management company, not the property owner.

- **Dedup is title-sensitive.** The `_deduplicate` function uses `(full_name.lower(), title.lower())` as the key. "John Smith, Owner" from WHOIS and "John Smith, Director" from ASIC become two separate decision makers for the same person.

**Warning signs:**
- Same name appearing across 5+ unrelated hotels (management company employee or web agency)
- Decision makers with source `["rdap"]` only, where the RDAP registrant email domain differs from the hotel domain
- DMs with `sources = ['llm_extract']` and email=None, phone=None (name-only hallucination)
- Hotels with 8+ decision makers from different sources with no name overlap

**Prevention:**
1. **Cross-source corroboration requirement.** A contact from a single source should cap at confidence 0.5. Require 2+ independent sources for confidence > 0.7. Currently, a single RDAP hit gets confidence ~0.8.
2. **Management company / web agency detection.** Build a blocklist of known management companies (Accor, IHG, Choice, Wyndham, and regional chains like NRMA, Discovery Parks) and web agencies. When WHOIS registrant org or email domain matches, flag as "management_company" and suppress from outreach lists.
3. **WHOIS registrant email domain cross-check.** If the WHOIS registrant email is `admin@webagency.com.au` but the hotel domain is `grandhotel.com.au`, the registrant is the agency. Flag or suppress.
4. **LLM confidence floor with structured field requirement.** Raise LLM extraction floor to 0.7 and require at least one structured field (email or phone) to persist. A name-only LLM extraction at 0.6 confidence pollutes the database.
5. **Title normalization before dedup.** Map title variants to canonical forms: "Owner" = "Domain Owner" = "Proprietor" = "Co-Owner". "General Manager" = "GM" = "Hotel Manager". Apply canonicalization before the `(full_name, title)` dedup key.
6. **Name plausibility checks.** Beyond the existing first-name-only filter, also drop: names shorter than 4 total characters, names matching the hotel name, names matching common entity words ("Holiday Park Trust").

**Detection (monitoring queries):**
```sql
-- Decision makers appearing at 10+ hotels (management company employee)
SELECT full_name, COUNT(DISTINCT hotel_id) AS hotel_count
FROM sadie_gtm.hotel_decision_makers
GROUP BY full_name HAVING COUNT(DISTINCT hotel_id) >= 10;

-- LLM-only contacts with no email/phone (likely hallucinations)
SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers
WHERE sources = ARRAY['llm_extract']
  AND (email IS NULL OR email = '')
  AND (phone IS NULL OR phone = '');
```

**Pipeline phase:** Owner Discovery (all layers) + Deduplication

---

### C2: Email Verification Blind Spots -- Catch-All Domains, Honeypots, and False Deliverability

**Severity:** CRITICAL

**What goes wrong:** SMTP verification reports emails as "exists" when they do not. Catch-all domains accept ANY address. Emails are marked verified, sales sends outreach, and emails bounce or hit spam traps, destroying sender reputation.

**Why it happens in this codebase:**

- **SMTP RCPT TO is ~50% accurate for business domains.** The code itself acknowledges this in `email_discovery.py` line 63: "Warning: Increasingly unreliable as servers block this." Industry data confirms that SMTP checks can only accurately validate about 50% of business domains because of catch-all configurations and security services (Mimecast, Proofpoint) that obscure mailbox existence.

- **Single-probe catch-all detection is insufficient.** `_detect_catch_all` tests one fake address (`xz9q7k2m_nonexistent_99999@domain`). Some catch-all configurations are partial: they accept mail to any address matching a pattern but reject truly random strings. A single probe misses these.

- **O365 is reliable but covers only one provider.** The `verify_o365_email` via `GetCredentialType` is the most reliable method (Microsoft does not throttle it). But it only works for Microsoft-hosted domains. Non-O365 domains fall through to SMTP.

- **No honeypot detection.** Emails scraped from web pages may include honeypot addresses -- hidden email addresses placed specifically to catch scrapers. `extract_emails_from_page` in `website_scraper.py` strips HTML tags but does not check for CSS-hidden or comment-embedded addresses. Sending to honeypots triggers immediate blacklisting.

- **Volume creates blocking risk.** At 100K hotels, email pattern generation produces ~7 patterns per person per domain. With 2 DMs per hotel average, that is 1.4M SMTP probes. Many mail servers will block or rate-limit this volume, causing the probing IP to be added to DNSBLs.

**Warning signs:**
- Bounce rate above 3% on the first outreach batch
- Multiple "verified" personal emails at the same small hotel domain (1-2 person hotel should not have 5 verified addresses)
- SMTP verification returning "exists" for all 7 pattern variants (catch-all)
- Sudden spike in overall verification success rate when processing a new batch (batch contains many catch-all domains)

**Prevention:**
1. **Multi-probe catch-all detection.** Test 3+ random fake addresses per domain before individual verification. If 2+ return "exists", mark the entire domain as catch-all. Skip SMTP verification for all addresses on catch-all domains.
2. **Honeypot filtering.** Before extracting emails from HTML, check for hidden elements. Skip emails found inside `display:none`, `visibility:hidden`, `<!-- comments -->`, or `font-size:0` elements. The current `_html_to_text` regex-strips all tags indiscriminately.
3. **Per-MX-host rate limiting.** Add per-MX-host semaphores. Max 3-5 SMTP probes per MX host per minute. The current code fires all verifications via `asyncio.gather` with no per-host limits.
4. **Verification staleness tracking.** Add `verified_at` timestamp to decision makers. An email verified 6 months ago should be re-verified before outreach. Verification decays.
5. **Separate verification IP.** SMTP probing should use a different IP/domain from actual outreach sending. If the probing IP gets blacklisted, outreach is unaffected.
6. **Third-party verification for high-value leads.** For the top 10% of hotels (by room count/revenue), use a paid verification service (NeverBounce, ZeroBounce) instead of DIY SMTP. Cost: ~$0.005/email = $50 for 10K high-value leads.

**Detection (monitoring):**
```sql
-- Catch-all domain detection gaps
SELECT d.domain, d.is_catch_all,
  COUNT(*) FILTER (WHERE dm.email_verified) AS verified_count
FROM sadie_gtm.domain_dns_cache d
JOIN sadie_gtm.hotels h ON h.website LIKE '%' || d.domain || '%'
JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id
WHERE d.is_catch_all IS NULL  -- never tested
GROUP BY d.domain, d.is_catch_all
ORDER BY verified_count DESC;
```

**Pipeline phase:** Email Verification (Layer 6) + Contact Enrichment pipeline

---

### C3: Stale Data -- Owners Change, Domains Transfer, Hotels Close

**Severity:** MODERATE-HIGH

**What goes wrong:** The enrichment pipeline runs once, populates decision makers, and the data is never refreshed. Meanwhile, hotel ownership changes (industry average: 15-20% of independent hotels change ownership annually), GMs rotate (average tenure 3-5 years at independent hotels), domains transfer, and the database becomes increasingly inaccurate.

**Why it happens in this codebase:**
- No TTL on enrichment results. Once `hotel_owner_enrichment.status = 1` (complete), the hotel is never re-enriched unless manually reset.
- Domain caches (`domain_whois_cache`, `domain_dns_cache`) have `queried_at` timestamps but no automatic expiration.
- Decision makers have `created_at`/`updated_at` but no `expires_at` or `last_confirmed_at`.
- The B2B contact data industry reports an average annual decay rate of 22.5%, with hospitality being higher due to seasonal staff and ownership turnover.

**Warning signs:**
- Outreach bounce rates increasing over time (month-over-month)
- Decision makers from `whois_history_wayback` source with pre-2020 WHOIS data still being used
- Hotels with `last_attempt` timestamp older than 6 months that are still in active outreach lists
- Emails returning "mailbox not found" that were previously verified

**Prevention:**
1. **Automatic re-enrichment schedule.** Re-queue hotels enriched more than 6 months ago: `WHERE last_attempt < NOW() - INTERVAL '6 months' AND status IN (1, 2)`.
2. **Confidence decay over time.** Apply time-based confidence reduction: `effective_confidence = confidence * power(0.95, months_since_enrichment)`. A 0.9 confidence contact from 12 months ago displays as ~0.54.
3. **Domain cache TTL.** WHOIS data: TTL 90 days. DNS data: TTL 30 days. Check cache freshness before enrichment.
4. **Hotel closure detection.** When website scraping returns 404 or domain DNS resolves to a parking page, flag the hotel as potentially closed. Do not continue enriching closed hotels.
5. **Outreach bounce feedback loop.** When sales reports a bounced email or wrong contact, reduce confidence to 0.1 and trigger re-enrichment.

**Pipeline phase:** All enrichment pipelines (scheduling/re-run logic)

---

### C4: Deduplication Failures at Scale

**Severity:** MODERATE

**What goes wrong:** The same person appears multiple times with slightly different name spellings, initials, or titles from different enrichment sources. At 100K hotels, even a 2% dedup failure rate means 2,000 duplicate contacts cluttering the database and confusing sales.

**Current state in codebase:**
- The UNIQUE constraint on `(hotel_id, full_name, title)` prevents exact duplicates. But:
  - "John Smith" (WHOIS, title="Domain Owner") and "John Smith" (website, title="Owner") = 2 records
  - "J. Smith" (reviews) = 3rd record
  - "JOHN SMITH" (government data) = prevented by ON CONFLICT only if case matches
- The `_deduplicate` function in `owner_enricher.py` compares `(full_name.lower(), title.lower())` but this runs in Python before DB insert. The DB constraint is case-sensitive.

**Prevention:**
1. **Case-insensitive unique constraint.** Change to `UNIQUE(hotel_id, LOWER(full_name), LOWER(title))` or use a function-based index.
2. **Fuzzy name matching pre-insert.** Before inserting, query existing DMs for the same hotel using trigram similarity (`pg_trgm`). If `similarity(new_name, existing_name) > 0.8`, merge instead of insert.
3. **Title canonicalization table.** Create a mapping: "Owner" = "Domain Owner" = "Proprietor" = "Co-Owner" = "Hotel Owner". Apply before insert.
4. **Initial/full name resolution.** When "J. Smith" exists and "John Smith" is found, merge them (prefer the fuller name).

**Pipeline phase:** Database persistence (batch_persist_results)

---

## 2. Scale Pitfalls

---

### S1: IP Blocking and Rate Limiting (Even With Proxies)

**Severity:** MODERATE-HIGH

**What goes wrong:** At 100K hotels, the pipeline makes millions of outbound requests across RDAP, WHOIS, DNS, website scraping, Common Crawl, SMTP. Even with the CF Worker proxy, target services rate-limit or block. Unlike small batches where errors are noise, at scale they become systemic.

**Why it happens in this codebase:**

- **CF Worker uses datacenter IPs.** Cloudflare Workers run on datacenter IPs, not residential. Anti-bot systems (including Cloudflare's own Bot Management on hotel websites) can detect and block datacenter IP ranges.

- **No per-domain rate limiting on website scraping.** `httpx_fetch_pages` in `enrich_contacts.py` uses a global semaphore (500 concurrent) with no per-domain cap. A hotel chain with 50 properties on the same domain triggers 50 x 18 paths = 900 requests to one server.

- **crt.sh already collapsed under load.** The comment on `LAYERS_DEFAULT` says "0% hit rate, clogs crt.sh under load" -- CT layer was disabled because it rate-limited too aggressively. This is a preview of what happens to other services at scale.

- **RDAP per-registrar limits.** RDAP queries go to individual registrars (GoDaddy, Namecheap, Cloudflare). Each has its own rate limit. At 100K domains, popular registrars will see thousands of queries and start blocking.

- **SMTP blacklisting.** Sending SMTP RCPT TO probes from a single IP at volume triggers DNSBL blacklisting. Once blacklisted, all SMTP verification from that IP fails.

**Prevention:**
1. **Per-domain request limiter.** Add per-domain semaphores (max 3 concurrent requests per domain) alongside global limits. Prevents hammering hotel chain servers.
2. **Exponential backoff on 429/403.** Unified retry wrapper across all HTTP layers. Currently only LLM calls have backoff.
3. **RDAP/WHOIS cache-first.** Check `domain_whois_cache.queried_at` before making RDAP queries. If queried in last 30 days, use cached data. The cache table exists but is not checked pre-query in the enrichment flow.
4. **SMTP rate limiting per MX host.** Max 5 probes per MX host per minute. Track probe count in a TTL dict.
5. **Adaptive throttling.** Track 429/403 rates per service per batch. When error rate exceeds 10%, automatically halve concurrency for that service.
6. **IP diversity.** Deploy CF Workers to multiple Cloudflare accounts (different IP pools). Add BrightData residential as automatic fallback.

**Pipeline phase:** All network-bound layers

---

### S2: Database Performance at 100K+ With Wide Tables and Many Indexes

**Severity:** MODERATE

**What goes wrong:** The `hotels` table accumulates columns from every enrichment pipeline (40+ columns). Multiple concurrent enrichment pipelines update the same table. Write throughput degrades, and the partial index with regex evaluation slows every insert.

**Why it happens in this codebase:**

- **Wide `hotels` table.** Columns from: basic info, booking engine detection, website enrichment, location/geocoding, room count enrichment, customer proximity, contact info (email, emails array, phone_google, phone_website), and more.

- **Concurrent pipeline contention.** Room count enricher, website enricher, owner enricher, contact enricher, and location normalizer all run `UPDATE hotels SET ... WHERE id = $1` on overlapping rows. Row-level locks cause contention.

- **Complex partial index.** `idx_dm_people_needing_email` uses a regex predicate. Every INSERT/UPDATE to `hotel_decision_makers` must evaluate this regex to determine partial index membership. At bulk insert scale, this becomes measurable.

- **Batch unnest queries.** The `batch_persist_results` function runs 5 separate unnest-based queries per flush (WHOIS cache, DNS cache, cert cache, DMs, status). With `FLUSH_INTERVAL = 20` hotels, each flush processes up to 100+ DM rows through complex ON CONFLICT logic.

- **Supavisor connection pooling.** The `statement_cache_size=0` confirms Supavisor transaction mode. Long transactions (>5s) from large batch flushes can exhaust the connection pool.

**Prevention:**
1. **Keep enrichment data normalized.** Continue the pattern of separate tables (`hotel_decision_makers`, `hotel_owner_enrichment`, `domain_*_cache`). Avoid adding more columns to `hotels`.
2. **Tune batch sizes.** Profile flush time at current batch sizes. If flush takes >2s, reduce `FLUSH_INTERVAL` from 20 to 10. Monitor with `EXPLAIN ANALYZE` on the batch upsert queries.
3. **Partial index maintenance.** Consider dropping `idx_dm_people_needing_email` before bulk enrichment and recreating after. Or simplify the predicate to avoid regex evaluation per row.
4. **Stagger pipeline execution.** Don't run owner enricher and contact enricher simultaneously on the same hotels. Sequential execution avoids write contention.
5. **Monitor query duration.** Add timing logs to all batch persist operations. Alert if any single query exceeds 5 seconds.

**Pipeline phase:** All enrichment pipelines (database write path)

---

### S3: Common Crawl Processing Gotchas

**Severity:** MODERATE

**What goes wrong:** Common Crawl data has encoding issues, truncated content, and staleness that silently corrupt extracted contacts. At scale, these edge cases affect thousands of records.

**Why it happens in this codebase:**

- **WARC parsing fragility.** `enrich_contacts.py` splits WARC records on `\r\n\r\n` to extract the HTML body. This fails with non-standard delimiters or chunked transfer encoding in the HTTP response. The code gets `parts = raw.split(b'\r\n\r\n', 2)` and takes `parts[2]`, which may include HTTP trailers or chunked encoding markers.

- **Encoding corruption.** The fallback `html_bytes.decode('utf-8', errors='replace')` silently corrupts non-UTF-8 characters. Australian hotel websites commonly use Windows-1252 (default for older IIS servers). A name like "Rene" (with accent) becomes "Ren?" which then fails email pattern matching (`first.last@domain` becomes `ren?.lastname@domain`).

- **Truncation without detection.** CC truncated content at 1 MiB before March 2025 (now 5 MiB). The code skips records >500KB compressed, but does not check the CC `truncated` field in index metadata. A contact page truncated mid-name loses data silently.

- **Staleness.** The code queries 3 indexes: Dec 2024, Oct 2024, Aug 2024. For a pipeline running in Feb 2026, this data is 14-18 months old. Hotel contact pages change frequently. A GM listed in Aug 2024 may have left by Feb 2026.

- **Over-fetching.** The CC query pattern `*.domain/*` returns ALL pages, then filters for contact URLs. For large hotel chain domains, this returns hundreds of entries, consuming CC Index API quota and WARC fetch bandwidth.

**Prevention:**
1. **Check CC truncation indicator.** CC index includes a `truncated` field for records since CC-MAIN-2019-47. Skip truncated records or flag extracted data as LOW confidence.
2. **Better encoding detection.** Use `charset_normalizer` library instead of trusting CC metadata encoding field. Many entries have incorrect encoding metadata.
3. **Staleness-aware confidence.** Record CC crawl date with extracted data. Contacts from CC data older than 6 months should have confidence capped at 0.5.
4. **Targeted CC queries.** Instead of `*.domain/*`, query specific paths: `domain.com/about*`, `domain.com/team*`, `domain.com/contact*`. Reduces API load and irrelevant fetches.
5. **CC + live fetch dedup.** When both CC and httpx return data for the same URL, prefer the live (httpx) version. Currently both are processed independently, potentially creating duplicate contacts.

**Pipeline phase:** Contact Enrichment (CC Harvest step)

---

### S4: LLM API Costs Spiraling With Scale

**Severity:** MODERATE

**What goes wrong:** LLM extraction runs as a fallback on every page where regex/JSON-LD extraction fails. Since most hotel websites do not use JSON-LD Person schemas, the LLM fallback fires for ~60-70% of pages. Costs scale linearly.

**Why it happens in this codebase:**

- **Two separate LLM integrations.** Azure OpenAI GPT-3.5-turbo in `website_scraper.py` (owner discovery, up to 4000 chars per call) and AWS Bedrock Nova Micro in `enrich_contacts.py` (contact enrichment, up to 20000 chars per call).

- **No pre-filtering before LLM.** The owner discovery LLM runs whenever regex and JSON-LD both fail. It does not check whether the page text even contains person-like patterns. Many pages are pure navigation, image galleries, or booking widgets with no extractable contacts.

- **No LLM result caching.** Re-running the pipeline re-invokes the LLM for the same pages. No hash-based cache.

- **Cost math at 100K hotels:**
  - Owner discovery: 100K hotels x 0.7 LLM-rate x 3 pages x ~1.5K tokens = ~315M tokens. GPT-3.5-turbo at $0.50/M = ~$157.
  - Contact enrichment: 100K hotels x 0.5 LLM-rate x 2 pages x ~5K tokens = ~500M tokens. Nova Micro at $0.035/M = ~$17.
  - Total: ~$175 per full pipeline run. Not catastrophic but adds up with re-enrichment.

**Prevention:**
1. **Pre-filter before LLM.** Check if page text contains any name-like patterns (two+ capitalized words together), email patterns, or phone patterns. If none found, skip LLM.
2. **Cache LLM results.** Hash page text content. Store `(domain, page_path, text_hash) -> llm_result` in a cache table. On re-enrichment, check cache first.
3. **Batch LLM calls.** Combine multiple short pages into a single prompt (up to token limit). Reduces per-call overhead.
4. **Model cost tiers.** Already partially done (Nova Micro for contact enrichment is cheap). Consider moving owner discovery from GPT-3.5-turbo to Nova Micro as well, or to GPT-4o-mini Batch API (50% discount for non-urgent).
5. **Cost ceiling per batch.** Track API spend per batch. Set a budget ceiling and fall through to pattern-only extraction when budget is exhausted.

**Pipeline phase:** Owner Discovery (Layer 4) + Contact Enrichment (LLM step)

---

## 3. Pipeline Pitfalls

---

### P1: Silent Failures in Multi-Stage Pipeline

**Severity:** HIGH

**What goes wrong:** Individual enrichment layers fail silently, are logged as warnings, but the hotel is marked as "complete" or "no_results" despite missing data from failed layers. The operator sees green dashboards while actual data coverage is degraded.

**Why it happens in this codebase:**

- **Exception swallowing at layer level.** In `owner_enricher.py` line 597-601, each layer result is checked with `if isinstance(res, Exception): logger.warning(...); continue`. The exception is logged but no structured tracking exists for which layers failed.

- **No "partial failure" status.** The enrichment status is either 1 (found contacts), 2 (no results), or derived from the bitmask. But a hotel where RDAP, WHOIS, and website all timed out (3 failures) gets the same status=2 as a hotel where all layers ran successfully but found nothing.

- **Contact enrichment has zero status tracking.** `enrich_contacts.py` runs, processes targets, and optionally writes results. If it crashes after processing 900 of 1000 targets, all 900 results are lost -- there is no incremental persistence during the run. The `conn.close()` at line 1307 releases the DB connection, and results are only written at the end (line 1406).

- **Inconsistent timeout handling.** CT layer: explicit 15s timeout via `asyncio.wait_for`. RDAP: no explicit timeout (falls through to httpx 30s default). DNS: system resolver timeout (may be 30s+). Website scraping: httpx 15s. Email verification: SMTP 10s. These inconsistencies mean some layers can block the entire pipeline.

- **Error rates are invisible.** No per-layer success/failure/timeout metrics are exposed. An operator running `enrich_owners.py --limit 500` sees the final summary but not that RDAP failed for 400 of 500 hotels.

**Warning signs:**
- Hotels with `layers_completed = 0b000100100` (only DNS + email verify completed -- other layers silently failed)
- Large gaps between `status=1` count and actual DM count per hotel
- Batch logs showing many "Layer (rdap) error" or "Layer (website) error" warnings

**Prevention:**
1. **Add "partial failure" status (status=3).** Track `layers_failed` bitmask alongside `layers_completed`. A hotel with `layers_completed=0b00100` and `layers_failed=0b01011` clearly shows which layers need retry.
2. **Automatic retry for failed layers.** Query `WHERE layers_failed & LAYER_RDAP != 0` to selectively re-run failed layers for specific hotels. The `--layer` CLI flag already supports targeting specific layers.
3. **Incremental persistence for contact enrichment.** Write results every N targets (e.g., every 50) rather than all-or-nothing at the end. The owner enricher already does this with `FLUSH_INTERVAL = 20` -- apply the same pattern to contact enrichment.
4. **Unified timeout policy.** Wrap all layer calls in `asyncio.wait_for(timeout=20.0)`. Currently only CT has explicit timeout wrapping.
5. **Per-layer health reporting.** After each batch, emit structured per-layer metrics: `{layer: "rdap", success: 350, failed: 120, timeout: 30, skipped: 0}`. Alert when failure rate for any layer exceeds 20%.

**Pipeline phase:** Owner Discovery orchestrator + Contact Enrichment orchestrator

---

### P2: Data Drift Between Pipeline Stages

**Severity:** MODERATE-HIGH

**What goes wrong:** The owner discovery pipeline and the contact enrichment pipeline run at different times on different data snapshots. Between runs, hotels change (new ones added, existing ones updated, some closed). The contact enrichment pipeline enriches decision makers that were created by the owner discovery pipeline using stale hotel data.

**Why it happens in this codebase:**

- **Loose coupling between pipelines.** `enrich_owners.py` populates `hotel_decision_makers`. Days or weeks later, `enrich_contacts.py` reads those DMs and tries to find emails. If the hotel's website changed between the two runs, the domain list may be wrong.

- **Entity name changes.** Government data (DBPR, ABN/ASIC) may update entity names. If a DM was created from ABN data referencing "Old Entity Pty Ltd" but the entity has since been renamed, the contact enrichment pipeline uses a stale entity name for domain guessing.

- **Schema drift between sources.** Different ingestion workflows produce slightly different field formats: "John Smith" vs "JOHN SMITH" vs "Smith, John". Phone numbers with/without country codes. State names vs abbreviations. The `normalize_data.py` and location inference modules handle some of this, but not all sources go through normalization.

- **Hotels table as a shared mutable resource.** Multiple workflows update the `hotels` table concurrently. One workflow may update `website`, another updates `state`. If the owner enricher cached the hotel's website as `oldsite.com` but a website enricher later corrected it to `newsite.com`, the DMs are associated with the wrong domain.

**Prevention:**
1. **Pipeline dependency tracking.** Record which hotel data snapshot was used for each enrichment run. When hotel data changes materially (website, name, state), flag associated DMs for re-enrichment.
2. **Refresh domain before contact enrichment.** The contact enrichment pipeline should re-read hotel website from the DB at runtime, not use a cached value. Currently `load_dms_needing_contacts` joins with hotels to get `h.website`, which is current -- this is correct.
3. **Data normalization on all write paths.** Ensure all ingestion sources normalize names (Title Case), phones (E.164 or consistent format), and states (full name or consistent abbreviation) before writing to the database.
4. **Atomic enrichment snapshots.** When running contact enrichment for a hotel, first read the current hotel record + all DMs in a single transaction. This prevents mid-enrichment changes from affecting the run.

**Pipeline phase:** Cross-pipeline coordination

---

### P3: Re-Enrichment Cycles Causing Data Loss or Corruption

**Severity:** HIGH

**What goes wrong:** Running the enrichment pipeline again on already-enriched hotels overwrites good data with worse data, or creates duplicate decision makers that dilute confidence scores. Conversely, the pipeline may fail to update stale data because the upsert logic preserves existing values too aggressively.

**Why it happens in this codebase:**

- **COALESCE-based upserts preserve wrong data.** `batch_persist_results` uses `COALESCE(EXCLUDED.email, existing.email)` -- new NULL does not overwrite existing data (good), but new non-NULL always overwrites existing non-NULL (potentially bad if new data is lower quality).

- **Confidence ratcheting via GREATEST.** `GREATEST(EXCLUDED.confidence, existing.confidence)` means confidence can only increase across re-enrichment runs. If the original high-confidence source (RDAP showing registrant) is no longer valid (domain transferred), re-enrichment cannot downgrade the contact.

- **Bitmask accumulation.** `layers_completed = existing | EXCLUDED.layers_completed` only grows. Once a layer is marked complete, it stays complete forever even if re-enrichment of that layer found nothing new. You can never "undo" a layer.

- **No enrichment run versioning.** There is no `enrichment_run_id` or `enrichment_batch_id` column. When investigating data quality issues, you cannot determine which enrichment run produced which data.

- **Ghost decision makers.** On initial enrichment, 5 DMs are found. Six months later, re-enrichment finds only 2 (the other 3 sources no longer return data). But the 3 ghost DMs persist because nothing deletes them.

**Warning signs:**
- DMs with `confidence = 1.0` but no verified email (confidence was ratcheted up over multiple runs)
- Hotels with 15+ DMs (accumulation from multiple runs without cleanup)
- `updated_at` timestamps that are recent but `created_at` from months ago on stale data being "refreshed"

**Prevention:**
1. **Version enrichment runs.** Add `last_enrichment_run_id` to `hotel_owner_enrichment`. Track which run last touched each hotel. Enables "which run introduced this contact?" investigation.
2. **Soft-delete + re-activate pattern.** Before re-enrichment, mark existing DMs for the hotel as `stale=true`. After re-enrichment, re-activate confirmed contacts and leave unconfirmed ones as stale. After 90 days, purge stale contacts.
3. **Replace GREATEST with weighted average for confidence.** `new_conf = 0.7 * new_enrichment + 0.3 * existing`. Prevents ratcheting while still giving weight to history.
4. **Add `last_confirmed_at` to decision makers.** On re-enrichment that confirms a contact still exists (same name from same source), update this timestamp. Contacts not confirmed in 90 days get flagged.
5. **Layer reset capability.** Add a `--fresh` flag that resets `layers_completed` before re-enrichment, allowing full re-run without bitmask accumulation.

**Pipeline phase:** Owner Discovery orchestrator + Database persistence

---

### P4: Over-Engineering Orchestration Before Pipeline Stability

**Severity:** MODERATE

**What goes wrong:** Adding SQS queues, Fargate tasks, distributed workers, and complex state machines before the pipeline produces reliably correct data at small scale.

**Current state in codebase:** The codebase has `infra/` with Fargate definitions and SQS queue patterns (`enrich_owners_enqueue.py`, `enrich_owners_consumer.py`), alongside simpler CLI workflows (`enrich_owners.py`, `enrich_contacts.py`). The queue-based approach adds operational complexity without improving data quality.

**Prevention:**
1. **Get accuracy right first.** Run the pipeline on 1000 hotels. Manually validate 50 contacts. Measure precision (what % of found contacts are correct) and recall (what % of actual contacts were found). Fix data quality before scaling.
2. **Simple CLI with batching is sufficient for 100K.** The CLI workflows with `--limit` and `--offset` can process 100K hotels in sequential batches. At 5 hotels/minute with 5 concurrency, 100K hotels takes ~6 hours. This is fine for a weekly/monthly refresh.
3. **Add observability before orchestration.** Structured logging with per-layer metrics, data quality dashboards, and alerting. These pay off immediately. Distributed orchestration only pays off when you need continuous enrichment.

**Pipeline phase:** Infrastructure/Operations

---

## 4. Legal/Compliance Pitfalls

---

### L1: WHOIS Privacy and GDPR -- Using Data You Legally Cannot

**Severity:** CRITICAL

**What goes wrong:** The pipeline scrapes WHOIS registrant data, mines Wayback Machine for historical WHOIS, and extracts personal contact info from websites. Some of this data is protected by GDPR, the Australian Privacy Act, or domain registrant privacy services. Using it for unsolicited commercial contact violates privacy law and exposes the business to significant fines.

**Why it happens in this codebase:**

- **Deliberate privacy circumvention.** `whois_history.py` explicitly crawls Wayback Machine snapshots of who.is pages to find pre-GDPR WHOIS data. This is an intentional attempt to recover data that registrants chose to protect. A regulator could view this as deliberate circumvention of privacy protections.

- **No data provenance per field.** The pipeline tracks which source provided a decision maker (`sources` array), but not which specific field (name, email, phone) came from which source. If a contact requests deletion under GDPR, you cannot demonstrate which data came from which source for the compliance response.

- **Australian Privacy Act exposure.** The primary targets are Australian hotels. The Australian Privacy Act 1988 (post-2022 amendments) imposes fines up to AUD 50 million for serious/repeated privacy breaches. The Act requires that personal information be collected "by lawful and fair means" and that individuals be "made aware of the collection." The pipeline does neither.

- **No right-to-deletion mechanism.** There is no endpoint or process to delete a contact's data upon request. Under GDPR (30-day response) and the Australian Privacy Act (30-day response), this is a compliance requirement.

- **No opt-out tracking.** The database has no mechanism to track contacts who have opted out of communication or requested data deletion.

**Consequences:**
- GDPR: up to EUR 20M or 4% of global annual turnover per violation
- Australian Privacy Act: up to AUD 50M for serious/repeated breaches
- CAN-SPAM: up to $51,744 per violating email (2025 rate, no cap on total)
- Reputational damage
- Cease-and-desist from domain registrars

**Prevention:**
1. **Legitimate interest assessment.** For B2B cold email to Australian/US hotels, document a formal legitimate interest assessment (LIA). This is a legal requirement under GDPR and good practice under the Australian Privacy Act. For B2B contacts: you are offering a relevant product/service to the business, using professional (not personal) email addresses, and providing easy opt-out.
2. **Country-aware pipeline rules.** For EU domains (.eu, .de, .fr, .it, etc.): skip WHOIS/Wayback layers entirely. For AU domains: comply with APPs. For US domains: CAN-SPAM is permissive but still requires opt-out.
3. **Evaluate Wayback WHOIS risk.** This is the riskiest source. The MEMORY.md notes ~40-60% hit rate for pre-2018 domains, but the data is often 5+ years old AND was explicitly protected by the registrant. Consider whether this marginal data gain is worth the legal exposure.
4. **Source provenance per field.** Extend the data model: track which source provided `name`, `email`, and `phone` separately. Enables source-specific deletion on request.
5. **Opt-out/suppression table.** Create `sadie_gtm.contact_suppressions(email TEXT PRIMARY KEY, reason TEXT, suppressed_at TIMESTAMPTZ)`. Check this table before any outreach. Include in all enrichment queries as a NOT EXISTS filter.
6. **Right-to-deletion process.** Implement a documented process: when a contact requests deletion, delete from `hotel_decision_makers` and add to suppression table. Respond within 30 days.

**Detection:**
```sql
-- Contacts from EU hotel domains using Wayback data
SELECT dm.full_name, dm.email, h.website, dm.sources
FROM sadie_gtm.hotel_decision_makers dm
JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
WHERE 'whois_history' = ANY(dm.sources)
  AND (h.website LIKE '%.eu/%' OR h.website LIKE '%.de/%'
       OR h.website LIKE '%.fr/%' OR h.website LIKE '%.it/%'
       OR h.website LIKE '%.es/%');
```

**Pipeline phase:** Owner Discovery (Layers 1-2) + Outreach Operations

---

### L2: Email Outreach Compliance (CAN-SPAM, GDPR, Spam Act)

**Severity:** CRITICAL

**What goes wrong:** Outreach emails sent using enriched data violate CAN-SPAM (US), GDPR (EU), or the Spam Act 2003 (Australia). Penalties apply per-email with no cap.

**Key requirements by jurisdiction:**

| Requirement | CAN-SPAM (US) | GDPR (EU) | Spam Act (AU) |
|------------|---------------|-----------|---------------|
| Prior consent | NOT required for commercial email | Legitimate interest (B2B) | Consent OR existing business relationship |
| Opt-out mechanism | Required in every email | Required | Required |
| Physical address | Required | Required (registered address) | Required |
| Sender identification | Required (accurate From/Reply-To) | Required | Required |
| Unsubscribe processing | 10 business days | "Without delay" | 5 business days |
| Penalty per violation | $51,744 (2025 rate) | Up to EUR 20M or 4% turnover | AUD 2.2M per contravention |

**Why it matters for this codebase:**
- The primary target market is Australian hotels. The Spam Act 2003 requires consent OR an existing business relationship to send commercial emails. Cold B2B email is allowed under an "inferred consent" provision if the recipient's email address was published in a business context and the message is relevant to their business role. But this defense requires that the email was obtained from a public business source (not scraped from private WHOIS data).
- Email authentication (SPF, DKIM, DMARC) is not just best practice -- many ESPs and recipient servers now reject unauthenticated email. The pipeline discovers email addresses but does not enforce that the sending domain has proper authentication.

**Prevention:**
1. **Every outreach email must include:** clear sender identification, physical business address, working unsubscribe link, and honest subject line. This is non-negotiable across all jurisdictions.
2. **Track consent basis per contact.** Add a `consent_basis` field: "inferred_business" (public business email), "legitimate_interest" (B2B relevance), "explicit" (opted in). Filter outreach by consent basis appropriate to the jurisdiction.
3. **Unsubscribe processing.** Build a suppression system that processes opt-outs within 5 business days (Spam Act requirement). Propagate suppressions to all email sending systems.
4. **Email authentication.** Ensure the sending domain has SPF, DKIM, and DMARC configured. Use the DNS intelligence already collected (`domain_dns_cache`) to verify authentication status.
5. **Track data source for compliance defense.** If challenged, you need to demonstrate that the email address was obtained from a legitimate public business source. The `sources` array and `raw_source_url` provide this -- ensure they are always populated.

**Pipeline phase:** Post-enrichment outreach operations (not currently in codebase, but downstream consumer)

---

### L3: Government Data Usage Restrictions

**Severity:** MODERATE

**What goes wrong:** Government data sources (Florida DBPR, ABN Lookup, ASIC) have terms of service restricting commercial use of queried data. Using government registry data to build a commercial lead list may violate these terms.

**Why it happens in this codebase:**
- The `find_gov_matches` function queries government-sourced hotel records for owner/operator names.
- ABN Lookup (Australian Business Register) data is available for "permitted purposes" under the ABN Act 1999, which includes verifying business details but may not explicitly cover commercial lead generation.
- ASIC director data is public record but the ASIC website terms of service may restrict automated scraping.

**Prevention:**
1. **Review terms of service for each government data source.** Document the legal basis for using each source.
2. **Use government data for validation, not prospecting.** Use DBPR/ABN data to confirm names found from other sources rather than as a primary discovery channel. This is a defensible "verification" use case.
3. **Respect robots.txt and API rate limits.** Government APIs (ABN Lookup, ASIC) have explicit rate limits. The `abn_lookup.py` and `asic_lookup.py` modules should respect these.
4. **Cache responsibly.** The `abn_cache` table stores ABN lookup results. Ensure cache TTL aligns with the data source's intended refresh frequency.

**Pipeline phase:** Owner Discovery (Layer 7: Gov Data + Layer 8: ABN/ASIC)

---

## Phase-Specific Warning Summary

| Phase/Layer | Primary Pitfall | Severity | Key Mitigation |
|-------------|----------------|----------|----------------|
| RDAP/WHOIS (L1-2) | Registrant is web agency, not hotel owner | Critical | Agency detection list, domain cross-check |
| Wayback WHOIS (L2) | Legal exposure for circumventing privacy | Critical | Skip for EU, document legal basis for AU |
| DNS Intelligence (L3) | SOA email is admin, not person | Minor | Already handled (not created as DM) |
| Website Scraping (L4) | Regex matches non-decision-makers | Moderate | Tighter title patterns, require full name |
| LLM Extraction (L4) | Hallucinated names/contacts | Critical | Raise confidence floor, require structured field |
| Google Reviews (L5) | Response author is social media manager | Moderate | Cross-validate with other sources |
| Email Verification (L6) | Catch-all domains give false positives | Critical | Multi-probe catch-all detection |
| Gov Data (L7) | Usage restrictions on commercial lead gen | Moderate | Use for validation only, review ToS |
| ABN/ASIC (L8) | Entity name != hotel operating name | Minor | Name matching already fuzzy |
| Contact Enrichment | CC encoding/staleness corrupts data | Moderate | Better encoding detection, staleness tracking |
| Contact Enrichment | SMTP probing causes IP blacklisting | Moderate | Per-MX rate limiting, dedicated IP |
| Database Writes | Concurrent pipeline contention | Moderate | Batch tuning, stagger execution |
| Re-Enrichment | Data corruption from overwrite logic | High | Versioned runs, confidence decay |
| Deduplication | Same person with different names/titles | Moderate | Fuzzy matching, title canonicalization |
| Compliance | No opt-out, no deletion procedure | Critical | Suppression table, documented process |

---

## Sources

**Email Verification:**
- [How to Verify Catch All Emails (2026)](https://verified.email/blog/email-deliverability/verify-catch-all-emails)
- [Tackling False Positives in Email Validation](https://www.serviceobjects.com/blog/tackling-false-positives-in-email-validation/)
- [Data Demystified: Email Accuracy (ZoomInfo)](https://pipeline.zoominfo.com/sales/data-demystified-email-accuracy-verification) -- SMTP can only verify ~50% of business domains

**Web Scraping:**
- [Web Scraping Challenges 2025 (ScrapingBee)](https://www.scrapingbee.com/blog/web-scraping-challenges/)
- [How to Avoid IP Bans: Scraping Guide 2026](https://affinco.com/avoid-ip-bans-scraping/)
- [Rate Limiting in Web Scraping (Scrape.do)](https://scrape.do/blog/web-scraping-rate-limit/)

**WHOIS/GDPR:**
- [WHOIS vs RDAP: Privacy and Compliance](https://blog.whoisjsonapi.com/whois-vs-rdap-privacy-access-levels-and-compliance-compared/)
- [Can You Identify Domain Owners After GDPR?](https://blog.whoisjsonapi.com/can-you-identify-domain-owners-after-gdpr-legal-methods-explained/)
- [GDPR Cold Emailing Compliance Guide](https://secureprivacy.ai/blog/gdpr-compliant-cold-email-guide)

**Email Outreach Compliance:**
- [Cold Email Compliance 101: CAN-SPAM, GDPR, CASL (2026)](https://outreachbloom.com/cold-email-compliance)
- [Cold Email Laws: GDPR, CAN-SPAM, CCPA](https://www.salesforge.ai/blog/cold-email-laws)
- [Cold Email Compliance Checklist 2025](https://www.mailforge.ai/blog/cold-email-compliance-checklist-2025)

**LLM Costs:**
- [LLM Cost Optimization: Reducing AI Expenses 80%](https://ai.koombea.com/blog/llm-cost-optimization)
- [LLM Cost Control (Radicalbit)](https://radicalbit.ai/resources/blog/cost-control/)

**Pipeline Reliability:**
- [When 'Successful' Pipelines Quietly Corrupt Your Data (Medium)](https://medium.com/towards-data-engineering/when-successful-pipelines-quietly-corrupt-your-data-4a134544bb73)
- [5 Reasons Your Data Pipeline is Silently Failing (Medium)](https://medium.com/datachecks/5-reasons-why-your-data-pipeline-is-silently-failing-7d45d2868547)

**Common Crawl:**
- [Common Crawl Errata](https://commoncrawl.org/errata)
- [Navigating the WARC File Format](https://commoncrawl.org/blog/navigating-the-warc-file-format)

**Data Freshness:**
- [B2B Lead Generation Challenges (Callbox)](https://www.callboxinc.com/lead-generation/b2b-lead-generation-challenges/) -- 22.5% annual data decay
- [Stale Data: How to Identify and Prevent (Tacnode)](https://tacnode.io/post/what-is-stale-data)

**Database Performance:**
- [Managing Large PostgreSQL Tables (Medium)](https://medium.com/@digitake/my-fun-journey-of-managing-a-large-table-of-postgresql-b8d09cb19444)
- [PostgreSQL Indexing Best Practices (pgMustard)](https://www.pgmustard.com/blog/indexing-best-practices-postgresql)

**Codebase Sources (direct analysis):**
- `services/enrichment/owner_enricher.py` -- waterfall orchestrator, dedup logic, batch persist
- `services/enrichment/owner_models.py` -- data models, layer bitmask constants
- `services/enrichment/repo.py` -- database operations, batch unnest queries
- `lib/owner_discovery/email_discovery.py` -- SMTP/O365 verification, catch-all detection
- `lib/owner_discovery/website_scraper.py` -- regex extraction, LLM extraction, JSON-LD parsing
- `lib/owner_discovery/whois_history.py` -- Wayback Machine WHOIS mining
- `lib/proxy.py` -- CF Worker proxy, BrightData integration
- `workflows/enrich_contacts.py` -- contact enrichment pipeline, CC harvest, httpx fetch
- `db/migrations/20260216_add_owner_enrichment.sql` -- schema design
- `db/migrations/20260221_dm_people_partial_index.sql` -- regex partial index
