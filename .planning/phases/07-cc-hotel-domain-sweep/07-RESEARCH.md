# Phase 7: CC Hotel Domain Sweep - Research

**Researched:** 2026-02-21
**Domain:** Common Crawl batch harvesting + LLM extraction for hotel owner discovery
**Confidence:** HIGH

## Summary

Phase 7 builds a bulk owner discovery pipeline using Common Crawl (CC) as the primary data source. The exact CC patterns needed already exist in `workflows/enrich_contacts.py` -- CC Index batch querying, WARC HTML decompression, and Nova Micro LLM extraction. The key difference: instead of finding email/phone for **known** people, this pipeline discovers **who** owns/manages hotels from about/team/management pages.

The existing codebase provides all the building blocks. The `enrich_contacts.py` file has battle-tested CC Index querying via CF Worker `/batch` endpoint (~80% domain coverage), WARC record fetching and gzip decompression, and Bedrock Nova Micro LLM extraction. The `owner_enricher.py` + `repo.py` files show the existing per-hotel owner discovery waterfall and the `batch_persist_results()` function that does bulk upserts to `hotel_decision_makers`. The new workflow combines these: CC-scale batch fetching with owner-focused LLM extraction and incremental DB persistence.

**Primary recommendation:** Create a new `workflows/discover_owners.py` that reuses the CC harvest, WARC fetch, and LLM infrastructure from `enrich_contacts.py` but with an owner-extraction LLM prompt, targeting `/about`, `/team`, `/management`, `/contact`, `/staff` pages, and flushing results to `hotel_decision_makers` every N hotels via the existing `batch_persist_results()` pattern.

## Standard Stack

The established libraries/tools for this domain:

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| aiohttp | existing | CC Index queries + WARC fetches via CF Worker /batch | Already proven in enrich_contacts.py, handles 1000+ concurrent connections |
| asyncpg | existing | Database reads/writes | Project standard, used everywhere |
| boto3 (bedrock-runtime) | existing | Nova Micro LLM extraction | Already integrated in enrich_contacts.py |
| loguru | existing | Structured logging | Project standard |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pydantic | existing | DecisionMaker, DomainIntel models | Already defined in owner_models.py |
| argparse | stdlib | CLI parsing | Match enrich_contacts.py pattern |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Nova Micro (Bedrock) | Azure OpenAI GPT-3.5 | Nova Micro is faster + cheaper; Azure OpenAI used in existing website_scraper.py but enrich_contacts.py already migrated to Nova Micro |
| aiohttp | httpx | aiohttp handles concurrent connections natively; httpx requires connection pool tuning |

**Installation:** No new packages needed. Everything is already installed.

## Architecture Patterns

### Recommended Project Structure
```
workflows/
  discover_owners.py          # NEW: CC-based bulk owner discovery CLI
services/enrichment/
  owner_enricher.py           # EXISTING: per-hotel waterfall (unchanged)
  owner_models.py             # EXISTING: DecisionMaker, DomainIntel models (unchanged)
  repo.py                     # EXISTING: batch_persist_results() (reuse as-is)
db/queries/
  owner_enrichment.sql        # EXISTING: insert_decision_maker query (used by repo)
```

### Pattern 1: CC Harvest -> WARC Fetch -> LLM Extract Pipeline (from enrich_contacts.py)

**What:** Three-phase pipeline that (1) queries CC Indexes for hotel domain pages, (2) fetches WARC records and decompresses HTML, (3) runs LLM extraction on the HTML.

**When to use:** This is the ONLY pattern for this phase. It is proven and should be reused directly.

**Phase 1 - CC Index Batch Query:**
```python
# Source: workflows/enrich_contacts.py lines 593-659
# For each hotel domain, query multiple CC indexes for pages matching OWNER_PATHS
# All queries fire via CF Worker /batch in parallel

CC_INDEXES = [
    "https://index.commoncrawl.org/CC-MAIN-2026-04-index",  # Jan 2026
    "https://index.commoncrawl.org/CC-MAIN-2025-51-index",  # Dec 2025
    "https://index.commoncrawl.org/CC-MAIN-2025-47-index",  # Nov 2025
]

# Build batch requests: one per domain per index
from urllib.parse import quote
for idx_url in CC_INDEXES:
    for domain in all_domains:
        cc_url = f"{idx_url}?url={quote(f'*.{domain}/*', safe='')}&output=json&limit=200"
        batch.append({'url': cc_url, 'accept': 'application/json'})

# Fire all via CF Worker /batch
results = await _proxy_batch(session, batch)

# Parse NDJSON responses, filter to owner-relevant pages
for line in body.strip().split('\n'):
    entry = json.loads(line)
    if 'html' in (entry.get('mime', '') + entry.get('mime-detected', '')):
        if _is_owner_url(entry.get('url', '')):  # NEW filter function
            entries.append(entry)
```

**Phase 2 - WARC Record Fetch:**
```python
# Source: workflows/enrich_contacts.py lines 664-713
# Build range requests for each CC entry
for entry in unique_entries:
    length = int(entry.get('length', 0))
    if length > 500_000:  # skip huge pages
        continue
    filename = entry.get('filename', '')
    offset = int(entry.get('offset', 0))
    warc_url = f"https://data.commoncrawl.org/{filename}"
    range_header = f"bytes={offset}-{offset+length-1}"
    warc_requests.append({'url': warc_url, 'range': range_header})

# Fetch via CF Worker /batch
warc_results = await _proxy_batch(session, warc_requests, chunk_size=200)

# Decompress: gzip -> split WARC parts -> extract HTML
raw_data = base64.b64decode(r['body']) if r.get('binary') else r['body'].encode()
raw = gzip.decompress(raw_data)
parts = raw.split(b'\r\n\r\n', 2)
html_bytes = parts[2]  # Third part is the HTML body
```

**Phase 3 - LLM Extraction (owner-specific prompt):**
```python
# Source: workflows/enrich_contacts.py lines 1377-1430 (pattern)
# But with OWNER DISCOVERY prompt instead of contact finding prompt

bedrock = boto3.client('bedrock-runtime', region_name='eu-north-1')
resp = await asyncio.to_thread(
    bedrock.converse,
    modelId='eu.amazon.nova-micro-v1:0',
    messages=[{"role": "user", "content": [{"text": prompt}]}],
    inferenceConfig={"maxTokens": 500, "temperature": 0.0},
)
```

### Pattern 2: Incremental Flush Persistence (from owner_enricher.py)

**What:** Buffer enrichment results and flush to DB every N hotels, so crashes preserve partial progress.

**When to use:** For the main pipeline loop. Do not accumulate all results in memory.

**Example:**
```python
# Source: services/enrichment/owner_enricher.py lines 693-756
FLUSH_INTERVAL = 20  # Persist every 20 hotels

pending_buffer = []
flush_lock = asyncio.Lock()

async def _flush():
    nonlocal pending_buffer, total_saved
    async with flush_lock:
        if not pending_buffer:
            return
        to_flush = pending_buffer
        pending_buffer = []
    # Uses batch_persist_results() which does 5 bulk SQL queries
    count = await repo.batch_persist_results(to_flush)
    total_saved += count

# After processing each hotel's results:
async with flush_lock:
    pending_buffer.append(result)
    should_flush = len(pending_buffer) >= flush_interval
if should_flush:
    await _flush()

# Final flush at end
await _flush()
```

### Pattern 3: CLI Pattern (from enrich_contacts.py)

**What:** Argparse CLI with --source, --limit, --apply, --audit, --dry-run flags.

**When to use:** For the workflow entrypoint.

**Example:**
```python
# Source: workflows/enrich_contacts.py lines 1742-1797
parser = argparse.ArgumentParser(description="Owner discovery via CC")
parser.add_argument("--source", required=True, help="Source config name")
parser.add_argument("--audit", action="store_true", help="Show coverage stats")
parser.add_argument("--apply", action="store_true", help="Write to DB")
parser.add_argument("--limit", type=int, default=None, help="Max hotels")
parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
parser.add_argument("-v", "--verbose", action="store_true")
```

### Anti-Patterns to Avoid
- **Per-hotel serial processing:** The old `enrich_single_hotel()` in `owner_enricher.py` processes one hotel at a time through a waterfall. This phase must NOT do that. Use batch-first: collect all domains, query CC in bulk, fetch all WARC records in bulk, then run LLM on all pages.
- **All-or-nothing persistence:** Never accumulate all results and write at the end. A crash at hotel 900/1000 must preserve the first 880 (flush every 20).
- **httpx for CC fetching:** Use `aiohttp` for CC Index and WARC fetches via CF Worker `/batch`. httpx has connection pool limitations at high concurrency.
- **Azure OpenAI for LLM:** Use Nova Micro via Bedrock. Faster, cheaper, and already integrated in `enrich_contacts.py`. Azure OpenAI is the older pattern from `website_scraper.py`.

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CC Index querying | Custom CC client | `_proxy_batch()` from enrich_contacts.py | Handles chunking, error handling, concurrent batch fetching through CF Worker edge |
| WARC decompression | Manual WARC parser | The gzip/split pattern from enrich_contacts.py L687-713 | WARC format is simple (gzip -> 3 parts split by \r\n\r\n -> part[2] is HTML) |
| LLM extraction | Custom prompt engineering | Adapt `llm_extract_contacts()` pattern from enrich_contacts.py L1377-1430 | Bedrock retry on 429, JSON parsing, error handling all done |
| Batch DB persistence | Individual INSERTs | `batch_persist_results()` from repo.py | Does 5 bulk SQL queries with unnest, handles ON CONFLICT upsert, deduplication |
| CLI framework | Custom arg parsing | Copy enrich_contacts.py CLI pattern | Source configs, audit mode, dry-run already proven |
| URL filtering | Regex-only filtering | `_is_contact_url()` pattern from enrich_contacts.py L349-354 | Path-based filtering with a set of keywords |

**Key insight:** Every component of this pipeline already exists in the codebase. The phase is about COMPOSING existing patterns, not building new ones. The only genuinely new code is the LLM prompt for owner extraction (vs contact extraction) and the hotel-domain loading query.

## Common Pitfalls

### Pitfall 1: Owner Pages vs Contact Pages - Different URL Filtering
**What goes wrong:** Using the same URL filter as enrich_contacts.py (CONTACT_PATHS) would work for most pages, but the emphasis differs. Owner discovery needs `/management`, `/our-story`, `/ownership`, `/the-hotel` while contact discovery emphasizes `/contact-us`, `/people`.
**Why it happens:** The CONTACT_PATHS set in enrich_contacts.py is already quite broad and includes most owner-relevant paths. But `_is_contact_url()` filters CC Index results.
**How to avoid:** Define OWNER_PATHS as a superset. The existing CONTACT_PATHS already includes: `about`, `about-us`, `team`, `our-team`, `leadership`, `leadership-team`, `board`, `board-of-directors`, `contact`, `contact-us`, `our-story`, `people`, `staff`, `management`, `executive-team`, `directors`, `our-people`, `who-we-are`, `meet-the-team`, `company`, `ownership`. This is already comprehensive. Add: `our-hotel`, `the-hotel`, `hotel`, `proprietor`, `the-team`.
**Warning signs:** Low hit rate on CC Index queries (< 50% of domains returning any owner-relevant page).

### Pitfall 2: LLM Prompt Must Extract Structured Owner Data, Not Just Contacts
**What goes wrong:** Reusing the contact extraction prompt verbatim produces `{name, email, phone}` but NOT `{name, title/role, organizational_relationship}`.
**Why it happens:** The contact extraction prompt in `llm_extract_contacts()` asks "Find email addresses and phone numbers for these specific people." Owner discovery needs to find the PEOPLE themselves along with their roles.
**How to avoid:** Write a new prompt: "Extract all hotel owners, general managers, and key decision makers from this text. For each person found, return their full name, title/role, and whether they are an owner, general manager, management company representative, or other role."
**Warning signs:** LLM returning empty results for pages that clearly contain owner info when manually inspected.

### Pitfall 3: Hotels Without Websites Still Need Processing
**What goes wrong:** The pipeline only processes hotels with websites (domains), skipping hotels that have no website field.
**Why it happens:** CC harvest requires a domain to search.
**How to avoid:** The query to load hotels should focus on hotels that HAVE a website/domain. Hotels without websites are out of scope for CC sweep (they need other layers like RDAP, gov data, etc.). But ensure the query also catches hotels whose website is a booking.com or similar aggregator URL -- these should be excluded from CC sweep since the CC pages belong to the aggregator, not the hotel.
**Warning signs:** Processing booking.com or expedia.com domains and getting decision makers for the wrong hotels.

### Pitfall 4: Deduplication with Existing Decision Makers
**What goes wrong:** CC sweep finds "John Smith, General Manager" but the hotel already has "John Smith, Owner" from RDAP. The unique constraint is (hotel_id, full_name, title), so both get inserted as separate records.
**Why it happens:** Same person, different title/role text from different sources.
**How to avoid:** This is actually fine. The `ON CONFLICT (hotel_id, full_name, title) DO UPDATE` in `batch_persist_results()` handles exact matches. Different titles for the same person are distinct records, which is valuable data (person may be both "Owner" and "General Manager"). The `sources` array gets merged so you can see which layers found the same person+title.
**Warning signs:** This is not actually a problem -- it is correct behavior. Only raise concern if the same exact (name, title) pair is being inserted multiple times within the same batch run.

### Pitfall 5: Flush Interval vs Batch Size Trade-off
**What goes wrong:** Setting flush_interval too high (e.g., 500) means a crash loses 499 hotels of work. Setting it too low (e.g., 1) causes excessive DB round-trips.
**Why it happens:** The existing `owner_enricher.py` uses `FLUSH_INTERVAL = 20`.
**How to avoid:** Use 20-50 as the flush interval. CC sweep is bulk I/O bound (CC fetch takes 30-60s, LLM takes 10-20s), so DB flush cost (< 1s) is negligible. Flush every 20 hotels is the proven default.
**Warning signs:** Very long total runtime with no DB writes until the end.

### Pitfall 6: CC Worker Batch Size Limits
**What goes wrong:** Sending more than ~500 URLs in a single `/batch` POST causes the CF Worker to timeout or hit subrequest limits.
**Why it happens:** CF Workers paid plan allows 1000 subrequests per invocation, but practical limits are lower due to timeout.
**How to avoid:** The existing `_proxy_batch()` function already handles chunking at `chunk_size=200`. Do not override this.
**Warning signs:** Batch fetch returning empty results or timeout errors.

## Code Examples

### Loading Hotels Needing Owner Discovery via CC

```python
# New query: hotels with websites that haven't been CC-swept yet
# Must exclude aggregator domains (booking.com, etc.)
async def load_hotels_for_cc_sweep(conn, cfg, limit=None):
    """Load hotels with website domains for CC owner discovery."""
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    rows = await conn.fetch(
        f"SELECT h.id AS hotel_id, h.name, h.website"
        f" FROM sadie_gtm.hotels h"
        f" {jc}"
        f" WHERE ({wc})"
        f"  AND h.website IS NOT NULL AND h.website != ''"
        f" ORDER BY h.id"
        f" {f'LIMIT {limit}' if limit else ''}",
    )
    return rows
```

### Owner-specific LLM Extraction Prompt

```python
# New prompt for owner/GM discovery (different from contact extraction)
async def llm_extract_owners(text: str, hotel_name: str) -> list[dict]:
    """Use Nova Micro to extract owner/GM info from page text."""
    prompt = f"""Extract all hotel owners, general managers, and key decision makers from this text.
Hotel: {hotel_name}

Text:
{text}

Rules:
- Each person MUST have both first name AND surname (e.g. "John Smith", not just "John")
- Do NOT return company names, trust names, or business entities as person names
- For each person, identify their role: owner, general_manager, director, manager, or other
- Only include people clearly associated with this hotel/property
- Respond with ONLY a JSON array, no explanation

JSON format: [{{"name":"First Last","title":"Their Title","role":"owner|general_manager|director|manager|other"}}]
If no people found, respond with exactly: []"""

    # Use same Bedrock pattern as enrich_contacts.py
    for attempt in range(3):
        try:
            bedrock = _get_bedrock()
            resp = await asyncio.to_thread(
                bedrock.converse,
                modelId=BEDROCK_MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 500, "temperature": 0.0},
            )
            content = resp["output"]["message"]["content"][0]["text"].strip()
            # Parse JSON (same cleanup as enrich_contacts.py)
            if content.startswith("```"):
                content = re.sub(r'^```\w*\n?', '', content)
                content = re.sub(r'\n?```$', '', content)
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            return json.loads(content)
        except Exception as e:
            if attempt < 2 and ("throttl" in str(e).lower() or "429" in str(e)):
                await asyncio.sleep(2 ** (attempt + 1))
                continue
            return []
    return []
```

### Converting LLM Results to DecisionMaker Objects

```python
# Map LLM extraction results to the existing DecisionMaker model
from services.enrichment.owner_models import DecisionMaker

def llm_results_to_decision_makers(results: list[dict], source_url: str) -> list[DecisionMaker]:
    dms = []
    for r in results:
        name = (r.get("name") or "").strip()
        if not name or " " not in name:
            continue  # Skip first-name-only
        title = (r.get("title") or r.get("role") or "").strip()
        if not title:
            title = "Unknown Role"
        dms.append(DecisionMaker(
            full_name=name,
            title=title.title(),
            sources=["cc_website_llm"],
            confidence=0.65,  # CC HTML + LLM = medium confidence
            raw_source_url=source_url,
        ))
    return dms
```

### Incremental Persistence with OwnerEnrichmentResult

```python
# Reuse the existing OwnerEnrichmentResult model and batch_persist_results()
from services.enrichment.owner_models import OwnerEnrichmentResult
from services.enrichment import repo

FLUSH_INTERVAL = 20

async def flush_results(buffer: list[OwnerEnrichmentResult]) -> int:
    """Flush buffered results to DB using existing batch_persist_results."""
    if not buffer:
        return 0
    count = await repo.batch_persist_results(buffer)
    return count
```

### Full Pipeline Skeleton

```python
async def discover_owners_cc(args, cfg):
    """Main CC owner discovery pipeline."""
    conn = await asyncpg.connect(**DB_CONFIG)

    # 1. Load hotels
    hotels = await load_hotels_for_cc_sweep(conn, cfg, limit=args.limit)
    await conn.close()

    if args.dry_run:
        print(f"Would process {len(hotels)} hotels")
        return

    # 2. Extract unique domains
    all_domains = set()
    hotel_domain_map = {}  # domain -> [hotel_ids]
    for h in hotels:
        domain = _get_domain(h['website'])
        if domain and domain not in SKIP_DOMAINS:
            all_domains.add(domain)
            hotel_domain_map.setdefault(domain, []).append(h)

    # 3. CC Harvest (reuse exact pattern from enrich_contacts.py)
    pages = await cc_harvest_owner_pages(all_domains)

    # 4. LLM extraction + incremental persistence
    pending = []
    total_saved = 0
    for domain, domain_pages in group_pages_by_domain(pages).items():
        for hotel in hotel_domain_map.get(domain, []):
            dms = []
            for url, html in domain_pages.items():
                cleaned = _clean_text_for_llm(html)[:20000]
                results = await llm_extract_owners(cleaned, hotel['name'])
                dms.extend(llm_results_to_decision_makers(results, url))

            result = OwnerEnrichmentResult(
                hotel_id=hotel['hotel_id'],
                domain=domain,
                decision_makers=dms,
            )
            pending.append(result)

            if len(pending) >= FLUSH_INTERVAL:
                total_saved += await flush_results(pending)
                pending = []

    # Final flush
    if pending:
        total_saved += await flush_results(pending)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Per-hotel waterfall (owner_enricher.py) | Batch CC sweep (this phase) | Feb 2026 | 100x faster: batch all domains at once vs sequential per-hotel |
| Azure OpenAI GPT-3.5 (website_scraper.py) | AWS Nova Micro via Bedrock (enrich_contacts.py) | Feb 2026 | Faster, cheaper LLM extraction |
| httpx for live crawling | aiohttp for batch fetching | Feb 2026 | Better concurrent connection handling |
| Single CC index | Multiple CC indexes (3) | Feb 2026 | Higher coverage (page may be in older crawl) |
| Direct CC requests | CF Worker /batch proxy | Feb 2026 | IP rotation, lower latency from edge, parallel fetching at edge |

**Deprecated/outdated:**
- `scrape_hotel_website()` in `website_scraper.py`: Still works for live crawling fallback but CC harvest is preferred for bulk operations.
- `enrich_single_hotel()` in `owner_enricher.py`: Per-hotel waterfall is still valid for real-time enrichment but not for bulk CC sweep.

## Key Decisions Derived from Codebase Analysis

### 1. URL Path Filtering for Owner Pages
The CC Index returns ALL pages for a domain. Must filter to owner-relevant pages. Use this set (superset of CONTACT_PATHS):

```python
OWNER_PATHS = {
    # About/story pages (most productive for owner names)
    'about', 'about-us', 'our-story', 'who-we-are', 'company',
    # Team/people pages
    'team', 'our-team', 'the-team', 'leadership', 'leadership-team',
    'management', 'executive-team', 'board', 'board-of-directors',
    'directors', 'people', 'our-people', 'meet-the-team', 'staff',
    # Hotel-specific
    'our-hotel', 'the-hotel', 'hotel', 'ownership',
    # Contact pages (often list owner/GM name)
    'contact', 'contact-us',
}
```

### 2. Source Configuration Reuse
The SOURCE_CONFIGS from enrich_contacts.py can be reused directly. The new workflow needs the same source-based hotel filtering.

### 3. Database Schema - No Changes Needed
The `hotel_decision_makers` table already has all needed columns:
- `full_name`, `title`, `email`, `email_verified`, `phone`
- `sources` (text array -- will add `'cc_website_llm'`)
- `confidence` (0.0-1.0)
- `raw_source_url` (CC page URL)
- `UNIQUE(hotel_id, full_name, title)` with ON CONFLICT upsert

### 4. CF Worker /batch Endpoint Format
```
POST /batch
Headers: X-Auth-Key, Content-Type: application/json
Body: {"requests": [{"url": "...", "range": "bytes=X-Y", "accept": "..."}, ...]}
Response: {"results": [{"url": "...", "status": 200, "body": "...", "binary": true/false}], "colo": "SYD"}
```
- For CC Index queries: `accept: "application/json"`, no range
- For WARC fetches: range header set, response is base64 binary
- Chunk size: 200 per /batch call (CF Worker limit practical)
- All chunks fired concurrently via asyncio.gather

### 5. Bedrock Nova Micro Configuration
```python
AWS_REGION = 'eu-north-1'
BEDROCK_MODEL_ID = 'eu.amazon.nova-micro-v1:0'
# Concurrency: use asyncio.Semaphore(30) -- Bedrock throttles above ~30
# Retry: 3 attempts with exponential backoff on 429
```

### 6. Confidence Levels for CC-Sourced Decision Makers
Based on existing patterns:
- JSON-LD structured data (if found in CC HTML): 0.9
- Regex name+title extraction from CC HTML: 0.7
- LLM extraction from CC HTML: 0.65
- Source tag: `cc_website_llm` or `cc_website_regex` or `cc_website_jsonld`

## Open Questions

1. **Should CC sweep create OwnerEnrichmentResult objects or directly insert DMs?**
   - What we know: `batch_persist_results()` expects `OwnerEnrichmentResult` objects with domain_intel
   - What's unclear: CC sweep does not produce domain_intel (no RDAP/WHOIS/DNS)
   - Recommendation: Create OwnerEnrichmentResult with domain_intel=None, only populate decision_makers. The batch_persist_results function handles None domain_intel gracefully (it only writes to cache tables when intel is present).

2. **How to handle hotels with multiple domains?**
   - What we know: Some hotel chains have entity domains separate from hotel domains
   - What's unclear: enrich_contacts uses a domain discovery step; should CC sweep do the same?
   - Recommendation: For v1, use only the hotel's primary website domain. Entity domain discovery can be a follow-up optimization.

3. **Should the workflow also run regex/JSON-LD extraction before LLM?**
   - What we know: website_scraper.py has `extract_json_ld_persons()` and `extract_name_title_regex()` that work on raw HTML
   - What's unclear: Whether these add significant value on top of LLM extraction
   - Recommendation: Yes, run JSON-LD and regex first (they're free and fast). Only send pages to LLM if no structured data found. This matches the existing website_scraper.py pattern and saves Bedrock costs.

## Sources

### Primary (HIGH confidence)
- `workflows/enrich_contacts.py` - CC harvest, WARC fetch, LLM extraction patterns (lines 593-714, 1377-1430)
- `services/enrichment/owner_enricher.py` - Incremental flush pattern (lines 693-756)
- `services/enrichment/repo.py` - batch_persist_results() (lines 1733-1929)
- `services/enrichment/owner_models.py` - DecisionMaker, OwnerEnrichmentResult models
- `lib/owner_discovery/website_scraper.py` - JSON-LD, regex, LLM extraction patterns
- `infra/cf-worker-proxy/src/worker.js` - CF Worker /batch endpoint (lines 108-128)
- `db/migrations/20260216_add_owner_enrichment.sql` - hotel_decision_makers schema
- `db/queries/owner_enrichment.sql` - insert_decision_maker, update_enrichment_status queries

### Secondary (MEDIUM confidence)
- `db/migrations/20260217_dm_source_to_sources_array.sql` - sources column is TEXT[] not TEXT
- `db/migrations/20260221_dm_people_partial_index.sql` - partial index on DMs needing email

### Tertiary (LOW confidence)
- None. All findings verified directly from codebase.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries already in use, no new dependencies
- Architecture: HIGH - all patterns already implemented in enrich_contacts.py and owner_enricher.py
- Pitfalls: HIGH - identified from direct codebase analysis and understanding of CC/WARC format
- LLM prompt: MEDIUM - the prompt structure follows existing patterns but owner extraction is untested

**Research date:** 2026-02-21
**Valid until:** 2026-03-21 (30 days -- codebase is stable, CC indexes update monthly)
