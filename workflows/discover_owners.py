"""Owner discovery via Common Crawl — find hotel owners, GMs, and decision makers.

Discovers NEW people by extracting owner/manager names from CC-cached hotel
website pages (/about, /team, /management, etc.). This is different from
enrich_contacts.py which finds contact info for EXISTING known people.

Pipeline:
  Phase 1: Query CC Indexes for hotel domain pages (batch via CF Worker)
  Phase 2: Fetch WARC records and decompress HTML (batch via CF Worker)
  Phase 3: Extract owner names via JSON-LD, regex, then LLM (Nova Micro)
  Phase 4: Persist results to hotel_decision_makers (incremental flush)

Usage:
    uv run python3 workflows/discover_owners.py --source big4 --apply --limit 100
    uv run python3 workflows/discover_owners.py --source big4 --audit
    uv run python3 workflows/discover_owners.py --source big4 --dry-run
"""

import argparse
import asyncio
import base64
import gzip
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, quote

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
import aiohttp
from loguru import logger

from services.enrichment.owner_models import DecisionMaker, OwnerEnrichmentResult, LAYER_WEBSITE


# ── Environment ──────────────────────────────────────────────────────────────

def _read_env():
    env = dict(os.environ)
    try:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass
    return env

_ENV = _read_env()


def _parse_db_config() -> dict:
    """Build asyncpg connect kwargs from DATABASE_URL or individual SADIE_DB_ vars."""
    db_url = _ENV.get('DATABASE_URL', '')
    if db_url:
        from urllib.parse import urlparse
        u = urlparse(db_url)
        return dict(
            host=u.hostname,
            port=u.port or 6543,
            database=u.path.lstrip('/') or 'postgres',
            user=u.username,
            password=u.password,
            statement_cache_size=0,
        )
    return dict(
        host=_ENV['SADIE_DB_HOST'],
        port=int(_ENV.get('SADIE_DB_PORT', '6543')),
        database=_ENV.get('SADIE_DB_NAME', 'postgres'),
        user=_ENV['SADIE_DB_USER'],
        password=_ENV['SADIE_DB_PASSWORD'],
        statement_cache_size=0,
    )


DB_CONFIG = _parse_db_config()
AWS_REGION = _ENV.get('AWS_REGION', 'eu-north-1')
BEDROCK_MODEL_ID = 'eu.amazon.nova-micro-v1:0'
CF_WORKER_URL = _ENV.get('CF_WORKER_PROXY_URL', _ENV.get('CF_WORKER_URL', ''))
CF_WORKER_AUTH = _ENV.get('CF_WORKER_AUTH_KEY', '')


# ── Constants ────────────────────────────────────────────────────────────────

# Common Crawl — search recent indexes for coverage
CC_INDEXES = [
    "https://index.commoncrawl.org/CC-MAIN-2026-04-index",  # Jan 2026
    "https://index.commoncrawl.org/CC-MAIN-2025-51-index",  # Dec 2025
    "https://index.commoncrawl.org/CC-MAIN-2025-47-index",  # Nov 2025
]
CC_WARC_BASE = "https://data.commoncrawl.org/"

ENTITY_RE_STR = (
    r'(PTY|LTD|LIMITED|LLC|INC\b|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|'
    r'COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|'
    r'TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT|PROPRIETARY|COMPANY|'
    r'COMMISSION|FOUNDATION|TRADING|NOMINEES|SUPERANNUATION|ENTERPRISES)'
)

# Owner page path keywords — superset of CONTACT_PATHS in enrich_contacts.py
OWNER_PATHS = {
    'about', 'about-us', 'our-story', 'who-we-are', 'company',
    'team', 'our-team', 'the-team', 'leadership', 'leadership-team',
    'management', 'executive-team', 'board', 'board-of-directors',
    'directors', 'people', 'our-people', 'meet-the-team', 'staff',
    'our-hotel', 'the-hotel', 'hotel', 'ownership', 'proprietor',
    'contact', 'contact-us',
}

# Aggregator/OTA domains to exclude from CC sweep
SKIP_DOMAINS = {
    'booking.com', 'expedia.com', 'hotels.com', 'tripadvisor.com',
    'agoda.com', 'trivago.com', 'kayak.com', 'priceline.com',
    'wotif.com', 'lastminute.com', 'hostelworld.com',
    'airbnb.com', 'vrbo.com', 'stayz.com.au',
    'facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com',
    'google.com', 'youtube.com', 'tiktok.com', 'x.com',
}

FLUSH_INTERVAL = 20


# ── Source configurations ────────────────────────────────────────────────────

SOURCE_CONFIGS = {
    "big4": {
        "label": "Big4 Holiday Parks",
        "where": "(h.external_id_type = 'big4' OR h.source LIKE '%::big4%')",
        "join": None,
        "country": "AU",
        "serper_gl": "au",
    },
    "rms_au": {
        "label": "RMS Cloud Australia",
        "where": "hbe.booking_engine_id = 12 AND h.country IN ('Australia', 'AU') AND h.status = 1",
        "join": "JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id",
        "country": "AU",
        "serper_gl": "au",
    },
}


# ── Utilities ────────────────────────────────────────────────────────────────

def _proxy_headers() -> dict:
    """Auth headers for CF Worker proxy."""
    return {"X-Auth-Key": CF_WORKER_AUTH} if CF_WORKER_AUTH else {}


def _proxy_url(target_url: str) -> str:
    """Route a URL through CF Worker for IP rotation. Falls back to direct if no worker configured."""
    if not CF_WORKER_URL:
        return target_url
    return f"{CF_WORKER_URL.rstrip('/')}/?url={quote(target_url, safe='')}"


async def _proxy_batch(session: aiohttp.ClientSession,
                       requests: list[dict],
                       chunk_size: int = 200) -> list[dict]:
    """Batch-fetch URLs via CF Worker /batch endpoint.

    Sends up to chunk_size URLs per call, all fetched in parallel at the edge.
    Each request: {url: str, range?: str, accept?: str}
    Returns list of: {url, status, body, binary, error?}
    """
    if not CF_WORKER_URL or not requests:
        return []

    batch_url = f"{CF_WORKER_URL.rstrip('/')}/batch"
    headers = {'Content-Type': 'application/json'}
    if CF_WORKER_AUTH:
        headers['X-Auth-Key'] = CF_WORKER_AUTH

    # Split into chunks (CF Workers paid plan: 1000 subrequests per invocation)
    chunks = [requests[i:i + chunk_size] for i in range(0, len(requests), chunk_size)]

    async def _send_chunk(chunk):
        try:
            async with session.post(
                batch_url,
                json={'requests': chunk},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=180),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Batch fetch failed: HTTP {resp.status}")
                    return []
                data = await resp.json()
                colo = data.get('colo', '?')
                ok_count = sum(1 for r in data.get('results', []) if r.get('status') in (200, 206))
                logger.debug(f"Batch: {ok_count}/{len(chunk)} OK (colo={colo})")
                return data.get('results', [])
        except Exception as e:
            logger.warning(f"Batch fetch error: {type(e).__name__}: {e}")
            return []

    # Fire ALL chunks concurrently
    chunk_results = await asyncio.gather(*[_send_chunk(c) for c in chunks],
                                         return_exceptions=True)
    return [r for batch in chunk_results if isinstance(batch, list) for r in batch]


def _get_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().removeprefix("www.")
    except Exception:
        return ""


def _clean_text_for_llm(md: str) -> str:
    from html import unescape as html_unescape
    md = html_unescape(md)
    md = re.sub(r'<[^>]+>', ' ', md)
    md = re.sub(r'!\[[^\]]*\]\([^)]*\)', '', md)
    md = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', md)
    md = re.sub(r'#{1,6}\s*', '', md)
    md = re.sub(r'\*{1,3}([^*]+)\*{1,3}', r'\1', md)
    md = re.sub(r'[-=_]{3,}', '', md)
    md = re.sub(r'[ \t]+', ' ', md)
    md = re.sub(r'\n{3,}', '\n\n', md)
    return md.strip()


def _is_owner_url(url: str) -> bool:
    """Check if URL looks like an owner/about/team page (or homepage)."""
    path = urlparse(url).path.lower().strip('/')
    if not path:
        return True  # Homepage often has owner info
    return any(p in path for p in OWNER_PATHS)


# ── Hotel loading + domain extraction ────────────────────────────────────────

async def load_hotels_for_cc_sweep(conn, cfg: dict, limit: int = None) -> list[dict]:
    """Load hotels with website domains for CC owner discovery.

    Excludes hotels without websites and aggregator-domain hotels.
    """
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
    return [dict(r) for r in rows]


def extract_hotel_domains(hotels: list[dict]) -> tuple[set[str], dict[str, list[dict]]]:
    """Extract unique domains from hotels, mapping domain -> hotels.

    Excludes aggregator/OTA domains.
    Returns (all_domains, domain_to_hotels_map)
    """
    all_domains = set()
    domain_map = defaultdict(list)  # domain -> [hotel dicts]
    for h in hotels:
        domain = _get_domain(h['website'])
        if domain and not any(domain == s or domain.endswith('.' + s) for s in SKIP_DOMAINS):
            all_domains.add(domain)
            domain_map[domain].append(h)
    return all_domains, domain_map


# ── CC Harvest: Index query + WARC fetch + HTML decompress ───────────────────

async def cc_harvest_owner_pages(all_domains: set[str]) -> dict[str, str]:
    """Batch CC Index query + WARC fetch for owner-relevant pages.

    Phase 1: Query all CC indexes for all domains via CF Worker /batch
    Phase 2: Filter to owner-relevant URLs (about, team, management, etc.)
    Phase 3: Fetch WARC records and decompress to HTML

    Returns {page_url: html_content}
    """
    if not all_domains:
        return {}

    total_queries = len(all_domains) * len(CC_INDEXES)
    logger.info(f"CC: searching {len(all_domains)} domains across "
                f"{len(CC_INDEXES)} indexes ({total_queries} queries)...")

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        # ── Phase 1: Batch CC Index queries — one /batch call per index, all concurrent
        per_index_batches = []
        for idx_url in CC_INDEXES:
            batch = []
            for domain in all_domains:
                cc_url = f"{idx_url}?url={quote(f'*.{domain}/*', safe='')}&output=json&limit=200"
                batch.append({'url': cc_url, 'accept': 'application/json'})
            per_index_batches.append(batch)

        all_index_results = await asyncio.gather(*[
            _proxy_batch(session, batch) for batch in per_index_batches])
        results = [r for batch_results in all_index_results for r in batch_results]

        # Parse all index results into entries
        all_entries = []
        domains_hit = set()
        for r in results:
            if r.get('status') != 200 or not r.get('body'):
                continue
            for line in r['body'].strip().split('\n'):
                try:
                    entry = json.loads(line)
                    if 'html' in (entry.get('mime', '') + entry.get('mime-detected', '')):
                        if _is_owner_url(entry.get('url', '')):
                            all_entries.append(entry)
                            d = _get_domain(entry.get('url', ''))
                            if d:
                                domains_hit.add(d)
                except Exception:
                    pass

        # Deduplicate by URL (keep first = newest index)
        seen_urls = set()
        unique_entries = []
        for e in all_entries:
            u = e.get('url', '')
            if u not in seen_urls:
                seen_urls.add(u)
                unique_entries.append(e)

        logger.info(f"CC: {len(domains_hit)}/{len(all_domains)} domains hit, "
                    f"{len(unique_entries)} owner pages to fetch")

        if not unique_entries:
            return {}

        # ── Phase 2: Batch WARC fetches ───────────────────────────────────
        warc_requests = []
        entry_map = {}  # index in warc_requests -> entry
        for entry in unique_entries:
            length = int(entry.get('length', 0))
            if length > 500_000:  # skip huge pages
                continue
            filename = entry.get('filename', '')
            offset = int(entry.get('offset', 0))
            warc_url = f"{CC_WARC_BASE}{filename}"
            range_header = f"bytes={offset}-{offset+length-1}"
            entry_map[len(warc_requests)] = entry
            warc_requests.append({'url': warc_url, 'range': range_header})

        logger.info(f"CC: fetching {len(warc_requests)} WARC records via batch...")
        warc_results = await _proxy_batch(session, warc_requests, chunk_size=200)

        # ── Phase 3: Decompress WARC records into HTML ────────────────────
        fetched_pages: dict[str, str] = {}
        ok_count = sum(1 for r in warc_results if r.get('status') in (200, 206))
        err_count = sum(1 for r in warc_results if r.get('error'))
        logger.info(f"CC WARC batch: {ok_count} ok, {err_count} errors, "
                    f"{len(warc_results) - ok_count - err_count} other")
        for i, r in enumerate(warc_results):
            if r.get('status') not in (200, 206) or not r.get('body'):
                continue
            entry = entry_map.get(i)
            if not entry:
                continue
            page_url = entry.get('url', '')
            try:
                raw_data = base64.b64decode(r['body']) if r.get('binary') else r['body'].encode()
                raw = gzip.decompress(raw_data)
                parts = raw.split(b'\r\n\r\n', 2)
                if len(parts) < 3:
                    continue
                html_bytes = parts[2]
                encoding = entry.get('encoding', 'UTF-8') or 'UTF-8'
                try:
                    html = html_bytes.decode(encoding)
                except Exception:
                    html = html_bytes.decode('utf-8', errors='replace')
                if len(html) >= 100:
                    fetched_pages[page_url] = html
            except Exception as e:
                logger.warning(f"WARC decode error for {page_url}: {e}")

        logger.info(f"CC: {len(fetched_pages)} pages extracted from "
                    f"{len(domains_hit)}/{len(all_domains)} domains")

    return fetched_pages


# ── Extraction: JSON-LD, Regex, LLM ─────────────────────────────────────────

# Decision maker title keywords (for JSON-LD and regex filtering)
DECISION_MAKER_TITLES = [
    "owner", "co-owner", "proprietor", "founder", "co-founder",
    "general manager", "hotel manager", "managing director",
    "director of operations", "chief executive", "ceo",
    "president", "vice president", "innkeeper",
    "director", "principal",
]

# Regex patterns for extracting name + title combinations from text
# Matches patterns like: "John Smith, General Manager" or "General Manager: John Smith"
NAME_TITLE_PATTERNS = [
    # "Name, Title" or "Name - Title"
    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*[,\-\u2013\u2014]\s*(" + "|".join(DECISION_MAKER_TITLES) + r")",
    # "Title: Name" or "Title - Name"
    r"(" + "|".join(DECISION_MAKER_TITLES) + r")\s*[:\-\u2013\u2014]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
]


def extract_json_ld_persons(html: str) -> list[DecisionMaker]:
    """Extract Person entities from JSON-LD structured data in HTML.

    Adapted from lib/owner_discovery/website_scraper.py.
    Source tag: cc_website_jsonld (confidence 0.9).
    """
    results = []
    for match in re.finditer(r'<script\s+type="application/ld\+json">(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(match.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                _extract_persons_from_jsonld(item, results)
        except (json.JSONDecodeError, KeyError):
            continue
    return results


def _extract_persons_from_jsonld(data: dict, results: list[DecisionMaker]):
    """Recursively extract Person types from JSON-LD data."""
    if not isinstance(data, dict):
        return

    schema_type = data.get("@type", "")
    types = schema_type if isinstance(schema_type, list) else [schema_type]

    if "Person" in types:
        name = data.get("name")
        title = data.get("jobTitle")
        email = data.get("email")
        phone = data.get("telephone")

        if name and title:
            title_lower = title.lower()
            if any(dt in title_lower for dt in DECISION_MAKER_TITLES):
                results.append(DecisionMaker(
                    full_name=name,
                    title=title,
                    email=email,
                    phone=phone,
                    sources=["cc_website_jsonld"],
                    confidence=0.9,
                ))

    # Check nested entities (employees, members, etc.)
    for key in ("employee", "employees", "member", "members", "founder", "author"):
        nested = data.get(key)
        if isinstance(nested, list):
            for item in nested:
                _extract_persons_from_jsonld(item, results)
        elif isinstance(nested, dict):
            _extract_persons_from_jsonld(nested, results)


def extract_name_title_regex(text: str) -> list[DecisionMaker]:
    """Extract name+title combinations using regex patterns.

    Adapted from lib/owner_discovery/website_scraper.py.
    Source tag: cc_website_regex (confidence 0.7).
    """
    results = []
    for pattern in NAME_TITLE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            groups = match.groups()
            if len(groups) == 2:
                g0, g1 = groups
                if any(t in g0.lower() for t in DECISION_MAKER_TITLES):
                    title, name = g0, g1
                else:
                    name, title = g0, g1

                name = name.strip()
                title = title.strip().title()

                # Validate name looks like a person name (2-4 words, capitalized)
                name_parts = name.split()
                if 2 <= len(name_parts) <= 4 and all(p[0].isupper() for p in name_parts if p):
                    results.append(DecisionMaker(
                        full_name=name,
                        title=title,
                        sources=["cc_website_regex"],
                        confidence=0.7,
                    ))
    return results


# ── LLM extraction via Bedrock Nova Micro ────────────────────────────────────

_bedrock_client = None


def _get_bedrock():
    global _bedrock_client
    if _bedrock_client is None:
        import boto3
        _bedrock_client = boto3.client('bedrock-runtime', region_name=AWS_REGION)
    return _bedrock_client


_llm_sem = asyncio.Semaphore(30)  # Bedrock throttles above ~30 concurrent


async def llm_extract_owners(text: str, hotel_name: str) -> list[dict]:
    """Use Nova Micro to extract owner/GM info from page text.

    Returns list of dicts: [{"name": "First Last", "title": "Title", "role": "owner|general_manager|..."}]
    """
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

    async with _llm_sem:
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
                # Parse JSON -- same cleanup as enrich_contacts.py
                if content.startswith("```"):
                    content = re.sub(r'^```\w*\n?', '', content)
                    content = re.sub(r'\n?```$', '', content)
                json_match = re.search(r'\[.*\]', content, re.DOTALL)
                if json_match:
                    content = json_match.group(0)
                return json.loads(content)
            except Exception as e:
                err_str = str(e)
                if attempt < 2 and ("throttl" in err_str.lower() or "429" in err_str or "Too Many" in err_str):
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Bedrock throttled, retry in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                logger.warning(f"LLM extraction error: {e}")
                return []
    return []


def llm_results_to_decision_makers(results: list[dict], source_url: str) -> list[DecisionMaker]:
    """Convert LLM extraction results to DecisionMaker objects."""
    dms = []
    for r in results:
        name = (r.get("name") or "").strip()
        if not name or " " not in name:
            continue  # Skip first-name-only
        # Filter out entity names (companies, trusts, etc.)
        if re.search(ENTITY_RE_STR, name, re.IGNORECASE):
            continue
        title = (r.get("title") or r.get("role") or "").strip()
        if not title:
            title = "Unknown Role"
        dms.append(DecisionMaker(
            full_name=name,
            title=title.title(),
            sources=["cc_website_llm"],
            confidence=0.65,
            raw_source_url=source_url,
        ))
    return dms


async def extract_owners_from_page(html: str, url: str, hotel_name: str) -> list[DecisionMaker]:
    """Extract owner/manager names from a single page using three-tier strategy.

    1. JSON-LD structured data (free, 0.9 confidence)
    2. Regex name+title patterns (free, 0.7 confidence)
    3. LLM extraction (costs money, 0.65 confidence) -- only if 1+2 found nothing
    """
    # Tier 1: JSON-LD (structured data in HTML)
    dms = extract_json_ld_persons(html)
    if dms:
        for dm in dms:
            dm.raw_source_url = url
        logger.debug(f"JSON-LD: {len(dms)} persons from {url}")
        return dms

    # Tier 2: Regex patterns on cleaned text
    cleaned = _clean_text_for_llm(html)
    dms = extract_name_title_regex(cleaned)
    if dms:
        for dm in dms:
            dm.raw_source_url = url
            dm.sources = ["cc_website_regex"]
        logger.debug(f"Regex: {len(dms)} persons from {url}")
        return dms

    # Tier 3: LLM extraction (only if structured methods found nothing)
    if len(cleaned) < 50:
        return []  # Too little text for LLM
    truncated = cleaned[:20000]  # Nova Micro context limit
    results = await llm_extract_owners(truncated, hotel_name)
    dms = llm_results_to_decision_makers(results, url)
    if dms:
        logger.debug(f"LLM: {len(dms)} persons from {url}")
    return dms


# ── Pipeline: Incremental persistence + orchestration ────────────────────────

def group_pages_by_domain(pages: dict[str, str]) -> dict[str, dict[str, str]]:
    """Group fetched pages by domain.

    Returns {domain: {url: html, url2: html2, ...}}
    """
    grouped = defaultdict(dict)
    for url, html in pages.items():
        domain = _get_domain(url)
        if domain:
            grouped[domain][url] = html
    return dict(grouped)


async def discover_owners_cc(args, cfg: dict) -> dict:
    """Main CC owner discovery pipeline.

    Returns stats dict: {hotels_loaded, domains_queried, pages_fetched,
                         owners_extracted, owners_saved, hotels_with_owners,
                         llm_calls, jsonld_hits, regex_hits}
    """
    from services.enrichment import repo

    stats = {
        'hotels_loaded': 0, 'domains_queried': 0,
        'pages_fetched': 0, 'owners_extracted': 0,
        'owners_saved': 0, 'hotels_with_owners': 0,
        'llm_calls': 0, 'jsonld_hits': 0, 'regex_hits': 0,
    }

    # 1. Load hotels
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        hotels = await load_hotels_for_cc_sweep(conn, cfg, limit=args.limit)
    finally:
        await conn.close()

    stats['hotels_loaded'] = len(hotels)
    logger.info(f"Loaded {len(hotels)} hotels for CC sweep")

    if not hotels:
        logger.info("No hotels to process")
        return stats

    if args.dry_run:
        # Just show what would be processed
        all_domains, domain_map = extract_hotel_domains(hotels)
        print(f"\nDRY RUN:")
        print(f"  Hotels: {len(hotels)}")
        print(f"  Unique domains: {len(all_domains)}")
        print(f"  CC indexes to query: {len(CC_INDEXES)}")
        print(f"  Total CC queries: {len(all_domains) * len(CC_INDEXES)}")
        return stats

    # 2. Extract unique domains
    all_domains, domain_map = extract_hotel_domains(hotels)
    stats['domains_queried'] = len(all_domains)
    logger.info(f"Extracted {len(all_domains)} unique domains from {len(hotels)} hotels")

    if not all_domains:
        logger.warning("No valid domains found")
        return stats

    # 3. CC Harvest (index query + WARC fetch)
    pages = await cc_harvest_owner_pages(all_domains)
    stats['pages_fetched'] = len(pages)
    logger.info(f"CC harvest: {len(pages)} pages from {len(all_domains)} domains")

    if not pages:
        logger.warning("No pages fetched from CC")
        return stats

    # 4. Group pages by domain
    pages_by_domain = group_pages_by_domain(pages)

    # 5. Extract owners + incremental persistence
    pending_buffer: list[OwnerEnrichmentResult] = []
    flush_lock = asyncio.Lock()

    async def _flush():
        nonlocal pending_buffer
        async with flush_lock:
            if not pending_buffer:
                return 0
            to_flush = pending_buffer
            pending_buffer = []
        if not args.apply:
            # Not persisting, just count
            count = sum(len(r.decision_makers) for r in to_flush)
            return count
        try:
            count = await repo.batch_persist_results(to_flush)
            stats['owners_saved'] += count
            logger.info(f"Flushed {len(to_flush)} hotels ({count} DMs saved, {stats['owners_saved']} total)")
            return count
        except Exception as e:
            logger.error(f"Flush failed: {e}")
            async with flush_lock:
                pending_buffer = to_flush + pending_buffer
            return 0

    hotels_processed = 0
    for domain, domain_pages in pages_by_domain.items():
        hotels_for_domain = domain_map.get(domain, [])
        if not hotels_for_domain:
            continue

        # Extract owners from all pages for this domain
        all_dms_for_domain: list[DecisionMaker] = []
        for url, html in domain_pages.items():
            dms = await extract_owners_from_page(html, url, hotels_for_domain[0]['name'])
            all_dms_for_domain.extend(dms)

            # Track extraction method stats
            for dm in dms:
                if 'jsonld' in dm.sources[0]:
                    stats['jsonld_hits'] += 1
                elif 'regex' in dm.sources[0]:
                    stats['regex_hits'] += 1
                elif 'llm' in dm.sources[0]:
                    stats['llm_calls'] += 1

        stats['owners_extracted'] += len(all_dms_for_domain)

        # Create result for each hotel on this domain
        for hotel in hotels_for_domain:
            result = OwnerEnrichmentResult(
                hotel_id=hotel['hotel_id'],
                domain=domain,
                decision_makers=all_dms_for_domain,
                layers_completed=LAYER_WEBSITE,
            )
            if result.found_any:
                stats['hotels_with_owners'] += 1

            async with flush_lock:
                pending_buffer.append(result)
                should_flush = len(pending_buffer) >= FLUSH_INTERVAL

            if should_flush:
                await _flush()

            hotels_processed += 1

    # Final flush
    await _flush()

    # Summary
    logger.info(
        f"\nCC Sweep complete:\n"
        f"  Hotels processed: {hotels_processed}\n"
        f"  Domains queried: {stats['domains_queried']}\n"
        f"  Pages fetched: {stats['pages_fetched']}\n"
        f"  Owners extracted: {stats['owners_extracted']}\n"
        f"  Hotels with owners: {stats['hotels_with_owners']}\n"
        f"  Extraction: JSON-LD={stats['jsonld_hits']}, "
        f"Regex={stats['regex_hits']}, LLM={stats['llm_calls']}\n"
        f"  Owners saved to DB: {stats['owners_saved']}"
    )

    return stats
