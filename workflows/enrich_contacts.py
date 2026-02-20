"""Contact enrichment — find email/phone for known decision makers.

Enriches EXISTING hotel_decision_makers who have names but are missing
contact info (email, phone). Does NOT discover new people — that's enrich_owners.

Pipeline (concurrent where possible):
  Phase 1 (sequential): Load DMs, discover domains (hotel + entity + guesses)
  Phase 2 (concurrent): CC harvest + MX detection, then httpx page fetch
  Phase 3: Email pattern guessing + O365/SMTP verification
  Phase 4 (opt-in): Serper web search
  Write: Batch update DM records + hotel email lists

Usage:
    # Default pipeline (CC + httpx + email patterns)
    uv run python3 workflows/enrich_contacts.py --source big4 --apply --limit 50

    # With Serper web search (costs money, finds more)
    uv run python3 workflows/enrich_contacts.py --source big4 --serper --apply

    # Audit contact coverage
    uv run python3 workflows/enrich_contacts.py --source big4 --audit
"""

import argparse
import asyncio
import gzip
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
import httpx
from html import unescape as html_unescape
from loguru import logger

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.async_dispatcher import SemaphoreDispatcher


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

DB_CONFIG = dict(
    host=_ENV['SADIE_DB_HOST'],
    port=int(_ENV.get('SADIE_DB_PORT', '6543')),
    database=_ENV.get('SADIE_DB_NAME', 'postgres'),
    user=_ENV['SADIE_DB_USER'],
    password=_ENV['SADIE_DB_PASSWORD'],
    statement_cache_size=0,
)
SERPER_API_KEY = _ENV.get('SERPER_API_KEY', '')
AWS_REGION = _ENV.get('AWS_REGION', 'eu-north-1')
BEDROCK_MODEL_ID = 'eu.amazon.nova-micro-v1:0'
CF_WORKER_URL = _ENV.get('CF_WORKER_PROXY_URL', _ENV.get('CF_WORKER_URL', ''))
CF_WORKER_AUTH = _ENV.get('CF_WORKER_AUTH_KEY', '')


# ── Constants ────────────────────────────────────────────────────────────────

# Common Crawl — search multiple recent indexes for better coverage
CC_INDEXES = [
    "https://index.commoncrawl.org/CC-MAIN-2024-51-index",  # Dec 2024
    "https://index.commoncrawl.org/CC-MAIN-2024-42-index",  # Oct 2024
    "https://index.commoncrawl.org/CC-MAIN-2024-33-index",  # Aug 2024
]
CC_WARC_BASE = "https://data.commoncrawl.org/"

ENTITY_RE_STR = (
    r'(PTY|LTD|LIMITED|LLC|INC\b|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|'
    r'COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|'
    r'TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT|PROPRIETARY|COMPANY|'
    r'COMMISSION|FOUNDATION|TRADING|NOMINEES|SUPERANNUATION|ENTERPRISES)'
)

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

# Contact page path keywords (for filtering CC results + building crawl URLs)
CONTACT_PATHS = {
    'about', 'about-us', 'team', 'our-team', 'leadership',
    'leadership-team', 'board', 'board-of-directors',
    'contact', 'contact-us', 'our-story', 'people', 'staff',
    'management', 'executive-team', 'directors', 'our-people',
    'who-we-are', 'meet-the-team', 'company',
    'investor', 'investors', 'governance', 'annual-report',
    'media', 'news', 'press', 'ownership',
}

# httpx crawl paths — derived from CONTACT_PATHS (most productive subset)
CRAWL_PATHS = [
    '/about', '/about-us', '/team', '/our-team', '/contact', '/contact-us',
    '/leadership', '/leadership-team', '/board', '/people', '/staff',
    '/management', '/directors', '/our-people', '/our-story',
    '/who-we-are', '/meet-the-team', '/company',
]

# Entity name stop words for domain guessing
ENTITY_STOP = {
    'pty', 'ltd', 'limited', 'the', 'trustee', 'for', 'trust', 'no',
    'as', 'of', 'and', 'trading', 'operations', 'proprietary', 'company',
    'abn', 'atf', 'inc', 'llc', 'corp', 'corporation', 'group',
    'australia', 'australian', 'aust', 'nsw', 'qld', 'vic',
    'tas', 'sa', 'wa', 'nt', 'act', 'new', 'south', 'north',
    'west', 'east', 'central',
    'holiday', 'holidays', 'park', 'parks', 'caravan', 'resort',
    'resorts', 'tourism', 'tourist', 'motel', 'hotel', 'retreat',
    'village', 'villages', 'camping', 'camp',
    'national', 'royal', 'first', 'holdings', 'assets', 'management',
    'services', 'investments', 'properties', 'development',
    'discretionary', 'family', 'subsidiary',
}

# Domains to skip in Serper results
SEARCH_SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "linkedin.com",
    "tiktok.com", "youtube.com", "pinterest.com",
    "abr.business.gov.au", "abr.gov.au", "asic.gov.au",
    "rocketreach.co", "zoominfo.com", "apollo.io",
    "issuu.com", "scribd.com", "yumpu.com", "calameo.com",
    "researchgate.net", "academia.edu",
}

PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "live.com",
    "icloud.com", "me.com", "protonmail.com", "proton.me",
    "bigpond.com", "optusnet.com.au", "westnet.com.au", "internode.on.net",
}

GENERIC_EMAIL_PREFIXES = {
    'info', 'admin', 'contact', 'hello', 'enquiries', 'bookings',
    'reservations', 'reception', 'sales', 'support', 'help',
    'noreply', 'no-reply', 'stay', 'office', 'mail', 'enquiry',
    'book', 'general', 'accounts', 'marketing',
}

AU_PHONE_RE = re.compile(
    r'(?:\+61\s?\d|\(0\d\)|\b0[2-478])\s*\d{4}\s*\d{4}'
    r'|(?:\+61\s?\d)\s*\d{4}\s*\d{4}'
    r'|\b(?:1300|1800)\s*\d{3}\s*\d{3}'
)
US_PHONE_RE = re.compile(
    r'(?:\+1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}'
)
DEFAULT_PHONE_RE = re.compile(r'\+?\d[\d\s.\-()]{8,}\d')
PHONE_PATTERNS = {"AU": AU_PHONE_RE, "US": US_PHONE_RE, "DEFAULT": DEFAULT_PHONE_RE}


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


# ── Dataclass ────────────────────────────────────────────────────────────────

@dataclass
class DMTarget:
    """A decision maker who needs contact info."""
    dm_id: int
    full_name: str
    title: str
    hotel_id: int
    hotel_name: str
    hotel_website: str
    entity_name: Optional[str] = None
    # Domains to try for email patterns — ordered by preference
    all_domains: list = field(default_factory=list)
    # Results
    found_email: Optional[str] = None
    found_phone: Optional[str] = None
    email_verified: bool = False
    email_source: Optional[str] = None
    phone_source: Optional[str] = None
    all_emails: list = field(default_factory=list)


# ── Utilities ────────────────────────────────────────────────────────────────

def _proxy_headers() -> dict:
    """Auth headers for CF Worker proxy."""
    return {"X-Auth-Key": CF_WORKER_AUTH} if CF_WORKER_AUTH else {}


def _proxy_url(target_url: str) -> str:
    """Route a URL through CF Worker for IP rotation. Falls back to direct if no worker configured."""
    if not CF_WORKER_URL:
        return target_url
    from urllib.parse import quote
    return f"{CF_WORKER_URL.rstrip('/')}/?url={quote(target_url, safe='')}"


def _get_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().removeprefix("www.")
    except Exception:
        return ""


def _clean_text_for_llm(md: str) -> str:
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


def _entity_to_domain_guesses(entity_name: str) -> list[str]:
    """Generate plausible domain names from entity name.

    "INGENIA PTY LTD" → ["ingenia.com.au", "ingenia.com"]
    "HAMPSHIRE VILLAGES PTY LTD" → ["hampshire.com.au", "hampshire.com"]
    """
    words = re.sub(r'[^a-zA-Z0-9\s]', '', entity_name.lower()).split()
    words = [w for w in words if w not in ENTITY_STOP and len(w) >= 4]
    if not words:
        return []

    guesses = []
    seen = set()
    for w in words:
        if w not in seen:
            seen.add(w)
            guesses.append(w)
    # Also try first two words combined
    if len(words) >= 2:
        combo = words[0] + words[1]
        if combo not in seen:
            guesses.append(combo)

    domains = []
    for g in guesses:
        domains.append(f"{g}.com.au")
        domains.append(f"{g}.com")
    return domains


def _is_contact_url(url: str) -> bool:
    """Check if URL looks like a contact/about/team page (or homepage)."""
    path = urlparse(url).path.lower().strip('/')
    if not path:
        return True  # Homepage
    return any(p in path for p in CONTACT_PATHS)


def _match_email_to_person(target: DMTarget, emails: list[str]) -> Optional[str]:
    """Try to match an email to a specific person by name. Returns best match or None."""
    name_parts = target.full_name.lower().split()
    first = name_parts[0] if name_parts else ""
    last = name_parts[-1] if len(name_parts) > 1 else ""
    if not (first and last and len(first) > 1 and len(last) > 1):
        return None

    hotel_domain = _get_domain(target.hotel_website) if target.hotel_website else ""
    target_domains = set(target.all_domains)

    best_email = None
    best_score = -1

    for e in emails:
        parts = e.split('@')
        if len(parts) != 2:
            continue
        local = parts[0].lower()
        email_domain = parts[1].lower()

        if local in GENERIC_EMAIL_PREFIXES:
            continue

        # Name match: require first AND last name parts in the local part
        name_match = False
        if first in local and last in local:
            name_match = True
        elif local.startswith(first[0]) and last in local:
            name_match = True
        elif last in local and local.startswith(first[:2]):
            name_match = True
        if not name_match:
            continue

        # Score by domain relevance
        if hotel_domain and (email_domain == hotel_domain or email_domain.endswith('.' + hotel_domain)):
            score = 4
        elif email_domain in target_domains:
            score = 3
        elif email_domain in PERSONAL_EMAIL_DOMAINS:
            score = 2
        else:
            score = 1

        if score > best_score:
            best_email = e
            best_score = score

    return best_email


# ── Step 1: Load DMs needing contacts ────────────────────────────────────────

async def load_dms_needing_contacts(conn, cfg: dict, need: str = "email") -> list[DMTarget]:
    """Load decision makers who have names but are missing contact info."""
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    if need == "email":
        contact_filter = "AND (dm.email IS NULL OR dm.email = '')"
    elif need == "phone":
        contact_filter = "AND (dm.phone IS NULL OR dm.phone = '')"
    else:
        contact_filter = "AND (dm.email IS NULL OR dm.email = '') AND (dm.phone IS NULL OR dm.phone = '')"

    rows = await conn.fetch(
        f"SELECT dm.id AS dm_id, dm.full_name, dm.title,"
        f"  h.id AS hotel_id, h.name AS hotel_name, h.website"
        f" FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc}"
        f" WHERE ({wc})"
        f"  AND dm.full_name !~* $1"
        f"  {contact_filter}"
        f" ORDER BY dm.id",
        ENTITY_RE_STR,
    )

    # Load entity names (DMs matching entity regex on same hotels)
    hotel_ids = list({r['hotel_id'] for r in rows})
    entity_map = {}
    if hotel_ids:
        entity_rows = await conn.fetch(
            "SELECT dm.hotel_id, dm.full_name"
            " FROM sadie_gtm.hotel_decision_makers dm"
            " WHERE dm.hotel_id = ANY($1) AND dm.full_name ~* $2",
            hotel_ids, ENTITY_RE_STR,
        )
        for er in entity_rows:
            entity_map[er['hotel_id']] = er['full_name']

    targets = []
    for r in rows:
        targets.append(DMTarget(
            dm_id=r['dm_id'],
            full_name=r['full_name'],
            title=r['title'] or '',
            hotel_id=r['hotel_id'],
            hotel_name=r['hotel_name'],
            hotel_website=r['website'] or '',
            entity_name=entity_map.get(r['hotel_id']),
        ))
    return targets


# ── Step 2: Domain discovery ─────────────────────────────────────────────────

async def discover_domains(conn, targets: list[DMTarget]) -> None:
    """Populate all_domains for each target: hotel domain + entity domains + guesses."""
    entity_groups = defaultdict(list)
    for t in targets:
        hd = _get_domain(t.hotel_website)
        if hd:
            t.all_domains.append(hd)
        if t.entity_name:
            entity_groups[t.entity_name].append(t)

    for entity_name, entity_ts in entity_groups.items():
        hotel_ids = list({t.hotel_id for t in entity_ts})

        entity_urls = await conn.fetch(
            "SELECT DISTINCT raw_source_url FROM sadie_gtm.hotel_decision_makers"
            " WHERE hotel_id = ANY($1) AND raw_source_url IS NOT NULL"
            " AND raw_source_url LIKE 'http%'"
            " AND raw_source_url NOT LIKE '%abr.business%'"
            " AND raw_source_url NOT LIKE '%abr.gov%'"
            " AND raw_source_url NOT LIKE '%big4.com.au%'"
            " AND raw_source_url NOT LIKE '%businessnews%'"
            " AND raw_source_url NOT LIKE '%hotelconversation%'"
            " AND raw_source_url NOT LIKE '%californiarevealed%'",
            hotel_ids,
        )
        for r in entity_urls:
            domain = _get_domain(r['raw_source_url'])
            if domain:
                for t in entity_ts:
                    if domain not in t.all_domains:
                        t.all_domains.append(domain)

        hotel_websites = await conn.fetch(
            "SELECT DISTINCT website FROM sadie_gtm.hotels"
            " WHERE id = ANY($1) AND website IS NOT NULL AND website != ''",
            hotel_ids,
        )
        for r in hotel_websites:
            d = _get_domain(r['website'])
            if d:
                for t in entity_ts:
                    if d not in t.all_domains:
                        t.all_domains.append(d)

        guesses = _entity_to_domain_guesses(entity_name)
        for t in entity_ts:
            for g in guesses:
                if g not in t.all_domains:
                    t.all_domains.append(g)

    # Summary
    total_domains = len({d for t in targets for d in t.all_domains})
    avg = sum(len(t.all_domains) for t in targets) / len(targets) if targets else 0
    logger.info(f"Domain discovery: {total_domains} unique domains, avg {avg:.1f} per target")


# ── Step 3: Common Crawl harvest ─────────────────────────────────────────────

async def _cc_query(client: httpx.AsyncClient, index_url: str,
                    url_pattern: str, limit: int = 200) -> list[dict]:
    """Query one CC Index API via CF Worker proxy for IP rotation."""
    try:
        from urllib.parse import quote
        cc_url = f"{index_url}?url={quote(url_pattern, safe='')}&output=json&limit={limit}"
        resp = await client.get(
            _proxy_url(cc_url),
            headers=_proxy_headers(),
            timeout=15.0,
        )
        if resp.status_code != 200:
            return []
        entries = []
        for line in resp.text.strip().split('\n'):
            try:
                entries.append(json.loads(line))
            except Exception:
                pass
        return entries
    except Exception as e:
        logger.debug(f"CC query error for {url_pattern}: {e}")
        return []


async def _cc_fetch_warc(client: httpx.AsyncClient, entry: dict) -> tuple[str, str]:
    """Fetch one WARC record from CC archive and extract HTML. Returns (url, html)."""
    url = entry.get('url', '')
    try:
        filename = entry['filename']
        offset = int(entry['offset'])
        length = int(entry['length'])

        # Skip huge pages (>500KB compressed)
        if length > 500_000:
            return url, ''

        resp = await client.get(
            f"{CC_WARC_BASE}{filename}",
            headers={'Range': f'bytes={offset}-{offset+length-1}'},
            timeout=30.0,
        )
        if resp.status_code not in (200, 206):
            return url, ''

        raw = gzip.decompress(resp.content)

        # WARC record: WARC-header \r\n\r\n HTTP-header \r\n\r\n body
        parts = raw.split(b'\r\n\r\n', 2)
        if len(parts) < 3:
            return url, ''

        html_bytes = parts[2]
        encoding = entry.get('encoding', 'UTF-8') or 'UTF-8'
        try:
            return url, html_bytes.decode(encoding)
        except Exception:
            return url, html_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        logger.debug(f"WARC fetch error for {url}: {e}")
        return url, ''


async def cc_harvest(targets: list[DMTarget], cfg: dict) -> dict[str, str]:
    """Common Crawl: search multiple indexes + fetch per domain pipelined.

    Searches 3 recent CC indexes in parallel for each domain, deduplicates by URL,
    then fetches WARC records for contact pages.
    Returns {url: html} of fetched pages.
    """
    all_domains = {d for t in targets for d in t.all_domains}
    if not all_domains:
        return {}

    total_queries = len(all_domains) * len(CC_INDEXES)
    logger.info(f"CC: searching {len(all_domains)} domains across "
                f"{len(CC_INDEXES)} indexes ({total_queries} queries)...")

    fetched_pages: dict[str, str] = {}
    search_sem = asyncio.Semaphore(150)  # CF Worker IP rotation handles distribution
    fetch_sem = asyncio.Semaphore(200)   # WARC via S3/CloudFront, direct
    domains_hit = set()

    async with httpx.AsyncClient(
        timeout=30.0,
        limits=httpx.Limits(max_connections=500, max_keepalive_connections=50),
    ) as client:

        async def _fetch_entry(entry):
            async with fetch_sem:
                url, html = await _cc_fetch_warc(client, entry)
            if html and len(html) >= 100:
                fetched_pages[url] = html

        async def _search_and_fetch(domain, index_url):
            async with search_sem:
                entries = await _cc_query(client, index_url, f'*.{domain}/*', limit=200)
            if not entries:
                return
            entries = [e for e in entries if 'html' in (e.get('mime', '') + e.get('mime-detected', ''))]
            contact_entries = [e for e in entries
                              if _is_contact_url(e.get('url', ''))
                              and e.get('url', '') not in fetched_pages]  # skip already fetched
            if not contact_entries:
                return
            domains_hit.add(domain)
            await asyncio.gather(*[_fetch_entry(e) for e in contact_entries])

        # Search all domains × all indexes concurrently
        tasks = [_search_and_fetch(d, idx) for d in all_domains for idx in CC_INDEXES]
        await asyncio.gather(*tasks)

    logger.info(f"CC: {len(domains_hit)}/{len(all_domains)} domains hit, "
                f"{len(fetched_pages)} pages fetched")
    return fetched_pages


# ── Step 3b: httpx page fetch (primary live fetcher) ─────────────────────────

async def httpx_fetch_pages(targets: list[DMTarget], cfg: dict) -> dict[str, str]:
    """Fetch contact pages via plain httpx for all target domains.

    Returns {url: html} of all successfully fetched pages.
    Extraction is done separately so this can run concurrently with CC harvest.
    """
    # Collect unique domains from all targets
    domain_targets: dict[str, list[DMTarget]] = defaultdict(list)
    for t in targets:
        for d in t.all_domains[:5]:  # Top 5 domains per person
            domain_targets[d].append(t)

    urls_to_fetch: list[str] = []
    url_domain_map: dict[str, str] = {}
    for domain in domain_targets:
        # Homepage
        url = f"https://{domain}/"
        if url not in url_domain_map:
            urls_to_fetch.append(url)
            url_domain_map[url] = domain
        # Contact paths
        for path in CRAWL_PATHS:
            url = f"https://{domain}{path}"
            if url not in url_domain_map:
                urls_to_fetch.append(url)
                url_domain_map[url] = domain

    if not urls_to_fetch:
        return {}

    logger.info(f"httpx: fetching {len(urls_to_fetch)} pages across {len(domain_targets)} domains...")

    fetched: dict[str, str] = {}

    async def _fetch_one(client: httpx.AsyncClient, url: str):
        try:
            resp = await client.get(
                url, follow_redirects=True, timeout=10.0,
            )
            if resp.status_code == 200:
                ct = resp.headers.get('content-type', '')
                if 'html' in ct or 'text' in ct:
                    text = resp.text
                    if len(text) >= 100:
                        fetched[url] = text
        except Exception:
            pass

    async with httpx.AsyncClient(
        timeout=10.0,
        limits=httpx.Limits(max_connections=1000, max_keepalive_connections=100),
    ) as client:
        await asyncio.gather(*[_fetch_one(client, u) for u in urls_to_fetch])

    logger.info(f"httpx: fetched {len(fetched)}/{len(urls_to_fetch)} pages with HTML content")
    return fetched


def extract_contacts_from_pages(targets: list[DMTarget], pages: dict[str, str],
                                cfg: dict, source_label: str = "httpx_fetch") -> int:
    """Extract emails/phones from fetched HTML pages and match to targets.

    Works for both CC-fetched and httpx-fetched pages.
    Returns number of targets that got a new email match.
    """
    country = cfg.get('country', 'AU')
    phone_re = PHONE_PATTERNS.get(country, PHONE_PATTERNS['DEFAULT'])

    # Build domain → targets mapping
    domain_targets: dict[str, list[DMTarget]] = defaultdict(list)
    for t in targets:
        for d in t.all_domains:
            domain_targets[d].append(t)

    found_count = 0
    for url, html in pages.items():
        text = html_unescape(html)
        page_emails = list(set(EMAIL_RE.findall(text)))
        page_phones = phone_re.findall(text)

        if not page_emails and not page_phones:
            continue

        page_domain = _get_domain(url)
        relevant_targets = domain_targets.get(page_domain, [])

        for t in relevant_targets:
            for e in page_emails:
                if e.lower() not in {x.lower() for x in t.all_emails}:
                    t.all_emails.append(e)

            if not t.found_email:
                best = _match_email_to_person(t, page_emails)
                if best:
                    t.found_email = best
                    t.email_source = source_label
                    found_count += 1
                    logger.debug(f"  {source_label}: {t.full_name} → {best}")

            if not t.found_phone and page_phones:
                t.found_phone = page_phones[0]
                t.phone_source = source_label

    return found_count


async def llm_extract_from_pages(targets: list[DMTarget], pages: dict[str, str],
                                  cfg: dict) -> int:
    """Run LLM extraction on fetched pages for targets still missing email.

    Only processes pages >500 chars associated with targets that have no email yet.
    Runs all LLM calls concurrently (Bedrock handles throttling).
    """
    needs = [t for t in targets if not t.found_email]
    if not needs or not pages:
        return 0

    country = cfg.get('country', 'AU')
    phone_re = PHONE_PATTERNS.get(country, PHONE_PATTERNS['DEFAULT'])

    # Build domain → needy targets mapping
    domain_targets: dict[str, list[DMTarget]] = defaultdict(list)
    for t in needs:
        for d in t.all_domains:
            domain_targets[d].append(t)

    # Collect (url, text, targets) for LLM — only pages with enough content
    llm_jobs: list[tuple[str, str, list[DMTarget]]] = []
    for url, html in pages.items():
        if len(html) < 500:
            continue
        page_domain = _get_domain(url)
        page_targets = [t for t in domain_targets.get(page_domain, []) if not t.found_email]
        if page_targets:
            llm_jobs.append((url, html, page_targets))

    if not llm_jobs:
        return 0

    logger.info(f"LLM: extracting contacts from {len(llm_jobs)} pages for "
                f"{len(needs)} people still needing email...")

    llm_sem = asyncio.Semaphore(20)  # Bedrock throttle
    found_count = 0

    async def _extract_one(url: str, html: str, page_targets: list[DMTarget]):
        nonlocal found_count
        names = [t.full_name for t in page_targets]
        cleaned = _clean_text_for_llm(html)[:20000]
        async with llm_sem:
            contacts = await llm_extract_contacts(cleaned, names)
        for t in page_targets:
            if t.found_email:
                continue
            match = contacts.get(t.full_name.lower())
            if not match:
                continue
            email = match.get('email')
            if email and '@' in email:
                if email.lower() not in {x.lower() for x in t.all_emails}:
                    t.all_emails.append(email)
                prefix = email.split('@')[0].lower()
                if prefix not in GENERIC_EMAIL_PREFIXES:
                    t.found_email = email
                    t.email_verified = False
                    t.email_source = "llm_extract"
                    found_count += 1
            phone = match.get('phone')
            if phone and phone != 'null' and not t.found_phone:
                cleaned_phone = phone_re.search(phone)
                if cleaned_phone:
                    t.found_phone = cleaned_phone.group()
                    t.phone_source = "llm_extract"

    await asyncio.gather(*[_extract_one(u, h, ts) for u, h, ts in llm_jobs])

    if found_count:
        logger.info(f"LLM: found {found_count} emails")
    return found_count


# ── Step 4: Email pattern discovery ──────────────────────────────────────────

async def _detect_email_providers(domains: set[str]) -> dict[str, Optional[str]]:
    """Batch-detect email provider for each domain via MX lookup.

    Returns {domain: "microsoft_365" | "google_workspace" | None}.
    O365 domains use fast HTTP verification; others fall through to SMTP.
    """
    results = {}

    def _check_mx(domain):
        try:
            import dns.resolver
            resolver = dns.resolver.Resolver()
            resolver.timeout = 5
            resolver.lifetime = 8
            mx_records = resolver.resolve(domain, "MX")
            mx_hosts = [str(r.exchange).rstrip(".").lower() for r in mx_records]
            mx_str = " ".join(mx_hosts)
            if "outlook.com" in mx_str or "protection.outlook" in mx_str:
                return "microsoft_365"
            elif "google" in mx_str or "aspmx" in mx_str:
                return "google_workspace"
            return None
        except Exception:
            return None

    loop = asyncio.get_event_loop()

    async def _check_one(domain):
        provider = await loop.run_in_executor(None, _check_mx, domain)
        results[domain] = provider

    await asyncio.gather(*[_check_one(d) for d in domains])
    o365_count = sum(1 for v in results.values() if v == "microsoft_365")
    logger.info(f"MX detection: {len(results)} domains checked, {o365_count} are O365")
    return results


async def email_pattern_discovery(targets: list[DMTarget],
                                   providers: dict[str, Optional[str]] | None = None) -> int:
    """Try email pattern guessing + O365/SMTP verification on ALL domains per target.

    If providers dict is passed (from concurrent MX detection), uses it directly.
    Otherwise detects providers inline.
    """
    from lib.owner_discovery.email_discovery import discover_emails

    needs_email = [t for t in targets if not t.found_email]
    if not needs_email:
        return 0

    # Use pre-computed providers or detect now
    if providers is None:
        all_domains = {d for t in needs_email for d in t.all_domains}
        providers = await _detect_email_providers(all_domains)

    # Group targets by domain to batch verify — avoids repeating SMTP for same domain
    domain_people: dict[str, list[DMTarget]] = defaultdict(list)
    for t in needs_email:
        parts = t.full_name.strip().split()
        if len(parts) < 2:
            continue
        for d in t.all_domains:
            if d and '.' in d:
                domain_people[d].append(t)

    found_count = 0

    async def _verify_domain(domain: str, people: list[DMTarget]):
        """Verify all people on a single domain in one batch."""
        nonlocal found_count
        provider = providers.get(domain)

        async def _discover_one(t):
            try:
                return t, await discover_emails(
                    domain=domain, full_name=t.full_name,
                    email_provider=provider)
            except Exception as e:
                logger.debug(f"  Email discovery error for {t.full_name}@{domain}: {e}")
                return t, []

        results_list = await asyncio.gather(*[_discover_one(t) for t in people])

        for t, results in results_list:
            # Collect all candidate emails
            for d in results:
                e = d.get('email', '')
                if e and e.lower() not in {x.lower() for x in t.all_emails}:
                    t.all_emails.append(e)

            if t.found_email and t.email_verified:
                continue  # Already have verified email from another domain

            verified = [d for d in results if d.get('verified')]
            unverified = [d for d in results if not d.get('verified')]
            personal_v = [d for d in verified if not any(
                d['email'].lower().startswith(r + '@')
                for r in ['gm', 'owner', 'manager', 'director', 'management']
            )]

            if personal_v:
                t.found_email = personal_v[0]['email']
                t.email_verified = True
                t.email_source = f"email_pattern_{personal_v[0].get('method', 'unknown')}"
                if t.email_verified:
                    found_count += 1
                logger.debug(f"  Pattern: {t.full_name} → {t.found_email} (verified) on {domain}")
            elif verified:
                t.found_email = verified[0]['email']
                t.email_verified = True
                t.email_source = f"email_pattern_{verified[0].get('method', 'unknown')}"
                found_count += 1
            elif unverified and not t.found_email:
                # Save best unverified — may be overwritten by verified from another domain
                personal_u = [d for d in unverified if not any(
                    d['email'].lower().startswith(r + '@')
                    for r in ['gm', 'owner', 'manager', 'director', 'management']
                )]
                best = personal_u[0] if personal_u else unverified[0]
                t.found_email = best['email']
                t.email_verified = False
                t.email_source = f"email_pattern_{best.get('method', 'unknown')}_unverified"
                found_count += 1
                logger.debug(f"  Pattern: {t.full_name} → {t.found_email} (unverified) on {domain}")

    # Run all domains concurrently
    await asyncio.gather(*[_verify_domain(d, ppl) for d, ppl in domain_people.items()])
    return found_count


# ── Step 5: crawl4ai live crawl fallback ─────────────────────────────────────

async def crawl4ai_enrich(targets: list[DMTarget], cfg: dict,
                          cc_fetched_urls: set, concurrency: int = 10) -> int:
    """Live crawl hotel/entity contact pages not found in Common Crawl."""
    needs = [t for t in targets if not t.found_email]
    if not needs:
        return 0

    # Collect unique domains from targets still needing email
    domain_targets: dict[str, list[DMTarget]] = defaultdict(list)
    for t in needs:
        for d in t.all_domains[:3]:  # Top 3 domains per person
            domain_targets[d].append(t)

    # Build URLs: domain × contact paths, minus what CC already fetched
    # Normalize CC URLs to comparable form
    cc_paths = set()
    for u in cc_fetched_urls:
        try:
            p = urlparse(u)
            cc_paths.add(f"{_get_domain(u)}{p.path.rstrip('/')}")
        except Exception:
            pass

    urls_to_crawl = []
    url_domain_map: dict[str, str] = {}
    for domain in domain_targets:
        for path in CRAWL_PATHS:
            check_key = f"{domain}{path}"
            if check_key not in cc_paths:
                url = f"https://{domain}{path}"
                if url not in url_domain_map:
                    urls_to_crawl.append(url)
                    url_domain_map[url] = domain
        # Also homepage
        if f"{domain}" not in cc_paths and f"{domain}/" not in cc_paths:
            url = f"https://{domain}/"
            if url not in url_domain_map:
                urls_to_crawl.append(url)
                url_domain_map[url] = domain

    if not urls_to_crawl:
        logger.info("crawl4ai: no additional pages to crawl (all covered by CC)")
        return 0

    logger.info(f"crawl4ai: live crawling {len(urls_to_crawl)} pages for {len(needs)} DMs...")

    browser_config = BrowserConfig(
        headless=True, text_mode=True, light_mode=True, verbose=False,
        extra_args=["--disable-gpu", "--disable-extensions",
                     "--disable-dev-shm-usage", "--no-first-run",
                     "--disable-background-networking"],
    )
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_until="domcontentloaded",
        delay_before_return_html=0,
        mean_delay=0, max_range=0,
        page_timeout=10000,
        scan_full_page=False,
        wait_for_images=False,
        excluded_tags=["nav", "footer", "script", "style", "noscript", "header", "aside"],
    )

    country = cfg.get('country', 'AU')
    phone_re = PHONE_PATTERNS.get(country, PHONE_PATTERNS['DEFAULT'])

    async with AsyncWebCrawler(config=browser_config) as crawler:
        crawl_results = await crawler.arun_many(
            urls=urls_to_crawl, config=run_config,
            dispatcher=SemaphoreDispatcher(
                max_session_permit=min(concurrency, len(urls_to_crawl))),
        )

    found_count = 0
    for cr in crawl_results:
        if not cr.success:
            continue
        md = (cr.markdown.raw_markdown if hasattr(cr.markdown, 'raw_markdown')
              else cr.markdown) or ""
        if len(md) < 50:
            continue

        text = html_unescape(md)
        page_emails = list(set(EMAIL_RE.findall(text)))
        page_phones = phone_re.findall(text)

        if not page_emails and not page_phones:
            continue

        page_domain = url_domain_map.get(cr.url, _get_domain(cr.url))
        relevant_targets = domain_targets.get(page_domain, [])

        for t in relevant_targets:
            for e in page_emails:
                if e.lower() not in {x.lower() for x in t.all_emails}:
                    t.all_emails.append(e)

            if not t.found_email:
                best = _match_email_to_person(t, page_emails)
                if best:
                    t.found_email = best
                    t.email_source = "crawl4ai"
                    found_count += 1
                    logger.debug(f"  crawl4ai: {t.full_name} → {best}")

            if not t.found_phone and page_phones:
                t.found_phone = page_phones[0]
                t.phone_source = "crawl4ai"

    return found_count


# ── Step 6: Serper search + crawl (opt-in) ───────────────────────────────────

async def _serper_search(query: str, gl: str = "au", num: int = 5) -> list[dict]:
    if not SERPER_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "gl": gl, "num": num},
            )
            if resp.status_code != 200:
                return []
            return resp.json().get("organic", [])
    except Exception as e:
        logger.debug(f"Serper error: {e}")
        return []


BINARY_EXT_RE = re.compile(
    r'(?i)\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|jpg|jpeg|png|gif|svg|mp4|mp3|wav|webp|ico)(\?|$)'
)


def _filter_serper_results(results: list[dict]) -> list[str]:
    urls = []
    for r in results:
        link = r.get("link", "")
        if not link:
            continue
        if BINARY_EXT_RE.search(link):
            continue
        domain = _get_domain(link)
        if any(domain == skip or domain.endswith('.' + skip) for skip in SEARCH_SKIP_DOMAINS):
            continue
        urls.append(link)
    return urls


async def serper_search_contacts(targets: list[DMTarget], cfg: dict,
                                 concurrency: int = 10) -> int:
    """Search for contact info via Serper + crawl found pages."""
    needs = [t for t in targets if not t.found_email]
    if not needs:
        return 0

    gl = cfg.get("serper_gl", "au")
    country = cfg.get("country", "AU")
    phone_re = PHONE_PATTERNS.get(country, PHONE_PATTERNS["DEFAULT"])

    logger.info(f"Serper: searching for {len(needs)} people...")
    search_sem = asyncio.Semaphore(5)
    url_map: dict[str, list[DMTarget]] = {}

    async def _search_one(t: DMTarget):
        entity = t.entity_name or t.hotel_name
        query = f'"{t.full_name}" {entity} email contact'
        async with search_sem:
            results = await _serper_search(query, gl=gl)

        # Check snippets for emails first
        for r in results:
            snippet = r.get("snippet", "")
            emails = EMAIL_RE.findall(snippet)
            for e in emails:
                if e.lower() not in {x.lower() for x in t.all_emails}:
                    t.all_emails.append(e)
            if not t.found_email:
                best = _match_email_to_person(t, emails)
                if best:
                    t.found_email = best
                    t.email_source = "serper_snippet"
                    return

        # Collect URLs to crawl
        urls = _filter_serper_results(results)
        for url in urls[:3]:
            url_map.setdefault(url, []).append(t)

    await asyncio.gather(*[_search_one(t) for t in needs])

    # Crawl found pages
    urls_to_crawl = [url for url, targets_list in url_map.items()
                     if any(not t.found_email for t in targets_list)]

    if not urls_to_crawl:
        return sum(1 for t in needs if t.found_email)

    logger.info(f"Serper: crawling {len(urls_to_crawl)} pages...")
    browser_config = BrowserConfig(
        headless=True, text_mode=True, light_mode=True, verbose=False,
        extra_args=["--disable-gpu", "--disable-extensions", "--disable-dev-shm-usage",
                     "--no-first-run", "--disable-background-networking"],
    )
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_until="domcontentloaded",
        delay_before_return_html=0,
        mean_delay=0, max_range=0,
        page_timeout=15000,
        scan_full_page=False,
        wait_for_images=False,
        excluded_tags=["nav", "script", "style", "noscript", "aside"],
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        crawl_results = await crawler.arun_many(
            urls=urls_to_crawl, config=run_config,
            dispatcher=SemaphoreDispatcher(
                max_session_permit=min(concurrency, len(urls_to_crawl))),
        )

    found_count = 0
    for cr in crawl_results:
        if not cr.success:
            continue
        md = (cr.markdown.raw_markdown if hasattr(cr.markdown, 'raw_markdown')
              else cr.markdown) or ""
        if len(md) < 50:
            continue

        page_emails = list(set(EMAIL_RE.findall(md)))
        page_phones = phone_re.findall(md)
        page_targets = url_map.get(cr.url, [])

        for t in page_targets:
            for e in page_emails:
                if e.lower() not in {x.lower() for x in t.all_emails}:
                    t.all_emails.append(e)

            if not t.found_email:
                best = _match_email_to_person(t, page_emails)
                if best:
                    t.found_email = best
                    t.email_verified = False
                    t.email_source = "serper_crawl"
                    found_count += 1

            if not t.found_phone and page_phones:
                t.found_phone = page_phones[0]
                t.phone_source = "serper_crawl"

    # LLM extraction for remaining
    llm_targets = []
    for cr in crawl_results:
        if not cr.success:
            continue
        md = (cr.markdown.raw_markdown if hasattr(cr.markdown, 'raw_markdown')
              else cr.markdown) or ""
        if len(md) < 100:
            continue
        page_targets = [t for t in url_map.get(cr.url, []) if not t.found_email]
        if page_targets:
            llm_targets.append((cr.url, md, page_targets))

    if llm_targets:
        logger.info(f"Serper: LLM extracting from {len(llm_targets)} pages...")

        async def _llm_extract(url, md, page_targets):
            names = [t.full_name for t in page_targets]
            cleaned = _clean_text_for_llm(md)[:20000]
            contacts = await llm_extract_contacts(cleaned, names)
            for t in page_targets:
                if t.found_email:
                    continue
                match = contacts.get(t.full_name.lower())
                if match:
                    email = match.get('email')
                    if email and '@' in email:
                        if email.lower() not in {x.lower() for x in t.all_emails}:
                            t.all_emails.append(email)
                        prefix = email.split('@')[0].lower()
                        if prefix not in GENERIC_EMAIL_PREFIXES:
                            t.found_email = email
                            t.email_verified = False
                            t.email_source = "llm_extract"
                    phone = match.get('phone')
                    if phone and phone != 'null' and not t.found_phone:
                        t.found_phone = phone
                        t.phone_source = "llm_extract"

        await asyncio.gather(*[_llm_extract(u, m, pts) for u, m, pts in llm_targets])

    return sum(1 for t in needs if t.found_email)


# ── LLM extraction ───────────────────────────────────────────────────────────

_bedrock_client = None

def _get_bedrock():
    global _bedrock_client
    if _bedrock_client is None:
        import boto3
        _bedrock_client = boto3.client('bedrock-runtime', region_name=AWS_REGION)
    return _bedrock_client


async def llm_extract_contacts(text: str, names: list[str]) -> dict:
    """Use LLM to extract contact info for specific people from page text."""
    names_str = ", ".join(names)
    prompt = f"""Find email addresses and phone numbers for these specific people in the text below:
{names_str}

Text:
{text}

Rules:
- Only return contact info you can clearly associate with one of the named people
- Return email and phone if found
- Respond with ONLY a JSON array, no explanation

JSON format: [{{"name":"First Last","email":"or null","phone":"or null"}}]
If no contact info found for any of them, respond with exactly: []"""

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
            if content.startswith("```"):
                content = re.sub(r'^```\w*\n?', '', content)
                content = re.sub(r'\n?```$', '', content)
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)
            data = json.loads(content)
            result = {}
            for d in (data if isinstance(data, list) else [data]):
                name = (d.get("name") or "").strip().lower()
                email = (d.get("email") or "").strip() or None
                phone = (d.get("phone") or "").strip() or None
                if email and ('@' not in email or '.' not in email.split('@')[-1]):
                    email = None
                if name:
                    result[name] = {"email": email, "phone": phone}
            return result
        except Exception as e:
            err_str = str(e)
            if attempt < 2 and ("throttl" in err_str.lower() or "429" in err_str or "Too Many" in err_str):
                wait = 2 ** (attempt + 1)
                logger.warning(f"Bedrock throttled, retry in {wait}s...")
                await asyncio.sleep(wait)
                continue
            logger.warning(f"LLM error: {e}")
            return {}
    return {}


# ── Batch update ─────────────────────────────────────────────────────────────

async def apply_results(conn, targets: list[DMTarget], dry_run: bool = True) -> None:
    """Batch update DMs with found contact info + save all_emails to hotels."""
    updates = [t for t in targets if t.found_email or t.found_phone]
    has_all_emails = [t for t in targets if t.all_emails]

    if not updates and not has_all_emails:
        print("No contact info found to update.")
        return

    if dry_run:
        print(f"\nDRY RUN — would update {len(updates)} DMs, "
              f"{len(has_all_emails)} hotels with scraped emails. Run with --apply to write.")
        return

    # Update DM records with found personal email/phone
    if updates:
        dm_ids = [t.dm_id for t in updates]
        emails = [t.found_email for t in updates]
        phones = [t.found_phone for t in updates]
        verified = [t.email_verified for t in updates]
        await conn.execute(
            "UPDATE sadie_gtm.hotel_decision_makers dm"
            " SET email = COALESCE(NULLIF(dm.email, ''), v.email),"
            "     email_verified = CASE WHEN v.email IS NOT NULL AND (dm.email IS NULL OR dm.email = '')"
            "                       THEN v.verified ELSE dm.email_verified END,"
            "     phone = COALESCE(NULLIF(dm.phone, ''), v.phone),"
            "     updated_at = NOW()"
            " FROM unnest($1::int[], $2::text[], $3::text[], $4::bool[])"
            "   AS v(id, email, phone, verified)"
            " WHERE dm.id = v.id",
            dm_ids, emails, phones, verified,
        )

    # Save all scraped emails to hotels.emails (merge with existing)
    for t in has_all_emails:
        await conn.execute(
            "UPDATE sadie_gtm.hotels"
            " SET emails = (SELECT array_agg(DISTINCT e) FROM unnest("
            "   array_cat(COALESCE(emails, ARRAY[]::text[]), $2::text[])) e),"
            "     updated_at = NOW()"
            " WHERE id = $1",
            t.hotel_id, t.all_emails,
        )

    print(f"\nAPPLIED: Updated {len(updates)} DMs + {len(has_all_emails)} hotel email lists.")


# ── Main enrichment flow ─────────────────────────────────────────────────────

async def enrich(args, cfg: dict):
    """Main enrichment pipeline.

    Sequential: Load targets + discover domains (needs DB)
    Concurrent: CC harvest | httpx fetch | (MX detect → email patterns)
    Optional: Serper search
    """
    conn = await asyncpg.connect(**DB_CONFIG)
    label = cfg["label"]

    # ── Phase 1: Load + discover (sequential) ──
    targets = await load_dms_needing_contacts(conn, cfg, need="email")
    logger.info(f"Found {len(targets)} {label} DMs missing email")

    if not targets:
        print(f"All {label} DMs have email addresses!")
        await conn.close()
        return

    if args.offset and args.offset < len(targets):
        targets = targets[args.offset:]
        logger.info(f"Skipping first {args.offset} DMs (--offset)")
    if args.limit and args.limit < len(targets):
        targets = targets[:args.limit]
        logger.info(f"Limited to {len(targets)} DMs (--limit)")

    await discover_domains(conn, targets)
    await conn.close()  # Done with DB reads — release connection

    # Collect all domains for concurrent tasks
    all_domains = {d for t in targets for d in t.all_domains}

    # ── All I/O fires concurrently ──
    # httpx: blast all URLs at once (independent domains, no throttle)
    # CC: search+fetch pipelined per domain (CC Index has some latency)
    # MX → email patterns: detect providers then verify all domains×people
    async def _mx_then_email_patterns():
        providers = await _detect_email_providers(all_domains)
        found = await email_pattern_discovery(targets, providers=providers)
        return providers, found

    logger.info(f"Firing all I/O concurrently ({len(all_domains)} domains)...")

    # All three are independent — they all start at the same instant
    httpx_task = asyncio.create_task(httpx_fetch_pages(targets, cfg))
    cc_task = asyncio.create_task(cc_harvest(targets, cfg))
    patterns_task = asyncio.create_task(_mx_then_email_patterns())

    # httpx finishes first (unconstrained, direct fetch), extract + LLM immediately
    httpx_pages = await httpx_task
    httpx_found = extract_contacts_from_pages(targets, httpx_pages, cfg, "httpx_fetch")
    logger.info(f"httpx done: {httpx_found} emails from {len(httpx_pages)} pages")

    # Kick off LLM extraction on httpx pages right away (runs concurrent with CC + patterns)
    httpx_llm_task = asyncio.create_task(llm_extract_from_pages(targets, httpx_pages, cfg))

    # CC and patterns finish on their own schedule
    cc_pages, (providers, pattern_found) = await asyncio.gather(cc_task, patterns_task)
    cc_found = extract_contacts_from_pages(targets, cc_pages, cfg, "cc_harvest")
    logger.info(f"CC: {cc_found} emails ({len(cc_pages)} pages), "
                f"patterns: {pattern_found} emails")

    # LLM on CC pages (only targets still missing email)
    cc_llm_task = asyncio.create_task(llm_extract_from_pages(targets, cc_pages, cfg))

    # Wait for both LLM tasks
    httpx_llm_found = await httpx_llm_task
    cc_llm_found = await cc_llm_task
    if httpx_llm_found or cc_llm_found:
        logger.info(f"LLM extraction: {httpx_llm_found} from httpx pages, "
                    f"{cc_llm_found} from CC pages")

    # ── Serper search (opt-in) ──
    still_need = sum(1 for t in targets if not t.found_email)
    if still_need > 0 and args.serper:
        if not SERPER_API_KEY:
            logger.warning("--serper enabled but no SERPER_API_KEY set — skipping")
        else:
            logger.info(f"Phase 4: Serper search for {still_need} remaining...")
            serper_found = await serper_search_contacts(
                targets, cfg, concurrency=args.concurrency)
            logger.info(f"Serper: {serper_found} found")
    elif still_need > 0 and not args.serper:
        logger.info(f"Serper skipped: {still_need} still need email (use --serper to enable)")

    # Results summary
    total_email = sum(1 for t in targets if t.found_email)
    total_phone = sum(1 for t in targets if t.found_phone)
    verified = sum(1 for t in targets if t.email_verified)
    total_all_emails = sum(len(t.all_emails) for t in targets)
    hotels_with_emails = len({t.hotel_id for t in targets if t.all_emails})

    print(f"\n{'='*60}")
    print(f"{label.upper()} CONTACT ENRICHMENT RESULTS")
    print(f"{'='*60}")
    print(f"DMs targeted:        {len(targets)}")
    print(f"Personal emails:     {total_email} ({verified} verified)")
    print(f"Phones found:        {total_phone}")
    print(f"All emails scraped:  {total_all_emails} across {hotels_with_emails} hotels")
    print(f"Still missing email: {len(targets) - total_email}")

    sources = {}
    for t in targets:
        if t.email_source:
            sources[t.email_source] = sources.get(t.email_source, 0) + 1
    if sources:
        print(f"\nEmail sources:")
        for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
            print(f"  {src:<30} {cnt:>5}")

    found = [t for t in targets if t.found_email or t.found_phone]
    if found:
        print(f"\nFound contacts:")
        for t in found[:50]:
            parts = []
            if t.found_email:
                v = " (verified)" if t.email_verified else ""
                parts.append(f"{t.found_email}{v}")
            if t.found_phone:
                parts.append(t.found_phone)
            print(f"  {t.full_name:30s} | {t.hotel_name[:35]:35s} | {', '.join(parts)}")
        if len(found) > 50:
            print(f"  ... and {len(found) - 50} more")

    # Reconnect to DB for writing results
    conn = await asyncpg.connect(**DB_CONFIG)
    await apply_results(conn, targets, dry_run=not args.apply)
    await conn.close()


# ── Audit ────────────────────────────────────────────────────────────────────

async def audit(args, cfg: dict):
    """Print contact coverage stats for DMs in this source."""
    conn = await asyncpg.connect(**DB_CONFIG)
    jc = cfg.get("join") or ""
    wc = cfg["where"]
    label = cfg["label"]

    total_dms = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc})"
    )
    real_people = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1", ENTITY_RE_STR
    )
    with_email = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1"
        f" AND dm.email IS NOT NULL AND dm.email != ''", ENTITY_RE_STR
    )
    with_verified = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1"
        f" AND dm.email_verified = true", ENTITY_RE_STR
    )
    with_phone = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1"
        f" AND dm.phone IS NOT NULL AND dm.phone != ''", ENTITY_RE_STR
    )
    no_email = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1"
        f" AND (dm.email IS NULL OR dm.email = '')", ENTITY_RE_STR
    )
    no_both = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1"
        f" AND (dm.email IS NULL OR dm.email = '')"
        f" AND (dm.phone IS NULL OR dm.phone = '')", ENTITY_RE_STR
    )
    sources = await conn.fetch(
        f"SELECT unnest(dm.sources) AS src, COUNT(*) AS cnt"
        f" FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
        f" WHERE ({wc}) AND dm.full_name !~* $1"
        f" GROUP BY src ORDER BY cnt DESC", ENTITY_RE_STR
    )

    pct = lambda n, d: f"{100*n//d}%" if d else "0%"

    print(f"{'='*60}")
    print(f"{label.upper()} CONTACT COVERAGE AUDIT")
    print(f"{'='*60}")
    print(f"Total DM rows:           {total_dms}")
    print(f"Real people:             {real_people}")
    print(f"  With email:            {with_email} ({pct(with_email, real_people)})")
    print(f"  With verified email:   {with_verified} ({pct(with_verified, real_people)})")
    print(f"  With phone:            {with_phone} ({pct(with_phone, real_people)})")
    print(f"  Missing email:         {no_email} ({pct(no_email, real_people)})")
    print(f"  Missing both:          {no_both} ({pct(no_both, real_people)})")
    print(f"\nDM Source Breakdown:")
    for r in sources:
        print(f"  {r['src']:<30} {r['cnt']:>5}")

    if args.verbose:
        samples = await conn.fetch(
            f"SELECT dm.full_name, dm.title, h.name AS hotel_name, h.website"
            f" FROM sadie_gtm.hotel_decision_makers dm"
            f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id {jc}"
            f" WHERE ({wc}) AND dm.full_name !~* $1"
            f" AND (dm.email IS NULL OR dm.email = '')"
            f" ORDER BY random() LIMIT 20", ENTITY_RE_STR
        )
        print(f"\nSample DMs missing email:")
        for r in samples:
            web = (r['website'] or 'none')[:40]
            print(f"  {r['full_name']:30s} | {(r['title'] or ''):20s} | {r['hotel_name'][:35]:35s} | {web}")

    await conn.close()


# ── CLI ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Contact enrichment — find email/phone for known decision makers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available sources: {', '.join(SOURCE_CONFIGS.keys())}, custom",
    )
    parser.add_argument("--source", required=True,
                        help=f"Source config: {', '.join(SOURCE_CONFIGS.keys())}, or 'custom'")
    parser.add_argument("--where", type=str, default=None,
                        help="Custom WHERE clause (for --source custom)")

    parser.add_argument("--audit", action="store_true",
                        help="Print contact coverage stats")

    parser.add_argument("--apply", action="store_true", help="Write to DB (default: dry-run)")
    parser.add_argument("--serper", action="store_true",
                        help="Enable Serper web search (expensive, off by default)")
    parser.add_argument("--limit", type=int, default=None, help="Max DMs to process")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N DMs")
    parser.add_argument("--concurrency", type=int, default=10,
                        help="Browser concurrency for crawling")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    if args.source == "custom":
        if not args.where:
            print("ERROR: --source custom requires --where")
            sys.exit(1)
        cfg = {
            "label": "Custom",
            "where": args.where,
            "join": None,
            "country": "AU",
            "serper_gl": "au",
        }
    elif args.source in SOURCE_CONFIGS:
        cfg = SOURCE_CONFIGS[args.source]
    else:
        print(f"ERROR: Unknown source '{args.source}'. "
              f"Available: {', '.join(SOURCE_CONFIGS.keys())}, custom")
        sys.exit(1)

    logger.info(f"Source: {cfg['label']} | Country: {cfg.get('country', '?')}")

    if args.audit:
        await audit(args, cfg)
    else:
        await enrich(args, cfg)


if __name__ == "__main__":
    asyncio.run(main())
