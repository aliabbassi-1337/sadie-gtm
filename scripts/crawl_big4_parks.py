"""Crawl Big4 park external websites to find owner/manager names and missing contact info.

Uses crawl4ai with arun_many() for fast concurrent crawling + Azure OpenAI LLM extraction.

Usage:
    uv run python3 scripts/crawl_big4_parks.py --limit 10
    uv run python3 scripts/crawl_big4_parks.py --all
    uv run python3 scripts/crawl_big4_parks.py --blanks-only       # only parks with zero DMs
    uv run python3 scripts/crawl_big4_parks.py --chain-fill --apply # fill known chain execs
    uv run python3 scripts/crawl_big4_parks.py --enrich-missing --apply  # full targeted enrichment
    uv run python3 scripts/crawl_big4_parks.py --enrich-missing --apply --concurrency 10
"""

import argparse
import asyncio
import json
import re
import sys
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse

import asyncpg
import httpx
from loguru import logger

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.async_dispatcher import SemaphoreDispatcher, MemoryAdaptiveDispatcher

# Read .env manually (Python 3.14 dotenv bug)
def _read_env():
    env = {}
    try:
        with open('/Users/administrator/projects/sadie_gtm_owner_enrichment/.env') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

_ENV = _read_env()

# DB config — credentials from .env
DB_CONFIG = dict(
    host=_ENV.get('SADIE_DB_HOST', 'aws-1-ap-southeast-1.pooler.supabase.com'),
    port=int(_ENV.get('SADIE_DB_PORT', '6543')),
    database=_ENV.get('SADIE_DB_NAME', 'postgres'),
    user=_ENV.get('SADIE_DB_USER', 'postgres.yunairadgmaqesxejqap'),
    password=_ENV.get('SADIE_DB_PASSWORD', ''),
    statement_cache_size=0,
)
AZURE_KEY = _ENV.get('AZURE_OPENAI_API_KEY', '')
AZURE_ENDPOINT = _ENV.get('AZURE_OPENAI_ENDPOINT', '').rstrip('/')
AZURE_VERSION = _ENV.get('AZURE_OPENAI_API_VERSION', '2024-12-01-preview')
AZURE_DEPLOY = 'gpt-35-turbo'  # actual model: gpt-4.1-mini
SERPER_API_KEY = _ENV.get('SERPER_API_KEY', '')

# Key pages to crawl per park (reduced for speed)
OWNER_PATHS = ["/about", "/about-us", "/our-story", "/contact"]

# Australian phone: +61 or 0 prefix, 8-10 digits, with spaces/hyphens
AU_PHONE_RE = re.compile(
    r'(?:\+61\s?\d|\(0\d\)|\b0[2-478])\s*\d{4}\s*\d{4}'
    r'|(?:\+61\s?\d)\s*\d{4}\s*\d{4}'
    r'|\b(?:1300|1800)\s*\d{3}\s*\d{3}'
)
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
GENERIC_PREFIXES = {"noreply", "no-reply", "info", "reservations", "bookings",
                     "enquiries", "enquiry", "sales", "reception", "admin",
                     "support", "help", "hello", "contact", "stay"}

# Tourism portal domains (not park-specific websites)
BAD_WEBSITE_DOMAINS = {
    "visitvictoria.com", "visitnsw.com", "visitgippsland.com.au",
    "discovertasmania.com.au", "southaustralia.com",
    "visitqueensland.com", "big4.com.au", "wotif.com",
    "booking.com", "tripadvisor.com",
}

# Domains to skip in Serper search results
SEARCH_SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com",
    "booking.com", "expedia.com", "tripadvisor.com", "hotels.com",
    "agoda.com", "airbnb.com", "vrbo.com", "kayak.com",
    "wotif.com", "google.com", "maps.google.com",
    "abn.business.gov.au", "abr.business.gov.au",
    "asic.gov.au", "yellowpages.com.au", "whitepages.com.au",
    "truelocal.com.au", "opencorporates.com", "big4.com.au",
    "insolvencynotices.com.au", "companieslist.com", "businesslist.com.au",
    "dnb.com", "zoominfo.com", "linkedin.com",
}

ENTITY_RE_STR = (
    r'(PTY|LTD|LIMITED|LLC|INC|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|'
    r'COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|'
    r'TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT)'
)

BUSINESS_RE = re.compile(
    r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism|beach|river|'
    r'motel|camping|management|holdings|assets|council|association)'
)


@dataclass
class ParkInfo:
    hotel_id: int
    hotel_name: str
    website: str
    existing_phones: list[str] = field(default_factory=list)
    existing_email: Optional[str] = None


@dataclass
class ParkResult:
    park: ParkInfo
    pages_crawled: int = 0
    page_texts: list[str] = field(default_factory=list)
    owner_names: list[dict] = field(default_factory=list)
    new_phones: list[str] = field(default_factory=list)
    new_emails: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _norm_phone(p: str) -> str:
    return re.sub(r'[^\d]', '', p).lstrip('0').lstrip('61')


BIG4_WHERE = "(h.external_id_type = 'big4' OR h.source LIKE '%::big4%')"

# Known chain executives to fill for parks missing people
CHAIN_EXECS = {
    "Holiday Haven": {
        "match": "h.name ILIKE '%holiday haven%' OR h.website ILIKE '%holidayhaven%'",
        "people": [
            ("Andrew McEvoy", "Chair"),
            ("David Galvin", "Chief Executive Officer"),
        ],
    },
    "NRMA Parks": {
        "match": "h.name ILIKE '%nrma%' OR h.website ILIKE '%nrmaparks%'",
        "people": [
            ("Rohan Lund", "Group CEO, NRMA"),
            ("Paul Davies", "CEO, NRMA Parks and Resorts"),
        ],
    },
    "Tasman Holiday Parks": {
        "match": "h.name ILIKE '%tasman%' OR h.website ILIKE '%tasman%'",
        "people": [
            ("Nikki Milne", "Chief Executive Officer"),
            ("Bill Dimitropoulos", "Chief Financial Officer"),
            ("Corrie Milne", "General Manager"),
        ],
    },
}


async def get_big4_hotels(conn, blanks_only: bool = False) -> list[ParkInfo]:
    if blanks_only:
        rows = await conn.fetch(
            "SELECT DISTINCT ON (LOWER(TRIM(h.name)))"
            "  h.id, h.name, h.phone_google, h.phone_website, h.email,"
            "  h.website, h.city, h.state"
            " FROM sadie_gtm.hotels h"
            " WHERE " + BIG4_WHERE
            + " AND h.website IS NOT NULL AND h.website != ''"
            " AND h.website NOT ILIKE '%big4.com.au%'"
            " AND h.id NOT IN (SELECT dm.hotel_id FROM sadie_gtm.hotel_decision_makers dm)"
            " ORDER BY LOWER(TRIM(h.name)), h.phone_website DESC NULLS LAST"
        )
    else:
        rows = await conn.fetch(
            "SELECT DISTINCT ON (LOWER(TRIM(h.name)))"
            "  h.id, h.name, h.phone_google, h.phone_website, h.email,"
            "  h.website, h.city, h.state"
            " FROM sadie_gtm.hotels h"
            " WHERE " + BIG4_WHERE
            + " AND h.website IS NOT NULL AND h.website != ''"
            " AND h.website NOT ILIKE '%big4.com.au%'"
            " AND h.name NOT ILIKE '%demo%'"
            " AND h.name NOT ILIKE '%datawarehouse%'"
            " ORDER BY LOWER(TRIM(h.name)), h.phone_website DESC NULLS LAST"
        )
    parks = []
    for r in rows:
        parks.append(ParkInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'],
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))
    return parks


async def chain_fill(conn, dry_run: bool = True) -> int:
    """Fill known chain executives into parks that have zero DMs. Batch insert."""
    dm_ids, dm_names, dm_titles, dm_sources_json, dm_conf = [], [], [], [], []

    for chain_name, chain in CHAIN_EXECS.items():
        parks = await conn.fetch(
            "SELECT h.id, h.name FROM sadie_gtm.hotels h"
            " WHERE " + BIG4_WHERE
            + " AND (" + chain["match"] + ")"
            " AND h.id NOT IN ("
            "   SELECT dm2.hotel_id FROM sadie_gtm.hotel_decision_makers dm2"
            "   WHERE dm2.full_name !~* $1"
            " ) ORDER BY h.name",
            r'(PTY|LTD|TRUST|HOLDINGS|GROUP|CORP)',
        )
        if not parks:
            continue
        logger.info(f"{chain_name}: {len(parks)} parks need people")
        for person_name, person_title in chain["people"]:
            for park in parks:
                dm_ids.append(park['id'])
                dm_names.append(person_name)
                dm_titles.append(person_title)
                dm_sources_json.append(json.dumps(["chain_mgmt_lookup"]))
                dm_conf.append(0.60)

    if not dm_ids:
        print("No chain parks need filling.")
        return 0

    for i in range(len(dm_ids)):
        logger.info(f"  + {dm_names[i]} ({dm_titles[i]}) -> hotel_id={dm_ids[i]}")

    if dry_run:
        print(f"\nDRY RUN: Would insert {len(dm_ids)} chain DMs")
        return len(dm_ids)

    await conn.execute(
        "INSERT INTO sadie_gtm.hotel_decision_makers"
        "  (hotel_id, full_name, title, sources, confidence)"
        " SELECT v.hotel_id, v.full_name, v.title,"
        "  ARRAY(SELECT jsonb_array_elements_text(v.sources_json)),"
        "  v.confidence"
        " FROM unnest($1::int[], $2::text[], $3::text[], $4::jsonb[], $5::float4[])"
        "  AS v(hotel_id, full_name, title, sources_json, confidence)"
        " ON CONFLICT (hotel_id, full_name, title) DO NOTHING",
        dm_ids, dm_names, dm_titles, dm_sources_json, dm_conf,
    )
    print(f"\nInserted up to {len(dm_ids)} chain DMs")
    return len(dm_ids)


def _ensure_https(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def _build_urls(website: str) -> list[str]:
    """Build list of URLs to crawl for a park."""
    base = _ensure_https(website)
    parsed = urlparse(base)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    urls = [base]
    for p in OWNER_PATHS:
        urls.append(urljoin(origin + "/", p.lstrip("/")))
    return urls


def _extract_from_md(md: str) -> dict:
    phones = list(dict.fromkeys(AU_PHONE_RE.findall(md)))
    raw_emails = EMAIL_RE.findall(md)
    emails = []
    for e in raw_emails:
        prefix = e.split('@')[0].lower()
        if prefix not in GENERIC_PREFIXES:
            emails.append(e)
    return {"phones": phones, "emails": list(dict.fromkeys(emails))}


async def llm_extract_owners(
    client: httpx.AsyncClient, text: str, hotel_name: str,
) -> list[dict]:
    if not AZURE_KEY:
        return []
    prompt = f"""Extract person names mentioned as owner, manager, director, or host for this holiday park.
Park: {hotel_name}

Text:
{text[:5000]}

Rules:
- Every name MUST include both first name AND surname (e.g. "John Smith", not just "John")
- If the text only mentions a first name with no surname, do NOT include that person
- Do NOT include company names, trust names, or business entities
- Do NOT include town/place names
- Only include real person names explicitly stated in the text
- Include their email and phone if mentioned near their name

Return JSON array: [{{"name": "First Last", "title": "Role", "email": "if found or null", "phone": "if found or null"}}]
If no full names found, return: []"""

    url = f"{AZURE_ENDPOINT}/openai/deployments/{AZURE_DEPLOY}/chat/completions?api-version={AZURE_VERSION}"
    try:
        resp = await client.post(url,
            headers={"api-key": AZURE_KEY, "Content-Type": "application/json"},
            json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 500, "temperature": 0.1},
            timeout=20.0)
        if resp.status_code == 429:
            await asyncio.sleep(3)
            return []
        if resp.status_code != 200:
            return []
        content = resp.json()["choices"][0]["message"]["content"].strip()
        logger.debug(f"LLM raw response for {hotel_name}: {content[:300]}")
        data = json.loads(content)
        valid = []
        for d in (data if isinstance(data, list) else [data]):
            name = (d.get("name") or "").strip()
            if not name or " " not in name:
                logger.debug(f"  SKIP (no full name): {name!r}")
                continue  # Must have first + last name
            # Reject business/place patterns
            if re.search(r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism)', name):
                logger.debug(f"  SKIP (business pattern): {name!r}")
                continue
            d["name"] = name
            valid.append(d)
        logger.debug(f"LLM result for {hotel_name}: {len(valid)} valid from {len(data if isinstance(data, list) else [data])} raw")
        return valid
    except Exception as e:
        logger.warning(f"LLM error for {hotel_name}: {e}")
        return []


async def crawl_and_extract(
    crawler, llm_client: httpx.AsyncClient, parks: list[ParkInfo],
    concurrency: int = 5, use_llm: bool = True,
) -> list[ParkResult]:
    """Crawl all parks concurrently and extract owner data."""

    # Build all URLs with park index mapping
    all_urls = []
    url_to_park_idx = {}
    for i, park in enumerate(parks):
        for url in _build_urls(park.website):
            all_urls.append(url)
            url_to_park_idx[url] = i

    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_until="domcontentloaded",
        delay_before_return_html=0.5,
        page_timeout=15000,
        excluded_tags=["nav", "footer", "script", "style", "noscript", "header", "aside"],
    )

    # Initialize results
    results = [ParkResult(park=p) for p in parks]

    # Crawl all URLs concurrently
    logger.info(f"Crawling {len(all_urls)} URLs across {len(parks)} parks (concurrency={concurrency})...")
    crawl_results = await crawler.arun_many(
        urls=all_urls,
        config=config,
        max_concurrent=concurrency,
    )

    # Process crawl results
    for cr in crawl_results:
        if cr.url not in url_to_park_idx:
            continue
        idx = url_to_park_idx[cr.url]
        r = results[idx]

        if not cr.success:
            r.errors.append(f"{cr.url}: failed")
            continue

        r.pages_crawled += 1
        md = cr.markdown or ""
        if len(md) < 50:
            continue

        r.page_texts.append(md)
        extracted = _extract_from_md(md)

        # Phones: filter out existing
        existing_norm = {_norm_phone(p) for p in r.park.existing_phones}
        for p in extracted["phones"]:
            if _norm_phone(p) not in existing_norm:
                r.new_phones.append(p)

        # Emails
        for e in extracted["emails"]:
            if not r.park.existing_email or e.lower() != r.park.existing_email.lower():
                r.new_emails.append(e)

    # Deduplicate
    for r in results:
        r.new_phones = list(dict.fromkeys(r.new_phones))
        r.new_emails = list(dict.fromkeys(r.new_emails))

    # LLM extraction — batch with semaphore for rate limiting
    if use_llm and AZURE_KEY:
        sem = asyncio.Semaphore(3)  # max 3 concurrent LLM calls

        async def extract_one(r):
            if not r.page_texts:
                return
            combined = "\n---\n".join(r.page_texts)
            async with sem:
                r.owner_names = await llm_extract_owners(llm_client, combined, r.park.hotel_name)

        await asyncio.gather(*[extract_one(r) for r in results])

    return results


def _get_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().removeprefix("www.")
    except Exception:
        return ""


def _is_bad_website(url: str) -> bool:
    if not url:
        return True
    domain = _get_domain(url)
    return any(domain.endswith(bad) for bad in BAD_WEBSITE_DOMAINS)


async def load_parks_needing_people(conn) -> tuple[list[ParkInfo], dict[int, str]]:
    """Load Big4 parks without real people (blank or entity-only).
    Returns (parks, entity_names_by_hotel_id)."""
    rows = await conn.fetch(
        "SELECT h.id, h.name, h.website, h.email,"
        "  h.phone_google, h.phone_website"
        " FROM sadie_gtm.hotels h"
        " WHERE " + BIG4_WHERE
        + " AND h.id NOT IN ("
        "   SELECT dm2.hotel_id FROM sadie_gtm.hotel_decision_makers dm2"
        "   WHERE dm2.full_name !~* $1"
        " ) ORDER BY h.name",
        ENTITY_RE_STR,
    )
    parks = []
    for r in rows:
        parks.append(ParkInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'] or '',
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))
    # Get entity names for entity-only parks
    entity_rows = await conn.fetch(
        "SELECT dm.hotel_id, dm.full_name"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE + " AND dm.full_name ~* $1",
        ENTITY_RE_STR,
    )
    target_ids = {p.hotel_id for p in parks}
    entity_map = {}
    for r in entity_rows:
        if r['hotel_id'] in target_ids:
            entity_map[r['hotel_id']] = r['full_name']
    return parks, entity_map


async def _serper_find_website(client, park_name, sem) -> str | None:
    """Search Serper for a park's actual website URL."""
    if not SERPER_API_KEY:
        return None
    clean = re.sub(r'(?i)\b(big4|big 4)\b', 'BIG4', park_name).strip()
    # Remove " - City" suffixes for better search
    clean = re.sub(r'\s*-\s*[A-Z][a-z]+.*$', '', clean)
    query = f'{clean} holiday park official website Australia'
    async with sem:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 5, "gl": "au"},
                timeout=10.0,
            )
            if resp.status_code != 200:
                return None
            for item in resp.json().get("organic", []):
                url = item.get("link", "")
                domain = _get_domain(url)
                if domain and domain not in SEARCH_SKIP_DOMAINS and not _is_bad_website(url):
                    return url
        except Exception:
            pass
    return None


async def _serper_search_entity(client, entity_name, sem) -> list[str]:
    """Search for entity/holding company URLs to find directors."""
    if not SERPER_API_KEY:
        return []
    clean = re.sub(r'(?i)\b(pty\.?|ltd\.?|limited|the trustee for|the)\b', '', entity_name).strip()
    clean = re.sub(r'\s+', ' ', clean).strip()
    if len(clean) < 3:
        return []
    query = f'"{clean}" Australia director OR owner OR team'
    async with sem:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 5, "gl": "au"},
                timeout=10.0,
            )
            if resp.status_code != 200:
                return []
            urls = []
            for item in resp.json().get("organic", []):
                url = item.get("link", "")
                domain = _get_domain(url)
                if domain and not any(domain == s or domain.endswith('.' + s) for s in SEARCH_SKIP_DOMAINS):
                    urls.append(url)
                if len(urls) >= 3:
                    break
            return urls
        except Exception:
            return []


async def fix_websites(args):
    """Find real websites for all Big4 parks with bad/missing URLs, plus entity websites."""
    conn = await asyncpg.connect(**DB_CONFIG)
    dry_run = not args.apply

    # Load ALL Big4 parks
    rows = await conn.fetch(
        "SELECT h.id, h.name, h.website FROM sadie_gtm.hotels h WHERE " + BIG4_WHERE + " ORDER BY h.name"
    )
    all_parks = [(r['id'], r['name'], r['website'] or '') for r in rows]
    bad_parks = [(hid, name, url) for hid, name, url in all_parks if _is_bad_website(url)]

    # Load existing entity DMs
    entity_rows = await conn.fetch(
        "SELECT dm.hotel_id, dm.full_name"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE + " AND dm.full_name ~* $1",
        ENTITY_RE_STR,
    )
    parks_with_entity = set()
    unique_entities: dict[str, dict] = {}
    for r in entity_rows:
        key = r['full_name'].strip().upper()
        parks_with_entity.add(r['hotel_id'])
        if key not in unique_entities:
            unique_entities[key] = {'name': r['full_name'], 'hotel_ids': []}
        unique_entities[key]['hotel_ids'].append(r['hotel_id'])

    # For parks WITHOUT an entity name, search using the park name itself
    parks_without_entity = [(hid, name) for hid, name, _ in all_parks if hid not in parks_with_entity]
    for hid, name in parks_without_entity:
        key = name.strip().upper()
        if key not in unique_entities:
            unique_entities[key] = {'name': name, 'hotel_ids': []}
        unique_entities[key]['hotel_ids'].append(hid)

    print(f"Total Big4 parks: {len(all_parks)}")
    print(f"Parks with bad/missing website: {len(bad_parks)}")
    print(f"Parks with known entity: {len(parks_with_entity)}")
    print(f"Parks without entity (using park name): {len(parks_without_entity)}")
    print(f"Total searches to run: {len(unique_entities)}")

    updated = 0
    entity_urls_found = 0

    async with httpx.AsyncClient() as client:
        # 1) Fix hotel websites
        if bad_parks:
            logger.info(f"Finding websites for {len(bad_parks)} parks via Serper...")
            sem = asyncio.Semaphore(10)
            found = await asyncio.gather(*[
                _serper_find_website(client, name, sem) for _, name, _ in bad_parks
            ])
            for (hid, name, old_url), new_url in zip(bad_parks, found):
                if new_url:
                    logger.info(f"  {name} -> {new_url}")
                    if not dry_run:
                        await conn.execute(
                            "UPDATE sadie_gtm.hotels SET website = $1, updated_at = NOW() WHERE id = $2",
                            new_url, hid,
                        )
                    updated += 1
                else:
                    logger.warning(f"  No website found: {name} (was: {old_url or 'blank'})")

        # 2) Find entity/park owner websites for ALL 307 parks
        if unique_entities:
            logger.info(f"Searching {len(unique_entities)} entities/parks via Serper...")
            sem = asyncio.Semaphore(20)
            items = list(unique_entities.values())
            results = await asyncio.gather(*[
                _serper_search_entity(client, item['name'], sem) for item in items
            ])
            for item, urls in zip(items, results):
                if urls:
                    entity_urls_found += 1
                    logger.info(f"  {item['name'][:50]} -> {len(urls)} URLs")

    print(f"\n{'DRY RUN' if dry_run else 'APPLIED'}")
    print(f"Hotel websites fixed: {updated}/{len(bad_parks)}")
    print(f"Entities/parks with URLs: {entity_urls_found}/{len(unique_entities)}")
    if dry_run:
        print("\nRun with --apply to write to DB")

    await conn.close()


async def load_all_big4_parks(conn) -> tuple[list[ParkInfo], dict[int, str]]:
    """Load ALL Big4 parks with their entity names."""
    rows = await conn.fetch(
        "SELECT h.id, h.name, h.website, h.email,"
        "  h.phone_google, h.phone_website"
        " FROM sadie_gtm.hotels h"
        " WHERE " + BIG4_WHERE
        + " ORDER BY h.name",
    )
    parks = []
    for r in rows:
        parks.append(ParkInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'] or '',
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))
    # Get entity names for all parks
    entity_rows = await conn.fetch(
        "SELECT dm.hotel_id, dm.full_name"
        " FROM sadie_gtm.hotel_decision_makers dm"
        " JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        " WHERE " + BIG4_WHERE + " AND dm.full_name ~* $1",
        ENTITY_RE_STR,
    )
    entity_map = {}
    for r in entity_rows:
        entity_map[r['hotel_id']] = r['full_name']
    return parks, entity_map


async def enrich_missing(args, all_parks: bool = False):
    """Targeted enrichment using 3-pass crawl with link discovery.
    Pass 1: Crawl homepages, discover internal links (about/team/contact pages).
    Pass 2: Crawl discovered relevant pages + entity URLs from Serper.
    Pass 3: Crawl deeper pages discovered from Pass 2.
    Then LLM extract people and batch insert."""
    conn = await asyncpg.connect(**DB_CONFIG)
    dry_run = not args.apply

    # Step 1: Load targets
    if all_parks:
        parks, entity_map = await load_all_big4_parks(conn)
        logger.info(f"Loaded ALL {len(parks)} Big4 parks ({len(entity_map)} with entity names)")
    else:
        parks, entity_map = await load_parks_needing_people(conn)
        logger.info(f"Found {len(parks)} parks needing real people ({len(entity_map)} with entity names)")
    if not parks:
        print("All parks have real people!")
        await conn.close()
        return

    # Filter to crawlable parks
    crawlable = [p for p in parks if p.website and not _is_bad_website(p.website)]
    if args.limit and args.limit < len(crawlable):
        crawlable = crawlable[:args.limit]
    logger.info(f"{len(crawlable)} with crawlable websites, {len(parks) - len(crawlable)} skipped (bad/missing URL)")

    if not crawlable:
        print("No crawlable parks!")
        await conn.close()
        return

    # Step 2: Browser config — max speed, direct (no proxy)
    browser_config = BrowserConfig(
        headless=True,
        text_mode=True,
        light_mode=True,
        verbose=False,
        extra_args=[
            "--disable-gpu", "--disable-extensions", "--disable-dev-shm-usage",
            "--no-first-run", "--disable-background-networking",
        ],
    )

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_until="domcontentloaded",
        delay_before_return_html=0,
        mean_delay=0, max_range=0,   # No inter-request delays
        page_timeout=10000,  # 10s — fail fast
        scan_full_page=False,
        wait_for_images=False,
        excluded_tags=["nav", "footer", "script", "style", "noscript", "header", "aside"],
    )

    # Use MemoryAdaptiveDispatcher — auto-scales concurrency based on RAM
    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=85.0,
        max_session_permit=args.concurrency,
    )

    # Helper: run crawl across N parallel browser instances for speed
    NUM_BROWSERS = 4  # 4 parallel Chromium instances
    async def _parallel_crawl(urls_to_crawl: list[str]) -> list:
        """Split URLs across multiple crawler instances for true parallelism."""
        if not urls_to_crawl:
            return []
        if len(urls_to_crawl) <= 50:
            # Small batch — single browser is fine
            async with AsyncWebCrawler(config=browser_config) as c:
                return await c.arun_many(
                    urls=urls_to_crawl, config=run_config,
                    dispatcher=SemaphoreDispatcher(max_session_permit=min(args.concurrency, len(urls_to_crawl))),
                )
        # Split URLs into chunks across N browsers
        chunks = [[] for _ in range(NUM_BROWSERS)]
        for i, url in enumerate(urls_to_crawl):
            chunks[i % NUM_BROWSERS].append(url)

        async def _crawl_chunk(chunk_urls):
            async with AsyncWebCrawler(config=browser_config) as c:
                return await c.arun_many(
                    urls=chunk_urls, config=run_config,
                    dispatcher=SemaphoreDispatcher(
                        max_session_permit=max(1, args.concurrency // NUM_BROWSERS)
                    ),
                )

        chunk_results = await asyncio.gather(*[_crawl_chunk(ch) for ch in chunks if ch])
        # Flatten results
        all_results = []
        for cr_list in chunk_results:
            all_results.extend(cr_list)
        return all_results

    RELEVANT_RE = re.compile(
        r'(?i)(about|team|contact|our.?story|management|director|owner|staff|people|leadership|who.?we.?are)'
    )
    BINARY_EXT_RE = re.compile(
        r'(?i)\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|jpg|jpeg|png|gif|svg|mp4|mp3|wav|webp|ico)(\?|$)'
    )

    results_list = [ParkResult(park=p) for p in crawlable]

    # Build homepage URL list
    homepage_urls: list[str] = []
    url_to_idx: dict[str, int] = {}
    for i, park in enumerate(crawlable):
        url = _ensure_https(park.website)
        homepage_urls.append(url)
        url_to_idx[url] = i

    def _process_crawl_result(cr, idx):
        """Extract text + phones/emails from a crawl result."""
        r = results_list[idx]
        if not cr.success:
            r.errors.append(f"{cr.url}: failed")
            return
        r.pages_crawled += 1
        md = (cr.markdown.raw_markdown if hasattr(cr.markdown, 'raw_markdown') else cr.markdown) or ""
        if len(md) < 50:
            return
        r.page_texts.append(md)
        extracted = _extract_from_md(md)
        existing_norm = {_norm_phone(p) for p in r.park.existing_phones}
        for p in extracted["phones"]:
            if _norm_phone(p) not in existing_norm:
                r.new_phones.append(p)
        for e in extracted["emails"]:
            if not r.park.existing_email or e.lower() != r.park.existing_email.lower():
                r.new_emails.append(e)

    # ── Pass 1: Crawl homepages, discover internal links ──
    logger.info(f"Pass 1: {len(homepage_urls)} homepages ({NUM_BROWSERS} browsers, concurrency={args.concurrency})...")
    hp_results = await _parallel_crawl(homepage_urls)

    discovered_urls: list[str] = []
    disc_url_to_idx: dict[str, int] = {}
    seen_urls = set(homepage_urls)

    for cr in hp_results:
        if cr.url not in url_to_idx:
            continue
        idx = url_to_idx[cr.url]
        _process_crawl_result(cr, idx)

        # Discover relevant internal links from homepage
        links = cr.links if isinstance(cr.links, dict) else {}
        for link in links.get("internal", []):
            href = (link.get("href") or "").strip()
            text = (link.get("text") or "").strip()
            if not href or href in seen_urls or BINARY_EXT_RE.search(href):
                continue
            if RELEVANT_RE.search(href) or RELEVANT_RE.search(text):
                seen_urls.add(href)
                discovered_urls.append(href)
                disc_url_to_idx[href] = idx

    hp_ok = sum(1 for r in results_list if r.pages_crawled > 0)
    logger.info(f"Pass 1 done: {hp_ok}/{len(crawlable)} homepages OK, {len(discovered_urls)} internal pages discovered")

    # ── Entity website search via Serper (run concurrently) ──
    if entity_map and SERPER_API_KEY:
        unique_entities: dict[str, list[int]] = {}
        for i, park in enumerate(crawlable):
            ename = entity_map.get(park.hotel_id)
            if ename:
                key = ename.strip().upper()
                if key not in unique_entities:
                    unique_entities[key] = {'name': ename, 'indices': []}
                unique_entities[key]['indices'].append(i)

        logger.info(f"Searching {len(unique_entities)} entity websites via Serper...")
        async with httpx.AsyncClient() as serper_client:
            sem = asyncio.Semaphore(50)  # Higher Serper concurrency
            entity_items = list(unique_entities.values())
            entity_url_results = await asyncio.gather(*[
                _serper_search_entity(serper_client, item['name'], sem)
                for item in entity_items
            ])

        entity_urls_added = 0
        for item, urls in zip(entity_items, entity_url_results):
            for url in urls:
                if url not in seen_urls and not BINARY_EXT_RE.search(url):
                    seen_urls.add(url)
                    idx = item['indices'][0]
                    discovered_urls.append(url)
                    disc_url_to_idx[url] = idx
                    entity_urls_added += 1
        logger.info(f"Added {entity_urls_added} entity URLs to crawl list")

    # ── Pass 2: Crawl discovered park pages + entity websites ──
    deeper_urls: list[str] = []
    deeper_url_to_idx: dict[str, int] = {}
    if discovered_urls:
        logger.info(f"Pass 2: {len(discovered_urls)} pages (internal + entity)...")
        disc_results = await _parallel_crawl(discovered_urls)
        for cr in disc_results:
            if cr.url not in disc_url_to_idx:
                continue
            idx = disc_url_to_idx[cr.url]
            _process_crawl_result(cr, idx)
            links = cr.links if isinstance(cr.links, dict) else {}
            for link in links.get("internal", []):
                href = (link.get("href") or "").strip()
                text = (link.get("text") or "").strip()
                if not href or href in seen_urls or BINARY_EXT_RE.search(href):
                    continue
                if RELEVANT_RE.search(href) or RELEVANT_RE.search(text):
                    seen_urls.add(href)
                    deeper_urls.append(href)
                    deeper_url_to_idx[href] = idx

    # ── Pass 3: Crawl deeper pages (e.g. /about/our-team, /about/management) ──
    if deeper_urls:
        logger.info(f"Pass 3: {len(deeper_urls)} deeper pages...")
        deep_results = await _parallel_crawl(deeper_urls)
        for cr in deep_results:
            if cr.url not in deeper_url_to_idx:
                continue
            _process_crawl_result(cr, deeper_url_to_idx[cr.url])

    # Deduplicate
    for r in results_list:
        r.new_phones = list(dict.fromkeys(r.new_phones))
        r.new_emails = list(dict.fromkeys(r.new_emails))

    total_pages = sum(r.pages_crawled for r in results_list)
    logger.info(f"Total pages crawled: {total_pages} across {len(crawlable)} parks")

    # Step 3: LLM extraction — high concurrency
    parks_with_text = sum(1 for r in results_list if r.page_texts)
    total_text_len = sum(sum(len(t) for t in r.page_texts) for r in results_list)
    logger.info(f"Pre-LLM: {parks_with_text}/{len(results_list)} parks have page text, total {total_text_len} chars")
    for r in results_list:
        if r.page_texts:
            logger.debug(f"  {r.park.hotel_name}: {len(r.page_texts)} pages, {sum(len(t) for t in r.page_texts)} chars")
        else:
            logger.debug(f"  {r.park.hotel_name}: NO TEXT ({r.pages_crawled} pages crawled, {len(r.errors)} errors)")
    if AZURE_KEY:
        logger.info("LLM extracting people...")
        async with httpx.AsyncClient() as llm_client:
            sem = asyncio.Semaphore(50)
            async def _extract_one(r):
                if not r.page_texts:
                    return
                combined = "\n---\n".join(r.page_texts)
                logger.debug(f"LLM input for {r.park.hotel_name}: {len(combined)} chars, first 200: {combined[:200]!r}")
                async with sem:
                    r.owner_names = await llm_extract_owners(llm_client, combined, r.park.hotel_name)
            await asyncio.gather(*[_extract_one(r) for r in results_list])

    # Step 6: Print results & insert
    total_found = sum(1 for r in results_list if r.owner_names)
    total_people = sum(len(r.owner_names) for r in results_list)
    pages_ok = sum(r.pages_crawled for r in results_list)
    pages_fail = sum(len(r.errors) for r in results_list)

    print(f"\n{'='*70}")
    print(f"ENRICH MISSING RESULTS")
    print(f"{'='*70}")
    print(f"Parks targeted:      {len(parks)}")
    print(f"Parks crawled:       {len(crawlable)}")
    print(f"Pages OK / failed:   {pages_ok} / {pages_fail}")
    print(f"Parks with people:   {total_found}")
    print(f"Total people found:  {total_people}")

    for r in results_list:
        if r.owner_names:
            for o in r.owner_names:
                extras = []
                if o.get('email'): extras.append(o['email'])
                if o.get('phone'): extras.append(o['phone'])
                extra_str = f" ({', '.join(extras)})" if extras else ""
                print(f"  {r.park.hotel_name[:50]:<50} | {o['name']} - {o.get('title','?')}{extra_str}")
        elif not r.pages_crawled:
            print(f"  {r.park.hotel_name[:50]:<50} | NO PAGES ({len(r.errors)} errors)")

    # Build batch arrays for DM insert
    dm_ids, dm_names, dm_titles, dm_emails = [], [], [], []
    dm_phones, dm_sources_json, dm_conf, dm_urls = [], [], [], []
    dm_verified = []
    seen = set()

    for r in results_list:
        for owner in r.owner_names:
            name = owner.get('name', '').strip()
            title = owner.get('title', 'Owner').strip()
            if not name or len(name) < 4 or ' ' not in name:
                continue
            if BUSINESS_RE.search(name):
                continue
            key = (r.park.hotel_id, name.lower(), title.lower())
            if key in seen:
                continue
            seen.add(key)
            dm_ids.append(r.park.hotel_id)
            dm_names.append(name)
            dm_titles.append(title)
            dm_emails.append(owner.get('email') or None)
            dm_verified.append(False)
            dm_phones.append(owner.get('phone') or None)
            dm_sources_json.append(json.dumps(["website_rescrape"]))
            dm_conf.append(0.70)
            dm_urls.append(r.park.website or None)

    # Map orphan contact info (from page scraping) to person/entity DMs
    # For parks with person DMs: fill in missing email/phone on those DMs
    # For parks with NO person DMs: update entity DM in DB with contact info
    entity_contact_ids, entity_contact_emails, entity_contact_phones = [], [], []
    for r in results_list:
        orphan_email = None
        orphan_phone = r.new_phones[0] if r.new_phones else None
        if r.new_emails:
            for e in r.new_emails:
                prefix = e.split('@')[0].lower()
                if prefix not in GENERIC_PREFIXES:
                    orphan_email = e
                    break
            if not orphan_email:
                orphan_email = r.new_emails[0]

        if not orphan_email and not orphan_phone:
            continue

        # Check if this park has person DMs in the batch
        park_dm_indices = [i for i, hid in enumerate(dm_ids) if hid == r.park.hotel_id]
        if park_dm_indices:
            # Fill orphan contact into person DMs that lack email/phone
            for idx in park_dm_indices:
                if not dm_emails[idx] and orphan_email:
                    dm_emails[idx] = orphan_email
                if not dm_phones[idx] and orphan_phone:
                    dm_phones[idx] = orphan_phone
        else:
            # No person DMs — map contact to entity DM in DB
            if r.park.hotel_id in entity_map:
                entity_contact_ids.append(r.park.hotel_id)
                entity_contact_emails.append(orphan_email)
                entity_contact_phones.append(orphan_phone)

    dm_with_email = sum(1 for e in dm_emails if e)
    dm_with_phone = sum(1 for p in dm_phones if p)
    print(f"\nNew DMs to insert: {len(dm_ids)} ({dm_with_email} with email, {dm_with_phone} with phone)")
    print(f"Entity contact updates: {len(entity_contact_ids)}")

    if dry_run:
        print(f"\nDRY RUN — run with --apply to write to DB")
    else:
        if dm_ids:
            await conn.execute(
                "INSERT INTO sadie_gtm.hotel_decision_makers"
                "  (hotel_id, full_name, title, email, email_verified, phone,"
                "   sources, confidence, raw_source_url)"
                " SELECT"
                "  v.hotel_id, v.full_name, v.title, v.email, v.email_verified, v.phone,"
                "  ARRAY(SELECT jsonb_array_elements_text(v.sources_json)),"
                "  v.confidence, v.raw_source_url"
                " FROM unnest("
                "  $1::int[], $2::text[], $3::text[], $4::text[],"
                "  $5::bool[], $6::text[], $7::jsonb[], $8::float4[], $9::text[]"
                " ) AS v(hotel_id, full_name, title, email, email_verified, phone,"
                "        sources_json, confidence, raw_source_url)"
                " ON CONFLICT (hotel_id, full_name, title) DO UPDATE"
                " SET sources = (SELECT array_agg(DISTINCT s) FROM unnest("
                "     array_cat(sadie_gtm.hotel_decision_makers.sources, EXCLUDED.sources)) s),"
                "     confidence = GREATEST(EXCLUDED.confidence, sadie_gtm.hotel_decision_makers.confidence),"
                "     email = COALESCE(EXCLUDED.email, sadie_gtm.hotel_decision_makers.email),"
                "     phone = COALESCE(EXCLUDED.phone, sadie_gtm.hotel_decision_makers.phone),"
                "     updated_at = NOW()",
                dm_ids, dm_names, dm_titles, dm_emails,
                dm_verified, dm_phones, dm_sources_json, dm_conf, dm_urls,
            )
        # Update entity DMs with orphan contact info
        if entity_contact_ids:
            await conn.execute(
                "UPDATE sadie_gtm.hotel_decision_makers dm"
                " SET email = COALESCE(dm.email, v.email),"
                "     phone = COALESCE(dm.phone, v.phone),"
                "     updated_at = NOW()"
                " FROM unnest($1::int[], $2::text[], $3::text[])"
                "   AS v(hotel_id, email, phone)"
                " WHERE dm.hotel_id = v.hotel_id"
                "   AND dm.full_name ~* $4",
                entity_contact_ids, entity_contact_emails, entity_contact_phones,
                ENTITY_RE_STR,
            )
        print(f"\nAPPLIED to database!")

    await conn.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--blanks-only", action="store_true", help="Only crawl parks with zero DMs")
    parser.add_argument("--chain-fill", action="store_true", help="Fill known chain execs into blank parks")
    parser.add_argument("--fix-websites", action="store_true",
                        help="Find real websites for all Big4 parks with bad/missing URLs via Serper")
    parser.add_argument("--enrich-missing", action="store_true",
                        help="Full enrichment for parks without real people")
    parser.add_argument("--enrich-all", action="store_true",
                        help="Full enrichment for ALL Big4 parks (re-enrich everything)")
    parser.add_argument("--apply", action="store_true", help="Write results to DB (default: dry-run)")
    parser.add_argument("--hotel-ids", type=str, help="Comma-separated hotel IDs to crawl")
    parser.add_argument("--no-llm", action="store_true")
    parser.add_argument("--concurrency", type=int, default=300)
    parser.add_argument("--output", type=str, default="/tmp/big4_crawl_results.json", help="JSON output file")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    # Fix websites — find real URLs for parks with bad/missing websites + entities
    if args.fix_websites:
        await fix_websites(args)
        return

    # Chain fill mode — fast batch insert, no crawling needed
    if args.chain_fill:
        conn = await asyncpg.connect(**DB_CONFIG)
        await chain_fill(conn, dry_run=not args.apply)
        await conn.close()
        return

    # Enrich missing — comprehensive targeted enrichment
    if args.enrich_missing:
        await enrich_missing(args, all_parks=False)
        return

    # Enrich all — re-enrich ALL Big4 parks
    if args.enrich_all:
        await enrich_missing(args, all_parks=True)
        return

    conn = await asyncpg.connect(**DB_CONFIG)
    parks = await get_big4_hotels(conn, blanks_only=args.blanks_only)
    await conn.close()

    logger.info(f"Found {len(parks)} Big4 parks with external websites")
    if args.hotel_ids:
        ids = {int(x.strip()) for x in args.hotel_ids.split(",")}
        parks = [p for p in parks if p.hotel_id in ids]
    elif not args.all:
        parks = parks[:args.limit]
    logger.info(f"Processing {len(parks)} parks (LLM={not args.no_llm}, concurrency={args.concurrency})")

    browser_config = BrowserConfig(headless=True, verbose=False)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        async with httpx.AsyncClient() as llm_client:
            results = await crawl_and_extract(
                crawler, llm_client, parks,
                concurrency=args.concurrency,
                use_llm=not args.no_llm,
            )

    # Print results
    total_owners = sum(1 for r in results if r.owner_names)
    total_emails = sum(1 for r in results if r.new_emails)
    total_phones = sum(1 for r in results if r.new_phones)
    total_failed = sum(1 for r in results if not r.pages_crawled)

    print(f"\n{'='*70}")
    print(f"RESULTS: {len(results)} parks crawled")
    print(f"{'='*70}")
    print(f"Found owner/manager names: {total_owners}")
    print(f"Found new emails:          {total_emails}")
    print(f"Found new phones:          {total_phones}")
    print(f"Failed to crawl:           {total_failed}")

    for r in results:
        markers = []
        if r.owner_names:
            markers.append(f"OWNERS: {', '.join(o['name'] for o in r.owner_names)}")
        if r.new_emails:
            markers.append(f"emails: {', '.join(r.new_emails[:3])}")
        if r.new_phones:
            markers.append(f"phones: {len(r.new_phones)} new")
        if markers:
            print(f"  {r.park.hotel_name[:50]:<50} | {' | '.join(markers)}")
        elif not r.pages_crawled:
            print(f"  {r.park.hotel_name[:50]:<50} | FAILED")

    # JSON output for parks with new data
    json_out = []
    for r in results:
        if r.owner_names or r.new_emails or r.new_phones:
            json_out.append({
                "hotel_id": r.park.hotel_id,
                "hotel_name": r.park.hotel_name,
                "website": r.park.website,
                "owner_names": r.owner_names,
                "new_phones": r.new_phones,
                "new_emails": r.new_emails,
                "pages_crawled": r.pages_crawled,
            })
    if json_out:
        with open(args.output, "w") as f:
            json.dump(json_out, f, indent=2)
        print(f"\n--- JSON ({len(json_out)} parks with new data) written to {args.output} ---")


if __name__ == "__main__":
    asyncio.run(main())
