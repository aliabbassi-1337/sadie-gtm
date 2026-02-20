"""Generic contact enrichment workflow — crawl hotel websites + LLM extraction + Serper search.

Refactored from scripts/crawl_big4_parks.py. Works on any hotel source via SOURCE_CONFIGS.

Usage:
    # Big4 (same as original)
    uv run python3 workflows/enrich_contacts.py --source big4 --enrich-all --apply --concurrency 300
    uv run python3 workflows/enrich_contacts.py --source big4 --chain-fill --apply
    uv run python3 workflows/enrich_contacts.py --source big4 --audit

    # RMS Australia
    uv run python3 workflows/enrich_contacts.py --source rms_au --enrich-missing --apply --concurrency 300
    uv run python3 workflows/enrich_contacts.py --source rms_au --enrich-missing --apply --offset 500

    # Ad-hoc
    uv run python3 workflows/enrich_contacts.py --source custom --where "h.city = 'Sydney'" --enrich-missing --limit 10
"""

import argparse
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse

import asyncpg
import httpx
from loguru import logger

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.async_dispatcher import SemaphoreDispatcher


# ── Environment ──────────────────────────────────────────────────────────────

def _read_env():
    """Read .env file manually (avoids dotenv issues on some Python versions)."""
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
AZURE_KEY = _ENV.get('AZURE_OPENAI_API_KEY', '')
AZURE_ENDPOINT = _ENV.get('AZURE_OPENAI_ENDPOINT', '').rstrip('/')
AZURE_VERSION = _ENV.get('AZURE_OPENAI_API_VERSION', '2024-12-01-preview')
AZURE_DEPLOY = 'gpt-35-turbo'  # actual model: gpt-4.1-mini
SERPER_API_KEY = _ENV.get('SERPER_API_KEY', '')


# ── Shared constants ─────────────────────────────────────────────────────────

OWNER_PATHS = ["/about", "/about-us", "/our-story", "/contact", "/team", "/our-team"]

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
GENERIC_PREFIXES = {
    "noreply", "no-reply", "info", "reservations", "bookings",
    "enquiries", "enquiry", "sales", "reception", "admin",
    "support", "help", "hello", "contact", "stay",
}

BAD_WEBSITE_DOMAINS = {
    "visitvictoria.com", "visitnsw.com", "visitgippsland.com.au",
    "discovertasmania.com.au", "southaustralia.com",
    "visitqueensland.com", "big4.com.au", "wotif.com",
    "booking.com", "tripadvisor.com",
}

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
    r'(PTY|LTD|LIMITED|LLC|INC\b|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|'
    r'COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|'
    r'TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT|PROPRIETARY|COMPANY|'
    r'COMMISSION|FOUNDATION|TRADING|NOMINEES|SUPERANNUATION|ENTERPRISES)'
)

BUSINESS_RE = re.compile(
    r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism|beach|river|'
    r'motel|camping|management|holdings|assets|council|association)'
)

RELEVANT_RE = re.compile(
    r'(?i)(about|team|contact|our.?story|management|director|owner|staff|people|leadership|who.?we.?are)'
)
BINARY_EXT_RE = re.compile(
    r'(?i)\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|jpg|jpeg|png|gif|svg|mp4|mp3|wav|webp|ico)(\?|$)'
)


# ── Phone patterns by country ────────────────────────────────────────────────

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
        "entity_search_suffix": "Australia director OR owner OR team",
        "enable_abn": True,
        "chain_execs": {
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
        },
    },
    "rms_au": {
        "label": "RMS Cloud Australia",
        "where": "hbe.booking_engine_id = 12 AND h.country IN ('Australia', 'AU')",
        "join": "JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id",
        "country": "AU",
        "serper_gl": "au",
        "entity_search_suffix": "Australia director OR owner OR team",
        "enable_abn": True,
        "chain_execs": {},
    },
}


# ── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class HotelInfo:
    hotel_id: int
    hotel_name: str
    website: str
    existing_phones: list[str] = field(default_factory=list)
    existing_email: Optional[str] = None


@dataclass
class HotelResult:
    hotel: HotelInfo
    pages_crawled: int = 0
    page_texts: list[str] = field(default_factory=list)
    owner_names: list[dict] = field(default_factory=list)
    new_phones: list[str] = field(default_factory=list)
    new_emails: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── Utility functions ────────────────────────────────────────────────────────

def _norm_phone(p: str) -> str:
    return re.sub(r'[^\d]', '', p).lstrip('0').lstrip('61')


def _ensure_https(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


def _build_urls(website: str) -> list[str]:
    base = _ensure_https(website)
    parsed = urlparse(base)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    urls = [base]
    for p in OWNER_PATHS:
        urls.append(urljoin(origin + "/", p.lstrip("/")))
    return urls


def _extract_from_md(md: str, country: str = "AU") -> dict:
    phone_re = PHONE_PATTERNS.get(country, PHONE_PATTERNS["DEFAULT"])
    phones = list(dict.fromkeys(phone_re.findall(md)))
    raw_emails = EMAIL_RE.findall(md)
    emails = []
    for e in raw_emails:
        prefix = e.split('@')[0].lower()
        if prefix not in GENERIC_PREFIXES:
            emails.append(e)
    return {"phones": phones, "emails": list(dict.fromkeys(emails))}


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


# ── Query builders ───────────────────────────────────────────────────────────

async def get_hotels(conn, cfg: dict, blanks_only: bool = False) -> list[HotelInfo]:
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    if blanks_only:
        rows = await conn.fetch(
            f"SELECT DISTINCT ON (LOWER(TRIM(h.name)))"
            f"  h.id, h.name, h.phone_google, h.phone_website, h.email,"
            f"  h.website, h.city, h.state"
            f" FROM sadie_gtm.hotels h {jc}"
            f" WHERE ({wc})"
            f"  AND h.website IS NOT NULL AND h.website != ''"
            f"  AND h.id NOT IN (SELECT dm.hotel_id FROM sadie_gtm.hotel_decision_makers dm)"
            f" ORDER BY LOWER(TRIM(h.name)), h.phone_website DESC NULLS LAST"
        )
    else:
        rows = await conn.fetch(
            f"SELECT DISTINCT ON (LOWER(TRIM(h.name)))"
            f"  h.id, h.name, h.phone_google, h.phone_website, h.email,"
            f"  h.website, h.city, h.state"
            f" FROM sadie_gtm.hotels h {jc}"
            f" WHERE ({wc})"
            f"  AND h.website IS NOT NULL AND h.website != ''"
            f"  AND h.name NOT ILIKE '%demo%'"
            f"  AND h.name NOT ILIKE '%datawarehouse%'"
            f" ORDER BY LOWER(TRIM(h.name)), h.phone_website DESC NULLS LAST"
        )

    hotels = []
    for r in rows:
        hotels.append(HotelInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'],
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))
    return hotels


async def load_hotels_needing_people(conn, cfg: dict) -> tuple[list[HotelInfo], dict[int, str]]:
    """Load hotels without real people (blank or entity-only).
    Returns (hotels, entity_names_by_hotel_id)."""
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    rows = await conn.fetch(
        f"SELECT h.id, h.name, h.website, h.email,"
        f"  h.phone_google, h.phone_website"
        f" FROM sadie_gtm.hotels h {jc}"
        f" WHERE ({wc})"
        f"  AND h.id NOT IN ("
        f"    SELECT dm2.hotel_id FROM sadie_gtm.hotel_decision_makers dm2"
        f"    WHERE dm2.full_name !~* $1"
        f"  ) ORDER BY h.name",
        ENTITY_RE_STR,
    )
    hotels = []
    for r in rows:
        hotels.append(HotelInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'] or '',
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))

    entity_rows = await conn.fetch(
        f"SELECT dm.hotel_id, dm.full_name"
        f" FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc}"
        f" WHERE ({wc}) AND dm.full_name ~* $1",
        ENTITY_RE_STR,
    )
    target_ids = {h.hotel_id for h in hotels}
    entity_map = {}
    for r in entity_rows:
        if r['hotel_id'] in target_ids:
            entity_map[r['hotel_id']] = r['full_name']
    return hotels, entity_map


async def load_all_hotels(conn, cfg: dict) -> tuple[list[HotelInfo], dict[int, str]]:
    """Load ALL hotels in this source with their entity names."""
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    rows = await conn.fetch(
        f"SELECT h.id, h.name, h.website, h.email,"
        f"  h.phone_google, h.phone_website"
        f" FROM sadie_gtm.hotels h {jc}"
        f" WHERE ({wc}) ORDER BY h.name",
    )
    hotels = []
    for r in rows:
        hotels.append(HotelInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'] or '',
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))

    entity_rows = await conn.fetch(
        f"SELECT dm.hotel_id, dm.full_name"
        f" FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc}"
        f" WHERE ({wc}) AND dm.full_name ~* $1",
        ENTITY_RE_STR,
    )
    entity_map = {}
    for r in entity_rows:
        entity_map[r['hotel_id']] = r['full_name']
    return hotels, entity_map


# ── Serper functions ─────────────────────────────────────────────────────────

async def _serper_find_website(client, hotel_name: str, sem, cfg: dict) -> str | None:
    """Search Serper for a hotel's actual website URL."""
    if not SERPER_API_KEY:
        return None
    clean = re.sub(r'\s*-\s*[A-Z][a-z]+.*$', '', hotel_name).strip()
    query = f'{clean} official website'
    async with sem:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 5, "gl": cfg.get("serper_gl", "")},
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


async def _serper_search_entity(client, entity_name: str, sem, cfg: dict) -> list[str]:
    """Search for entity/holding company URLs to find directors."""
    if not SERPER_API_KEY:
        return []
    clean = re.sub(r'(?i)\b(pty\.?|ltd\.?|limited|the trustee for|the)\b', '', entity_name).strip()
    clean = re.sub(r'\s+', ' ', clean).strip()
    if len(clean) < 3:
        return []
    suffix = cfg.get("entity_search_suffix", "director OR owner OR team")
    query = f'"{clean}" {suffix}'
    async with sem:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 5, "gl": cfg.get("serper_gl", "")},
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


# ── LLM extraction ───────────────────────────────────────────────────────────

async def llm_extract_owners(
    client: httpx.AsyncClient, text: str, hotel_name: str,
) -> list[dict]:
    if not AZURE_KEY:
        return []
    prompt = f"""Extract person names mentioned as owner, manager, director, or host for this property.
Property: {hotel_name}

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
                continue
            if re.search(r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism)', name):
                continue
            d["name"] = name
            valid.append(d)
        return valid
    except Exception as e:
        logger.warning(f"LLM error for {hotel_name}: {e}")
        return []


# ── 3-pass enrichment ────────────────────────────────────────────────────────

async def enrich(args, cfg: dict, all_hotels: bool = False):
    """Targeted enrichment using 3-pass crawl with link discovery.
    Pass 1: Crawl homepages, discover internal links.
    Pass 2: Crawl discovered pages + entity URLs from Serper.
    Pass 3: Crawl deeper pages discovered from Pass 2.
    Then LLM extract people and batch insert."""
    conn = await asyncpg.connect(**DB_CONFIG)
    dry_run = not args.apply
    country = cfg.get("country", "AU")
    label = cfg["label"]

    # Step 1: Load targets
    if all_hotels:
        hotels, entity_map = await load_all_hotels(conn, cfg)
        logger.info(f"Loaded ALL {len(hotels)} {label} hotels ({len(entity_map)} with entity names)")
    else:
        hotels, entity_map = await load_hotels_needing_people(conn, cfg)
        logger.info(f"Found {len(hotels)} {label} hotels needing real people ({len(entity_map)} with entity names)")

    if not hotels:
        print(f"All {label} hotels have real people!")
        await conn.close()
        return

    # Filter to crawlable hotels
    crawlable = [h for h in hotels if h.website and not _is_bad_website(h.website)]

    # Apply offset for resumability on large batches
    if args.offset and args.offset < len(crawlable):
        crawlable = crawlable[args.offset:]
        logger.info(f"Skipping first {args.offset} hotels (--offset)")

    if args.limit and args.limit < len(crawlable):
        crawlable = crawlable[:args.limit]

    logger.info(f"{len(crawlable)} with crawlable websites, {len(hotels) - len(crawlable)} skipped (bad/missing URL)")

    if not crawlable:
        print(f"No crawlable {label} hotels!")
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
        mean_delay=0, max_range=0,
        page_timeout=10000,
        scan_full_page=False,
        wait_for_images=False,
        excluded_tags=["nav", "footer", "script", "style", "noscript", "header", "aside"],
    )

    NUM_BROWSERS = 4

    async def _parallel_crawl(urls_to_crawl: list[str]) -> list:
        """Split URLs across multiple crawler instances for true parallelism."""
        if not urls_to_crawl:
            return []
        if len(urls_to_crawl) <= 50:
            async with AsyncWebCrawler(config=browser_config) as c:
                return await c.arun_many(
                    urls=urls_to_crawl, config=run_config,
                    dispatcher=SemaphoreDispatcher(max_session_permit=min(args.concurrency, len(urls_to_crawl))),
                )
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
        all_results = []
        for cr_list in chunk_results:
            all_results.extend(cr_list)
        return all_results

    results_list = [HotelResult(hotel=h) for h in crawlable]

    # Build homepage URL list
    homepage_urls: list[str] = []
    url_to_idx: dict[str, int] = {}
    for i, hotel in enumerate(crawlable):
        url = _ensure_https(hotel.website)
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
        extracted = _extract_from_md(md, country)
        existing_norm = {_norm_phone(p) for p in r.hotel.existing_phones}
        for p in extracted["phones"]:
            if _norm_phone(p) not in existing_norm:
                r.new_phones.append(p)
        for e in extracted["emails"]:
            if not r.hotel.existing_email or e.lower() != r.hotel.existing_email.lower():
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
        unique_entities: dict[str, dict] = {}
        for i, hotel in enumerate(crawlable):
            ename = entity_map.get(hotel.hotel_id)
            if ename:
                key = ename.strip().upper()
                if key not in unique_entities:
                    unique_entities[key] = {'name': ename, 'indices': []}
                unique_entities[key]['indices'].append(i)

        logger.info(f"Searching {len(unique_entities)} entity websites via Serper...")
        async with httpx.AsyncClient() as serper_client:
            sem = asyncio.Semaphore(50)
            entity_items = list(unique_entities.values())
            entity_url_results = await asyncio.gather(*[
                _serper_search_entity(serper_client, item['name'], sem, cfg)
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

    # ── Pass 3: Crawl deeper pages ──
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
    logger.info(f"Total pages crawled: {total_pages} across {len(crawlable)} hotels")

    # Step 3: LLM extraction
    parks_with_text = sum(1 for r in results_list if r.page_texts)
    logger.info(f"Pre-LLM: {parks_with_text}/{len(results_list)} hotels have page text")
    if AZURE_KEY:
        logger.info("LLM extracting people...")
        async with httpx.AsyncClient() as llm_client:
            sem = asyncio.Semaphore(300)

            async def _extract_one(r):
                if not r.page_texts:
                    return
                combined = "\n---\n".join(r.page_texts)
                async with sem:
                    r.owner_names = await llm_extract_owners(llm_client, combined, r.hotel.hotel_name)

            await asyncio.gather(*[_extract_one(r) for r in results_list])

    # Results summary
    total_found = sum(1 for r in results_list if r.owner_names)
    total_people = sum(len(r.owner_names) for r in results_list)
    pages_ok = sum(r.pages_crawled for r in results_list)
    pages_fail = sum(len(r.errors) for r in results_list)

    print(f"\n{'='*70}")
    print(f"{label.upper()} ENRICHMENT RESULTS")
    print(f"{'='*70}")
    print(f"Hotels targeted:     {len(hotels)}")
    print(f"Hotels crawled:      {len(crawlable)}")
    print(f"Pages OK / failed:   {pages_ok} / {pages_fail}")
    print(f"Hotels with people:  {total_found}")
    print(f"Total people found:  {total_people}")

    for r in results_list:
        if r.owner_names:
            for o in r.owner_names:
                extras = []
                if o.get('email'): extras.append(o['email'])
                if o.get('phone'): extras.append(o['phone'])
                extra_str = f" ({', '.join(extras)})" if extras else ""
                print(f"  {r.hotel.hotel_name[:50]:<50} | {o['name']} - {o.get('title','?')}{extra_str}")
        elif not r.pages_crawled:
            print(f"  {r.hotel.hotel_name[:50]:<50} | NO PAGES ({len(r.errors)} errors)")

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
            key = (r.hotel.hotel_id, name.lower(), title.lower())
            if key in seen:
                continue
            seen.add(key)

            # Validate email/phone from LLM output
            raw_email = (owner.get('email') or '').strip() or None
            raw_phone = (owner.get('phone') or '').strip() or None
            if raw_email and (
                '@' not in raw_email
                or '.' not in raw_email.split('@')[-1]
                or re.match(r'^[\d\s]+$', raw_email)
            ):
                logger.warning(f"Bad email from LLM for {name}: {raw_email!r} — moving to phone")
                if not raw_phone and re.match(r'^[\d\s\-\+\(\)]+$', raw_email):
                    raw_phone = raw_email
                raw_email = None
            if raw_phone and len(re.sub(r'[^\d]', '', raw_phone)) < 8:
                logger.warning(f"Bad phone from LLM for {name}: {raw_phone!r} — dropping")
                raw_phone = None

            dm_ids.append(r.hotel.hotel_id)
            dm_names.append(name)
            dm_titles.append(title)
            dm_emails.append(raw_email)
            dm_verified.append(False)
            dm_phones.append(raw_phone)
            dm_sources_json.append(json.dumps(["website_rescrape"]))
            dm_conf.append(0.70)
            dm_urls.append(r.hotel.website or None)

    # Map orphan contact info (from page scraping) to person/entity DMs
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

        # Check if this hotel has person DMs in the batch
        park_dm_indices = [i for i, hid in enumerate(dm_ids) if hid == r.hotel.hotel_id]
        if park_dm_indices:
            for idx in park_dm_indices:
                if not dm_emails[idx] and orphan_email:
                    dm_emails[idx] = orphan_email
                if not dm_phones[idx] and orphan_phone:
                    dm_phones[idx] = orphan_phone
        else:
            if r.hotel.hotel_id in entity_map:
                entity_contact_ids.append(r.hotel.hotel_id)
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


# ── Chain fill ───────────────────────────────────────────────────────────────

async def chain_fill(conn, cfg: dict, dry_run: bool = True) -> int:
    """Fill known chain executives into hotels that have zero real people DMs."""
    chain_execs = cfg.get("chain_execs", {})
    if not chain_execs:
        print("No chain executives configured for this source.")
        return 0

    jc = cfg.get("join") or ""
    wc = cfg["where"]
    dm_ids, dm_names, dm_titles, dm_sources_json, dm_conf = [], [], [], [], []

    for chain_name, chain in chain_execs.items():
        parks = await conn.fetch(
            f"SELECT h.id, h.name FROM sadie_gtm.hotels h"
            f" {jc}"
            f" WHERE ({wc})"
            f"  AND ({chain['match']})"
            f"  AND h.id NOT IN ("
            f"    SELECT dm2.hotel_id FROM sadie_gtm.hotel_decision_makers dm2"
            f"    WHERE dm2.full_name !~* $1"
            f"  ) ORDER BY h.name",
            r'(PTY|LTD|TRUST|HOLDINGS|GROUP|CORP)',
        )
        if not parks:
            continue
        logger.info(f"{chain_name}: {len(parks)} hotels need people")
        for person_name, person_title in chain["people"]:
            for park in parks:
                dm_ids.append(park['id'])
                dm_names.append(person_name)
                dm_titles.append(person_title)
                dm_sources_json.append(json.dumps(["chain_mgmt_lookup"]))
                dm_conf.append(0.60)

    if not dm_ids:
        print("No chain hotels need filling.")
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


# ── Fix websites ─────────────────────────────────────────────────────────────

async def fix_websites(args, cfg: dict):
    """Find real websites for hotels with bad/missing URLs via Serper."""
    conn = await asyncpg.connect(**DB_CONFIG)
    dry_run = not args.apply
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    rows = await conn.fetch(
        f"SELECT h.id, h.name, h.website FROM sadie_gtm.hotels h"
        f" {jc} WHERE ({wc}) ORDER BY h.name"
    )
    all_hotels = [(r['id'], r['name'], r['website'] or '') for r in rows]
    bad_hotels = [(hid, name, url) for hid, name, url in all_hotels if _is_bad_website(url)]

    print(f"Total {cfg['label']} hotels: {len(all_hotels)}")
    print(f"Hotels with bad/missing website: {len(bad_hotels)}")

    updated = 0
    if bad_hotels:
        async with httpx.AsyncClient() as client:
            sem = asyncio.Semaphore(10)
            found = await asyncio.gather(*[
                _serper_find_website(client, name, sem, cfg) for _, name, _ in bad_hotels
            ])
            for (hid, name, old_url), new_url in zip(bad_hotels, found):
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

    print(f"\n{'DRY RUN' if dry_run else 'APPLIED'}")
    print(f"Hotel websites fixed: {updated}/{len(bad_hotels)}")
    if dry_run:
        print("\nRun with --apply to write to DB")
    await conn.close()


# ── Audit ────────────────────────────────────────────────────────────────────

async def audit(args, cfg: dict):
    """Print enrichment summary stats."""
    conn = await asyncpg.connect(**DB_CONFIG)
    jc = cfg.get("join") or ""
    wc = cfg["where"]
    label = cfg["label"]

    total = await conn.fetchval(
        f"SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h {jc} WHERE ({wc})"
    )
    with_dm = await conn.fetchval(
        f"SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        f" JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        f" {jc} WHERE ({wc})"
    )
    with_real = await conn.fetchval(
        f"SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        f" JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        f" {jc} WHERE ({wc}) AND dm.full_name !~* $1",
        ENTITY_RE_STR,
    )
    total_dms = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc} WHERE ({wc})"
    )
    total_people = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc} WHERE ({wc}) AND dm.full_name !~* $1",
        ENTITY_RE_STR,
    )
    blanks = await conn.fetchval(
        f"SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h {jc}"
        f" WHERE ({wc})"
        f"  AND h.id NOT IN (SELECT dm.hotel_id FROM sadie_gtm.hotel_decision_makers dm)"
    )
    entity_only = await conn.fetchval(
        f"SELECT COUNT(DISTINCT h.id) FROM sadie_gtm.hotels h"
        f" JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id"
        f" {jc} WHERE ({wc})"
        f"  AND h.id NOT IN ("
        f"    SELECT dm2.hotel_id FROM sadie_gtm.hotel_decision_makers dm2"
        f"    WHERE dm2.full_name !~* $1"
        f"  )",
        ENTITY_RE_STR,
    )
    with_email = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc} WHERE ({wc}) AND dm.email IS NOT NULL AND dm.email != ''"
    )
    with_phone = await conn.fetchval(
        f"SELECT COUNT(*) FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc} WHERE ({wc}) AND dm.phone IS NOT NULL AND dm.phone != ''"
    )
    sources = await conn.fetch(
        f"SELECT unnest(dm.sources) AS src, COUNT(*) AS cnt"
        f" FROM sadie_gtm.hotel_decision_makers dm"
        f" JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id"
        f" {jc} WHERE ({wc})"
        f" GROUP BY src ORDER BY cnt DESC"
    )

    pct = lambda n: f"{100*n//total}%" if total else "0%"

    print(f"{'='*60}")
    print(f"{label.upper()} ENRICHMENT AUDIT")
    print(f"{'='*60}")
    print(f"Total hotels:               {total}")
    print(f"Hotels with any DM:         {with_dm} ({pct(with_dm)})")
    print(f"Hotels with real people:     {with_real} ({pct(with_real)})")
    print(f"Hotels entity-only:          {entity_only}")
    print(f"Hotels with ZERO DMs:        {blanks}")
    print(f"Total DM rows:               {total_dms}")
    print(f"  - Real people:             {total_people}")
    print(f"  - With email:              {with_email}")
    print(f"  - With phone:              {with_phone}")
    print(f"\nDM Source Breakdown:")
    for r in sources:
        print(f"  {r['src']:<30} {r['cnt']:>5}")

    await conn.close()


# ── CLI ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Generic contact enrichment — crawl + LLM + Serper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available sources: {', '.join(SOURCE_CONFIGS.keys())}, custom",
    )
    parser.add_argument("--source", required=True,
                        help=f"Source config: {', '.join(SOURCE_CONFIGS.keys())}, or 'custom'")
    parser.add_argument("--where", type=str, default=None,
                        help="Custom WHERE clause (required for --source custom)")

    # Modes
    parser.add_argument("--enrich-missing", action="store_true",
                        help="Enrich hotels without real people")
    parser.add_argument("--enrich-all", action="store_true",
                        help="Re-enrich ALL hotels in source")
    parser.add_argument("--chain-fill", action="store_true",
                        help="Fill known chain execs into blank hotels")
    parser.add_argument("--fix-websites", action="store_true",
                        help="Find real websites via Serper for hotels with bad URLs")
    parser.add_argument("--audit", action="store_true",
                        help="Print enrichment summary stats")

    # Options
    parser.add_argument("--apply", action="store_true", help="Write to DB (default: dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="Max hotels to process")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N hotels (for resumability)")
    parser.add_argument("--concurrency", type=int, default=300, help="Browser concurrency")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    # Resolve source config
    if args.source == "custom":
        if not args.where:
            print("ERROR: --source custom requires --where")
            sys.exit(1)
        cfg = {
            "label": "Custom",
            "where": args.where,
            "join": None,
            "country": "AU",
            "serper_gl": "",
            "entity_search_suffix": "director OR owner OR team",
            "enable_abn": False,
            "chain_execs": {},
        }
    elif args.source in SOURCE_CONFIGS:
        cfg = SOURCE_CONFIGS[args.source]
    else:
        print(f"ERROR: Unknown source '{args.source}'. Available: {', '.join(SOURCE_CONFIGS.keys())}, custom")
        sys.exit(1)

    logger.info(f"Source: {cfg['label']} | Country: {cfg.get('country', '?')}")

    # Route to mode
    if args.audit:
        await audit(args, cfg)
    elif args.fix_websites:
        await fix_websites(args, cfg)
    elif args.chain_fill:
        conn = await asyncpg.connect(**DB_CONFIG)
        await chain_fill(conn, cfg, dry_run=not args.apply)
        await conn.close()
    elif args.enrich_missing:
        await enrich(args, cfg, all_hotels=False)
    elif args.enrich_all:
        await enrich(args, cfg, all_hotels=True)
    else:
        print("ERROR: Specify a mode: --enrich-missing, --enrich-all, --chain-fill, --fix-websites, or --audit")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
