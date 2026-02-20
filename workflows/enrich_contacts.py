"""Contact enrichment — find email/phone for known decision makers.

Enriches EXISTING hotel_decision_makers who have names but are missing
contact info (email, phone). Does NOT discover new people — that's enrich_owners.

Pipeline:
  1. Load DMs missing email/phone from hotel_decision_makers
  2. Email pattern guessing + O365/SMTP verification on hotel domain
  3. Serper search for person name + entity → crawl found pages → extract contacts
  4. Batch update DM records

Usage:
    # Enrich Big4 DMs missing contacts
    uv run python3 workflows/enrich_contacts.py --source big4 --apply --limit 50

    # Enrich RMS AU DMs
    uv run python3 workflows/enrich_contacts.py --source rms_au --apply

    # Audit contact coverage
    uv run python3 workflows/enrich_contacts.py --source big4 --audit

    # Ad-hoc
    uv run python3 workflows/enrich_contacts.py --source custom --where "h.city = 'Sydney'" --limit 10
"""

import argparse
import asyncio
import json
import os
import re
import sys
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
    """Read .env file manually."""
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


# ── Constants ────────────────────────────────────────────────────────────────

ENTITY_RE_STR = (
    r'(PTY|LTD|LIMITED|LLC|INC\b|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|'
    r'COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|'
    r'TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT|PROPRIETARY|COMPANY|'
    r'COMMISSION|FOUNDATION|TRADING|NOMINEES|SUPERANNUATION|ENTERPRISES)'
)

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

# Domains to skip in Serper results — social media, OTAs, registries, junk
SEARCH_SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "linkedin.com",
    "tiktok.com", "youtube.com", "pinterest.com",
    "booking.com", "tripadvisor.com", "wotif.com", "expedia.com",
    "agoda.com", "hotels.com", "trivago.com",
    "abr.business.gov.au", "abr.gov.au", "asic.gov.au",
    "rocketreach.co", "zoominfo.com", "apollo.io",
    "issuu.com", "scribd.com", "yumpu.com", "calameo.com",
    "researchgate.net", "academia.edu",
    "abc.net.au", "newbook.cloud",
}

# Personal email providers — accept matches from these even if not hotel domain
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "live.com",
    "icloud.com", "me.com", "protonmail.com", "proton.me",
    "bigpond.com", "optusnet.com.au", "westnet.com.au", "internode.on.net",
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
    entity_name: Optional[str] = None  # e.g. "XYZ Pty Ltd" if hotel has one
    # Results
    found_email: Optional[str] = None
    found_phone: Optional[str] = None
    email_verified: bool = False
    email_source: Optional[str] = None  # "o365", "smtp", "serper_crawl", "llm_extract"
    phone_source: Optional[str] = None


# ── Utilities ────────────────────────────────────────────────────────────────

def _get_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().removeprefix("www.")
    except Exception:
        return ""


def _clean_text_for_llm(md: str) -> str:
    """Strip crawl4ai markdown artifacts to reduce LLM token waste."""
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


# ── Step 1: Load DMs needing contacts ────────────────────────────────────────

async def load_dms_needing_contacts(conn, cfg: dict, need: str = "email") -> list[DMTarget]:
    """Load decision makers who have names but are missing contact info.

    Args:
        need: "email" (missing email), "phone" (missing phone), "both" (missing both)
    """
    jc = cfg.get("join") or ""
    wc = cfg["where"]

    if need == "email":
        contact_filter = "AND (dm.email IS NULL OR dm.email = '')"
    elif need == "phone":
        contact_filter = "AND (dm.phone IS NULL OR dm.phone = '')"
    else:  # both
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

    # Also load entity names for these hotels
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


# ── Step 2: Email pattern discovery ──────────────────────────────────────────

async def email_pattern_discovery(targets: list[DMTarget]) -> int:
    """Try email pattern guessing + O365/SMTP verification for each target."""
    from lib.owner_discovery.email_discovery import discover_emails

    # Group by domain to avoid duplicate DNS/MX lookups
    needs_email = [t for t in targets if not t.found_email and t.hotel_website]
    if not needs_email:
        return 0

    sem = asyncio.Semaphore(20)
    found_count = 0

    async def _try_one(t: DMTarget):
        nonlocal found_count
        domain = _get_domain(t.hotel_website)
        if not domain or '.' not in t.full_name.strip().replace('.', ''):
            return
        # Need at least first + last name
        parts = t.full_name.strip().split()
        if len(parts) < 2:
            return

        async with sem:
            results = await discover_emails(domain=domain, full_name=t.full_name)

        verified = [d for d in results if d['verified']]
        # Prefer personal (name-based) over role-based
        personal = [d for d in verified if not any(
            d['email'].lower().startswith(r + '@') for r in ['gm', 'owner', 'manager', 'director', 'management']
        )]

        if personal:
            t.found_email = personal[0]['email']
            t.email_verified = True
            t.email_source = personal[0]['method']
            found_count += 1
        elif verified:
            t.found_email = verified[0]['email']
            t.email_verified = True
            t.email_source = verified[0]['method']
            found_count += 1

    await asyncio.gather(*[_try_one(t) for t in needs_email])
    return found_count


# ── Step 3: Serper search + crawl ────────────────────────────────────────────

async def _serper_search(query: str, gl: str = "au", num: int = 5) -> list[dict]:
    """Search via Serper API. Returns list of {title, link, snippet}."""
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
            data = resp.json()
            return data.get("organic", [])
    except Exception as e:
        logger.debug(f"Serper error: {e}")
        return []


BINARY_EXT_RE = re.compile(
    r'(?i)\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|jpg|jpeg|png|gif|svg|mp4|mp3|wav|webp|ico)(\?|$)'
)


def _filter_serper_results(results: list[dict]) -> list[str]:
    """Filter Serper results to useful URLs, skipping social/OTA/registry sites."""
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
        # Catch TripAdvisor country variants (tripadvisor.ie, tripadvisor.co.nz, etc.)
        if 'tripadvisor' in domain:
            continue
        urls.append(link)
    return urls


async def serper_search_contacts(targets: list[DMTarget], cfg: dict, concurrency: int = 10) -> int:
    """Search for contact info via Serper + crawl found pages.

    For each person still missing email, searches:
      "{full_name} {hotel_name} email contact"
    Then crawls the top results and extracts emails/phones.
    """
    needs = [t for t in targets if not t.found_email]
    if not needs:
        return 0

    gl = cfg.get("serper_gl", "au")
    country = cfg.get("country", "AU")
    phone_re = PHONE_PATTERNS.get(country, PHONE_PATTERNS["DEFAULT"])

    # Step 3a: Serper search for each person
    logger.info(f"Serper: searching for {len(needs)} people...")
    search_sem = asyncio.Semaphore(5)  # Don't hammer Serper
    url_map: dict[str, list[DMTarget]] = {}  # url -> targets that need it

    async def _search_one(t: DMTarget):
        # Build query: "John Smith Big4 Riverside email contact"
        entity = t.entity_name or t.hotel_name
        query = f'"{t.full_name}" {entity} email contact'
        async with search_sem:
            results = await _serper_search(query, gl=gl)

        # Check snippets for emails first (saves crawling)
        name_parts = t.full_name.lower().split()
        first = name_parts[0] if name_parts else ""
        last = name_parts[-1] if len(name_parts) > 1 else ""
        hotel_domain = _get_domain(t.hotel_website) if t.hotel_website else ""
        for r in results:
            snippet = r.get("snippet", "")
            emails = EMAIL_RE.findall(snippet)
            for e in emails:
                el = e.split('@')[0].lower()
                email_domain = e.split('@')[1].lower() if '@' in e else ""
                # Require both first AND last name parts (e.g. john.smith@, jsmith@)
                if not (first and last and (
                    (first in el and last in el) or
                    (first[0] in el and last in el and el.index(first[0]) < el.index(last))
                )):
                    continue
                # Domain must be related: hotel domain, personal provider, or entity domain
                if hotel_domain and (email_domain == hotel_domain or email_domain.endswith('.' + hotel_domain)):
                    pass  # hotel domain — always accept
                elif email_domain in PERSONAL_EMAIL_DOMAINS:
                    pass  # personal email — accept
                else:
                    logger.debug(f"  Skipping unrelated email: {t.full_name} -> {e} (domain {email_domain})")
                    continue
                t.found_email = e
                t.email_source = "serper_snippet"
                logger.debug(f"  Found email in snippet: {t.full_name} -> {e}")
                return

        # Collect URLs to crawl
        urls = _filter_serper_results(results)
        for url in urls[:3]:  # top 3 per person
            url_map.setdefault(url, []).append(t)

    await asyncio.gather(*[_search_one(t) for t in needs])

    # How many still need crawling?
    still_needs = [t for t in needs if not t.found_email]
    urls_to_crawl = [url for url, targets in url_map.items()
                     if any(not t.found_email for t in targets)]

    if not urls_to_crawl:
        snippet_found = sum(1 for t in needs if t.found_email)
        return snippet_found

    # Step 3b: Crawl found pages
    logger.info(f"Crawling {len(urls_to_crawl)} pages from Serper results...")
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
            dispatcher=SemaphoreDispatcher(max_session_permit=min(concurrency, len(urls_to_crawl))),
        )

    # Step 3c: Extract contacts from crawled pages
    found_count = 0
    for cr in crawl_results:
        if not cr.success:
            continue
        md = (cr.markdown.raw_markdown if hasattr(cr.markdown, 'raw_markdown') else cr.markdown) or ""
        if len(md) < 50:
            continue

        page_emails = EMAIL_RE.findall(md)
        page_phones = phone_re.findall(md)

        # Match emails to targets that triggered this URL
        page_targets = url_map.get(cr.url, [])
        for t in page_targets:
            if t.found_email:
                continue
            # Look for email matching the person's name (require first+last)
            name_parts = t.full_name.lower().split()
            first = name_parts[0] if name_parts else ""
            last = name_parts[-1] if len(name_parts) > 1 else ""
            hotel_domain = _get_domain(t.hotel_website) if t.hotel_website else ""
            for e in page_emails:
                el = e.split('@')[0].lower()
                email_domain = e.split('@')[1].lower() if '@' in e else ""
                if not (first and last and (
                    (first in el and last in el) or
                    (first[0] in el and last in el and el.index(first[0]) < el.index(last))
                )):
                    continue
                # Domain relevance check
                is_related = (
                    (hotel_domain and (email_domain == hotel_domain or email_domain.endswith('.' + hotel_domain)))
                    or email_domain in PERSONAL_EMAIL_DOMAINS
                )
                if not is_related:
                    continue
                t.found_email = e
                t.email_verified = False
                t.email_source = "serper_crawl"
                found_count += 1
                logger.debug(f"  Found email on page: {t.full_name} -> {e}")
                break

            if not t.found_phone and page_phones:
                t.found_phone = page_phones[0]
                t.phone_source = "serper_crawl"

    # Step 3d: LLM extraction for pages that had text but no pattern match
    # Group remaining targets by crawl URL for LLM batch
    llm_targets = []
    for cr in crawl_results:
        if not cr.success:
            continue
        md = (cr.markdown.raw_markdown if hasattr(cr.markdown, 'raw_markdown') else cr.markdown) or ""
        if len(md) < 100:
            continue
        page_targets = [t for t in url_map.get(cr.url, []) if not t.found_email]
        if page_targets:
            llm_targets.append((cr.url, md, page_targets))

    if llm_targets:
        logger.info(f"LLM extracting contacts from {len(llm_targets)} pages...")
        sem = asyncio.Semaphore(10)

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
                        prefix = email.split('@')[0].lower()
                        # Reject generic emails (info@, admin@, etc.)
                        generic = {'info', 'admin', 'contact', 'hello', 'enquiries',
                                   'bookings', 'reservations', 'reception', 'sales',
                                   'support', 'help', 'noreply', 'no-reply', 'stay'}
                        if prefix not in generic:
                            t.found_email = email
                            t.email_verified = False
                            t.email_source = "llm_extract"
                    phone = match.get('phone')
                    if phone and phone != 'null' and not t.found_phone:
                        t.found_phone = phone
                        t.phone_source = "llm_extract"

        await asyncio.gather(*[_llm_extract(url, md, pts) for url, md, pts in llm_targets])

    total_found = sum(1 for t in needs if t.found_email)
    return total_found


# ── LLM extraction ───────────────────────────────────────────────────────────

_bedrock_client = None

def _get_bedrock():
    global _bedrock_client
    if _bedrock_client is None:
        import boto3
        _bedrock_client = boto3.client('bedrock-runtime', region_name=AWS_REGION)
    return _bedrock_client


async def llm_extract_contacts(text: str, names: list[str]) -> dict:
    """Use LLM to extract contact info for specific people from page text.

    Returns dict: {name_lower: {"email": ..., "phone": ...}}
    """
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
    """Batch update DMs with found contact info."""
    updates = [t for t in targets if t.found_email or t.found_phone]
    if not updates:
        print("No contact info found to update.")
        return

    dm_ids = [t.dm_id for t in updates]
    emails = [t.found_email for t in updates]
    phones = [t.found_phone for t in updates]
    verified = [t.email_verified for t in updates]

    if dry_run:
        print(f"\nDRY RUN — would update {len(updates)} DMs. Run with --apply to write.")
        return

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
    print(f"\nAPPLIED: Updated {len(updates)} DMs in database.")


# ── Main enrichment flow ─────────────────────────────────────────────────────

async def enrich(args, cfg: dict):
    """Main enrichment pipeline: email discovery → serper search → update."""
    conn = await asyncpg.connect(**DB_CONFIG)
    label = cfg["label"]

    # Step 1: Load targets
    targets = await load_dms_needing_contacts(conn, cfg, need="email")
    logger.info(f"Found {len(targets)} {label} DMs missing email")

    if not targets:
        print(f"All {label} DMs have email addresses!")
        await conn.close()
        return

    # Apply offset/limit
    if args.offset and args.offset < len(targets):
        targets = targets[args.offset:]
        logger.info(f"Skipping first {args.offset} DMs (--offset)")
    if args.limit and args.limit < len(targets):
        targets = targets[:args.limit]
        logger.info(f"Limited to {len(targets)} DMs (--limit)")

    # Step 2: Email pattern guessing + verification
    logger.info(f"Step 1: Email pattern discovery for {len(targets)} people...")
    email_found = await email_pattern_discovery(targets)
    logger.info(f"Step 1 done: {email_found}/{len(targets)} found via email patterns")

    # Step 3: Serper search for remaining
    still_need = sum(1 for t in targets if not t.found_email)
    if still_need > 0 and SERPER_API_KEY:
        logger.info(f"Step 2: Serper search for {still_need} remaining people...")
        serper_found = await serper_search_contacts(targets, cfg, concurrency=args.concurrency)
        logger.info(f"Step 2 done: {serper_found} additional found via Serper")
    elif still_need > 0:
        logger.warning("No SERPER_API_KEY — skipping web search step")

    # Results summary
    total_email = sum(1 for t in targets if t.found_email)
    total_phone = sum(1 for t in targets if t.found_phone)
    verified = sum(1 for t in targets if t.email_verified)

    print(f"\n{'='*60}")
    print(f"{label.upper()} CONTACT ENRICHMENT RESULTS")
    print(f"{'='*60}")
    print(f"DMs targeted:        {len(targets)}")
    print(f"Emails found:        {total_email} ({verified} verified)")
    print(f"Phones found:        {total_phone}")
    print(f"Still missing email: {len(targets) - total_email}")

    # Show by source
    sources = {}
    for t in targets:
        if t.email_source:
            sources[t.email_source] = sources.get(t.email_source, 0) + 1
    if sources:
        print(f"\nEmail sources:")
        for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
            print(f"  {src:<25} {cnt:>5}")

    # Show found contacts
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

    # Apply
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

    # Show sample of people missing email
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
                        help="Custom WHERE clause (required for --source custom)")

    # Modes
    parser.add_argument("--enrich", action="store_true", default=True,
                        help="Enrich DMs missing contacts (default)")
    parser.add_argument("--audit", action="store_true",
                        help="Print contact coverage stats")

    # Options
    parser.add_argument("--apply", action="store_true", help="Write to DB (default: dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="Max DMs to process")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N DMs (for resumability)")
    parser.add_argument("--concurrency", type=int, default=10, help="Browser concurrency for crawling")
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
            "serper_gl": "au",
        }
    elif args.source in SOURCE_CONFIGS:
        cfg = SOURCE_CONFIGS[args.source]
    else:
        print(f"ERROR: Unknown source '{args.source}'. Available: {', '.join(SOURCE_CONFIGS.keys())}, custom")
        sys.exit(1)

    logger.info(f"Source: {cfg['label']} | Country: {cfg.get('country', '?')}")

    if args.audit:
        await audit(args, cfg)
    else:
        await enrich(args, cfg)


if __name__ == "__main__":
    asyncio.run(main())
