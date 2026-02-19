"""Crawl Big4 park external websites to find owner/manager names and missing contact info.

Uses crawl4ai with arun_many() for fast concurrent crawling + Azure OpenAI LLM extraction.

Usage:
    uv run python3 scripts/crawl_big4_parks.py --limit 10
    uv run python3 scripts/crawl_big4_parks.py --all
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

# DB config
DB_CONFIG = dict(
    host='aws-1-ap-southeast-1.pooler.supabase.com',
    port=6543, database='postgres',
    user='postgres.yunairadgmaqesxejqap',
    password='SadieGTM321-',
    statement_cache_size=0,
)

# Azure OpenAI - read .env manually (Python 3.14 dotenv bug)
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
AZURE_KEY = _ENV.get('AZURE_OPENAI_API_KEY', '')
AZURE_ENDPOINT = _ENV.get('AZURE_OPENAI_ENDPOINT', '').rstrip('/')
AZURE_VERSION = _ENV.get('AZURE_OPENAI_API_VERSION', '2024-12-01-preview')
AZURE_DEPLOY = 'gpt-35-turbo'  # actual model: gpt-4.1-mini

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


async def get_big4_hotels(conn) -> list[ParkInfo]:
    rows = await conn.fetch("""
        SELECT DISTINCT ON (LOWER(TRIM(h.name)))
            h.id, h.name, h.phone_google, h.phone_website, h.email,
            h.website, h.city, h.state
        FROM sadie_gtm.hotels h
        WHERE (h.external_id_type = 'big4' OR h.source LIKE '%::big4%')
          AND h.website IS NOT NULL AND h.website != ''
          AND h.website NOT ILIKE '%big4.com.au%'
          AND h.name NOT ILIKE '%demo%'
          AND h.name NOT ILIKE '%datawarehouse%'
        ORDER BY LOWER(TRIM(h.name)), h.phone_website DESC NULLS LAST
    """)
    parks = []
    for r in rows:
        parks.append(ParkInfo(
            hotel_id=r['id'], hotel_name=r['name'],
            website=r['website'],
            existing_phones=[p for p in [r.get('phone_google'), r.get('phone_website')] if p],
            existing_email=r.get('email'),
        ))
    return parks


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

Return JSON array: [{{"name": "First Last", "title": "Role"}}]
If no full names found, return: []"""

    url = f"{AZURE_ENDPOINT}/openai/deployments/{AZURE_DEPLOY}/chat/completions?api-version={AZURE_VERSION}"
    try:
        resp = await client.post(url,
            headers={"api-key": AZURE_KEY, "Content-Type": "application/json"},
            json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 300, "temperature": 0.1},
            timeout=20.0)
        if resp.status_code == 429:
            await asyncio.sleep(3)
            return []
        if resp.status_code != 200:
            return []
        content = resp.json()["choices"][0]["message"]["content"].strip()
        data = json.loads(content)
        valid = []
        for d in (data if isinstance(data, list) else [data]):
            name = (d.get("name") or "").strip()
            if not name or " " not in name:
                continue  # Must have first + last name
            # Reject business/place patterns
            if re.search(r'(?i)(pty|ltd|trust|holiday|park|resort|caravan|tourism)', name):
                continue
            d["name"] = name
            valid.append(d)
        return valid
    except Exception as e:
        logger.debug(f"LLM error: {e}")
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
        delay_before_return_html=1.5,
        page_timeout=20000,
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


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--hotel-ids", type=str, help="Comma-separated hotel IDs to crawl")
    parser.add_argument("--no-llm", action="store_true")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--output", type=str, default="/tmp/big4_crawl_results.json", help="JSON output file")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    conn = await asyncpg.connect(**DB_CONFIG)
    parks = await get_big4_hotels(conn)
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
