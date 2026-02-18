"""Crawl entity websites to find real people behind PTY LTD/trust entities.

For each entity DM (e.g. "SULTAN HOLDINGS PTY LTD"), searches Serper for
the entity name, fetches top result pages with httpx (no browser), and
uses Azure OpenAI to extract person names, titles, emails, and phones.

Usage:
    uv run python3 scripts/crawl_entity_websites.py --dry-run
    uv run python3 scripts/crawl_entity_websites.py --apply
    uv run python3 scripts/crawl_entity_websites.py --limit 5 -v
"""

import argparse
import asyncio
import json
import re
import sys
from dataclasses import dataclass, field
from urllib.parse import urlparse

import asyncpg
import httpx
from loguru import logger

# ── DB ──────────────────────────────────────────────────────────────────────
DB_CONFIG = dict(
    host="aws-1-ap-southeast-1.pooler.supabase.com",
    port=6543,
    database="postgres",
    user="postgres.yunairadgmaqesxejqap",
    password="SadieGTM321-",
    statement_cache_size=0,
)

# ── Azure OpenAI ────────────────────────────────────────────────────────────
def _read_env():
    env = {}
    try:
        with open("/Users/administrator/projects/sadie_gtm_owner_enrichment/.env") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

_ENV = _read_env()
AZURE_KEY = _ENV.get("AZURE_OPENAI_API_KEY", "")
AZURE_ENDPOINT = _ENV.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
AZURE_VERSION = _ENV.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
AZURE_DEPLOY = "gpt-35-turbo"
SERPER_API_KEY = _ENV.get("SERPER_API_KEY", "")

# ── Entity regex ────────────────────────────────────────────────────────────
ENTITY_RE = re.compile(
    r"(?i)(PTY|LTD|LIMITED|LLC|INC|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|"
    r"COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|"
    r"TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT)"
)

# Skip these domains in search results
SKIP_DOMAINS = frozenset({
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com",
    "booking.com", "expedia.com", "tripadvisor.com", "hotels.com",
    "agoda.com", "airbnb.com", "vrbo.com", "kayak.com",
    "wotif.com", "google.com", "maps.google.com",
    # Gov/registry pages — no people info
    "abn.business.gov.au", "abr.business.gov.au",
    "asic.gov.au", "connectonline.asic.gov.au",
    "bizly.com.au", "abnlookup.com", "companieslist.com",
    "opencorporates.com", "yellowpages.com.au",
    "whitepages.com.au", "truelocal.com.au",
})

# Name validation
BUSINESS_WORDS = re.compile(
    r"(?i)(pty|ltd|limited|trust|holiday|park|resort|caravan|camping|"
    r"tourism|motel|inc|corp|holdings|assets|management|pty\.|"
    r"beach|river|creek|mountain|lake|bay|island|point|head)"
)

BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# ── Data models ─────────────────────────────────────────────────────────────
@dataclass
class Entity:
    name: str
    hotel_ids: list[int] = field(default_factory=list)
    hotel_names: list[str] = field(default_factory=list)


@dataclass
class PersonFound:
    name: str
    title: str = ""
    email: str = ""
    phone: str = ""
    source_url: str = ""


@dataclass
class EntityResult:
    entity: Entity
    urls_found: list[str] = field(default_factory=list)
    pages_fetched: int = 0
    people: list[PersonFound] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ── Step 1: Load entities ───────────────────────────────────────────────────
async def load_entities(conn) -> list[Entity]:
    rows = await conn.fetch("""
        SELECT dm.full_name, dm.hotel_id, h.name as hotel_name
        FROM sadie_gtm.hotel_decision_makers dm
        JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
        WHERE (h.name ILIKE '%%big4%%' OR h.name ILIKE '%%big 4%%')
        AND (dm.full_name ~* '(PTY|LTD|LIMITED|LLC|INC|TRUST|TRUSTEE|HOLDINGS|ASSOCIATION|CORP|COUNCIL|MANAGEMENT|ASSETS|VILLAGES|HOLIDAY|CARAVAN|PARKS|RESORT|TOURISM|TOURIST|NRMA|RAC |MOTEL|RETREAT)')
    """)

    # Group by entity name
    entity_map: dict[str, Entity] = {}
    for r in rows:
        key = r["full_name"].strip().upper()
        if key not in entity_map:
            entity_map[key] = Entity(name=r["full_name"].strip())
        e = entity_map[key]
        if r["hotel_id"] not in e.hotel_ids:
            e.hotel_ids.append(r["hotel_id"])
            e.hotel_names.append(r["hotel_name"])

    return list(entity_map.values())


# ── Step 2: Serper search ───────────────────────────────────────────────────
def _extract_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


@dataclass
class SearchResult:
    url: str
    title: str = ""
    snippet: str = ""


async def serper_search(
    client: httpx.AsyncClient,
    entity_name: str,
    sem: asyncio.Semaphore,
) -> list[SearchResult]:
    """Search Serper for entity, return top results with URLs + snippets."""
    if not SERPER_API_KEY:
        return []

    # Clean entity name for better search
    clean_name = re.sub(r"(?i)\b(pty\.?|ltd\.?|limited|the trustee for|the)\b", "", entity_name).strip()
    clean_name = re.sub(r"\s+", " ", clean_name).strip()
    query = f'"{clean_name}" Australia director OR owner OR manager'

    async with sem:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 10},
                timeout=15.0,
            )
            if resp.status_code != 200:
                logger.debug(f"Serper {resp.status_code} for {entity_name!r}")
                return []

            data = resp.json()
            results = []
            for item in data.get("organic", []):
                url = item.get("link", "")
                domain = _extract_domain(url)
                if domain and domain not in SKIP_DOMAINS:
                    results.append(SearchResult(
                        url=url,
                        title=item.get("title", ""),
                        snippet=item.get("snippet", ""),
                    ))
                if len(results) >= 5:
                    break
            return results
        except Exception as e:
            logger.debug(f"Serper error for {entity_name!r}: {e}")
            return []


# ── Step 3: Fetch pages ─────────────────────────────────────────────────────
def _html_to_text(html: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def fetch_page(
    client: httpx.AsyncClient,
    url: str,
    sem: asyncio.Semaphore,
) -> str:
    """Fetch page and return text content."""
    # Skip PDFs and other binary files
    if re.search(r"\.(pdf|doc|docx|xls|xlsx|zip|png|jpg|gif)(\?|$)", url, re.IGNORECASE):
        return ""

    async with sem:
        try:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": BROWSER_UA,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                },
                timeout=10.0,
                follow_redirects=True,
            )
            if resp.status_code == 200:
                ct = resp.headers.get("content-type", "")
                if "text/html" not in ct and "text/plain" not in ct:
                    return ""
                text = _html_to_text(resp.text)
                # Cap text to avoid sending huge pages to LLM
                return text[:15000]
        except Exception as e:
            logger.debug(f"Fetch error {url}: {e}")
    return ""


# ── Step 4: LLM extraction ──────────────────────────────────────────────────
async def llm_extract_people(
    client: httpx.AsyncClient,
    entity_name: str,
    page_text: str,
    sem: asyncio.Semaphore,
) -> list[PersonFound]:
    """Extract people from page text using Azure OpenAI."""
    if not AZURE_KEY or not page_text:
        return []

    logger.debug(f"LLM input for {entity_name!r}: {len(page_text)} chars, first 200: {page_text[:200]!r}")

    prompt = f"""Extract ALL person names who are directors, owners, managers, or key people of this company.
Company: {entity_name}

Text:
{page_text[:6000]}

Rules:
- Every name MUST include both first AND last name (e.g. "John Smith", not just "John")
- Extract their title/role (Director, CEO, Owner, Manager, etc.)
- Extract email and phone if visible in the text
- Do NOT include company names, trust names, or business entities
- Do NOT include place names or generic roles without a person name
- If you find multiple people, include ALL of them

Return JSON array: [{{"name": "First Last", "title": "Director", "email": "email@example.com", "phone": "+61..."}}]
If no people found, return: []"""

    url = f"{AZURE_ENDPOINT}/openai/deployments/{AZURE_DEPLOY}/chat/completions?api-version={AZURE_VERSION}"

    async with sem:
        try:
            resp = await client.post(
                url,
                headers={"api-key": AZURE_KEY, "Content-Type": "application/json"},
                json={
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                    "temperature": 0.1,
                },
                timeout=25.0,
            )
            if resp.status_code == 429:
                await asyncio.sleep(3)
                return []
            if resp.status_code != 200:
                return []

            content = resp.json()["choices"][0]["message"]["content"].strip()
            logger.debug(f"LLM raw response for {entity_name!r}: {content[:200]}")
            # Handle markdown-wrapped JSON
            if content.startswith("```"):
                content = re.sub(r"^```(?:json)?\s*", "", content)
                content = re.sub(r"\s*```$", "", content)

            data = json.loads(content)
            people = []
            for d in (data if isinstance(data, list) else [data]):
                name = (d.get("name") or "").strip()
                if not name or " " not in name:
                    logger.debug(f"  Skip no-surname: {name!r}")
                    continue
                if BUSINESS_WORDS.search(name):
                    logger.debug(f"  Skip business word: {name!r}")
                    continue
                people.append(PersonFound(
                    name=name,
                    title=(d.get("title") or "").strip(),
                    email=(d.get("email") or "").strip(),
                    phone=(d.get("phone") or "").strip(),
                ))
            return people
        except Exception as e:
            logger.debug(f"LLM error for {entity_name!r}: {e}")
            return []


# ── Pipeline ─────────────────────────────────────────────────────────────────
async def process_entities(entities: list[Entity]) -> list[EntityResult]:
    """Process all entities: search -> fetch -> extract, max concurrency."""

    serper_sem = asyncio.Semaphore(20)
    fetch_sem = asyncio.Semaphore(50)
    llm_sem = asyncio.Semaphore(10)

    results = [EntityResult(entity=e) for e in entities]

    async with httpx.AsyncClient() as client:
        # Stage 1: Serper search all entities concurrently
        logger.info(f"Stage 1: Searching {len(entities)} entities via Serper...")
        search_tasks = [
            serper_search(client, e.name, serper_sem)
            for e in entities
        ]
        search_results = await asyncio.gather(*search_tasks)

        # Store search results and collect snippet text
        search_data: dict[int, list[SearchResult]] = {}
        for i, (r, sr_list) in enumerate(zip(results, search_results)):
            r.urls_found = [sr.url for sr in sr_list]
            search_data[i] = sr_list

        total_urls = sum(len(r.urls_found) for r in results)
        with_urls = sum(1 for r in results if r.urls_found)
        logger.info(f"Stage 1 done: {total_urls} URLs across {with_urls} entities")

        # Stage 2: Fetch all pages concurrently
        logger.info(f"Stage 2: Fetching {total_urls} pages...")
        fetch_jobs = []
        for i, sr_list in search_data.items():
            for sr in sr_list:
                fetch_jobs.append((i, sr.url))

        fetch_tasks = [
            fetch_page(client, url, fetch_sem)
            for _, url in fetch_jobs
        ]
        fetch_results = await asyncio.gather(*fetch_tasks)

        # Group texts by entity — include both page text AND snippet text
        entity_texts: dict[int, list[tuple[str, str]]] = {}  # idx -> [(url, text)]
        for (idx, url), text in zip(fetch_jobs, fetch_results):
            if text and len(text) > 100:
                results[idx].pages_fetched += 1
                entity_texts.setdefault(idx, []).append((url, text))

        # Also add snippet text from search results (free info, no fetch needed)
        for idx, sr_list in search_data.items():
            snippet_text = "\n".join(
                f"{sr.title}: {sr.snippet}" for sr in sr_list if sr.snippet
            )
            if snippet_text:
                entity_texts.setdefault(idx, []).append(("serper_snippets", snippet_text))

        fetched = sum(r.pages_fetched for r in results)
        logger.info(f"Stage 2 done: {fetched} pages fetched successfully")

        # Stage 3: LLM extraction concurrently
        llm_jobs = []
        for idx in range(len(results)):
            texts = entity_texts.get(idx, [])
            if not texts:
                continue
            combined = "\n---\n".join(t for _, t in texts)
            source_url = next((url for url, _ in texts if url != "serper_snippets"), "")
            llm_jobs.append((idx, results[idx].entity.name, combined, source_url))

        logger.info(f"Stage 3: LLM extracting from {len(llm_jobs)} entities...")
        llm_tasks = [
            llm_extract_people(client, name, text, llm_sem)
            for _, name, text, _ in llm_jobs
        ]
        llm_results = await asyncio.gather(*llm_tasks)

        for (idx, _, _, source_url), people in zip(llm_jobs, llm_results):
            for p in people:
                p.source_url = source_url
            results[idx].people = people

        total_people = sum(len(r.people) for r in results)
        with_people = sum(1 for r in results if r.people)
        logger.info(f"Stage 3 done: {total_people} people found across {with_people} entities")

    return results


# ── Insert ───────────────────────────────────────────────────────────────────
async def insert_results(conn, results: list[EntityResult], dry_run: bool = True):
    """Insert found people as decision makers."""
    inserted = 0
    skipped = 0

    for r in results:
        if not r.people:
            continue

        for person in r.people:
            # Validate name
            name = person.name.strip()
            if not name or " " not in name or len(name) < 4:
                continue
            if BUSINESS_WORDS.search(name):
                continue

            for hotel_id in r.entity.hotel_ids:
                # Check existing
                existing = await conn.fetchval(
                    "SELECT id FROM sadie_gtm.hotel_decision_makers "
                    "WHERE hotel_id = $1 AND LOWER(full_name) = LOWER($2)",
                    hotel_id, name,
                )
                if existing:
                    skipped += 1
                    continue

                if not dry_run:
                    await conn.execute(
                        """INSERT INTO sadie_gtm.hotel_decision_makers
                           (hotel_id, full_name, title, email, phone, sources, confidence, raw_source_url)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
                        hotel_id,
                        name,
                        person.title or "Director",
                        person.email or None,
                        person.phone or None,
                        ["entity_website_crawl"],
                        0.70,
                        person.source_url or None,
                    )
                logger.info(
                    f"  + {name} ({person.title}) -> {r.entity.hotel_names[r.entity.hotel_ids.index(hotel_id)]}"
                    f"  [entity: {r.entity.name[:40]}]"
                )
                inserted += 1

    return inserted, skipped


# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Crawl entity websites to find real people")
    parser.add_argument("--apply", action="store_true", help="Write results to DB")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--limit", type=int, default=0, help="Limit entities to process")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if not args.apply and not args.dry_run:
        print("Must specify --dry-run or --apply")
        sys.exit(1)

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    if not SERPER_API_KEY:
        print("ERROR: SERPER_API_KEY not found in .env")
        sys.exit(1)
    if not AZURE_KEY:
        print("ERROR: AZURE_OPENAI_API_KEY not found in .env")
        sys.exit(1)

    # Load entities
    conn = await asyncpg.connect(**DB_CONFIG)
    entities = await load_entities(conn)
    logger.info(f"Loaded {len(entities)} unique entities across {sum(len(e.hotel_ids) for e in entities)} hotel links")

    if args.limit:
        entities = entities[:args.limit]
        logger.info(f"Limited to {len(entities)} entities")

    # Process
    results = await process_entities(entities)

    # Print summary
    print(f"\n{'='*70}")
    print(f"RESULTS: {len(results)} entities processed")
    print(f"{'='*70}")
    print(f"Entities with search results: {sum(1 for r in results if r.urls_found)}")
    print(f"Entities with pages fetched:  {sum(1 for r in results if r.pages_fetched)}")
    print(f"Entities with people found:   {sum(1 for r in results if r.people)}")
    print(f"Total people extracted:       {sum(len(r.people) for r in results)}")

    for r in results:
        if r.people:
            names = ", ".join(f"{p.name} ({p.title})" for p in r.people)
            print(f"  {r.entity.name[:50]:<50} -> {names}")

    # Insert
    inserted, skipped = await insert_results(conn, results, dry_run=not args.apply)
    await conn.close()

    print(f"\n{'DRY RUN' if not args.apply else 'APPLIED'}")
    print(f"Inserted: {inserted}")
    print(f"Skipped (already exist): {skipped}")

    if not args.apply:
        print("\nRun with --apply to write to DB")


if __name__ == "__main__":
    asyncio.run(main())
