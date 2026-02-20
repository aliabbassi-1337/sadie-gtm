"""Enrich hotel website URLs via Serper search.

Usage:
    # By hotel IDs
    uv run python3 workflows/enrich_websites.py --hotel-ids 123,456,789
    uv run python3 workflows/enrich_websites.py --hotel-ids 123,456,789 --apply

    # By source (hotels missing websites)
    uv run python3 workflows/enrich_websites.py --source rms_au --blanks-only --limit 50
    uv run python3 workflows/enrich_websites.py --source rms_au --blanks-only --limit 50 --apply

    # Hotels with bad website URLs (OTAs, booking engines, etc.)
    uv run python3 workflows/enrich_websites.py --source rms_au --bad-only --limit 50 --apply
"""

import argparse
import asyncio
import os
import re
import sys
from urllib.parse import urlparse

import asyncpg
import httpx
from loguru import logger


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


# ── Source configs ───────────────────────────────────────────────────────────

SOURCE_CONFIGS = {
    "big4": {
        "label": "Big4 Holiday Parks",
        "where": "(h.external_id_type = 'big4' OR h.source LIKE '%::big4%')",
        "join": None,
        "serper_gl": "au",
    },
    "rms_au": {
        "label": "RMS Cloud Australia",
        "where": "hbe.booking_engine_id = 12 AND h.country IN ('Australia', 'AU') AND h.status = 1",
        "join": "JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id",
        "serper_gl": "au",
    },
}

# Domains that aren't real hotel websites
BAD_DOMAINS = {
    "booking.com", "expedia.com", "tripadvisor.com", "hotels.com",
    "agoda.com", "airbnb.com", "vrbo.com", "kayak.com", "wotif.com",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com", "linkedin.com",
    "google.com", "maps.google.com",
    "visitvictoria.com", "visitnsw.com", "visitgippsland.com.au",
    "discovertasmania.com.au", "southaustralia.com", "visitqueensland.com",
    "big4.com.au", "rmscloud.com",
    "abn.business.gov.au", "abr.business.gov.au", "asic.gov.au",
    "yellowpages.com.au", "whitepages.com.au", "truelocal.com.au",
    "zoominfo.com", "dnb.com", "opencorporates.com",
}


def _get_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host.lower().removeprefix("www.")
    except Exception:
        return ""


def _is_bad_url(url: str) -> bool:
    if not url:
        return True
    domain = _get_domain(url)
    return any(domain.endswith(bad) for bad in BAD_DOMAINS)


# ── Serper search ────────────────────────────────────────────────────────────

async def search_website(client: httpx.AsyncClient, hotel_name: str, city: str | None,
                         state: str | None, sem: asyncio.Semaphore, gl: str = "au") -> list[dict]:
    """Search Serper for a hotel's website. Returns top candidates."""
    if not SERPER_API_KEY:
        logger.error("SERPER_API_KEY not set")
        return []

    clean = re.sub(r'\s*-\s*[A-Z][a-z]+.*$', '', hotel_name).strip()
    clean = re.sub(r'(?i)\b(historical data only|test|demo)\b', '', clean).strip()
    if not clean or len(clean) < 3:
        return []

    location = " ".join(filter(None, [city, state]))
    query = f'{clean} {location} official website'.strip()

    async with sem:
        try:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 5, "gl": gl},
                timeout=10.0,
            )
            if resp.status_code != 200:
                logger.warning(f"Serper {resp.status_code} for: {query}")
                return []

            candidates = []
            for item in resp.json().get("organic", []):
                url = item.get("link", "")
                title = item.get("title", "")
                domain = _get_domain(url)
                if domain and not _is_bad_url(url):
                    candidates.append({"url": url, "title": title, "domain": domain})
                if len(candidates) >= 3:
                    break
            return candidates
        except Exception as e:
            logger.warning(f"Serper error for {hotel_name}: {e}")
            return []


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Enrich hotel website URLs via Serper")
    parser.add_argument("--hotel-ids", type=str, help="Comma-separated hotel IDs")
    parser.add_argument("--source", type=str, help=f"Source: {', '.join(SOURCE_CONFIGS.keys())}")
    parser.add_argument("--blanks-only", action="store_true", help="Only hotels with no website")
    parser.add_argument("--bad-only", action="store_true", help="Only hotels with bad/OTA website URLs")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--apply", action="store_true", help="Write to DB (default: dry-run)")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "INFO")

    if not args.hotel_ids and not args.source:
        print("ERROR: Specify --hotel-ids or --source")
        sys.exit(1)

    conn = await asyncpg.connect(**DB_CONFIG)

    # Load hotels
    if args.hotel_ids:
        ids = [int(x.strip()) for x in args.hotel_ids.split(",")]
        rows = await conn.fetch(
            "SELECT h.id, h.name, h.website, h.city, h.state"
            " FROM sadie_gtm.hotels h"
            " WHERE h.id = ANY($1)"
            " ORDER BY h.name",
            ids,
        )
    else:
        cfg = SOURCE_CONFIGS.get(args.source)
        if not cfg:
            print(f"ERROR: Unknown source '{args.source}'. Available: {', '.join(SOURCE_CONFIGS.keys())}")
            sys.exit(1)
        jc = cfg.get("join") or ""
        wc = cfg["where"]

        if args.blanks_only:
            rows = await conn.fetch(
                f"SELECT h.id, h.name, h.website, h.city, h.state"
                f" FROM sadie_gtm.hotels h {jc}"
                f" WHERE ({wc})"
                f"  AND (h.website IS NULL OR h.website = '')"
                f"  AND h.name NOT ILIKE '%demo%'"
                f"  AND h.name NOT ILIKE '%test%'"
                f"  AND h.name NOT ILIKE '%HISTORICAL%'"
                f" ORDER BY h.name"
            )
        elif args.bad_only:
            rows = await conn.fetch(
                f"SELECT h.id, h.name, h.website, h.city, h.state"
                f" FROM sadie_gtm.hotels h {jc}"
                f" WHERE ({wc})"
                f"  AND h.website IS NOT NULL AND h.website != ''"
                f"  AND h.name NOT ILIKE '%demo%'"
                f"  AND h.name NOT ILIKE '%test%'"
                f"  AND h.name NOT ILIKE '%HISTORICAL%'"
                f" ORDER BY h.name"
            )
            rows = [r for r in rows if _is_bad_url(r['website'])]
        else:
            rows = await conn.fetch(
                f"SELECT h.id, h.name, h.website, h.city, h.state"
                f" FROM sadie_gtm.hotels h {jc}"
                f" WHERE ({wc})"
                f"  AND h.name NOT ILIKE '%demo%'"
                f"  AND h.name NOT ILIKE '%test%'"
                f"  AND h.name NOT ILIKE '%HISTORICAL%'"
                f" ORDER BY h.name"
            )

    if args.limit:
        rows = rows[:args.limit]

    if not rows:
        print("No hotels found.")
        await conn.close()
        return

    hotels = []
    for r in rows:
        hotels.append({
            "id": r["id"],
            "name": r["name"],
            "website": r["website"] or "",
            "city": r.get("city") or "",
            "state": r.get("state") or "",
        })

    print(f"Hotels to search: {len(hotels)}")
    for h in hotels:
        status = "NO WEBSITE" if not h["website"] else ("BAD" if _is_bad_url(h["website"]) else "OK")
        print(f"  {h['id']:>6}  [{status:>10}]  {h['name'][:55]:<55}  {h['website'][:50]}")

    # Search Serper
    gl = ""
    if args.source and args.source in SOURCE_CONFIGS:
        gl = SOURCE_CONFIGS[args.source].get("serper_gl", "")

    sem = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[
            search_website(client, h["name"], h["city"], h["state"], sem, gl=gl)
            for h in hotels
        ])

    # Display results
    updates = []
    print(f"\n{'='*80}")
    print(f"SERPER RESULTS")
    print(f"{'='*80}")

    for h, candidates in zip(hotels, results):
        current = h["website"]
        current_bad = _is_bad_url(current)
        print(f"\n{h['name']} (id={h['id']})")
        if current:
            print(f"  Current: {current} {'[BAD]' if current_bad else '[OK]'}")
        else:
            print(f"  Current: (none)")

        if not candidates:
            print(f"  Serper:  NO RESULTS")
            continue

        best = candidates[0]
        for i, c in enumerate(candidates):
            marker = " <-- BEST" if i == 0 else ""
            print(f"  #{i+1}: {c['url'][:70]:<70}{marker}")
            print(f"      {c['title'][:70]}")

        if not current or current_bad:
            updates.append((h["id"], h["name"], current, best["url"]))

    # Summary
    print(f"\n{'='*80}")
    found = sum(1 for r in results if r)
    print(f"Searched: {len(hotels)} | Found: {found} | Updates: {len(updates)}")

    if updates:
        print(f"\nWebsite updates:")
        for hid, name, old, new in updates:
            print(f"  {hid:>6}  {name[:45]:<45}  {old[:25] or '(none)':<25} -> {new[:40]}")

    if not updates:
        print("\nNothing to update.")
    elif not args.apply:
        print(f"\nDRY RUN — run with --apply to write {len(updates)} updates to DB")
    else:
        for hid, name, old, new in updates:
            await conn.execute(
                "UPDATE sadie_gtm.hotels SET website = $1, updated_at = NOW() WHERE id = $2",
                new, hid,
            )
        print(f"\nAPPLIED {len(updates)} website updates to DB")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
