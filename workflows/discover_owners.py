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
