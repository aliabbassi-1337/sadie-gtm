"""Data access layer for deep-link sessions and short links.

Uses Supabase (Postgres) for persistent storage.
In-memory cache for proxy sessions (immutable after creation).
"""

import json
import secrets
import time
from typing import Optional

from db.client import get_conn

# ---------------------------------------------------------------------------
# In-memory session cache — sessions are immutable, safe to cache
# ---------------------------------------------------------------------------

_SESSION_CACHE: dict[str, tuple[dict, float]] = {}  # session_id -> (data, timestamp)
_CACHE_TTL = 3600  # 1 hour
_CACHE_MAX = 500


# ---------------------------------------------------------------------------
# Proxy sessions
# ---------------------------------------------------------------------------


async def store_proxy_session(
    cookies: dict[str, str],
    target_host: str,
    checkout_path: str,
    autobook: bool = False,
    autobook_engine: str = "cloudbeds",
) -> str:
    """Store proxy session in DB. Returns session_id."""
    session_id = secrets.token_hex(6)

    async with get_conn() as conn:
        await conn.execute(
            """INSERT INTO sadie_gtm.proxy_sessions
               (session_id, cookies, target_host, target_base, checkout_path, autobook, autobook_engine)
               VALUES ($1, $2, $3, $4, $5, $6, $7)""",
            session_id,
            json.dumps(cookies),
            target_host,
            f"https://{target_host}",
            checkout_path,
            autobook,
            autobook_engine,
        )
    return session_id


async def get_proxy_session(session_id: str) -> Optional[dict]:
    """Retrieve proxy session by ID. Cached in-memory (sessions are immutable)."""
    # Check cache first
    now = time.monotonic()
    cached = _SESSION_CACHE.get(session_id)
    if cached:
        data, ts = cached
        if now - ts < _CACHE_TTL:
            return data
        del _SESSION_CACHE[session_id]

    async with get_conn() as conn:
        row = await conn.fetchrow(
            "SELECT cookies, target_host, target_base, checkout_path, autobook, autobook_engine FROM sadie_gtm.proxy_sessions WHERE session_id = $1",
            session_id,
        )
    if not row:
        return None
    data = {
        "cookies": json.loads(row["cookies"]) if isinstance(row["cookies"], str) else row["cookies"],
        "target_host": row["target_host"],
        "target_base": row["target_base"],
        "checkout_path": row["checkout_path"],
        "autobook": row["autobook"],
        "autobook_engine": row["autobook_engine"],
    }

    # Evict oldest if cache is full
    if len(_SESSION_CACHE) >= _CACHE_MAX:
        oldest = min(_SESSION_CACHE, key=lambda k: _SESSION_CACHE[k][1])
        del _SESSION_CACHE[oldest]
    _SESSION_CACHE[session_id] = (data, now)

    return data


# ---------------------------------------------------------------------------
# Short links
# ---------------------------------------------------------------------------


async def store_short_link(code: str, url: str) -> None:
    """Store a short link mapping."""
    async with get_conn() as conn:
        await conn.execute(
            "INSERT INTO sadie_gtm.short_links (code, url) VALUES ($1, $2) ON CONFLICT (code) DO UPDATE SET url = $2",
            code, url,
        )


async def get_short_link(code: str) -> Optional[str]:
    """Retrieve URL for a short link code."""
    async with get_conn() as conn:
        row = await conn.fetchrow("SELECT url FROM sadie_gtm.short_links WHERE code = $1", code)
    return row["url"] if row else None


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------


async def get_hotel_booking_info(hotel_id: int) -> Optional[dict]:
    """Look up a hotel's booking URL from the database."""
    from db.client import queries

    async with get_conn() as conn:
        return await queries.get_hotel_booking_info(conn, hotel_id=hotel_id)
