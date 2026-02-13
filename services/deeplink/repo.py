"""Data access layer for deep-link sessions and short links.

In-memory stores for POC — swap to DB/Redis later.
"""

import secrets
from typing import Optional


# ---------------------------------------------------------------------------
# In-memory stores
# ---------------------------------------------------------------------------

_sessions: dict[str, dict] = {}
_links: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Proxy sessions
# ---------------------------------------------------------------------------


def store_proxy_session(
    cookies: dict[str, str],
    target_host: str,
    checkout_path: str,
    autobook: bool = False,
    autobook_engine: str = "cloudbeds",
) -> str:
    """Store proxy session. Returns session_id."""
    session_id = secrets.token_hex(6)

    _sessions[session_id] = {
        "cookies": cookies,
        "target_host": target_host,
        "target_base": f"https://{target_host}",
        "checkout_path": checkout_path,
        "autobook": autobook,
        "autobook_engine": autobook_engine,
    }
    return session_id


def get_proxy_session(session_id: str) -> Optional[dict]:
    """Retrieve proxy session by ID."""
    return _sessions.get(session_id)


# ---------------------------------------------------------------------------
# Short links
# ---------------------------------------------------------------------------


def store_short_link(code: str, url: str) -> None:
    """Store a short link mapping."""
    _links[code] = url


def get_short_link(code: str) -> Optional[str]:
    """Retrieve URL for a short link code."""
    return _links.get(code)


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------


async def get_hotel_booking_info(hotel_id: int) -> Optional[dict]:
    """Look up a hotel's booking URL from the database."""
    from db.client import get_conn, queries

    async with get_conn() as conn:
        return await queries.get_hotel_booking_info(conn, hotel_id=hotel_id)
