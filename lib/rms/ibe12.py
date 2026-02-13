"""RMS ibe12 availability checking API client.

Checks if Australia RMS hotels have availability via the ibe12 API.

Flow per hotel:
  1. Extract clientId from booking URL (agentId hardcoded to 1)
  2. GET ibe12.rmscloud.com/OnlineApi/GetConnectionURLs -> JWT cookie
  3. GET ibe12.rmscloud.com/OnlineApi/GetCatAvailRatesData -> categoryRows
  4. If 0 categoryRows -> has_availability=False (no rooms configured)
  5. If categoryRows > 0 -> has_availability=True (rooms configured)
"""

import asyncio
from typing import Optional
from urllib.parse import urlparse

import httpx
from loguru import logger

# Hardcoded agentId=1 -- the default/direct booking agent.
# Old bookings12 URLs use agentId=90 (OTA channel) which returns empty on ibe12.
AGENT_ID = "1"

IBE12_HOST = "ibe12.rmscloud.com"

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

MAX_RETRIES = 3
RETRY_BACKOFF = [0.5, 1.5, 3.0]


def extract_client_id(booking_url: str) -> Optional[str]:
    """Extract client_id from booking URL. Returns None if unparseable."""
    if not booking_url or "rmscloud.com" not in booking_url:
        return None

    try:
        parsed = urlparse(booking_url)
        parts = parsed.path.strip("/").split("/")

        # Standard: /Search/Index/{client_id}/{agent_id}
        for i, p in enumerate(parts):
            if p.lower() == "index" and i + 1 < len(parts):
                return parts[i + 1]

        # ibe12 style: /{client_id}/{agent_id}
        if len(parts) >= 1 and parts[0]:
            return parts[0]

        return None
    except Exception:
        return None


async def get_jwt_cookie(
    client: httpx.AsyncClient,
    client_id: str,
) -> str:
    """Call GetConnectionURLs to get ApiCookie JWT.

    Returns 'ok', 'not_found', or 'failed'.
    """
    url = f"https://{IBE12_HOST}/OnlineApi/GetConnectionURLs"
    params = {
        "clientId": client_id,
        "agentId": AGENT_ID,
        "qs": f"/{client_id}/{AGENT_ID}",
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.get(url, params=params, timeout=10.0, headers=REQUEST_HEADERS)
            if resp.status_code == 200 and resp.text.startswith("{"):
                return "ok"
            if resp.status_code in (404, 410):
                return "not_found"
            if resp.status_code == 429 or resp.status_code >= 500:
                logger.debug(f"    JWT retry {attempt+1}/{MAX_RETRIES} (HTTP {resp.status_code})")
                await asyncio.sleep(RETRY_BACKOFF[attempt])
                continue
            return "not_found"
        except httpx.HTTPError as e:
            logger.debug(f"    JWT retry {attempt+1}/{MAX_RETRIES} ({type(e).__name__})")
            await asyncio.sleep(RETRY_BACKOFF[attempt])
    return "failed"


async def check_availability(
    client: httpx.AsyncClient,
    client_id: str,
    arrive: str,
    depart: str,
) -> tuple[Optional[bool], int]:
    """Call GetCatAvailRatesData.

    Returns (has_rooms, n_categories).
    """
    url = f"https://{IBE12_HOST}/OnlineApi/GetCatAvailRatesData"
    params = {
        "clientId": client_id,
        "onlineAgentId": AGENT_ID,
        "adults": "2",
        "children": "0",
        "infants": "0",
        "arriveDate": arrive,
        "departDate": depart,
        "additional1": "0",
        "additional2": "0",
        "additional3": "0",
        "additional4": "0",
        "additional5": "0",
        "additional6": "0",
        "additional7": "0",
        "bookingType": "0",
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.get(url, params=params, timeout=8.0, headers=REQUEST_HEADERS)
            if resp.status_code in (404, 410):
                return False, 0
            if resp.status_code == 429 or resp.status_code >= 500:
                logger.debug(f"    Avail retry {attempt+1}/{MAX_RETRIES} (HTTP {resp.status_code})")
                await asyncio.sleep(RETRY_BACKOFF[attempt])
                continue
            if resp.status_code != 200 or not resp.text.startswith("{"):
                return None, 0

            data = resp.json()
            rows = data.get("categoryRows")
            if rows is None:
                return None, 0
            has_rooms = any(r.get("anyAvailRates") for r in rows)
            return has_rooms, len(rows)
        except httpx.HTTPError as e:
            logger.debug(f"    Avail retry {attempt+1}/{MAX_RETRIES} ({type(e).__name__})")
            await asyncio.sleep(RETRY_BACKOFF[attempt])
        except Exception:
            return None, 0
    return None, 0
