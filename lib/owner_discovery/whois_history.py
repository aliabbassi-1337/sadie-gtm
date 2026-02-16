"""Historical WHOIS mining via Wayback Machine.

Pre-GDPR (before May 2018), ~75% of domain records had identifiable registrant data.
The Wayback Machine crawled who.is WHOIS result pages, and those cached snapshots
still contain the full registrant records (name, address, phone, email).

Expected hit rate: ~40-60% for domains registered before 2018.
Cost: Free.
"""

import asyncio
import re
from typing import Optional
from urllib.parse import urlparse

import httpx
from loguru import logger

from lib.owner_discovery.models import DecisionMaker, DomainIntel

WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"

# who.is was heavily crawled by the Wayback Machine pre-GDPR
WHOIS_LOOKUP_SITES = [
    "https://who.is/whois/{domain}",
    "https://www.whois.com/whois/{domain}",
]

# Patterns to extract registrant data from cached who.is HTML
REGISTRANT_PATTERNS = {
    "name": [
        r"Registrant\s+Name[:\s]+([^\n<]+)",
        r"Registrant\s*:\s*([^\n<]+)",
        r"registrant_name[:\s]+([^\n<]+)",
        r"Owner\s+Name[:\s]+([^\n<]+)",
    ],
    "org": [
        r"Registrant\s+Organization[:\s]+([^\n<]+)",
        r"registrant_org[:\s]+([^\n<]+)",
        r"Organization[:\s]+([^\n<]+)",
    ],
    "email": [
        r"Registrant\s+Email[:\s]+([^\s<]+@[^\s<]+)",
        r"registrant_email[:\s]+([^\s<]+@[^\s<]+)",
        r"Admin\s+Email[:\s]+([^\s<]+@[^\s<]+)",
        r"Tech\s+Email[:\s]+([^\s<]+@[^\s<]+)",
    ],
    "phone": [
        r"Registrant\s+Phone[:\s]+([\+\d\.\-\s\(\)]+)",
        r"registrant_phone[:\s]+([\+\d\.\-\s\(\)]+)",
        r"Admin\s+Phone[:\s]+([\+\d\.\-\s\(\)]+)",
    ],
}

# Junk values to filter out
JUNK_VALUES = frozenset({
    "redacted for privacy",
    "data protected",
    "not disclosed",
    "domains by proxy",
    "whoisguard protected",
    "contact privacy",
    "n/a",
    "none",
    "null",
    "please query the rdap",
    "select contact domain holder link",
    "registration private",
    "statutory masking enabled",
})


def _extract_domain(url: str) -> Optional[str]:
    """Extract root domain from a URL."""
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        if host.startswith("www."):
            host = host[4:]
        return host.lower() if host else None
    except Exception:
        return None


def _is_junk(value: str) -> bool:
    """Check if a value is junk/placeholder."""
    if not value:
        return True
    lower = value.strip().lower()
    if len(lower) < 2:
        return True
    return any(j in lower for j in JUNK_VALUES)


def _extract_field(html: str, patterns: list[str]) -> Optional[str]:
    """Extract first matching field from HTML using regex patterns."""
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            # Strip HTML tags
            value = re.sub(r"<[^>]+>", "", value).strip()
            if not _is_junk(value):
                return value
    return None


async def _query_wayback_cdx(
    client: httpx.AsyncClient,
    url_pattern: str,
    limit: int = 5,
) -> list[dict]:
    """Query Wayback Machine CDX API for cached snapshots of a URL.

    Returns list of {timestamp, url} dicts, sorted newest first.
    Only returns snapshots from 2014-2018 (pre-GDPR, best data quality).
    """
    params = {
        "url": url_pattern,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "from": "20140101",
        "to": "20180525",  # GDPR enforcement: May 25, 2018
        "limit": limit,
        "sort": "reverse",  # Newest first
    }

    try:
        resp = await client.get(WAYBACK_CDX_URL, params=params, timeout=20.0)
        if resp.status_code != 200:
            return []

        lines = resp.json()
        if not lines or len(lines) < 2:
            return []

        # First line is header, rest are data
        results = []
        for row in lines[1:]:
            if len(row) >= 2:
                results.append({"timestamp": row[0], "url": row[1]})
        return results

    except Exception as e:
        logger.debug(f"Wayback CDX query failed for {url_pattern}: {e}")
        return []


async def _fetch_wayback_snapshot(
    client: httpx.AsyncClient,
    url: str,
    timestamp: str,
) -> Optional[str]:
    """Fetch a cached page from the Wayback Machine."""
    wayback_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
    try:
        resp = await client.get(wayback_url, timeout=20.0, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.debug(f"Failed to fetch Wayback snapshot: {e}")
    return None


def _parse_whois_page(html: str) -> dict:
    """Parse a who.is/whois.com HTML page for registrant data."""
    result = {
        "name": _extract_field(html, REGISTRANT_PATTERNS["name"]),
        "org": _extract_field(html, REGISTRANT_PATTERNS["org"]),
        "email": _extract_field(html, REGISTRANT_PATTERNS["email"]),
        "phone": _extract_field(html, REGISTRANT_PATTERNS["phone"]),
    }
    return {k: v for k, v in result.items() if v}


async def lookup_historical_whois(
    client: httpx.AsyncClient,
    domain: str,
) -> Optional[DomainIntel]:
    """Look up historical WHOIS data via Wayback Machine snapshots of who.is pages.

    Queries the Wayback Machine for cached who.is WHOIS result pages from 2014-2018
    (pre-GDPR era when registrant data was publicly visible).

    Args:
        client: httpx async client
        domain: Domain name (e.g., "grandhotel.com")

    Returns:
        DomainIntel with registrant info, or None if not found.
    """
    domain = _extract_domain(domain) or domain
    if not domain:
        return None

    for site_template in WHOIS_LOOKUP_SITES:
        whois_url = site_template.format(domain=domain)

        # Query CDX for cached snapshots
        snapshots = await _query_wayback_cdx(client, whois_url, limit=3)
        if not snapshots:
            continue

        # Try each snapshot (newest first)
        for snapshot in snapshots:
            html = await _fetch_wayback_snapshot(
                client, snapshot["url"], snapshot["timestamp"]
            )
            if not html:
                continue

            parsed = _parse_whois_page(html)
            if parsed.get("name") or parsed.get("email"):
                intel = DomainIntel(
                    domain=domain,
                    registrant_name=parsed.get("name"),
                    registrant_org=parsed.get("org"),
                    registrant_email=parsed.get("email"),
                    is_privacy_protected=False,
                    whois_source="whois_history_wayback",
                )
                logger.info(
                    f"Historical WHOIS hit for {domain}: "
                    f"name={parsed.get('name')}, email={parsed.get('email')}"
                )
                return intel

    return None


async def whois_history_to_decision_maker(
    client: httpx.AsyncClient,
    domain: str,
) -> Optional[DecisionMaker]:
    """Look up historical WHOIS and convert to DecisionMaker if found."""
    intel = await lookup_historical_whois(client, domain)
    if not intel or not intel.registrant_name:
        return None

    return DecisionMaker(
        full_name=intel.registrant_name,
        title="Domain Owner" if not intel.registrant_org else "Owner",
        email=intel.registrant_email,
        source="whois_history",
        confidence=0.7,  # Historical data, likely accurate for long-held domains
        raw_source_url=f"wayback://who.is/whois/{domain}",
    )


async def batch_whois_history(
    domains: list[str],
    concurrency: int = 3,
    delay: float = 1.5,
) -> list[tuple[str, Optional[DecisionMaker]]]:
    """Run historical WHOIS lookups for multiple domains.

    Args:
        domains: List of domain names or URLs
        concurrency: Max concurrent requests (be gentle with Wayback Machine)
        delay: Delay between requests in seconds

    Returns:
        List of (domain, DecisionMaker or None)
    """
    results = []
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(http2=True) as client:
        async def lookup_one(domain: str):
            async with sem:
                dm = await whois_history_to_decision_maker(client, domain)
                await asyncio.sleep(delay)
                return domain, dm

        tasks = [lookup_one(d) for d in domains]
        results = await asyncio.gather(*tasks)

    found = sum(1 for _, dm in results if dm is not None)
    logger.info(f"Historical WHOIS batch: {found}/{len(domains)} domains had pre-GDPR data")
    return list(results)
