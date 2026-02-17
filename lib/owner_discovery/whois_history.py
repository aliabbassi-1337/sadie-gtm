"""WHOIS domain lookup for registrant discovery.

Two strategies:
  1. Live WHOIS via python-whois (socket-based, works for all domains)
  2. Historical WHOIS via Wayback Machine (pre-GDPR snapshots, limited coverage)

Live WHOIS still returns real registrant data for ~10-15% of domains.
Small hotels on Bluehost, HostGator, etc. often skip privacy protection.

Expected hit rate: ~10-15% live, ~40-60% historical (for domains crawled pre-GDPR).
Cost: Free (socket WHOIS + Wayback Machine).
"""

import asyncio
import re
from typing import Optional
from urllib.parse import urlparse

import httpx
from loguru import logger

from services.enrichment.owner_models import DecisionMaker, DomainIntel

# ── Privacy / junk detection ─────────────────────────────────────────

PRIVACY_INDICATORS = frozenset({
    "redacted for privacy", "data protected", "not disclosed",
    "domains by proxy", "whoisguard", "contact privacy",
    "privacy protect", "withheld for privacy", "registration private",
    "domain protection", "identity protection", "private registration",
    "perfect privacy", "privacy service", "whois privacy",
    "domain privacy", "proxy protection", "select contact domain",
    "statutory masking", "gdpr masked", "data redacted",
    "registrant not identified", "none", "n/a", "null",
    "please query the rdap", "on behalf of", "identity protect",
})

# Registrar abuse/generic emails to skip
JUNK_EMAIL_DOMAINS = frozenset({
    "godaddy.com", "networksolutionsprivateregistration.com",
    "contactprivacy.com", "domainsbyproxy.com", "whoisguard.com",
    "privacyguardian.org", "withheldforprivacy.com", "bluehost.com",
    "hostmonster.com", "hostgator.com", "web.com", "cloudflare.com",
    "namecheap.com", "tucows.com", "identity-protect.org",
    "support.aws.com", "amazonaws.com", "registrarmail.net",
})

JUNK_EMAIL_PREFIXES = frozenset({
    "abuse@", "admin@", "noreply@", "support@", "domain.operations@",
    "support-domain@", "hostmaster@", "postmaster@", "trustandsafety@",
})


def _extract_domain(url: str) -> Optional[str]:
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


def _is_privacy(value: str) -> bool:
    """Check if a value is a privacy placeholder."""
    if not value:
        return True
    lower = value.strip().lower()
    if len(lower) < 2:
        return True
    return any(p in lower for p in PRIVACY_INDICATORS)


def _is_junk_email(email: str) -> bool:
    """Check if an email is a registrar/privacy junk email."""
    if not email:
        return True
    lower = email.lower().strip()
    domain = lower.split("@")[-1] if "@" in lower else ""
    if domain in JUNK_EMAIL_DOMAINS:
        return True
    return any(lower.startswith(p) for p in JUNK_EMAIL_PREFIXES)


def _filter_personal_emails(emails) -> list[str]:
    """Extract personal (non-junk) emails from WHOIS data."""
    if not emails:
        return []
    if isinstance(emails, str):
        emails = [emails]
    return [e.strip() for e in emails if not _is_junk_email(e)]


# ── Live WHOIS (python-whois) ────────────────────────────────────────

def _live_whois_lookup(domain: str) -> Optional[dict]:
    """Run live WHOIS lookup via python-whois. Blocking call.

    Returns dict with name, org, emails, registrar, creation_date or None.
    """
    try:
        import whois
        w = whois.whois(domain)
        if not w or not w.get("domain_name"):
            return None

        name = w.get("name") or w.get("registrant_name") or ""
        org = w.get("org") or ""
        emails = w.get("emails") or []
        registrar = w.get("registrar") or ""
        creation = w.get("creation_date")

        # Handle list of creation dates
        if isinstance(creation, list):
            creation = creation[0] if creation else None

        return {
            "name": name if not _is_privacy(name) else None,
            "org": org if not _is_privacy(org) else None,
            "emails": _filter_personal_emails(emails),
            "registrar": registrar,
            "creation_date": str(creation) if creation else None,
        }
    except Exception as e:
        logger.debug(f"Live WHOIS failed for {domain}: {e}")
        return None


async def live_whois_lookup(domain: str) -> Optional[DomainIntel]:
    """Async wrapper for live WHOIS lookup (runs in thread pool)."""
    domain = _extract_domain(domain) or domain
    if not domain:
        return None

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _live_whois_lookup, domain)

    if not result:
        return None

    has_person = result.get("name") or result.get("org")
    has_email = bool(result.get("emails"))

    intel = DomainIntel(
        domain=domain,
        registrant_name=result.get("name"),
        registrant_org=result.get("org"),
        registrant_email=result["emails"][0] if result.get("emails") else None,
        registrar=result.get("registrar"),
        registration_date=result.get("creation_date"),
        is_privacy_protected=not (has_person or has_email),
        whois_source="whois_live",
    )

    if has_person or has_email:
        logger.info(
            f"WHOIS hit for {domain}: name={result.get('name')}, "
            f"org={result.get('org')}, emails={result.get('emails')}"
        )

    return intel


# ── Historical WHOIS (Wayback Machine) ───────────────────────────────

WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"

WHOIS_LOOKUP_SITES = [
    "https://who.is/whois/{domain}",
    "https://www.whois.com/whois/{domain}",
    "https://whois.domaintools.com/{domain}",
]

REGISTRANT_PATTERNS = {
    "name": [
        r"Registrant\s+Name[:\s]+([^\n<]+)",
        r"Registrant\s*:\s*&nbsp;\s*&nbsp;\s*([^\n<&]+)",
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


def _extract_field(html: str, patterns: list[str]) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            value = re.sub(r"&nbsp;", " ", value)
            value = re.sub(r"<[^>]+>", "", value).strip()
            if not _is_privacy(value):
                return value
    return None


async def _query_wayback_cdx(
    client: httpx.AsyncClient,
    url_pattern: str,
    limit: int = 3,
) -> list[dict]:
    """Query Wayback Machine CDX API for cached snapshots."""
    params = {
        "url": url_pattern,
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "from": "20050101",
        "to": "20180525",
        "limit": limit,
        "sort": "reverse",
    }

    for attempt in range(2):
        try:
            resp = await client.get(
                WAYBACK_CDX_URL, params=params, timeout=45.0,
            )
            if resp.status_code != 200:
                return []

            text = resp.text.strip()
            if not text:
                return []

            lines = resp.json()
            if not lines or len(lines) < 2:
                return []

            return [
                {"timestamp": row[0], "url": row[1]}
                for row in lines[1:]
                if len(row) >= 2
            ]
        except (httpx.TimeoutException, httpx.ReadTimeout):
            if attempt == 0:
                logger.debug(f"Wayback CDX timeout for {url_pattern}, retrying...")
                await asyncio.sleep(3)
            continue
        except Exception as e:
            logger.debug(f"Wayback CDX query failed for {url_pattern}: {e}")
            return []

    return []


async def _fetch_wayback_snapshot(
    client: httpx.AsyncClient,
    url: str,
    timestamp: str,
) -> Optional[str]:
    """Fetch a cached page from the Wayback Machine."""
    wayback_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
    try:
        resp = await client.get(wayback_url, timeout=30.0, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        logger.debug(f"Failed to fetch Wayback snapshot: {e}")
    return None


def _parse_whois_page(html: str) -> dict:
    result = {
        "name": _extract_field(html, REGISTRANT_PATTERNS["name"]),
        "org": _extract_field(html, REGISTRANT_PATTERNS["org"]),
        "email": _extract_field(html, REGISTRANT_PATTERNS["email"]),
        "phone": _extract_field(html, REGISTRANT_PATTERNS["phone"]),
    }
    # Filter junk emails
    if result.get("email") and _is_junk_email(result["email"]):
        result["email"] = None
    return {k: v for k, v in result.items() if v}


async def lookup_historical_whois(
    client: httpx.AsyncClient,
    domain: str,
) -> Optional[DomainIntel]:
    """Look up historical WHOIS via Wayback Machine."""
    domain = _extract_domain(domain) or domain
    if not domain:
        return None

    for site_template in WHOIS_LOOKUP_SITES:
        whois_url = site_template.format(domain=domain)
        snapshots = await _query_wayback_cdx(client, whois_url, limit=3)
        if not snapshots:
            continue

        for snapshot in snapshots:
            html = await _fetch_wayback_snapshot(
                client, snapshot["url"], snapshot["timestamp"]
            )
            if not html:
                continue

            parsed = _parse_whois_page(html)
            if parsed.get("name") or parsed.get("email"):
                logger.info(
                    f"Historical WHOIS hit for {domain}: "
                    f"name={parsed.get('name')}, email={parsed.get('email')}"
                )
                return DomainIntel(
                    domain=domain,
                    registrant_name=parsed.get("name"),
                    registrant_org=parsed.get("org"),
                    registrant_email=parsed.get("email"),
                    is_privacy_protected=False,
                    whois_source="whois_history_wayback",
                )

    return None


# ── Combined lookup (live first, then historical) ─────────────────────

async def whois_lookup(
    client: httpx.AsyncClient,
    domain: str,
    skip_historical: bool = False,
) -> Optional[DomainIntel]:
    """Combined WHOIS lookup: live first, then historical fallback.

    Args:
        client: httpx async client (used for historical Wayback lookups)
        domain: Domain name
        skip_historical: Skip Wayback Machine lookup (faster, less coverage)

    Returns:
        DomainIntel with registrant info, or None if privacy-protected everywhere.
    """
    # 1. Live WHOIS (fast, covers all domains)
    intel = await live_whois_lookup(domain)
    if intel and not intel.is_privacy_protected:
        return intel

    # 2. Historical WHOIS via Wayback Machine (slow, limited coverage)
    if not skip_historical:
        hist_intel = await lookup_historical_whois(client, domain)
        if hist_intel:
            return hist_intel

    # Return the live intel (even if privacy-protected) for caching registrar info
    return intel


def whois_to_decision_maker(intel: Optional[DomainIntel]) -> Optional[DecisionMaker]:
    """Convert WHOIS DomainIntel to a DecisionMaker if registrant data found."""
    if not intel or intel.is_privacy_protected:
        return None
    if not intel.registrant_name and not intel.registrant_email:
        return None

    return DecisionMaker(
        full_name=intel.registrant_name,
        title="Domain Owner" if not intel.registrant_org else "Owner",
        email=intel.registrant_email,
        source=intel.whois_source or "whois",
        confidence=0.7,
        raw_source_url=f"whois://{intel.domain}",
    )
