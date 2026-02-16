"""RDAP domain lookup for registrant discovery.

Queries the RDAP protocol (WHOIS replacement) for domain registration data.
Returns structured JSON with registrant name, org, email when not privacy-protected.

Rate limit: ~10 requests per 10 seconds on rdap.org proxy.
Expected hit rate: ~10-15% (most domains have privacy protection post-GDPR).
"""

import asyncio
from typing import Optional
from urllib.parse import urlparse

import httpx
from loguru import logger

from lib.owner_discovery.models import DecisionMaker, DomainIntel

RDAP_PROXY = "https://rdap.org/domain"

# Direct registrar RDAP endpoints (faster, less rate-limited)
REGISTRAR_RDAP = {
    "verisign": "https://rdap.verisign.com/com/v1/domain",
    "pir": "https://rdap.publicinterestregistry.org/rdap/domain",
}

# Privacy protection indicators in registrant names
PRIVACY_INDICATORS = frozenset({
    "redacted for privacy",
    "data protected",
    "contact privacy",
    "domains by proxy",
    "whoisguard",
    "privacy protect",
    "withheld for privacy",
    "not disclosed",
    "registration private",
    "domain protection",
    "identity protection",
    "private registration",
    "redacted",
    "statutory masking",
})


def _extract_domain(url: str) -> Optional[str]:
    """Extract root domain from a URL or domain string."""
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        # Strip www prefix
        if host.startswith("www."):
            host = host[4:]
        return host.lower() if host else None
    except Exception:
        return None


def _is_privacy_protected(name: str) -> bool:
    """Check if a registrant name indicates privacy protection."""
    if not name:
        return True
    lower = name.lower().strip()
    return any(indicator in lower for indicator in PRIVACY_INDICATORS)


def _extract_vcard_fields(vcard_array: list) -> dict:
    """Extract name, org, email, phone from a vCard array (RFC 6350 in JSON)."""
    result = {"name": None, "org": None, "email": None, "phone": None}
    if not vcard_array or len(vcard_array) < 2:
        return result

    fields = vcard_array[1]  # Second element contains the properties
    for field in fields:
        if not isinstance(field, list) or len(field) < 4:
            continue
        prop_name = field[0]
        value = field[3]
        if not isinstance(value, str):
            continue
        if prop_name == "fn":
            result["name"] = value.strip() if value.strip() else None
        elif prop_name == "org":
            result["org"] = value.strip() if value.strip() else None
        elif prop_name == "email":
            result["email"] = value.strip() if value.strip() else None
        elif prop_name == "tel":
            result["phone"] = value.strip() if value.strip() else None
    return result


async def rdap_lookup(
    client: httpx.AsyncClient,
    domain: str,
) -> Optional[DomainIntel]:
    """Query RDAP for domain registration data.

    Args:
        client: httpx async client
        domain: Domain name (e.g., "grandhotel.com")

    Returns:
        DomainIntel with registrant info, or None on failure.
    """
    domain = _extract_domain(domain) or domain
    if not domain:
        return None

    url = f"{RDAP_PROXY}/{domain}"
    try:
        resp = await client.get(url, timeout=15.0, follow_redirects=True)
        if resp.status_code == 404:
            return DomainIntel(domain=domain, whois_source="rdap")
        if resp.status_code == 429:
            logger.warning(f"RDAP rate limited for {domain}, backing off")
            await asyncio.sleep(10)
            return None
        if resp.status_code != 200:
            return None

        data = resp.json()
    except Exception as e:
        logger.debug(f"RDAP lookup failed for {domain}: {e}")
        return None

    intel = DomainIntel(domain=domain, whois_source="rdap")

    # Extract registrar
    for entity in data.get("entities", []):
        roles = entity.get("roles", [])
        vcard = entity.get("vcardArray")

        if "registrar" in roles and vcard:
            fields = _extract_vcard_fields(vcard)
            intel.registrar = fields.get("name") or fields.get("org")

        if "registrant" in roles and vcard:
            fields = _extract_vcard_fields(vcard)
            name = fields.get("name")
            if name and not _is_privacy_protected(name):
                intel.registrant_name = name
                intel.registrant_org = fields.get("org")
                intel.registrant_email = fields.get("email")
                intel.is_privacy_protected = False
            else:
                intel.is_privacy_protected = True

    # Extract registration date
    for event in data.get("events", []):
        if event.get("eventAction") == "registration":
            intel.registration_date = event.get("eventDate")

    return intel


async def rdap_to_decision_maker(
    client: httpx.AsyncClient,
    domain: str,
) -> tuple[Optional[DecisionMaker], Optional[DomainIntel]]:
    """Run RDAP lookup and convert to a DecisionMaker if registrant data found.

    Returns:
        (DecisionMaker or None, DomainIntel or None)
    """
    intel = await rdap_lookup(client, domain)
    if not intel or intel.is_privacy_protected:
        return None, intel

    if intel.registrant_name:
        dm = DecisionMaker(
            full_name=intel.registrant_name,
            title="Domain Registrant" if not intel.registrant_org else "Owner",
            email=intel.registrant_email,
            source="rdap",
            confidence=0.6,  # Domain registrant is likely the owner but not guaranteed
            raw_source_url=f"rdap://{domain}",
        )
        return dm, intel

    return None, intel


async def batch_rdap_lookup(
    domains: list[str],
    concurrency: int = 5,
    delay: float = 1.2,
) -> list[tuple[str, Optional[DecisionMaker], Optional[DomainIntel]]]:
    """Run RDAP lookups for multiple domains with rate limiting.

    Args:
        domains: List of domain names or URLs
        concurrency: Max concurrent requests (keep low for rdap.org)
        delay: Delay between requests in seconds

    Returns:
        List of (domain, DecisionMaker or None, DomainIntel or None)
    """
    results = []
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(http2=True) as client:
        async def lookup_one(domain: str):
            async with sem:
                dm, intel = await rdap_to_decision_maker(client, domain)
                await asyncio.sleep(delay)
                return domain, dm, intel

        tasks = [lookup_one(d) for d in domains]
        results = await asyncio.gather(*tasks)

    found = sum(1 for _, dm, _ in results if dm is not None)
    logger.info(f"RDAP batch: {found}/{len(domains)} domains had registrant data")
    return list(results)
