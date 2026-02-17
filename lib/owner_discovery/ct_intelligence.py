"""Certificate Transparency intelligence for owner discovery.

Queries crt.sh (free public CT log search engine) to extract:
  - Organization names from OV/EV certificate subjects
  - Related domains from certificate SANs

Unlike WHOIS (80%+ privacy-protected), cert subject fields are never redacted.
OV/EV certs contain the legal entity name that purchased the certificate.

Rate limit: ~1 req/s against crt.sh JSON API.
"""

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import httpx
from loguru import logger

from services.enrichment.owner_models import DecisionMaker

CRT_SH_JSON = "https://crt.sh/"

# DV CAs that never have meaningful org names in the subject
DV_CA_PATTERNS = frozenset({
    "let's encrypt",
    "letsencrypt",
    "r3",
    "r10",
    "r11",
    "e5",
    "e6",
    "zerossl",
    "buypass",
    "ssl.com",
    "google trust services",
    "trustasia",
    "certum",
    "amazon",          # AWS ACM (DV-only)
    "starfield",       # GoDaddy DV subsidiary
    "cPanel",
    "cpanel",
})

# SANs containing these are CDN/hosting, not interesting sibling domains
IGNORE_SAN_PATTERNS = frozenset({
    "cloudflare",
    "amazonaws",
    "fastly",
    "akamai",
    "cloudfront",
    "herokuapp",
    "wpengine",
    "squarespace",
    "wixsite",
    "shopify",
    "sni.cloudflaressl",
    "ssl.com",
    "sectigo",
    "godaddy",
    "azurewebsites",
    "googleusercontent",
    "firebaseapp",
    "netlify",
    "vercel",
})

# Generic org names to filter out (not useful as owner names)
GENERIC_ORG_NAMES = frozenset({
    "cloudflare",
    "cloudflare inc",
    "cloudflare, inc.",
    "amazon",
    "amazon.com",
    "google llc",
    "google trust services llc",
    "microsoft corporation",
    "microsoft",
    "facebook",
    "meta platforms",
    "meta platforms, inc.",
    "akamai technologies",
    "akamai",
    "fastly",
    "fastly, inc.",
    "let's encrypt",
    "digicert",
    "sectigo",
    "godaddy",
    "godaddy.com",
    "squarespace",
    "automattic",
    "shopify",
})


@dataclass
class CertIntel:
    """Intelligence extracted from CT logs for a domain."""

    domain: str
    org_name: Optional[str] = None
    alt_domains: list[str] = field(default_factory=list)
    cert_count: int = 0
    earliest_cert: Optional[str] = None
    latest_cert: Optional[str] = None
    has_ov_ev: bool = False
    raw_certs: list[dict] = field(default_factory=list)


def _is_dv_issuer(issuer_name: str) -> bool:
    """Check if the issuer is a DV-only CA (no org in subject)."""
    lower = issuer_name.lower()
    return any(pattern in lower for pattern in DV_CA_PATTERNS)


def _is_ignored_san(san: str) -> bool:
    """Check if a SAN belongs to a CDN/hosting provider."""
    lower = san.lower()
    return any(pattern in lower for pattern in IGNORE_SAN_PATTERNS)


def _is_generic_org(org: str) -> bool:
    """Check if an org name is too generic to be useful."""
    return org.lower().strip() in GENERIC_ORG_NAMES


def _parse_org_from_subject(subject_dn: str) -> Optional[str]:
    """Extract O= field from a DN string like 'C=AU, ST=NSW, O=Big4 Holiday Parks Pty Ltd'."""
    match = re.search(r'O\s*=\s*([^,/]+)', subject_dn)
    if match:
        org = match.group(1).strip()
        if org and not _is_generic_org(org):
            return org
    return None


def _parse_org_from_pem_text(pem_text: str) -> Optional[str]:
    """Extract Organization from a PEM certificate's subject using cryptography lib."""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives.serialization import Encoding

        # The crt.sh ?d= endpoint returns PEM
        cert = x509.load_pem_x509_certificate(pem_text.encode())
        orgs = cert.subject.get_attributes_for_oid(x509.oid.NameOID.ORGANIZATION_NAME)
        if orgs:
            org = orgs[0].value
            if org and not _is_generic_org(org):
                return org
    except Exception as e:
        logger.debug(f"Failed to parse PEM: {e}")
    return None


async def ct_lookup(
    client: httpx.AsyncClient,
    domain: str,
) -> Optional[CertIntel]:
    """Query crt.sh for CT log entries for a domain.

    Args:
        client: httpx async client
        domain: Domain name (e.g. "big4.com.au")

    Returns:
        CertIntel with org name, alt domains, cert stats.
    """
    params = {"q": domain, "output": "json", "exclude": "expired"}
    try:
        resp = await client.get(CRT_SH_JSON, params=params, timeout=20.0)
        if resp.status_code == 429:
            logger.warning(f"crt.sh {domain}: 429 rate limited, backing off 5s")
            await asyncio.sleep(5)
            return None
        if resp.status_code != 200:
            logger.debug(f"crt.sh {domain}: HTTP {resp.status_code}")
            return None

        text = resp.text.strip()
        if not text or text == "[]":
            logger.debug(f"crt.sh {domain}: empty response")
            return CertIntel(domain=domain)
        data = resp.json()
        if not isinstance(data, list):
            logger.debug(f"crt.sh {domain}: unexpected response type")
            return None
    except Exception as e:
        logger.debug(f"crt.sh {domain}: request failed — {e}")
        return None

    # Deduplicate by cert id
    seen_ids = set()
    unique_certs = []
    for cert in data:
        cert_id = cert.get("id")
        if cert_id and cert_id not in seen_ids:
            seen_ids.add(cert_id)
            unique_certs.append(cert)

    intel = CertIntel(domain=domain, cert_count=len(unique_certs))

    # Collect all SANs across all certs
    all_sans = set()
    ov_ev_cert_ids = []
    dates = []

    for cert in unique_certs:
        issuer = cert.get("issuer_name", "")
        name_value = cert.get("name_value", "")
        not_before = cert.get("not_before", "")

        if not_before:
            dates.append(not_before)

        # Collect SANs (newline-separated in crt.sh JSON)
        for san in name_value.split("\n"):
            san = san.strip().lower()
            if san and san != domain.lower() and not san.startswith("*."):
                if not _is_ignored_san(san):
                    all_sans.add(san)

        # Identify OV/EV certs (non-DV issuers)
        if not _is_dv_issuer(issuer):
            ov_ev_cert_ids.append(cert.get("id"))

    intel.alt_domains = sorted(all_sans)

    if dates:
        dates.sort()
        intel.earliest_cert = dates[0]
        intel.latest_cert = dates[-1]

    if not ov_ev_cert_ids:
        logger.debug(f"crt.sh {domain}: {len(unique_certs)} certs, all DV — no org data")
        return intel

    intel.has_ov_ev = True

    # Fetch PEM for up to 3 OV/EV certs to extract subject.O
    org_names = set()
    for cert_id in ov_ev_cert_ids[:3]:
        try:
            pem_resp = await client.get(
                f"https://crt.sh/?d={cert_id}",
                timeout=15.0,
            )
            if pem_resp.status_code == 200:
                org = _parse_org_from_pem_text(pem_resp.text)
                if org:
                    org_names.add(org)
            await asyncio.sleep(1)  # Rate limit between PEM fetches
        except Exception as e:
            logger.debug(f"crt.sh PEM fetch {cert_id}: {e}")

    if org_names:
        # Use the most common org name (or first one)
        intel.org_name = sorted(org_names, key=lambda x: len(x))[0]
        logger.debug(f"crt.sh {domain}: org={intel.org_name} from {len(org_names)} OV/EV certs")

    # Store raw cert summaries for cache
    intel.raw_certs = [
        {
            "id": c.get("id"),
            "issuer_name": c.get("issuer_name"),
            "common_name": c.get("common_name"),
            "not_before": c.get("not_before"),
            "not_after": c.get("not_after"),
        }
        for c in unique_certs[:20]  # Cap at 20 for storage
    ]

    return intel


async def ct_to_decision_makers(
    client: httpx.AsyncClient,
    domain: str,
) -> tuple[list[DecisionMaker], Optional[CertIntel]]:
    """Run CT lookup and convert to DecisionMakers if org name found.

    Args:
        client: httpx async client
        domain: Domain name

    Returns:
        (list of DecisionMaker, CertIntel or None)
    """
    intel = await ct_lookup(client, domain)
    if not intel:
        return [], None

    dms = []

    if intel.org_name:
        dm = DecisionMaker(
            full_name=intel.org_name,
            title="Certificate Organization",
            sources=["ct_cert_subject"],
            confidence=0.65,
            raw_source_url=f"https://crt.sh/?q={domain}",
        )
        dms.append(dm)

    return dms, intel
