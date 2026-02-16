"""DNS intelligence for hotel domain analysis.

DNS records are always public and cannot be redacted (unlike WHOIS).
Extracts: email provider (MX), admin email (SOA), email infrastructure (SPF/DMARC),
service usage (TXT verification records), and catch-all detection.

Cost: Free. No rate limits worth worrying about.
"""

import asyncio
import socket
from typing import Optional
from urllib.parse import urlparse

from loguru import logger

from lib.owner_discovery.models import DomainIntel

# Lazy import dns.resolver - may not be installed
_resolver = None


def _get_resolver():
    global _resolver
    if _resolver is None:
        try:
            import dns.resolver
            _resolver = dns.resolver.Resolver()
            _resolver.timeout = 5
            _resolver.lifetime = 10
        except ImportError:
            logger.warning("dnspython not installed. Run: pip install dnspython")
            raise
    return _resolver


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


def get_email_provider(domain: str) -> tuple[Optional[str], list[str]]:
    """Detect email provider from MX records.

    Returns:
        (provider_name, list_of_mx_hosts)
    """
    resolver = _get_resolver()
    try:
        import dns.resolver
        mx_records = resolver.resolve(domain, "MX")
        mx_hosts = [str(r.exchange).lower().rstrip(".") for r in mx_records]
    except Exception:
        return None, []

    mx_str = " ".join(mx_hosts)

    if any(x in mx_str for x in ["google.com", "googlemail.com", "aspmx.l.google.com"]):
        return "google_workspace", mx_hosts
    if "outlook.com" in mx_str or "protection.outlook.com" in mx_str:
        return "microsoft_365", mx_hosts
    if "secureserver.net" in mx_str:
        return "godaddy_email", mx_hosts
    if "zoho.com" in mx_str:
        return "zoho", mx_hosts
    if "protonmail.ch" in mx_str or "protonmail.com" in mx_str:
        return "protonmail", mx_hosts
    if "emailsrvr.com" in mx_str:
        return "rackspace", mx_hosts
    if "mimecast.com" in mx_str:
        return "mimecast", mx_hosts
    if domain in mx_str:
        return "self_hosted", mx_hosts

    return f"other:{mx_hosts[0]}" if mx_hosts else None, mx_hosts


def get_soa_email(domain: str) -> Optional[str]:
    """Extract admin email from SOA RNAME field.

    The SOA rname field encodes an email: first dot = @, rest are literal dots.
    E.g., admin.example.com → admin@example.com
    """
    resolver = _get_resolver()
    try:
        import dns.resolver
        soa = resolver.resolve(domain, "SOA")
        for r in soa:
            rname = str(r.rname).rstrip(".")
            parts = rname.split(".", 1)  # Split on FIRST dot only
            if len(parts) == 2:
                email = f"{parts[0]}@{parts[1]}"
                # Filter out generic DNS admin emails
                generic_prefixes = {"hostmaster", "dns-admin", "dnsadmin", "admin", "root", "postmaster"}
                if parts[0].lower() not in generic_prefixes:
                    return email
    except Exception:
        pass
    return None


def get_spf_record(domain: str) -> Optional[str]:
    """Get SPF record for additional email infrastructure info."""
    resolver = _get_resolver()
    try:
        import dns.resolver
        txt_records = resolver.resolve(domain, "TXT")
        for r in txt_records:
            txt = str(r).strip('"')
            if txt.startswith("v=spf1"):
                return txt
    except Exception:
        pass
    return None


def get_dmarc_record(domain: str) -> Optional[str]:
    """Get DMARC record (may contain report-to email)."""
    resolver = _get_resolver()
    try:
        import dns.resolver
        dmarc = resolver.resolve(f"_dmarc.{domain}", "TXT")
        for r in dmarc:
            txt = str(r).strip('"')
            if "v=DMARC1" in txt:
                return txt
    except Exception:
        pass
    return None


def get_txt_services(domain: str) -> list[str]:
    """Detect third-party services from TXT verification records."""
    resolver = _get_resolver()
    services = []
    try:
        import dns.resolver
        txts = resolver.resolve(domain, "TXT")
        for r in txts:
            txt = str(r).strip('"')
            if "google-site-verification" in txt:
                services.append("google_search_console")
            if "facebook-domain-verification" in txt:
                services.append("facebook_business")
            if "MS=" in txt:
                services.append("microsoft_365_verified")
            if "hubspot" in txt.lower():
                services.append("hubspot")
            if "stripe-verification" in txt:
                services.append("stripe")
            if "apple-domain-verification" in txt:
                services.append("apple")
    except Exception:
        pass
    return services


async def analyze_domain(domain_or_url: str) -> Optional[DomainIntel]:
    """Run full DNS analysis on a domain. Non-blocking wrapper around sync DNS calls.

    Args:
        domain_or_url: Domain name or URL

    Returns:
        DomainIntel with all DNS findings
    """
    domain = _extract_domain(domain_or_url) or domain_or_url
    if not domain:
        return None

    loop = asyncio.get_event_loop()

    # Run DNS queries in thread pool (they're blocking I/O)
    try:
        provider, mx_hosts = await loop.run_in_executor(None, get_email_provider, domain)
        soa_email = await loop.run_in_executor(None, get_soa_email, domain)
        spf = await loop.run_in_executor(None, get_spf_record, domain)
        dmarc = await loop.run_in_executor(None, get_dmarc_record, domain)
    except Exception as e:
        logger.debug(f"DNS analysis failed for {domain}: {e}")
        return None

    intel = DomainIntel(
        domain=domain,
        email_provider=provider,
        mx_records=mx_hosts,
        soa_email=soa_email,
        spf_record=spf,
        dmarc_record=dmarc,
    )

    if soa_email:
        logger.info(f"DNS SOA email for {domain}: {soa_email}")

    return intel


async def batch_dns_analysis(
    domains: list[str],
    concurrency: int = 10,
) -> list[tuple[str, Optional[DomainIntel]]]:
    """Run DNS analysis for multiple domains concurrently.

    DNS queries are fast and have no meaningful rate limits.

    Returns:
        List of (domain, DomainIntel or None)
    """
    sem = asyncio.Semaphore(concurrency)

    async def analyze_one(domain: str):
        async with sem:
            intel = await analyze_domain(domain)
            return domain, intel

    tasks = [analyze_one(d) for d in domains]
    results = await asyncio.gather(*tasks)

    providers = {}
    for _, intel in results:
        if intel and intel.email_provider:
            p = intel.email_provider
            providers[p] = providers.get(p, 0) + 1

    logger.info(f"DNS batch: {len(domains)} domains analyzed. Providers: {providers}")
    return list(results)
