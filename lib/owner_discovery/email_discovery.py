"""Email discovery and verification for hotel decision makers.

Given a name and a hotel domain, generates candidate email addresses
using common patterns, then verifies them via SMTP and O365 autodiscover.

All methods are free. No paid APIs required.
"""

import asyncio
import re
import smtplib
import socket
from typing import Optional

import httpx
from loguru import logger

from services.enrichment.owner_models import DecisionMaker

# Common email patterns for hotels
ROLE_EMAILS = ["gm", "owner", "manager", "director", "management"]

# Pattern generators given first_name and last_name
def _generate_patterns(first: str, last: str, domain: str) -> list[str]:
    """Generate candidate email addresses from a person's name."""
    first = first.lower().strip()
    last = last.lower().strip()
    return [
        f"{first}.{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{first}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"{first[0]}.{last}@{domain}",
        f"{last}@{domain}",
        f"{first}{last[0]}@{domain}",
    ]


def _get_mx_host(domain: str) -> Optional[str]:
    """Get the primary MX host for a domain using dnspython."""
    try:
        import dns.resolver
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5
        resolver.lifetime = 10
        mx_records = resolver.resolve(domain, "MX")
        sorted_mx = sorted(mx_records, key=lambda r: r.preference)
        return str(sorted_mx[0].exchange).rstrip(".")
    except Exception:
        return None


async def _smtp_verify(
    email: str,
    mx_host: str,
    from_addr: str = "verify@example.com",
) -> Optional[str]:
    """Verify an email exists using SMTP RCPT TO.

    Returns: "exists", "not_found", "catch_all", or None on error.
    Warning: Increasingly unreliable as servers block this.
    """
    loop = asyncio.get_event_loop()

    def _verify():
        try:
            server = smtplib.SMTP(timeout=10)
            server.connect(mx_host, 25)
            server.ehlo("verify.example.com")
            server.mail(from_addr)
            code, _ = server.rcpt(email)
            server.quit()
            if code == 250:
                return "exists"
            elif code == 550:
                return "not_found"
            else:
                return None
        except Exception:
            return None

    return await loop.run_in_executor(None, _verify)


async def _detect_catch_all(mx_host: str, domain: str) -> bool:
    """Detect if a domain is catch-all by testing a definitely-fake address."""
    fake_email = f"xz9q7k2m_nonexistent_99999@{domain}"
    result = await _smtp_verify(fake_email, mx_host)
    return result == "exists"


async def verify_o365_email(
    client: httpx.AsyncClient,
    email: str,
) -> Optional[str]:
    """Verify email exists in Office 365 via GetCredentialType API.

    Microsoft does not consider this a vulnerability. Unthrottled.

    Returns: "exists", "not_found", or None on error.
    """
    try:
        resp = await client.post(
            "https://login.microsoftonline.com/common/GetCredentialType",
            json={"Username": email},
            timeout=10.0,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        # IfExistsResult: 0=exists, 1=doesn't exist, 5=exists (different IdP), 6=exists
        result = data.get("IfExistsResult")
        if result in (0, 5, 6):
            return "exists"
        elif result == 1:
            return "not_found"
        return None
    except Exception:
        return None


async def discover_emails(
    domain: str,
    full_name: Optional[str] = None,
    email_provider: Optional[str] = None,
) -> list[dict]:
    """Discover valid email addresses for a hotel domain.

    Args:
        domain: Hotel domain (e.g., "grandhotel.com")
        full_name: Optional person name to generate personal patterns
        email_provider: Optional provider hint from DNS analysis

    Returns:
        List of dicts: [{"email": "...", "verified": True/False, "method": "..."}]
    """
    candidates = []

    # Generate role-based candidates
    for role in ROLE_EMAILS:
        candidates.append(f"{role}@{domain}")

    # Generate name-based candidates if we have a name
    if full_name:
        parts = full_name.strip().split()
        if len(parts) >= 2:
            first = parts[0]
            last = parts[-1]
            candidates.extend(_generate_patterns(first, last, domain))

    candidates = list(dict.fromkeys(candidates))  # Deduplicate preserving order
    results = []

    logger.debug(
        f"Email discovery for {domain}: {len(candidates)} candidates | "
        f"provider={email_provider} | name={full_name}"
    )

    # Determine verification method based on email provider
    if email_provider == "microsoft_365":
        # Use O365 autodiscover (most reliable, unthrottled) — all in parallel
        logger.debug(f"Email {domain}: using O365 GetCredentialType verification")
        async with httpx.AsyncClient() as client:
            statuses = await asyncio.gather(
                *[verify_o365_email(client, email) for email in candidates]
            )
            for email, status in zip(candidates, statuses):
                if status == "exists":
                    logger.debug(f"Email {domain}: O365 verified {email}")
                    results.append({"email": email, "verified": True, "method": "o365_autodiscover"})
                elif status == "not_found":
                    continue
                else:
                    results.append({"email": email, "verified": False, "method": "o365_unknown"})
    else:
        # Try SMTP verification
        mx_host = await asyncio.get_event_loop().run_in_executor(
            None, _get_mx_host, domain
        )
        if mx_host:
            is_catch_all = await _detect_catch_all(mx_host, domain)
            if is_catch_all:
                logger.debug(f"Email {domain}: catch-all domain (mx={mx_host}), skipping SMTP verification")
                for email in candidates[:5]:
                    results.append({"email": email, "verified": False, "method": "catch_all_domain"})
            else:
                logger.debug(f"Email {domain}: SMTP verification via {mx_host}")
                statuses = await asyncio.gather(
                    *[_smtp_verify(email, mx_host) for email in candidates]
                )
                for email, status in zip(candidates, statuses):
                    if status == "exists":
                        logger.debug(f"Email {domain}: SMTP verified {email}")
                        results.append({"email": email, "verified": True, "method": "smtp_rcpt"})
        else:
            logger.debug(f"Email {domain}: no MX record, can't verify")
            for email in candidates[:3]:
                results.append({"email": email, "verified": False, "method": "no_mx"})

    logger.debug(
        f"Email discovery for {domain}: {len(results)} results "
        f"({sum(1 for r in results if r['verified'])} verified)"
    )
    return results


async def enrich_decision_maker_email(
    dm: DecisionMaker,
    domain: str,
    email_provider: Optional[str] = None,
) -> DecisionMaker:
    """Try to find/verify email for a DecisionMaker who doesn't have one.

    Mutates and returns the same DecisionMaker object.
    """
    if dm.email and dm.email_verified:
        return dm  # Already has verified email

    discovered = await discover_emails(
        domain=domain,
        full_name=dm.full_name,
        email_provider=email_provider,
    )

    if not discovered:
        return dm

    # Prefer verified personal emails over unverified ones
    verified = [d for d in discovered if d["verified"]]
    personal_verified = [
        d for d in verified
        if not any(d["email"].lower().startswith(r + "@") for r in ROLE_EMAILS)
    ]

    if personal_verified:
        dm.email = personal_verified[0]["email"]
        dm.email_verified = True
    elif verified:
        dm.email = verified[0]["email"]
        dm.email_verified = True
    elif not dm.email:
        # Use best unverified candidate
        dm.email = discovered[0]["email"]
        dm.email_verified = False

    return dm
