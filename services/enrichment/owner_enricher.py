"""Owner enrichment waterfall orchestrator.

Runs multiple enrichment layers in sequence for each hotel:
  1. RDAP domain lookup → registrant name/email (structured JSON protocol)
  2. WHOIS lookup → live python-whois (socket), then Wayback Machine fallback
  3. DNS intelligence → email provider, SOA admin email
  4. Website scraping → /about, /team, /contact pages + LLM extraction
  5. Google review mining → owner/GM names from review responses (Serper)
  6. Email verification → SMTP/O365 autodiscover

Each layer adds to the hotel's decision_makers list. Stops early
if high-confidence results found. Caches domain intel for reuse.
"""

import asyncio
import time
from typing import Optional
from urllib.parse import urlparse

import httpx
from loguru import logger

from lib.owner_discovery.models import (
    DecisionMaker, DomainIntel, OwnerEnrichmentResult,
    LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
    LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
)
from lib.owner_discovery.rdap_client import rdap_to_decision_maker
from lib.owner_discovery.whois_history import whois_lookup, whois_to_decision_maker
from lib.owner_discovery.dns_intel import analyze_domain
from lib.owner_discovery.website_scraper import scrape_hotel_website, extract_from_google_reviews
from lib.owner_discovery.email_discovery import enrich_decision_maker_email


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


async def enrich_single_hotel(
    client: httpx.AsyncClient,
    hotel: dict,
    layers: int = 0xFF,  # All layers by default
    skip_cache: bool = False,
) -> OwnerEnrichmentResult:
    """Run the owner enrichment waterfall for a single hotel.

    Args:
        client: httpx async client (shared across batch)
        hotel: Dict with hotel_id, name, website, city, state, country
        layers: Bitmask of which layers to run
        skip_cache: If True, re-run even if cached results exist

    Returns:
        OwnerEnrichmentResult with all discovered decision makers
    """
    hotel_id = hotel["hotel_id"]
    website = hotel.get("website", "")
    name = hotel.get("name", "")
    city = hotel.get("city")
    state = hotel.get("state")
    domain = _extract_domain(website)

    tag = f"[{hotel_id}|{domain or 'no-domain'}]"
    t_start = time.monotonic()

    result = OwnerEnrichmentResult(hotel_id=hotel_id, domain=domain)

    if not domain:
        result.error = "no_domain"
        logger.warning(f"{tag} Skipped — no domain extracted from website: {website!r}")
        return result

    logger.info(f"{tag} Starting enrichment waterfall | hotel={name!r} layers=0b{layers:06b}")
    domain_intel = DomainIntel(domain=domain)

    try:
        # === Layer 1: RDAP ===
        if layers & LAYER_RDAP:
            t0 = time.monotonic()
            logger.debug(f"{tag} [1/6 RDAP] Querying rdap.org for {domain}")
            dm, rdap_intel = await rdap_to_decision_maker(client, domain)
            elapsed = time.monotonic() - t0
            if rdap_intel:
                domain_intel = rdap_intel
                if rdap_intel.is_privacy_protected:
                    logger.info(
                        f"{tag} [1/6 RDAP] Privacy-protected "
                        f"(registrar={rdap_intel.registrar}) [{elapsed:.1f}s]"
                    )
                elif dm:
                    result.decision_makers.append(dm)
                    logger.info(
                        f"{tag} [1/6 RDAP] HIT: {dm.full_name} | {dm.email} "
                        f"(registrar={rdap_intel.registrar}) [{elapsed:.1f}s]"
                    )
                else:
                    logger.info(
                        f"{tag} [1/6 RDAP] No registrant data "
                        f"(registrar={rdap_intel.registrar}) [{elapsed:.1f}s]"
                    )
            else:
                logger.info(f"{tag} [1/6 RDAP] No response [{elapsed:.1f}s]")
            result.layers_completed |= LAYER_RDAP

        # === Layer 2: WHOIS (live python-whois + Wayback fallback) ===
        if layers & LAYER_WHOIS_HISTORY:
            t0 = time.monotonic()
            if result.found_any and not domain_intel.is_privacy_protected:
                logger.info(
                    f"{tag} [2/6 WHOIS] Skipped — RDAP already found registrant"
                )
            else:
                logger.debug(
                    f"{tag} [2/6 WHOIS] Running live WHOIS + Wayback fallback for {domain}"
                )
                whois_intel = await whois_lookup(client, domain)
                elapsed = time.monotonic() - t0
                if whois_intel and not whois_intel.is_privacy_protected:
                    if not domain_intel.registrant_name:
                        domain_intel.registrant_name = whois_intel.registrant_name
                    if not domain_intel.registrant_org:
                        domain_intel.registrant_org = whois_intel.registrant_org
                    if not domain_intel.registrant_email:
                        domain_intel.registrant_email = whois_intel.registrant_email
                    if not domain_intel.registrar:
                        domain_intel.registrar = whois_intel.registrar
                    dm = whois_to_decision_maker(whois_intel)
                    if dm:
                        result.decision_makers.append(dm)
                        logger.info(
                            f"{tag} [2/6 WHOIS] HIT via {whois_intel.whois_source}: "
                            f"{dm.full_name} | {dm.email} [{elapsed:.1f}s]"
                        )
                    else:
                        logger.info(
                            f"{tag} [2/6 WHOIS] Data found but no usable contact "
                            f"(src={whois_intel.whois_source}) [{elapsed:.1f}s]"
                        )
                elif whois_intel:
                    logger.info(
                        f"{tag} [2/6 WHOIS] Privacy-protected "
                        f"(registrar={whois_intel.registrar}) [{elapsed:.1f}s]"
                    )
                else:
                    logger.info(f"{tag} [2/6 WHOIS] No data returned [{elapsed:.1f}s]")
            result.layers_completed |= LAYER_WHOIS_HISTORY

        # === Layer 3: DNS Intelligence ===
        if layers & LAYER_DNS:
            t0 = time.monotonic()
            logger.debug(f"{tag} [3/6 DNS] Querying MX/SOA/SPF/DMARC for {domain}")
            dns_intel = await analyze_domain(domain)
            elapsed = time.monotonic() - t0
            if dns_intel:
                domain_intel.email_provider = dns_intel.email_provider
                domain_intel.mx_records = dns_intel.mx_records
                domain_intel.soa_email = dns_intel.soa_email
                domain_intel.spf_record = dns_intel.spf_record
                domain_intel.dmarc_record = dns_intel.dmarc_record

                parts = []
                if dns_intel.email_provider:
                    parts.append(f"provider={dns_intel.email_provider}")
                if dns_intel.mx_records:
                    parts.append(f"mx={len(dns_intel.mx_records)}")
                if dns_intel.soa_email:
                    parts.append(f"soa={dns_intel.soa_email}")
                    result.decision_makers.append(DecisionMaker(
                        full_name=None,
                        title=None,
                        email=dns_intel.soa_email,
                        source="dns_soa",
                        confidence=0.3,
                        raw_source_url=f"dns://{domain}/SOA",
                    ))
                if dns_intel.spf_record:
                    parts.append("spf=yes")
                if dns_intel.dmarc_record:
                    parts.append("dmarc=yes")
                logger.info(
                    f"{tag} [3/6 DNS] {' | '.join(parts) or 'no records'} [{elapsed:.1f}s]"
                )
            else:
                logger.info(f"{tag} [3/6 DNS] No DNS data [{elapsed:.1f}s]")
            result.layers_completed |= LAYER_DNS

        # === Layer 4: Website Scraping ===
        if layers & LAYER_WEBSITE:
            t0 = time.monotonic()
            logger.debug(f"{tag} [4/6 Website] Scraping {website}")
            try:
                website_dms = await scrape_hotel_website(client, website, name)
                elapsed = time.monotonic() - t0
                if website_dms:
                    result.decision_makers.extend(website_dms)
                    for dm in website_dms:
                        logger.info(
                            f"{tag} [4/6 Website] HIT: {dm.full_name} | "
                            f"{dm.title} | {dm.email} (src={dm.source}, "
                            f"conf={dm.confidence})"
                        )
                    logger.info(
                        f"{tag} [4/6 Website] {len(website_dms)} contacts [{elapsed:.1f}s]"
                    )
                else:
                    logger.info(f"{tag} [4/6 Website] No contacts found [{elapsed:.1f}s]")
            except Exception as e:
                elapsed = time.monotonic() - t0
                logger.warning(f"{tag} [4/6 Website] Error: {e} [{elapsed:.1f}s]")
            result.layers_completed |= LAYER_WEBSITE

        # === Layer 5: Google Review Responses ===
        if layers & LAYER_REVIEWS:
            t0 = time.monotonic()
            logger.debug(
                f"{tag} [5/6 Reviews] Searching Google reviews for {name!r} "
                f"in {city}, {state}"
            )
            try:
                review_dms = await extract_from_google_reviews(
                    client, name, city, state
                )
                elapsed = time.monotonic() - t0
                if review_dms:
                    result.decision_makers.extend(review_dms)
                    for dm in review_dms:
                        logger.info(
                            f"{tag} [5/6 Reviews] HIT: {dm.full_name} | "
                            f"{dm.title} (conf={dm.confidence})"
                        )
                    logger.info(
                        f"{tag} [5/6 Reviews] {len(review_dms)} contacts [{elapsed:.1f}s]"
                    )
                else:
                    logger.info(f"{tag} [5/6 Reviews] No contacts found [{elapsed:.1f}s]")
            except Exception as e:
                elapsed = time.monotonic() - t0
                logger.warning(f"{tag} [5/6 Reviews] Error: {e} [{elapsed:.1f}s]")
            result.layers_completed |= LAYER_REVIEWS

        # === Layer 6: Email Verification ===
        if layers & LAYER_EMAIL_VERIFY:
            t0 = time.monotonic()
            candidates = [
                dm for dm in result.decision_makers
                if dm.full_name and (not dm.email or not dm.email_verified)
            ]
            if candidates:
                logger.debug(
                    f"{tag} [6/6 Email] Verifying emails for {len(candidates)} "
                    f"contacts (provider={domain_intel.email_provider})"
                )
                for dm in candidates:
                    try:
                        before_email = dm.email
                        await enrich_decision_maker_email(
                            dm, domain, domain_intel.email_provider
                        )
                        if dm.email_verified:
                            logger.info(
                                f"{tag} [6/6 Email] Verified: {dm.full_name} → "
                                f"{dm.email}"
                            )
                        elif dm.email and dm.email != before_email:
                            logger.info(
                                f"{tag} [6/6 Email] Guessed (unverified): "
                                f"{dm.full_name} → {dm.email}"
                            )
                        else:
                            logger.debug(
                                f"{tag} [6/6 Email] No email found for {dm.full_name}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"{tag} [6/6 Email] Error verifying {dm.full_name}: {e}"
                        )
                elapsed = time.monotonic() - t0
                logger.info(
                    f"{tag} [6/6 Email] Processed {len(candidates)} contacts [{elapsed:.1f}s]"
                )
            else:
                logger.info(
                    f"{tag} [6/6 Email] Skipped — no contacts need email verification"
                )
            result.layers_completed |= LAYER_EMAIL_VERIFY

    except Exception as e:
        result.error = str(e)
        logger.error(f"{tag} Enrichment error: {e}", exc_info=True)

    result.domain_intel = domain_intel

    # Deduplicate decision makers by name
    before_dedup = len(result.decision_makers)
    seen = set()
    unique = []
    for dm in result.decision_makers:
        key = ((dm.full_name or "").lower(), (dm.title or "").lower())
        if key not in seen:
            seen.add(key)
            unique.append(dm)
    result.decision_makers = unique

    total_elapsed = time.monotonic() - t_start
    if before_dedup != len(unique):
        logger.debug(f"{tag} Deduped {before_dedup} → {len(unique)} contacts")

    logger.info(
        f"{tag} Done: {len(unique)} contacts | "
        f"layers=0b{result.layers_completed:06b} | {total_elapsed:.1f}s"
    )

    return result


async def enrich_batch(
    hotels: list[dict],
    concurrency: int = 5,
    layers: int = 0xFF,
) -> list[OwnerEnrichmentResult]:
    """Run owner enrichment for a batch of hotels.

    Args:
        hotels: List of hotel dicts with hotel_id, name, website, city, state, country
        concurrency: Max concurrent enrichments
        layers: Bitmask of layers to run

    Returns:
        List of OwnerEnrichmentResult
    """
    t_batch_start = time.monotonic()
    logger.info(
        f"Batch starting: {len(hotels)} hotels | "
        f"concurrency={concurrency} | layers=0b{layers:06b}"
    )

    sem = asyncio.Semaphore(concurrency)
    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def process_one(hotel: dict):
            async with sem:
                return await enrich_single_hotel(client, hotel, layers=layers)

        tasks = [process_one(h) for h in hotels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions and log them
    clean_results = []
    errors = 0
    for r in results:
        if isinstance(r, Exception):
            errors += 1
            logger.error(f"Batch enrichment exception: {r}")
        else:
            clean_results.append(r)

    if errors:
        logger.warning(f"Batch had {errors} uncaught exceptions out of {len(hotels)} hotels")

    # Summary
    batch_elapsed = time.monotonic() - t_batch_start
    total = len(clean_results)
    found = sum(1 for r in clean_results if r.found_any)
    total_contacts = sum(len(r.decision_makers) for r in clean_results)

    # Per-source breakdown
    source_counts: dict[str, int] = {}
    for r in clean_results:
        for dm in r.decision_makers:
            source_counts[dm.source] = source_counts.get(dm.source, 0) + 1

    logger.info(
        f"Batch complete: {found}/{total} hotels had contacts | "
        f"{total_contacts} contacts found | {batch_elapsed:.1f}s total"
    )
    if source_counts:
        breakdown = " | ".join(f"{src}={n}" for src, n in sorted(source_counts.items()))
        logger.info(f"Batch source breakdown: {breakdown}")

    return clean_results
