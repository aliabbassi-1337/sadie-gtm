"""Owner enrichment waterfall orchestrator.

Runs multiple enrichment layers in sequence for each hotel:
  1. RDAP domain lookup → registrant name/email
  2. Historical WHOIS via Wayback Machine → pre-GDPR registrant data
  3. DNS intelligence → email provider, SOA admin email
  4. Website scraping → /about, /team, /contact pages + LLM extraction
  5. Google review mining → owner/GM names from review responses
  6. Email verification → SMTP/O365 autodiscover

Each layer adds to the hotel's decision_makers list. Stops early
if high-confidence results found. Caches domain intel for reuse.
"""

import asyncio
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
from lib.owner_discovery.whois_history import whois_history_to_decision_maker
from lib.owner_discovery.dns_intel import analyze_domain
from lib.owner_discovery.website_scraper import scrape_hotel_website, extract_from_google_reviews
from lib.owner_discovery.email_discovery import enrich_decision_maker_email
from services.enrichment import owner_repo as repo


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

    result = OwnerEnrichmentResult(hotel_id=hotel_id, domain=domain)

    if not domain:
        result.error = "no_domain"
        return result

    domain_intel = DomainIntel(domain=domain)

    try:
        # === Layer 1: RDAP ===
        if layers & LAYER_RDAP:
            dm, rdap_intel = await rdap_to_decision_maker(client, domain)
            if rdap_intel:
                domain_intel = rdap_intel
                await repo.cache_domain_intel(rdap_intel)
            if dm:
                result.decision_makers.append(dm)
                logger.info(f"[{hotel_id}] RDAP hit: {dm.full_name}")
            result.layers_completed |= LAYER_RDAP

        # === Layer 2: Historical WHOIS ===
        if layers & LAYER_WHOIS_HISTORY:
            # Only run if RDAP didn't find anything (privacy protected)
            if not result.found_any or domain_intel.is_privacy_protected:
                dm = await whois_history_to_decision_maker(client, domain)
                if dm:
                    result.decision_makers.append(dm)
                    logger.info(f"[{hotel_id}] Historical WHOIS hit: {dm.full_name}")
            result.layers_completed |= LAYER_WHOIS_HISTORY

        # === Layer 3: DNS Intelligence ===
        if layers & LAYER_DNS:
            dns_intel = await analyze_domain(domain)
            if dns_intel:
                domain_intel.email_provider = dns_intel.email_provider
                domain_intel.mx_records = dns_intel.mx_records
                domain_intel.soa_email = dns_intel.soa_email
                domain_intel.spf_record = dns_intel.spf_record
                domain_intel.dmarc_record = dns_intel.dmarc_record
                await repo.cache_dns_intel(dns_intel)

                # SOA email might be a real person
                if dns_intel.soa_email:
                    result.decision_makers.append(DecisionMaker(
                        full_name=None,
                        title=None,
                        email=dns_intel.soa_email,
                        source="dns_soa",
                        confidence=0.3,
                        raw_source_url=f"dns://{domain}/SOA",
                    ))
            result.layers_completed |= LAYER_DNS

        # === Layer 4: Website Scraping ===
        if layers & LAYER_WEBSITE:
            try:
                website_dms = await scrape_hotel_website(client, website, name)
                result.decision_makers.extend(website_dms)
                if website_dms:
                    logger.info(
                        f"[{hotel_id}] Website scrape: {len(website_dms)} contacts found"
                    )
            except Exception as e:
                logger.debug(f"[{hotel_id}] Website scrape error: {e}")
            result.layers_completed |= LAYER_WEBSITE

        # === Layer 5: Google Review Responses ===
        if layers & LAYER_REVIEWS:
            try:
                review_dms = await extract_from_google_reviews(
                    client, name, city, state
                )
                result.decision_makers.extend(review_dms)
                if review_dms:
                    logger.info(
                        f"[{hotel_id}] Review mining: {len(review_dms)} contacts found"
                    )
            except Exception as e:
                logger.debug(f"[{hotel_id}] Review mining error: {e}")
            result.layers_completed |= LAYER_REVIEWS

        # === Layer 6: Email Verification ===
        if layers & LAYER_EMAIL_VERIFY:
            for dm in result.decision_makers:
                if dm.full_name and (not dm.email or not dm.email_verified):
                    try:
                        await enrich_decision_maker_email(
                            dm, domain, domain_intel.email_provider
                        )
                    except Exception as e:
                        logger.debug(f"[{hotel_id}] Email verification error: {e}")
            result.layers_completed |= LAYER_EMAIL_VERIFY

    except Exception as e:
        result.error = str(e)
        logger.error(f"[{hotel_id}] Enrichment error: {e}")

    result.domain_intel = domain_intel

    # Deduplicate decision makers by name
    seen = set()
    unique = []
    for dm in result.decision_makers:
        key = ((dm.full_name or "").lower(), (dm.title or "").lower())
        if key not in seen:
            seen.add(key)
            unique.append(dm)
    result.decision_makers = unique

    return result


async def enrich_batch(
    hotels: list[dict],
    concurrency: int = 5,
    layers: int = 0xFF,
) -> list[OwnerEnrichmentResult]:
    """Run owner enrichment for a batch of hotels.

    Args:
        hotels: List of hotel dicts from owner_repo.get_hotels_pending_owner_enrichment()
        concurrency: Max concurrent enrichments
        layers: Bitmask of layers to run

    Returns:
        List of OwnerEnrichmentResult
    """
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
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Batch enrichment exception: {r}")
        else:
            clean_results.append(r)

    # Persist results
    for result in clean_results:
        if result.decision_makers:
            count = await repo.batch_insert_decision_makers(
                result.hotel_id, result.decision_makers
            )
            status = 1  # complete
            logger.info(
                f"[{result.hotel_id}] Saved {count} decision makers"
            )
        else:
            status = 2  # no_results

        await repo.update_enrichment_status(
            result.hotel_id, status, result.layers_completed
        )

    # Summary
    total = len(clean_results)
    found = sum(1 for r in clean_results if r.found_any)
    total_contacts = sum(len(r.decision_makers) for r in clean_results)
    logger.info(
        f"Batch complete: {found}/{total} hotels had owner data "
        f"({total_contacts} total contacts)"
    )

    return clean_results
