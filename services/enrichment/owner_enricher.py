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
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from loguru import logger

from services.enrichment.owner_models import (
    DecisionMaker, DomainIntel, OwnerEnrichmentResult,
    LAYER_RDAP, LAYER_WHOIS_HISTORY, LAYER_DNS,
    LAYER_WEBSITE, LAYER_REVIEWS, LAYER_EMAIL_VERIFY,
    LAYER_GOV_DATA, LAYER_CT_CERTS, LAYER_ABN_ASIC,
)
from lib.proxy import CfWorkerProxy
from lib.owner_discovery.ct_intelligence import ct_to_decision_makers
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


# ── Layer functions ─────────────────────────────────────────────────


async def _run_ct(
    client: httpx.AsyncClient, tag: str, domain: str,
    cf_proxy: Optional[CfWorkerProxy] = None,
) -> Tuple[List[DecisionMaker], Optional[DomainIntel]]:
    """Layer 0: CT Certificate Intelligence."""
    t0 = time.monotonic()
    logger.debug(f"{tag} [0/7 CT] Querying crt.sh for {domain}")
    dms, ct_intel = await ct_to_decision_makers(client, domain, cf_proxy=cf_proxy)
    elapsed = time.monotonic() - t0

    domain_intel_partial = None
    if ct_intel:
        domain_intel_partial = DomainIntel(
            domain=domain,
            ct_org_name=ct_intel.org_name,
            ct_alt_domains=ct_intel.alt_domains,
            ct_cert_count=ct_intel.cert_count,
        )
        if ct_intel.org_name:
            logger.info(
                f"{tag} [0/7 CT] HIT: org={ct_intel.org_name} | "
                f"{ct_intel.cert_count} certs | "
                f"{len(ct_intel.alt_domains)} alt domains [{elapsed:.1f}s]"
            )
        else:
            logger.info(
                f"{tag} [0/7 CT] {ct_intel.cert_count} certs, "
                f"no OV/EV org found [{elapsed:.1f}s]"
            )
    else:
        logger.info(f"{tag} [0/7 CT] No response [{elapsed:.1f}s]")

    return dms, domain_intel_partial


async def _run_rdap(
    client: httpx.AsyncClient, tag: str, domain: str,
    cf_proxy: Optional[CfWorkerProxy] = None,
) -> Tuple[List[DecisionMaker], Optional[DomainIntel]]:
    """Layer 1: RDAP domain lookup."""
    t0 = time.monotonic()
    logger.debug(f"{tag} [1/6 RDAP] Querying rdap.org for {domain}")
    dm, rdap_intel = await rdap_to_decision_maker(client, domain, cf_proxy=cf_proxy)
    elapsed = time.monotonic() - t0

    dms = []
    if rdap_intel:
        if rdap_intel.is_privacy_protected:
            logger.info(
                f"{tag} [1/6 RDAP] Privacy-protected "
                f"(registrar={rdap_intel.registrar}) [{elapsed:.1f}s]"
            )
        elif dm:
            dms.append(dm)
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

    return dms, rdap_intel


async def _run_whois(
    client: httpx.AsyncClient, tag: str, domain: str,
    already_found: bool, domain_intel: DomainIntel,
    cf_proxy: Optional[CfWorkerProxy] = None,
) -> Tuple[List[DecisionMaker], DomainIntel]:
    """Layer 2: WHOIS (live python-whois + Wayback fallback)."""
    t0 = time.monotonic()
    dms = []

    if already_found and not domain_intel.is_privacy_protected:
        logger.info(f"{tag} [2/6 WHOIS] Skipped — RDAP already found registrant")
        return dms, domain_intel

    logger.debug(f"{tag} [2/6 WHOIS] Running live WHOIS + Wayback fallback for {domain}")
    whois_intel = await whois_lookup(client, domain, cf_proxy=cf_proxy)
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
            dms.append(dm)
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

    return dms, domain_intel


async def _run_dns(
    tag: str, domain: str, domain_intel: DomainIntel,
) -> Tuple[List[DecisionMaker], DomainIntel]:
    """Layer 3: DNS intelligence (MX, SOA, SPF, DMARC)."""
    t0 = time.monotonic()
    logger.debug(f"{tag} [3/6 DNS] Querying MX/SOA/SPF/DMARC for {domain}")
    dns_intel = await analyze_domain(domain)
    elapsed = time.monotonic() - t0
    dms = []

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
            dms.append(DecisionMaker(
                full_name=None,
                title=None,
                email=dns_intel.soa_email,
                sources=["dns_soa"],
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

    return dms, domain_intel


async def _run_website(
    client: httpx.AsyncClient, tag: str, website: str, name: str,
) -> List[DecisionMaker]:
    """Layer 4: Website scraping (/about, /team, /contact + LLM)."""
    t0 = time.monotonic()
    logger.debug(f"{tag} [4/6 Website] Scraping {website}")
    try:
        website_dms = await scrape_hotel_website(client, website, name)
        elapsed = time.monotonic() - t0
        if website_dms:
            for dm in website_dms:
                logger.info(
                    f"{tag} [4/6 Website] HIT: {dm.full_name} | "
                    f"{dm.title} | {dm.email} (src={dm.sources}, "
                    f"conf={dm.confidence})"
                )
            logger.info(
                f"{tag} [4/6 Website] {len(website_dms)} contacts [{elapsed:.1f}s]"
            )
            return website_dms
        else:
            logger.info(f"{tag} [4/6 Website] No contacts found [{elapsed:.1f}s]")
            return []
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.warning(f"{tag} [4/6 Website] Error: {e} [{elapsed:.1f}s]")
        return []


async def _run_reviews(
    client: httpx.AsyncClient, tag: str,
    name: str, city: Optional[str], state: Optional[str],
) -> List[DecisionMaker]:
    """Layer 5: Google review owner responses (Serper)."""
    t0 = time.monotonic()
    logger.debug(
        f"{tag} [5/6 Reviews] Searching Google reviews for {name!r} "
        f"in {city}, {state}"
    )
    try:
        review_dms = await extract_from_google_reviews(client, name, city, state)
        elapsed = time.monotonic() - t0
        if review_dms:
            for dm in review_dms:
                logger.info(
                    f"{tag} [5/6 Reviews] HIT: {dm.full_name} | "
                    f"{dm.title} (conf={dm.confidence})"
                )
            logger.info(
                f"{tag} [5/6 Reviews] {len(review_dms)} contacts [{elapsed:.1f}s]"
            )
            return review_dms
        else:
            logger.info(f"{tag} [5/6 Reviews] No contacts found [{elapsed:.1f}s]")
            return []
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.warning(f"{tag} [5/6 Reviews] Error: {e} [{elapsed:.1f}s]")
        return []


async def _run_email_verify(
    tag: str, candidates: List[DecisionMaker],
    domain: str, email_provider: Optional[str],
) -> None:
    """Layer 6: Email verification (SMTP/O365). Mutates candidates in place."""
    t0 = time.monotonic()
    if not candidates:
        logger.info(f"{tag} [6/6 Email] Skipped — no contacts need email verification")
        return

    logger.debug(
        f"{tag} [6/6 Email] Verifying emails for {len(candidates)} "
        f"contacts (provider={email_provider})"
    )
    for dm in candidates:
        try:
            before_email = dm.email
            await enrich_decision_maker_email(dm, domain, email_provider)
            if dm.email_verified:
                logger.info(
                    f"{tag} [6/6 Email] Verified: {dm.full_name} → {dm.email}"
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


async def _run_gov_data(
    tag: str, hotel_id: int, name: str,
    city: Optional[str], state: Optional[str],
) -> List[DecisionMaker]:
    """Layer 7: Government data lookup (DBPR, Texas tax, etc.)."""
    from services.enrichment import repo

    t0 = time.monotonic()
    if not city or not state:
        logger.info(f"{tag} [7 GOV] Skipped — no city/state")
        return []

    logger.debug(f"{tag} [7 GOV] Querying gov records for {name!r} in {city}, {state}")
    matches = await repo.find_gov_matches(hotel_id, name, city, state)
    elapsed = time.monotonic() - t0

    if not matches:
        logger.info(f"{tag} [7 GOV] No matching gov records [{elapsed:.1f}s]")
        return []

    dms = []
    for m in matches:
        gov_name = m["name"]
        phone = m.get("phone_google") or m.get("phone_website")
        email = m.get("email")
        source_type = m.get("source", "gov_registry")
        ext_id = m.get("external_id", "")

        # Build a DecisionMaker from the gov record
        dm = DecisionMaker(
            full_name=gov_name,
            title="Registered Business Entity",
            email=email,
            phone=phone,
            sources=[f"gov_{source_type}"],
            confidence=0.9,
            raw_source_url=f"gov://{source_type}/{ext_id}" if ext_id else None,
        )
        dms.append(dm)
        logger.info(
            f"{tag} [7 GOV] HIT: {gov_name} | {email} | "
            f"phone={phone} (src={source_type}, id={ext_id})"
        )

    logger.info(f"{tag} [7 GOV] {len(dms)} gov matches [{elapsed:.1f}s]")
    return dms


async def _run_abn_asic(
    client: httpx.AsyncClient, tag: str,
    name: str, state: Optional[str], country: Optional[str],
    domain: Optional[str],
) -> List[DecisionMaker]:
    """Layer 8: ABN Lookup + ASIC director enrichment (Australian hotels only)."""
    from lib.owner_discovery.abn_lookup import abn_to_decision_makers
    from lib.owner_discovery.asic_lookup import asic_to_decision_makers
    from services.enrichment import repo

    t0 = time.monotonic()

    # Only run for Australian hotels
    is_au = (
        (country and country.upper() in {"AU", "AUS", "AUSTRALIA"})
        or (domain and domain.endswith(".com.au"))
        or (domain and domain.endswith(".au"))
    )
    if not is_au:
        logger.debug(f"{tag} [8 ABN/ASIC] Skipped — not Australian (country={country}, domain={domain})")
        return []

    # Map Australian state names to codes for ABN search
    au_state = _normalize_au_state(state) if state else None

    logger.debug(f"{tag} [8 ABN/ASIC] Searching ABN for {name!r} (state={au_state})")

    dms, entity = await abn_to_decision_makers(client, name, state=au_state)
    elapsed = time.monotonic() - t0

    if not entity:
        logger.info(f"{tag} [8 ABN/ASIC] No ABN match for {name!r} [{elapsed:.1f}s]")
        return []

    # Cache the ABN entity
    try:
        await repo.cache_abn_entity(
            abn=entity.abn,
            entity_name=entity.entity_name,
            entity_type=entity.entity_type,
            status=entity.status,
            state=entity.state,
            postcode=entity.postcode,
            business_names=entity.business_names,
            acn=entity.acn,
        )
    except Exception as e:
        logger.warning(f"{tag} [8 ABN/ASIC] Cache write failed: {e}")

    # If company with ACN, try ASIC director lookup
    if entity.is_company and entity.acn:
        logger.debug(
            f"{tag} [8 ABN/ASIC] Company detected: {entity.entity_name} "
            f"(ACN={entity.acn}), querying ASIC for directors"
        )
        try:
            asic_dms = await asic_to_decision_makers(
                client, entity.acn, abn_entity_name=entity.entity_name,
            )
            if asic_dms:
                dms.extend(asic_dms)
                # Update cache with director names
                director_names = [d.full_name for d in asic_dms if d.full_name]
                try:
                    await repo.cache_abn_entity(
                        abn=entity.abn,
                        entity_name=entity.entity_name,
                        entity_type=entity.entity_type,
                        acn=entity.acn,
                        directors=director_names,
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"{tag} [8 ABN/ASIC] ASIC lookup failed for ACN {entity.acn}: {e}")

    elapsed = time.monotonic() - t0
    if dms:
        for dm in dms:
            logger.info(
                f"{tag} [8 ABN/ASIC] HIT: {dm.full_name} | {dm.title} "
                f"(conf={dm.confidence})"
            )
    logger.info(f"{tag} [8 ABN/ASIC] {len(dms)} contacts [{elapsed:.1f}s]")
    return dms


def _normalize_au_state(state: str) -> Optional[str]:
    """Normalize Australian state names to abbreviations."""
    if not state:
        return None
    s = state.strip().upper()
    mapping = {
        "NEW SOUTH WALES": "NSW", "NSW": "NSW",
        "VICTORIA": "VIC", "VIC": "VIC",
        "QUEENSLAND": "QLD", "QLD": "QLD",
        "SOUTH AUSTRALIA": "SA", "SA": "SA",
        "WESTERN AUSTRALIA": "WA", "WA": "WA",
        "TASMANIA": "TAS", "TAS": "TAS",
        "NORTHERN TERRITORY": "NT", "NT": "NT",
        "AUSTRALIAN CAPITAL TERRITORY": "ACT", "ACT": "ACT",
    }
    return mapping.get(s, s if len(s) <= 3 else None)


def _deduplicate(dms: List[DecisionMaker], tag: str) -> List[DecisionMaker]:
    """Deduplicate decision makers by (name, title)."""
    seen = set()
    unique = []
    for dm in dms:
        key = ((dm.full_name or "").lower(), (dm.title or "").lower())
        if key not in seen:
            seen.add(key)
            unique.append(dm)
    if len(dms) != len(unique):
        logger.debug(f"{tag} Deduped {len(dms)} → {len(unique)} contacts")
    return unique


# ── Orchestrator ────────────────────────────────────────────────────


async def enrich_single_hotel(
    client: httpx.AsyncClient,
    hotel: dict,
    layers: int = 0x1FF,
    skip_cache: bool = False,
    cf_proxy: Optional[CfWorkerProxy] = None,
) -> OwnerEnrichmentResult:
    """Run the owner enrichment waterfall for a single hotel.

    Args:
        client: httpx async client (shared across batch)
        hotel: Dict with hotel_id, name, website, city, state, country
        layers: Bitmask of which layers to run
        skip_cache: If True, re-run even if cached results exist
        cf_proxy: Optional CF Worker proxy to avoid rate limiting on RDAP/crt.sh/Wayback

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

    country = hotel.get("country")
    logger.info(f"{tag} Starting enrichment waterfall | hotel={name!r} layers=0b{layers:09b}")
    domain_intel = DomainIntel(domain=domain)

    try:
        # Layer 0: CT Certificate Intelligence
        if layers & LAYER_CT_CERTS:
            dms, ct_partial = await _run_ct(client, tag, domain, cf_proxy=cf_proxy)
            if ct_partial:
                domain_intel.ct_org_name = ct_partial.ct_org_name
                domain_intel.ct_alt_domains = ct_partial.ct_alt_domains
                domain_intel.ct_cert_count = ct_partial.ct_cert_count
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_CT_CERTS

        # Layer 1: RDAP
        if layers & LAYER_RDAP:
            dms, rdap_intel = await _run_rdap(client, tag, domain, cf_proxy=cf_proxy)
            if rdap_intel:
                domain_intel = rdap_intel
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_RDAP

        # Layer 2: WHOIS
        if layers & LAYER_WHOIS_HISTORY:
            dms, domain_intel = await _run_whois(
                client, tag, domain, result.found_any, domain_intel,
                cf_proxy=cf_proxy,
            )
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_WHOIS_HISTORY

        # Layer 3: DNS
        if layers & LAYER_DNS:
            dms, domain_intel = await _run_dns(tag, domain, domain_intel)
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_DNS

        # Layer 4: Website
        if layers & LAYER_WEBSITE:
            dms = await _run_website(client, tag, website, name)
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_WEBSITE

        # Layer 5: Reviews
        if layers & LAYER_REVIEWS:
            dms = await _run_reviews(client, tag, name, city, state)
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_REVIEWS

        # Layer 7: Government data (permits, licenses, tax records)
        if layers & LAYER_GOV_DATA:
            dms = await _run_gov_data(tag, hotel_id, name, city, state)
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_GOV_DATA

        # Layer 8: ABN Lookup + ASIC directors (Australian hotels)
        if layers & LAYER_ABN_ASIC:
            dms = await _run_abn_asic(client, tag, name, state, country, domain)
            result.decision_makers.extend(dms)
            result.layers_completed |= LAYER_ABN_ASIC

        # Layer 6: Email Verification
        if layers & LAYER_EMAIL_VERIFY:
            candidates = [
                dm for dm in result.decision_makers
                if dm.full_name and (not dm.email or not dm.email_verified)
            ]
            await _run_email_verify(tag, candidates, domain, domain_intel.email_provider)
            result.layers_completed |= LAYER_EMAIL_VERIFY

    except Exception as e:
        result.error = str(e)
        logger.error(f"{tag} Enrichment error: {e}", exc_info=True)

    result.domain_intel = domain_intel
    result.decision_makers = _deduplicate(result.decision_makers, tag)

    total_elapsed = time.monotonic() - t_start
    logger.info(
        f"{tag} Done: {len(result.decision_makers)} contacts | "
        f"layers=0b{result.layers_completed:09b} | {total_elapsed:.1f}s"
    )

    return result


async def enrich_batch(
    hotels: list[dict],
    concurrency: int = 5,
    layers: int = 0x1FF,
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

    # Create CF Worker proxy (auto-configures from env vars)
    cf_proxy = CfWorkerProxy()
    if cf_proxy.is_configured:
        logger.info(
            f"CF Worker proxy enabled — routing RDAP/crt.sh/Wayback through "
            f"{cf_proxy.worker_url}"
        )
    else:
        logger.info("CF Worker proxy not configured — using direct requests (set CF_WORKER_PROXY_URL)")

    logger.info(
        f"Batch starting: {len(hotels)} hotels | "
        f"concurrency={concurrency} | layers=0b{layers:09b}"
    )

    sem = asyncio.Semaphore(concurrency)
    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def process_one(hotel: dict):
            async with sem:
                return await enrich_single_hotel(
                    client, hotel, layers=layers, cf_proxy=cf_proxy,
                )

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
            for src in dm.sources:
                source_counts[src] = source_counts.get(src, 0) + 1

    logger.info(
        f"Batch complete: {found}/{total} hotels had contacts | "
        f"{total_contacts} contacts found | {batch_elapsed:.1f}s total"
    )
    if source_counts:
        breakdown = " | ".join(f"{src}={n}" for src, n in sorted(source_counts.items()))
        logger.info(f"Batch source breakdown: {breakdown}")

    return clean_results
