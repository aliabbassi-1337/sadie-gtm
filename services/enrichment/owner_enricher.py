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
    LAYERS_DEFAULT,
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
    try:
        dms, ct_intel = await asyncio.wait_for(
            ct_to_decision_makers(client, domain, cf_proxy=cf_proxy),
            timeout=15.0,
        )
    except asyncio.TimeoutError:
        logger.info(f"{tag} [0/7 CT] Timed out after 15s — skipping")
        return [], None
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
            # SOA email stored in domain_dns_cache, not as a decision maker
            # (it's an admin email, not a person)
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

    async def _verify_one(dm):
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

    await asyncio.gather(*[_verify_one(dm) for dm in candidates])

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
    """Deduplicate and filter decision makers."""
    seen = set()
    unique = []
    dropped = 0
    for dm in dms:
        # Drop first-name-only contacts (no surname = low value)
        if dm.full_name and " " not in dm.full_name and dm.title not in (
            "Registered Entity", "Trustee Entity", "Domain Owner",
            "Registered Partnership", "Certificate Organization",
        ):
            dropped += 1
            continue
        key = ((dm.full_name or "").lower(), (dm.title or "").lower())
        if key not in seen:
            seen.add(key)
            unique.append(dm)
    if dropped:
        logger.debug(f"{tag} Dropped {dropped} first-name-only contacts")
    if len(dms) - dropped != len(unique):
        logger.debug(f"{tag} Deduped {len(dms)} → {len(unique)} contacts")
    return unique


# ── Orchestrator ────────────────────────────────────────────────────


async def enrich_single_hotel(
    client: httpx.AsyncClient,
    hotel: dict,
    layers: int = LAYERS_DEFAULT,
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

    # Skip tourism board / aggregator domains — contacts belong to the board, not the hotel
    _SKIP_DOMAINS = {
        "visitmelbourne.com", "visitvictoria.com", "visitnsw.com",
        "visitqueensland.com", "queensland.com", "australia.com",
        "visitwa.com", "southaustralia.com", "discovertasmania.com.au",
        "visitcanberra.com.au", "tropicalnorthqueensland.org.au",
        "booking.com", "expedia.com", "tripadvisor.com", "agoda.com",
        "wotif.com", "hotels.com", "airbnb.com",
    }
    if domain in _SKIP_DOMAINS:
        result.error = "aggregator_domain"
        logger.info(f"{tag} Skipped — tourism board / aggregator domain: {domain}")
        return result

    country = hotel.get("country")
    logger.info(f"{tag} Starting enrichment waterfall | hotel={name!r} layers=0b{layers:09b}")
    domain_intel = DomainIntel(domain=domain)

    try:
        # ── Phase 1: All layers except email verify (all run in parallel) ──
        # WHOIS no longer waits for RDAP — the skip-if-found optimization
        # only saved ~5s for ~10% of hotels, not worth a separate phase.
        all_tasks = []
        all_keys = []

        if layers & LAYER_CT_CERTS:
            all_tasks.append(_run_ct(client, tag, domain, cf_proxy=cf_proxy))
            all_keys.append("ct")
        if layers & LAYER_RDAP:
            all_tasks.append(_run_rdap(client, tag, domain, cf_proxy=cf_proxy))
            all_keys.append("rdap")
        if layers & LAYER_DNS:
            all_tasks.append(_run_dns(tag, domain, domain_intel))
            all_keys.append("dns")
        if layers & LAYER_REVIEWS:
            all_tasks.append(_run_reviews(client, tag, name, city, state))
            all_keys.append("reviews")
        if layers & LAYER_GOV_DATA:
            all_tasks.append(_run_gov_data(tag, hotel_id, name, city, state))
            all_keys.append("gov")
        if layers & LAYER_WHOIS_HISTORY:
            all_tasks.append(_run_whois(
                client, tag, domain, False, domain_intel,
                cf_proxy=cf_proxy,
            ))
            all_keys.append("whois")
        if layers & LAYER_WEBSITE:
            all_tasks.append(_run_website(client, tag, website, name))
            all_keys.append("website")
        if layers & LAYER_ABN_ASIC:
            all_tasks.append(_run_abn_asic(client, tag, name, state, country, domain))
            all_keys.append("abn_asic")

        if all_tasks:
            all_results = await asyncio.gather(*all_tasks, return_exceptions=True)

            for key, res in zip(all_keys, all_results):
                if isinstance(res, Exception):
                    logger.warning(f"{tag} Layer ({key}) error: {res}")
                    continue

                if key == "ct":
                    dms, ct_partial = res
                    if ct_partial:
                        domain_intel.ct_org_name = ct_partial.ct_org_name
                        domain_intel.ct_alt_domains = ct_partial.ct_alt_domains
                        domain_intel.ct_cert_count = ct_partial.ct_cert_count
                    result.decision_makers.extend(dms)
                    result.layers_completed |= LAYER_CT_CERTS
                elif key == "rdap":
                    dms, rdap_intel = res
                    if rdap_intel:
                        if rdap_intel.registrant_name:
                            domain_intel.registrant_name = rdap_intel.registrant_name
                        if rdap_intel.registrant_org:
                            domain_intel.registrant_org = rdap_intel.registrant_org
                        if rdap_intel.registrant_email:
                            domain_intel.registrant_email = rdap_intel.registrant_email
                        if rdap_intel.registrar:
                            domain_intel.registrar = rdap_intel.registrar
                        if rdap_intel.is_privacy_protected:
                            domain_intel.is_privacy_protected = rdap_intel.is_privacy_protected
                    result.decision_makers.extend(dms)
                    result.layers_completed |= LAYER_RDAP
                elif key == "dns":
                    dms, dns_domain_intel = res
                    domain_intel.email_provider = dns_domain_intel.email_provider
                    domain_intel.mx_records = dns_domain_intel.mx_records
                    domain_intel.soa_email = dns_domain_intel.soa_email
                    domain_intel.spf_record = dns_domain_intel.spf_record
                    domain_intel.dmarc_record = dns_domain_intel.dmarc_record
                    result.decision_makers.extend(dms)
                    result.layers_completed |= LAYER_DNS
                elif key == "reviews":
                    result.decision_makers.extend(res)
                    result.layers_completed |= LAYER_REVIEWS
                elif key == "gov":
                    result.decision_makers.extend(res)
                    result.layers_completed |= LAYER_GOV_DATA
                elif key == "whois":
                    dms, whois_domain_intel = res
                    if whois_domain_intel.registrant_name and not domain_intel.registrant_name:
                        domain_intel.registrant_name = whois_domain_intel.registrant_name
                    if whois_domain_intel.registrant_org and not domain_intel.registrant_org:
                        domain_intel.registrant_org = whois_domain_intel.registrant_org
                    if whois_domain_intel.registrant_email and not domain_intel.registrant_email:
                        domain_intel.registrant_email = whois_domain_intel.registrant_email
                    if whois_domain_intel.registrar and not domain_intel.registrar:
                        domain_intel.registrar = whois_domain_intel.registrar
                    result.decision_makers.extend(dms)
                    result.layers_completed |= LAYER_WHOIS_HISTORY
                elif key == "website":
                    result.decision_makers.extend(res)
                    result.layers_completed |= LAYER_WEBSITE
                elif key == "abn_asic":
                    result.decision_makers.extend(res)
                    result.layers_completed |= LAYER_ABN_ASIC

        # ── Phase 2: Email verification (needs all contacts + DNS provider) ──
        if layers & LAYER_EMAIL_VERIFY:
            # Only verify actual people — skip entities, domain owners, and
            # first-name-only contacts (no surname = can't generate patterns)
            _ENTITY_TITLES = {"Registered Entity", "Trustee Entity", "Domain Owner",
                              "Registered Partnership", "Certificate Organization"}
            candidates = [
                dm for dm in result.decision_makers
                if dm.full_name
                and (not dm.email or not dm.email_verified)
                and dm.title not in _ENTITY_TITLES
                and " " in dm.full_name  # must have first + last name
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


FLUSH_INTERVAL = 20  # Persist to DB every N completed hotels


async def enrich_batch(
    hotels: list[dict],
    concurrency: int = 5,
    layers: int = LAYERS_DEFAULT,
    persist: bool = True,
    flush_interval: int = FLUSH_INTERVAL,
) -> list[OwnerEnrichmentResult]:
    """Run owner enrichment for a batch of hotels.

    Args:
        hotels: List of hotel dicts with hotel_id, name, website, city, state, country
        concurrency: Max concurrent enrichments
        layers: Bitmask of layers to run
        persist: If True, flush results to DB every flush_interval hotels
        flush_interval: How many completed hotels to buffer before batch-writing to DB

    Returns:
        List of OwnerEnrichmentResult
    """
    from services.enrichment import repo

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
    pending_buffer: list[OwnerEnrichmentResult] = []
    flush_lock = asyncio.Lock()
    total_saved = 0

    async def _flush():
        """Batch-write buffered results to DB."""
        nonlocal pending_buffer, total_saved
        async with flush_lock:
            if not pending_buffer:
                return
            to_flush = pending_buffer
            pending_buffer = []
        try:
            count = await repo.batch_persist_results(to_flush)
            total_saved += count
            logger.info(
                f"Flushed {len(to_flush)} hotels to DB "
                f"({count} DMs saved, {total_saved} total)"
            )
        except Exception as e:
            logger.error(f"Flush failed for {len(to_flush)} hotels: {e}")
            # Put them back so final flush can retry
            async with flush_lock:
                pending_buffer = to_flush + pending_buffer

    pool_limits = httpx.Limits(
        max_connections=concurrency * 15,     # ~15 outbound requests per hotel
        max_keepalive_connections=concurrency * 5,
    )
    async with httpx.AsyncClient(timeout=30.0, limits=pool_limits) as client:
        async def process_one(hotel: dict):
            nonlocal pending_buffer
            async with sem:
                result = await enrich_single_hotel(
                    client, hotel, layers=layers, cf_proxy=cf_proxy,
                )
            # Buffer + flush outside the semaphore so we don't waste
            # a concurrency slot on DB IO
            if persist:
                async with flush_lock:
                    pending_buffer.append(result)
                    should_flush = len(pending_buffer) >= flush_interval
                if should_flush:
                    await _flush()
            return result

        tasks = [process_one(h) for h in hotels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Final flush for any remaining buffered results
    if persist:
        await _flush()

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
        f"{total_contacts} contacts found | {total_saved} DMs saved | "
        f"{batch_elapsed:.1f}s total"
    )
    if source_counts:
        breakdown = " | ".join(f"{src}={n}" for src, n in sorted(source_counts.items()))
        logger.info(f"Batch source breakdown: {breakdown}")

    return clean_results
