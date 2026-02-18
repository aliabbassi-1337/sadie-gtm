"""ABN Lookup module — scrape abr.business.gov.au for business entity info.

For Australian hotels, the ABN (Australian Business Number) registry reveals:
  - Sole traders: entity name IS the person's name (e.g., "PHAN, DIEU HANH")
  - Pty Ltd companies: company name + ACN (for ASIC director follow-up)

Rate limit: 1 req/s against abr.business.gov.au
"""

import asyncio
import re
from dataclasses import dataclass, field
from typing import Optional

import httpx
from loguru import logger

from services.enrichment.owner_models import DecisionMaker

ABR_SEARCH_URL = "https://abr.business.gov.au/Search/ResultsActive"
ABR_ABN_URL = "https://abr.business.gov.au/ABN/View"

# Entity type codes from ABR
INDIVIDUAL_TYPES = {"IND", "Sole Trader", "Individual/Sole Trader"}
COMPANY_TYPES = {"PRV", "PUB", "Private Company", "Public Company",
                 "Australian Private Company", "Australian Public Company"}

# Headers to appear as a normal browser
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}


@dataclass
class AbnEntity:
    """An entity from the ABN Lookup registry."""
    abn: str
    entity_name: str            # "PHAN, DIEU HANH" or "AZZ PTY LTD"
    entity_type: str            # "IND", "PRV", "PUB", etc.
    status: str = "Active"      # "Active" / "Cancelled"
    state: Optional[str] = None
    postcode: Optional[str] = None
    business_names: list[str] = field(default_factory=list)
    acn: Optional[str] = None

    @property
    def is_individual(self) -> bool:
        return self.entity_type in INDIVIDUAL_TYPES or "sole trader" in self.entity_type.lower()

    @property
    def is_company(self) -> bool:
        return (
            self.entity_type in COMPANY_TYPES
            or "company" in self.entity_type.lower()
            or "pty" in self.entity_name.upper()
        )


def _flip_surname_first(name: str) -> str:
    """Convert 'SURNAME, FIRSTNAME MIDDLE' to 'Firstname Middle Surname'.

    ABN Lookup returns individual names as 'PHAN, DIEU HANH'.
    """
    if "," not in name:
        return name.strip().title()
    parts = name.split(",", 1)
    surname = parts[0].strip()
    given = parts[1].strip()
    return f"{given} {surname}".title()


def _normalize_entity_type(raw: str) -> str:
    """Normalize entity type string to a short code."""
    raw_lower = raw.lower().strip()
    if "individual" in raw_lower or "sole trader" in raw_lower:
        return "IND"
    if "private" in raw_lower:
        return "PRV"
    if "public" in raw_lower:
        return "PUB"
    if "trust" in raw_lower:
        return "TRUST"
    if "partnership" in raw_lower:
        return "PARTNERSHIP"
    if "government" in raw_lower:
        return "GOV"
    return raw.strip()


def _extract_abn_from_href(href: str) -> Optional[str]:
    """Extract ABN from a link like '/ABN/View?abn=12345678901'."""
    m = re.search(r'abn=(\d{11})', href)
    return m.group(1) if m else None


def _name_similarity(hotel_name: str, entity_name: str, business_names: list[str]) -> float:
    """Score how well an ABN entity matches a hotel name. 0.0-1.0."""
    hotel_lower = hotel_name.lower().strip()
    # Check entity name
    entity_lower = entity_name.lower().strip()
    if hotel_lower in entity_lower or entity_lower in hotel_lower:
        return 0.9

    # Check business/trading names
    for bn in business_names:
        bn_lower = bn.lower().strip()
        if hotel_lower in bn_lower or bn_lower in hotel_lower:
            return 0.95

    # Word overlap scoring
    hotel_words = set(re.findall(r'\w+', hotel_lower)) - {"the", "a", "an", "and", "of", "in", "at"}
    entity_words = set(re.findall(r'\w+', entity_lower))
    all_bn_words = set()
    for bn in business_names:
        all_bn_words.update(re.findall(r'\w+', bn.lower()))

    if not hotel_words:
        return 0.0

    entity_overlap = len(hotel_words & entity_words) / len(hotel_words)
    bn_overlap = len(hotel_words & all_bn_words) / len(hotel_words) if all_bn_words else 0.0
    return max(entity_overlap, bn_overlap)


async def abn_search_by_name(
    client: httpx.AsyncClient,
    name: str,
    state: Optional[str] = None,
    max_results: int = 5,
) -> list[AbnEntity]:
    """Search ABN Lookup by business name.

    Args:
        client: httpx async client
        name: Business name to search
        state: Australian state code (NSW, VIC, QLD, etc.) to filter
        max_results: Max results to return (limits detail fetches)

    Returns:
        List of AbnEntity results
    """
    form_data = {
        "SearchText": name,
        "SearchType": "BusinessNameSearch",
    }
    if state:
        form_data["State"] = state.upper()

    try:
        resp = await client.post(
            ABR_SEARCH_URL,
            data=form_data,
            headers=_HEADERS,
            follow_redirects=True,
            timeout=15.0,
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"ABN search failed for {name!r}: {e}")
        return []

    html = resp.text

    # Parse search results table — each row has ABN link + entity name + state
    # Pattern: <a href="/ABN/View?abn=XXXXXXXXXXX">XX XXX XXX XXX</a>
    results = []
    abn_links = re.findall(
        r'<a\s+href="(/ABN/View\?abn=\d{11})"[^>]*>\s*'
        r'(\d{2}\s*\d{3}\s*\d{3}\s*\d{3})\s*</a>',
        html,
    )

    if not abn_links:
        logger.debug(f"ABN search for {name!r}: no results in HTML")
        return []

    logger.debug(f"ABN search for {name!r}: {len(abn_links)} results found")

    # Fetch details for top results
    seen_abns = set()
    for href, abn_display in abn_links[:max_results]:
        abn = re.sub(r'\s+', '', abn_display)
        if abn in seen_abns:
            continue
        seen_abns.add(abn)

        entity = await _fetch_abn_detail(client, abn)
        if entity:
            results.append(entity)
        await asyncio.sleep(1.0)  # Rate limit

    return results


async def _fetch_abn_detail(client: httpx.AsyncClient, abn: str) -> Optional[AbnEntity]:
    """Fetch full ABN detail page and extract entity info."""
    try:
        resp = await client.get(
            ABR_ABN_URL,
            params={"abn": abn},
            headers=_HEADERS,
            follow_redirects=True,
            timeout=15.0,
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"ABN detail fetch failed for {abn}: {e}")
        return None

    html = resp.text

    # Extract entity name — in a <td> after "Entity name:" or similar label
    entity_name = _extract_field(html, r'Entity\s+name')
    if not entity_name:
        # Try alternate pattern — "Legal name" for individuals
        entity_name = _extract_field(html, r'Legal\s+name')
    if not entity_name:
        # Try "Trading name" as last resort
        entity_name = _extract_field(html, r'(?:Individual|Legal)\s+&amp;\s+trading\s+name')
    if not entity_name:
        logger.debug(f"ABN {abn}: could not extract entity name")
        return None

    # Extract entity type
    entity_type_raw = _extract_field(html, r'Entity\s+type') or ""
    entity_type = _normalize_entity_type(entity_type_raw)

    # Extract ABN status
    status = _extract_field(html, r'ABN\s+status') or "Unknown"
    # Simplify status
    if "active" in status.lower():
        status = "Active"
    elif "cancel" in status.lower():
        status = "Cancelled"

    # Extract state/postcode from "Main business location"
    location = _extract_field(html, r'(?:Main\s+business\s+location|Location)') or ""
    state = None
    postcode = None
    loc_match = re.search(r'(\d{4})\s+([A-Z]{2,3})\b', location)
    if loc_match:
        postcode = loc_match.group(1)
        state = loc_match.group(2)

    # Extract ACN if present
    acn = None
    acn_match = re.search(r'ACN[:\s]*(\d{3}\s*\d{3}\s*\d{3})', html)
    if acn_match:
        acn = re.sub(r'\s+', '', acn_match.group(1))

    # Extract business/trading names
    business_names = _extract_business_names(html)

    return AbnEntity(
        abn=abn,
        entity_name=entity_name,
        entity_type=entity_type,
        status=status,
        state=state,
        postcode=postcode,
        business_names=business_names,
        acn=acn,
    )


def _extract_field(html: str, label_pattern: str) -> Optional[str]:
    """Extract a field value from ABR HTML given a label regex.

    ABR uses table rows with label in first <td> and value in second <td>.
    """
    pattern = (
        rf'<t[hd][^>]*>\s*{label_pattern}\s*:?\s*</t[hd]>\s*'
        r'<t[hd][^>]*>\s*(.*?)\s*</t[hd]>'
    )
    m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
    if m:
        # Clean HTML tags from value
        value = re.sub(r'<[^>]+>', ' ', m.group(1))
        value = re.sub(r'\s+', ' ', value).strip()
        return value if value else None
    return None


def _extract_business_names(html: str) -> list[str]:
    """Extract all trading/business names from the ABN detail page."""
    names = []
    # Business names section — look for rows in the business names table
    # Pattern: rows containing business name entries
    bn_section = re.search(
        r'(?:Business|Trading)\s+name.*?<table[^>]*>(.*?)</table>',
        html, re.IGNORECASE | re.DOTALL,
    )
    if bn_section:
        # Extract individual names from table rows
        for m in re.finditer(r'<td[^>]*>\s*([^<]+?)\s*</td>', bn_section.group(1)):
            name = m.group(1).strip()
            # Skip dates, statuses, and empty values
            if name and not re.match(r'^\d{2}\s+\w+\s+\d{4}$', name) and name.lower() not in {
                'current', 'cancelled', 'active', 'from', 'to', 'status',
            }:
                names.append(name)

    return names


async def abn_to_decision_makers(
    client: httpx.AsyncClient,
    hotel_name: str,
    state: Optional[str] = None,
) -> tuple[list[DecisionMaker], Optional[AbnEntity]]:
    """Search ABN and convert results to DecisionMakers.

    Args:
        client: httpx async client
        hotel_name: Hotel name to search
        state: Australian state code for filtering

    Returns:
        Tuple of (decision_makers, best matching AbnEntity or None)
    """
    entities = await abn_search_by_name(client, hotel_name, state=state)

    if not entities:
        return [], None

    # Score and rank by match quality
    scored = []
    for entity in entities:
        score = _name_similarity(hotel_name, entity.entity_name, entity.business_names)
        # Boost active entities
        if entity.status == "Active":
            score += 0.05
        # Boost matching state
        if state and entity.state and entity.state.upper() == state.upper():
            score += 0.05
        scored.append((score, entity))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Need a reasonable match
    best_score, best = scored[0]
    if best_score < 0.3:
        logger.debug(
            f"ABN: best match for {hotel_name!r} was {best.entity_name!r} "
            f"(score={best_score:.2f}) — too low, skipping"
        )
        return [], None

    logger.info(
        f"ABN: matched {hotel_name!r} → {best.entity_name!r} "
        f"(ABN={best.abn}, type={best.entity_type}, score={best_score:.2f})"
    )

    dms = []

    if best.is_individual:
        # Sole trader — the entity name IS the person's name
        person_name = _flip_surname_first(best.entity_name)
        dms.append(DecisionMaker(
            full_name=person_name,
            title="Owner (Sole Trader)",
            sources=["abn_lookup"],
            confidence=0.85,
            raw_source_url=f"https://abr.business.gov.au/ABN/View?abn={best.abn}",
        ))
    elif best.is_company:
        # Pty Ltd / company — entity name is the company, not a person
        # Create a low-confidence org-level record
        dms.append(DecisionMaker(
            full_name=best.entity_name,
            title="Registered Entity",
            sources=["abn_lookup"],
            confidence=0.5,
            raw_source_url=f"https://abr.business.gov.au/ABN/View?abn={best.abn}",
        ))
        # ACN available for ASIC director follow-up (handled by caller)

    return dms, best
