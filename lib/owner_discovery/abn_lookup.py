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
    params = {"SearchText": name}

    try:
        resp = await client.get(
            ABR_SEARCH_URL,
            params=params,
            headers=_HEADERS,
            follow_redirects=True,
            timeout=15.0,
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning(f"ABN search failed for {name!r}: {e}")
        return []

    html = resp.text

    # Parse search results table rows.
    # Each <tr> has 4 <td>s:
    #   1. ABN link + status span
    #   2. Entity/business name
    #   3. Name type (Entity Name, Trading Name, Business Name)
    #   4. Postcode + state (e.g., "5068  SA")
    results = []
    # Extract ABN links and surrounding row data
    row_pattern = re.compile(
        r'<td[^>]*>\s*<a\s+href="/ABN/View\?abn=(\d{11})"[^>]*>'
        r'.*?</a>.*?</td>\s*'
        r'<td[^>]*>\s*(.*?)\s*</td>\s*'
        r'<td[^>]*>\s*(.*?)\s*</td>\s*'
        r'<td[^>]*>\s*(.*?)\s*</td>',
        re.DOTALL,
    )
    inline_results = []
    for m in row_pattern.finditer(html):
        abn = m.group(1)
        row_name = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        name_type = re.sub(r'<[^>]+>', '', m.group(3)).strip()
        location = re.sub(r'<[^>]+>', '', m.group(4)).strip()
        inline_results.append((abn, row_name, name_type, location))

    if not inline_results:
        logger.debug(f"ABN search for {name!r}: no results in HTML")
        return []

    logger.debug(f"ABN search for {name!r}: {len(inline_results)} results found")

    # Pre-filter by state if requested
    if state:
        state_upper = state.upper()
        filtered = [r for r in inline_results if state_upper in r[3].upper()]
        if filtered:
            inline_results = filtered

    # Fetch details for top results
    seen_abns = set()
    for abn, row_name, name_type, location in inline_results[:max_results]:
        if abn in seen_abns:
            continue
        seen_abns.add(abn)

        entity = await _fetch_abn_detail(client, abn)
        if entity:
            results.append(entity)
        await asyncio.sleep(0.3)  # Rate limit

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
    # ABR shows this as "SA 5068" or "5068 SA"
    location = _extract_field(html, r'(?:Main\s+business\s+location|Location)') or ""
    state = None
    postcode = None
    # Try "SA 5068" format
    loc_match = re.search(r'([A-Z]{2,3})\s+(\d{4})\b', location)
    if loc_match:
        state = loc_match.group(1)
        postcode = loc_match.group(2)
    else:
        # Try "5068 SA" format
        loc_match = re.search(r'(\d{4})\s+([A-Z]{2,3})\b', location)
        if loc_match:
            postcode = loc_match.group(1)
            state = loc_match.group(2)

    # Extract ACN — for Australian companies, ACN = last 9 digits of ABN
    acn = None
    # First check if ACN is explicitly on the page
    acn_match = re.search(r'ACN[:\s]*(\d{3}\s*\d{3}\s*\d{3})', html)
    if acn_match:
        acn = re.sub(r'\s+', '', acn_match.group(1))
    elif entity_type in ("PRV", "PUB") and len(abn) == 11:
        # Derive ACN from ABN (strip first 2 check digits)
        acn = abn[2:]

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
            if (
                name
                and not re.match(r'^\d{2}\s+\w+\s+\d{4}$', name)
                and name.lower() not in {
                    'current', 'cancelled', 'active', 'from', 'to', 'status',
                }
                and 'not entitled' not in name.lower()
                and 'tax deductible' not in name.lower()
                and len(name) < 100
            ):
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

    abn_url = f"https://abr.business.gov.au/ABN/View?abn={best.abn}"
    # Clean HTML entities from entity name
    import html as _html
    clean_name = _html.unescape(best.entity_name).strip()

    if best.is_individual:
        # Sole trader — the entity name IS the person's name
        person_name = _flip_surname_first(clean_name)
        dms.append(DecisionMaker(
            full_name=person_name,
            title="Owner (Sole Trader)",
            sources=["abn_lookup"],
            confidence=0.85,
            raw_source_url=abn_url,
        ))
    elif best.entity_type == "PARTNERSHIP":
        # Partnership — entity name often contains partner names
        # Pattern: "RN MURPHY & BI WHITEFORD" or "A SMITH & B JONES"
        partner_names = _parse_partnership_names(clean_name)
        for pn in partner_names:
            dms.append(DecisionMaker(
                full_name=pn,
                title="Owner (Partner)",
                sources=["abn_lookup"],
                confidence=0.80,
                raw_source_url=abn_url,
            ))
        if not partner_names:
            # Couldn't parse individual names, use the whole entity name
            dms.append(DecisionMaker(
                full_name=clean_name,
                title="Registered Partnership",
                sources=["abn_lookup"],
                confidence=0.5,
                raw_source_url=abn_url,
            ))
    elif best.is_company:
        # Pty Ltd / company — entity name is the company, not a person
        # Create a low-confidence org-level record
        dms.append(DecisionMaker(
            full_name=clean_name,
            title="Registered Entity",
            sources=["abn_lookup"],
            confidence=0.5,
            raw_source_url=abn_url,
        ))
        # ACN available for ASIC director follow-up (handled by caller)
    elif "trust" in best.entity_type.lower():
        # Trust — try to extract person names from trust name
        person_names = _parse_trust_names(clean_name)
        for pn in person_names:
            dms.append(DecisionMaker(
                full_name=pn,
                title="Owner (Trustee)",
                sources=["abn_lookup"],
                confidence=0.75,
                raw_source_url=abn_url,
            ))
        # Always also store the entity record (for company-level tracking)
        dms.append(DecisionMaker(
            full_name=clean_name,
            title="Trustee Entity",
            sources=["abn_lookup"],
            confidence=0.45,
            raw_source_url=abn_url,
        ))

    return dms, best


def _parse_partnership_names(entity_name: str) -> list[str]:
    """Parse individual names from a partnership entity name.

    ABR partnership names look like:
      "RN MURPHY & BI WHITEFORD"
      "A SMITH & B JONES & C DOE"
      "SMITH, JOHN & JONES, MARY"
    """
    # Split on & or AND
    parts = re.split(r'\s*&\s*|\s+AND\s+', entity_name, flags=re.IGNORECASE)
    names = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # Check if this looks like a person name (not a company/place)
        part_upper = part.upper()
        if any(kw in part_upper for kw in [
            "PTY", "LTD", "INC", "TRUST", "CORP", "PARK", "HOLIDAY",
            "RESORT", "CARAVAN", "CAMPING", "TOURISM", "MOTEL",
        ]):
            continue
        # Flip "SURNAME, FIRSTNAME" if needed
        name = _flip_surname_first(part)
        if name and len(name) > 2:
            names.append(name)
    return names


def _parse_trust_names(trust_name: str) -> list[str]:
    """Extract person names from Australian trust entity names.

    Common patterns:
      "THE TRUSTEE FOR D J & M A WATTS FAMILY TRUST"  → ["D J Watts", "M A Watts"]
      "THE TRUSTEE FOR G & C HELLINGS FAMILY TRUST"   → ["G Hellings", "C Hellings"]
      "The Trustee for LES LINDSAY FAMILY BUILDING TRUST" → ["Les Lindsay"]
      "The trustee for Moordys Family Trust"           → ["Moordys"]
      "THE TRUSTEE FOR MEECH TRUST TRADING AS ..."     → ["Meech"]

    Business-named trusts (no person names) return []:
      "The Trustee for Bundaberg Park Unit Trust"      → []
      "The Trustee for Tasman Tourism Trust"           → []
    """
    # Strip "The Trustee for" / "THE TRUSTEE FOR" prefix
    m = re.match(r'(?:the\s+)?trustee\s+for\s+(?:the\s+)?(.+)', trust_name, re.IGNORECASE)
    if not m:
        return []
    body = m.group(1).strip()

    # Strip trailing trust type keywords
    body = re.sub(
        r'\s+(FAMILY\s+)?(UNIT\s+|BUILDING\s+|DISCRETIONARY\s+)?TRUST(\s+NO\.?\s*\d+)?'
        r'(\s+TRADING\s+AS\s+.+)?$',
        '', body, flags=re.IGNORECASE
    ).strip()

    if not body:
        return []

    # Skip if remaining text looks like a business name (contains park, holiday, tourism, etc.)
    business_words = [
        "PARK", "HOLIDAY", "TOURIST", "TOURISM", "RESORT", "BEACH", "RIVER",
        "VILLAGE", "CARAVAN", "CAMPING", "COASTAL", "OPERATIONS", "COVE",
        "PARADISE", "GOLDEN", "PHOBOS", "RAINBOW", "VALLEY", "WEIR",
        "GORGE", "ISLAND", "HARBOUR", "PORT", "WYE", "PUTT", "MILDURA",
    ]
    body_upper = body.upper()
    if any(bw in body_upper for bw in business_words):
        return []

    # Pattern 1: "D J & M A WATTS" — initials + shared surname
    # Multiple people sharing a surname, separated by &
    m_multi = re.match(
        r'^([A-Z]\s+(?:[A-Z]\s+)?)'  # first person initials: "D J "
        r'&\s*'
        r'([A-Z]\s+(?:[A-Z]\s+)?)'   # second person initials: "M A "
        r'([A-Z][a-zA-Z]+)',          # shared surname: "WATTS"
        body, re.IGNORECASE,
    )
    if m_multi:
        surname = m_multi.group(3).strip().title()
        names = []
        for g in [m_multi.group(1), m_multi.group(2)]:
            initials = g.strip().title()
            names.append(f"{initials} {surname}")
        return names

    # Pattern 2: "G & C HELLINGS" — single initials + shared surname
    m_gc = re.match(
        r'^([A-Z])\s*&\s*([A-Z])\s+([A-Z][a-zA-Z]+)',
        body, re.IGNORECASE,
    )
    if m_gc:
        surname = m_gc.group(3).strip().title()
        return [
            f"{m_gc.group(1).upper()} {surname}",
            f"{m_gc.group(2).upper()} {surname}",
        ]

    # Pattern 3: "LES LINDSAY" or "MEECH" — a name (1-3 words, no business words)
    words = body.split()
    if 1 <= len(words) <= 3:
        # Check each word looks like a name (capitalized, no numbers, etc.)
        looks_like_name = all(
            re.match(r"^[A-Za-z][A-Za-z.'\\-]+$", w) and len(w) >= 2
            for w in words
        )
        if looks_like_name:
            return [" ".join(w.title() for w in words)]

    return []
