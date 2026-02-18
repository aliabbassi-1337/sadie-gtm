"""ASIC company/director lookup — query connectonline.asic.gov.au for directors.

Given an ACN (Australian Company Number) from ABN Lookup, retrieve the company's
current directors/officeholders from ASIC Connect.

STATUS: ASIC Connect uses Oracle ADF which requires full JavaScript execution.
The httpx-based approach cannot work without a browser engine. Current options:
  1. Playwright-based browser automation (not yet implemented)
  2. Vigil.sh paid API (https://developer.vigil.sh/api/asic/company/)
  3. Bulk data from data.gov.au (company register dataset)

For now, this module will gracefully return empty results. The ABN Lookup
module alone still provides value — sole trader names and company entity names.

Rate limit: 2s between requests to be respectful.
"""

import asyncio
import re
from dataclasses import dataclass, field
from typing import Optional

import httpx
from loguru import logger

from services.enrichment.owner_models import DecisionMaker

ASIC_SEARCH_URL = "https://connectonline.asic.gov.au/RegistrySearch/faces/landing/SearchRegisters.jspx"
ASIC_COMPANY_URL = "https://connectonline.asic.gov.au/RegistrySearch/faces/landing/panelSearch.jspx"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}


@dataclass
class AsicCompany:
    """Company info from ASIC Connect."""
    acn: str
    company_name: str
    status: str = "Unknown"
    directors: list[dict] = field(default_factory=list)
    # Each director: {"name": "John Smith", "role": "Director", "appointed": "01/01/2020"}


async def asic_company_lookup(
    client: httpx.AsyncClient,
    acn: str,
) -> Optional[AsicCompany]:
    """Look up an Australian company by ACN on ASIC Connect.

    ASIC Connect uses Oracle ADF which requires a session + viewstate dance.
    This function:
      1. GETs the search page to get a session cookie + ADF viewstate
      2. POSTs a search with the ACN
      3. Parses the results page for director names

    Args:
        client: httpx async client
        acn: 9-digit Australian Company Number

    Returns:
        AsicCompany with directors, or None if lookup failed
    """
    acn = re.sub(r'\s+', '', acn)
    if not re.match(r'^\d{9}$', acn):
        logger.warning(f"ASIC: invalid ACN format: {acn!r}")
        return None

    try:
        # Step 1: Get the search page (establishes session + JSESSIONID cookie)
        resp = await client.get(
            ASIC_SEARCH_URL,
            headers=_HEADERS,
            follow_redirects=True,
            timeout=20.0,
        )
        resp.raise_for_status()

        # Extract ADF viewstate and other hidden form fields
        page_html = resp.text
        view_state = _extract_viewstate(page_html)
        if not view_state:
            logger.debug("ASIC: could not extract viewstate from search page")
            return None

        await asyncio.sleep(2.0)  # Rate limit

        # Step 2: POST the search form with ACN
        # The ADF form structure varies but generally has a search input field
        form_data = _build_search_form(page_html, acn, view_state)
        if not form_data:
            logger.debug("ASIC: could not build search form")
            return None

        resp2 = await client.post(
            ASIC_SEARCH_URL,
            data=form_data,
            headers={
                **_HEADERS,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": ASIC_SEARCH_URL,
            },
            follow_redirects=True,
            timeout=20.0,
        )
        resp2.raise_for_status()

        await asyncio.sleep(2.0)  # Rate limit

        # Step 3: Parse results
        result_html = resp2.text
        return _parse_company_result(result_html, acn)

    except httpx.HTTPError as e:
        logger.warning(f"ASIC: HTTP error looking up ACN {acn}: {e}")
        return None
    except Exception as e:
        logger.warning(f"ASIC: unexpected error looking up ACN {acn}: {e}")
        return None


def _extract_viewstate(html: str) -> Optional[str]:
    """Extract javax.faces.ViewState from an ADF page."""
    m = re.search(
        r'name="javax\.faces\.ViewState"\s+(?:id="[^"]*"\s+)?value="([^"]+)"',
        html,
    )
    if m:
        return m.group(1)
    # Try alternate pattern
    m = re.search(r'javax\.faces\.ViewState[^"]*"[^"]*value="([^"]+)"', html)
    return m.group(1) if m else None


def _build_search_form(html: str, acn: str, view_state: str) -> Optional[dict]:
    """Build the search POST form data for ASIC Connect.

    Oracle ADF forms are complex with many hidden fields. We extract the
    form ID and search input field name from the page.
    """
    # Find the main form
    form_match = re.search(r'<form[^>]+id="([^"]+)"[^>]*>', html)
    if not form_match:
        return None
    form_id = form_match.group(1)

    # Find search input field (usually contains 'searchText' or 'searchNumber' in id)
    input_match = re.search(
        r'<input[^>]+id="([^"]*(?:search|Search|number|Number)[^"]*)"[^>]+type="text"',
        html,
    )
    search_field = input_match.group(1) if input_match else f"{form_id}:searchText"

    # Find submit button
    button_match = re.search(
        r'<(?:input|button)[^>]+id="([^"]*(?:search|Search|submit|Submit)[^"]*(?:btn|Btn|button|Button)[^"]*)"',
        html,
    )
    submit_field = button_match.group(1) if button_match else None

    data = {
        search_field: acn,
        "javax.faces.ViewState": view_state,
        f"{form_id}_SUBMIT": "1",
    }
    if submit_field:
        data[submit_field] = submit_field

    return data


def _parse_company_result(html: str, acn: str) -> Optional[AsicCompany]:
    """Parse ASIC Connect search results page for company info and directors."""
    # Try to find company name
    company_name = None

    # Pattern 1: Company name often appears as a heading or in a results table
    name_match = re.search(
        r'(?:Company\s+[Nn]ame|Organisation\s+[Nn]ame)[:\s]*</[^>]+>\s*'
        r'(?:<[^>]+>\s*)*([A-Z][^<]{2,80})',
        html,
    )
    if name_match:
        company_name = name_match.group(1).strip()

    if not company_name:
        # Pattern 2: Look for the ACN followed by company name
        name_match = re.search(
            rf'{re.escape(acn)}[^<]*</[^>]+>\s*(?:<[^>]+>\s*)*([A-Z][^<]{{2,80}})',
            html,
        )
        if name_match:
            company_name = name_match.group(1).strip()

    if not company_name:
        # Could not find company in results — may have no results or different format
        if "no results" in html.lower() or "not found" in html.lower():
            logger.debug(f"ASIC: no results for ACN {acn}")
        else:
            logger.debug(f"ASIC: could not parse company name for ACN {acn}")
        return None

    # Extract status
    status = "Unknown"
    status_match = re.search(r'(?:Status|Registration)[:\s]*</[^>]+>\s*(?:<[^>]+>\s*)*(\w[\w\s]{2,30})', html)
    if status_match:
        status = status_match.group(1).strip()

    # Extract directors/officeholders
    directors = _extract_directors(html)

    return AsicCompany(
        acn=acn,
        company_name=company_name,
        status=status,
        directors=directors,
    )


def _extract_directors(html: str) -> list[dict]:
    """Extract director/officeholder names from ASIC company details page."""
    directors = []

    # Look for officeholder section
    # ASIC typically lists them in a table with Name, Role, Appointed date
    officeholder_section = re.search(
        r'(?:Office\s*holder|Director|Officeholder).*?<table[^>]*>(.*?)</table>',
        html, re.IGNORECASE | re.DOTALL,
    )

    if officeholder_section:
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', officeholder_section.group(1), re.DOTALL)
        for row in rows:
            cells = re.findall(r'<td[^>]*>\s*(.*?)\s*</td>', row, re.DOTALL)
            if cells:
                # Clean HTML from cells
                clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                # Look for a name-like value (contains letters, might have a comma)
                for i, cell in enumerate(clean_cells):
                    if (
                        cell
                        and re.search(r'[A-Za-z]{2,}', cell)
                        and not re.match(r'^\d', cell)
                        and cell.lower() not in {'director', 'secretary', 'name', 'role', 'appointed'}
                    ):
                        role = "Director"
                        if i + 1 < len(clean_cells) and clean_cells[i + 1].lower() in {
                            'director', 'secretary', 'alternate director',
                        }:
                            role = clean_cells[i + 1].title()

                        appointed = None
                        for c in clean_cells:
                            if re.match(r'\d{2}/\d{2}/\d{4}', c):
                                appointed = c
                                break

                        directors.append({
                            "name": cell.strip().title(),
                            "role": role,
                            "appointed": appointed,
                        })
                        break  # One name per row

    # Fallback: look for name patterns near "Director" or "Officeholder" text
    if not directors:
        # Pattern: "SURNAME, Firstname" near director-related text
        dir_names = re.findall(
            r'(?:Director|Secretary|Officeholder)[^<]*?'
            r'([A-Z][A-Za-z]+(?:,\s*[A-Z][A-Za-z]+)+)',
            html,
        )
        for name in dir_names[:5]:  # Cap at 5
            directors.append({
                "name": name.strip().title(),
                "role": "Director",
                "appointed": None,
            })

    return directors


async def asic_to_decision_makers(
    client: httpx.AsyncClient,
    acn: str,
    abn_entity_name: Optional[str] = None,
) -> list[DecisionMaker]:
    """Look up ASIC directors and return DecisionMakers.

    Args:
        client: httpx async client
        acn: 9-digit ACN
        abn_entity_name: The company name from ABN Lookup (for context)

    Returns:
        List of DecisionMaker for each director found
    """
    company = await asic_company_lookup(client, acn)
    if not company or not company.directors:
        return []

    dms = []
    for d in company.directors:
        name = d.get("name", "").strip()
        if not name:
            continue

        # Flip "SURNAME, FIRSTNAME" if needed
        if "," in name:
            parts = name.split(",", 1)
            name = f"{parts[1].strip()} {parts[0].strip()}".title()

        role = d.get("role", "Director")
        dms.append(DecisionMaker(
            full_name=name,
            title=f"{role} — {company.company_name}" if abn_entity_name else role,
            sources=["asic_director"],
            confidence=0.80,
            raw_source_url=f"https://connectonline.asic.gov.au/RegistrySearch/faces/landing/SearchRegisters.jspx?acn={acn}",
        ))

    logger.info(
        f"ASIC: ACN {acn} ({company.company_name}) → "
        f"{len(dms)} directors: {[d.full_name for d in dms]}"
    )
    return dms
