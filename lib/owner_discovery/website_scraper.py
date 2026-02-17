"""Website scraping for hotel owner/GM discovery.

Crawls hotel website /about, /team, /contact pages and extracts decision maker
info via: 1) JSON-LD Person schema, 2) regex name+title patterns,
3) Azure OpenAI LLM extraction as fallback.

Also mines Google review owner responses for GM/owner names.

Expected hit rate: ~30-40% of hotels with websites.
Cost: Free (compute + existing Azure OpenAI).
"""

import asyncio
import json
import os
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from loguru import logger

from lib.owner_discovery.models import DecisionMaker

# Azure OpenAI config (reuse existing setup from room_count_enricher)
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")

# Pages likely to contain owner/team info
OWNER_PAGE_PATHS = [
    "/about", "/about-us", "/our-story", "/our-team",
    "/team", "/staff", "/leadership", "/management",
    "/contact", "/contact-us",
    "/the-hotel", "/our-hotel", "/hotel",
]

# Title patterns that indicate decision makers
DECISION_MAKER_TITLES = [
    "owner", "co-owner", "proprietor", "founder", "co-founder",
    "general manager", "hotel manager", "managing director",
    "director of operations", "chief executive", "ceo",
    "president", "vice president", "innkeeper",
    "director", "principal",
]

# Regex patterns for extracting name + title combinations from HTML text
# Matches patterns like: "John Smith, General Manager" or "General Manager: John Smith"
NAME_TITLE_PATTERNS = [
    # "Name, Title" or "Name - Title"
    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*[,\-–—]\s*(" + "|".join(DECISION_MAKER_TITLES) + r")",
    # "Title: Name" or "Title - Name"
    r"(" + "|".join(DECISION_MAKER_TITLES) + r")\s*[:\-–—]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
]

# Email extraction from page content
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Phone extraction
PHONE_REGEX = re.compile(
    r"(?:\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}"
)

# Skip domains that aren't the hotel's own site
SKIP_DOMAINS = frozenset({
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "tiktok.com", "pinterest.com",
    "booking.com", "expedia.com", "tripadvisor.com", "hotels.com",
    "agoda.com", "airbnb.com", "vrbo.com", "kayak.com",
})

# Common junk email prefixes to filter out
JUNK_EMAIL_PREFIXES = frozenset({
    "info@", "reservations@", "booking@", "bookings@", "frontdesk@",
    "reception@", "contact@", "hello@", "stay@", "welcome@",
    "noreply@", "no-reply@", "admin@", "support@", "help@",
    "sales@", "events@", "marketing@", "careers@", "jobs@",
    "webmaster@", "postmaster@",
})


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


def _html_to_text(html: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _is_personal_email(email: str) -> bool:
    """Check if an email looks personal (not generic role-based)."""
    lower = email.lower()
    return not any(lower.startswith(prefix) for prefix in JUNK_EMAIL_PREFIXES)


def extract_json_ld_persons(html: str) -> list[DecisionMaker]:
    """Extract Person entities from JSON-LD structured data."""
    results = []
    # Find all JSON-LD blocks
    for match in re.finditer(r'<script\s+type="application/ld\+json">(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(match.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                _extract_persons_from_jsonld(item, results)
        except (json.JSONDecodeError, KeyError):
            continue
    return results


def _extract_persons_from_jsonld(data: dict, results: list[DecisionMaker]):
    """Recursively extract Person types from JSON-LD data."""
    if not isinstance(data, dict):
        return

    schema_type = data.get("@type", "")
    types = schema_type if isinstance(schema_type, list) else [schema_type]

    if "Person" in types:
        name = data.get("name")
        title = data.get("jobTitle")
        email = data.get("email")
        phone = data.get("telephone")

        if name and title:
            title_lower = title.lower()
            if any(dt in title_lower for dt in DECISION_MAKER_TITLES):
                results.append(DecisionMaker(
                    full_name=name,
                    title=title,
                    email=email,
                    phone=phone,
                    source="website_scrape",
                    confidence=0.9,  # Structured data is high confidence
                ))

    # Check nested entities (employees, members, etc.)
    for key in ("employee", "employees", "member", "members", "founder", "author"):
        nested = data.get(key)
        if isinstance(nested, list):
            for item in nested:
                _extract_persons_from_jsonld(item, results)
        elif isinstance(nested, dict):
            _extract_persons_from_jsonld(nested, results)


def extract_name_title_regex(text: str) -> list[DecisionMaker]:
    """Extract name+title combinations using regex patterns."""
    results = []
    for pattern in NAME_TITLE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            groups = match.groups()
            if len(groups) == 2:
                # Determine which group is name and which is title
                g0, g1 = groups
                if any(t in g0.lower() for t in DECISION_MAKER_TITLES):
                    title, name = g0, g1
                else:
                    name, title = g0, g1

                name = name.strip()
                title = title.strip().title()

                # Validate name looks like a person name (2-4 words, capitalized)
                name_parts = name.split()
                if 2 <= len(name_parts) <= 4 and all(p[0].isupper() for p in name_parts if p):
                    results.append(DecisionMaker(
                        full_name=name,
                        title=title,
                        source="website_scrape",
                        confidence=0.7,
                    ))
    return results


def extract_emails_from_page(html: str, domain: str) -> list[str]:
    """Extract email addresses from page that belong to the hotel domain."""
    text = _html_to_text(html)
    emails = EMAIL_REGEX.findall(text)
    hotel_domain = _extract_domain(domain) or domain

    personal = []
    for email in set(emails):
        email_domain = email.split("@")[1].lower() if "@" in email else ""
        if email_domain == hotel_domain and _is_personal_email(email):
            personal.append(email)
    return personal


async def _fetch_page(
    client: httpx.AsyncClient,
    url: str,
) -> Optional[str]:
    """Fetch a page with standard browser headers."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    try:
        resp = await client.get(url, headers=headers, timeout=15.0, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


async def _llm_extract_owner(
    client: httpx.AsyncClient,
    page_text: str,
    hotel_name: str,
) -> Optional[DecisionMaker]:
    """Use Azure OpenAI to extract owner/GM info from unstructured page text."""
    if not AZURE_OPENAI_API_KEY or not AZURE_OPENAI_ENDPOINT:
        return None

    # Truncate text to avoid token limits
    text = page_text[:4000]

    prompt = f"""Extract the hotel owner or general manager's information from this text.
Hotel name: {hotel_name}

Text:
{text}

If you find an owner, general manager, or key decision maker, respond with EXACTLY this JSON format:
{{"name": "Full Name", "title": "Their Title", "email": "their@email.com", "phone": "their phone"}}

If no owner/GM info is found, respond with: {{"name": null}}
Only include fields you actually find in the text. Do not guess or make up information."""

    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version={AZURE_OPENAI_API_VERSION}"
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 150,
        "temperature": 0.1,
    }

    try:
        resp = await client.post(
            url,
            headers={"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"},
            json=payload,
            timeout=30.0,
        )
        if resp.status_code == 429:
            await asyncio.sleep(5)
            return None
        if resp.status_code != 200:
            return None

        content = resp.json()["choices"][0]["message"]["content"].strip()
        data = json.loads(content)

        if data.get("name"):
            return DecisionMaker(
                full_name=data["name"],
                title=data.get("title"),
                email=data.get("email"),
                phone=data.get("phone"),
                source="llm_extract",
                confidence=0.6,
            )
    except Exception as e:
        logger.debug(f"LLM extraction failed: {e}")

    return None


async def scrape_hotel_website(
    client: httpx.AsyncClient,
    website: str,
    hotel_name: str,
) -> list[DecisionMaker]:
    """Scrape a hotel website for owner/GM information.

    Tries multiple approaches in order:
    1. JSON-LD structured data (highest confidence)
    2. Regex name+title extraction (medium confidence)
    3. LLM extraction fallback (lower confidence)

    Args:
        client: httpx async client
        website: Hotel website URL
        hotel_name: Hotel name for context

    Returns:
        List of DecisionMaker objects found
    """
    domain = _extract_domain(website)
    if not domain or domain in SKIP_DOMAINS:
        return []

    base_url = f"https://{domain}"
    all_results = []
    all_personal_emails = []
    pages_text = []
    pages_fetched = 0
    pages_skipped = 0

    # Fetch about/team/contact pages
    for path in OWNER_PAGE_PATHS:
        url = urljoin(base_url, path)
        html = await _fetch_page(client, url)
        if not html:
            pages_skipped += 1
            continue
        pages_fetched += 1

        # Try JSON-LD extraction
        jsonld_results = extract_json_ld_persons(html)
        if jsonld_results:
            logger.debug(f"Website {domain}{path}: {len(jsonld_results)} JSON-LD persons")
        all_results.extend(jsonld_results)

        # Try regex extraction
        text = _html_to_text(html)
        regex_results = extract_name_title_regex(text)
        if regex_results:
            logger.debug(f"Website {domain}{path}: {len(regex_results)} regex matches")
        all_results.extend(regex_results)

        # Collect personal emails
        personal_emails = extract_emails_from_page(html, domain)
        if personal_emails:
            logger.debug(f"Website {domain}{path}: emails found: {personal_emails}")
        all_personal_emails.extend(personal_emails)

        # Save text for LLM fallback
        if text and len(text) > 100:
            pages_text.append(text)

    logger.debug(
        f"Website {domain}: fetched {pages_fetched}/{pages_fetched + pages_skipped} pages | "
        f"{len(all_results)} contacts pre-LLM | {len(all_personal_emails)} emails"
    )

    # If no results from structured/regex, try LLM on combined text
    if not all_results and pages_text:
        if AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT:
            logger.debug(f"Website {domain}: no regex/JSON-LD hits, trying LLM extraction on {len(pages_text)} pages")
            combined_text = "\n\n".join(pages_text[:3])  # Limit to 3 pages
            llm_result = await _llm_extract_owner(client, combined_text, hotel_name)
            if llm_result:
                logger.debug(f"Website {domain}: LLM found {llm_result.full_name} | {llm_result.title}")
                all_results.append(llm_result)
            else:
                logger.debug(f"Website {domain}: LLM extraction returned nothing")
        else:
            logger.debug(f"Website {domain}: LLM skipped — no AZURE_OPENAI_API_KEY set")
    elif not all_results and not pages_text:
        logger.debug(f"Website {domain}: no usable page text found for LLM")

    # Attach personal emails to results that don't have one
    if all_personal_emails:
        unique_emails = list(set(all_personal_emails))
        for dm in all_results:
            if not dm.email and unique_emails:
                dm.email = unique_emails[0]
                dm.raw_source_url = base_url

    # Set source URL
    for dm in all_results:
        if not dm.raw_source_url:
            dm.raw_source_url = base_url

    # Deduplicate by name
    seen_names = set()
    unique_results = []
    for dm in all_results:
        key = (dm.full_name or "").lower()
        if key and key not in seen_names:
            seen_names.add(key)
            unique_results.append(dm)

    return unique_results


async def extract_from_google_reviews(
    client: httpx.AsyncClient,
    hotel_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    serper_api_key: Optional[str] = None,
) -> list[DecisionMaker]:
    """Extract owner/GM names from Google review responses.

    Hotel owners/GMs respond to Google reviews, and their name + title
    appears in the response. This is public structured data.

    Uses Serper API for Google search (already integrated in the project).
    """
    if not serper_api_key:
        serper_api_key = os.getenv("SERPER_API_KEY")
    if not serper_api_key:
        logger.debug(f"Reviews {hotel_name!r}: skipped — no SERPER_API_KEY set")
        return []

    # Search for the hotel's Google reviews showing owner responses
    location = " ".join(filter(None, [city, state]))
    query = f'"{hotel_name}" {location} "Response from" "Owner" OR "General Manager" site:google.com/maps'

    try:
        resp = await client.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": serper_api_key, "Content-Type": "application/json"},
            json={"q": query, "num": 5},
            timeout=15.0,
        )
        if resp.status_code != 200:
            logger.debug(f"Reviews {hotel_name!r}: Serper returned HTTP {resp.status_code}")
            return []

        data = resp.json()
        organic = data.get("organic", [])
        logger.debug(f"Reviews {hotel_name!r}: Serper returned {len(organic)} organic results")
        results = []

        for item in organic:
            snippet = item.get("snippet", "")
            # Look for "Response from [Name], [Title]" patterns
            for match in re.finditer(
                r"Response from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\s*,?\s*(Owner|General Manager|Manager|Director|Innkeeper)",
                snippet,
                re.IGNORECASE,
            ):
                name = match.group(1).strip()
                title = match.group(2).strip().title()
                results.append(DecisionMaker(
                    full_name=name,
                    title=title,
                    source="review_response",
                    confidence=0.85,
                    raw_source_url=item.get("link"),
                ))

        # Deduplicate
        seen = set()
        unique = []
        for dm in results:
            key = (dm.full_name or "").lower()
            if key and key not in seen:
                seen.add(key)
                unique.append(dm)

        if unique:
            logger.debug(
                f"Reviews {hotel_name!r}: found {len(unique)} owners from review responses"
            )

        return unique

    except Exception as e:
        logger.debug(f"Reviews {hotel_name!r}: Serper request failed — {e}")
        return []


async def batch_scrape_websites(
    hotels: list[dict],  # Each dict has: hotel_id, website, name, city, state
    concurrency: int = 5,
    include_reviews: bool = True,
) -> list[tuple[int, list[DecisionMaker]]]:
    """Scrape multiple hotel websites for owner info.

    Args:
        hotels: List of hotel dicts with id, website, name
        concurrency: Max concurrent scrapes
        include_reviews: Also search Google reviews for owner responses

    Returns:
        List of (hotel_id, list[DecisionMaker])
    """
    sem = asyncio.Semaphore(concurrency)
    results = []

    async with httpx.AsyncClient(http2=True) as client:
        async def scrape_one(hotel: dict):
            async with sem:
                hotel_id = hotel["hotel_id"]
                website = hotel.get("website")
                name = hotel.get("name", "")
                city = hotel.get("city")
                state = hotel.get("state")

                dms = []

                # Scrape website
                if website:
                    try:
                        website_dms = await scrape_hotel_website(client, website, name)
                        dms.extend(website_dms)
                    except Exception as e:
                        logger.debug(f"Website scrape failed for hotel {hotel_id}: {e}")

                # Mine Google reviews
                if include_reviews and name:
                    try:
                        review_dms = await extract_from_google_reviews(
                            client, name, city, state
                        )
                        dms.extend(review_dms)
                    except Exception as e:
                        logger.debug(f"Review mining failed for hotel {hotel_id}: {e}")

                return hotel_id, dms

        tasks = [scrape_one(h) for h in hotels]
        results = await asyncio.gather(*tasks)

    total_found = sum(len(dms) for _, dms in results)
    hotels_with_results = sum(1 for _, dms in results if dms)
    logger.info(
        f"Website batch: {hotels_with_results}/{len(hotels)} hotels had owner info "
        f"({total_found} total decision makers)"
    )
    return list(results)
