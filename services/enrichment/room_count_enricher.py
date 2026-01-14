"""
Room Count Enricher (LLM-powered)
=================================
Uses Groq's fast LLM API to extract room counts from hotel websites.

This is an internal helper module - only service.py can call repo functions.
"""

import os
import re
import asyncio
from datetime import datetime
from typing import Optional, Tuple, List
from urllib.parse import urljoin, urlparse

import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GROQ_API_KEY = os.getenv("ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

# Fast model - good balance of speed and accuracy
MODEL = "llama-3.1-8b-instant"

# Pages to check for room count info - be thorough!
ABOUT_PAGE_PATTERNS = [
    "/about", "/about-us", "/about-hotel", "/the-hotel",
    "/our-hotel", "/hotel", "/property", "/accommodation",
    "/accommodations", "/rooms", "/our-rooms", "/guest-rooms",
    "/suites", "/lodging", "/stay", "/overview",
    # Common variations
    "/smoky-mountains-accommodations", "/hotel-accommodations",
    "/hotel-rooms", "/guest-accommodations", "/the-rooms",
    "/room-types", "/our-accommodations",
]

# Regex patterns for room count extraction - ordered by specificity
ROOM_COUNT_REGEX = [
    # Most specific patterns first
    r'(?i)(?:our|the|with|featuring|offers?|has|have|boasts?|includes?)\s+(\d+)\s+(?:comfortable\s+)?(?:guest\s+)?(?:room|suite|unit|accommodation)s?',
    r'(?i)(\d+)\s+(?:comfortable\s+)?(?:guest\s+)?(?:room|suite|unit|accommodation)s?\s+(?:and\s+suites?)?',
    r'(?i)(\d+)[\s-]+room\s+(?:hotel|motel|inn|lodge|resort)',
    r'(?i)(?:total\s+of\s+)?(\d+)\s+(?:guest\s+)?rooms?',
    r'(?i)(\d+)\s+(?:spacious|luxurious|elegant|cozy|comfortable)\s+(?:guest\s+)?(?:room|suite)s?',
    # Fallback patterns
    r'(?i)(\d+)\s+(?:guest\s+)?(?:room|suite|unit)s?\b',
    r'(?i)(\d+)\s+accommodations?\b',
]


def log(msg: str) -> None:
    """Print timestamped log message."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


async def fetch_page_raw(client: httpx.AsyncClient, url: str) -> str:
    """Fetch raw HTML from a page."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        resp = await client.get(url, timeout=15.0, follow_redirects=True, headers=headers)
        if resp.status_code != 200:
            return ""
        return resp.text

    except httpx.ConnectError as e:
        log(f"    Connection error: {url} - {str(e)[:50]}")
        return ""
    except httpx.TimeoutException:
        log(f"    Timeout: {url}")
        return ""
    except Exception as e:
        log(f"    Fetch error: {url} - {type(e).__name__}: {str(e)[:50]}")
        return ""


def html_to_text(html: str) -> str:
    """Convert HTML to plain text."""
    # Remove script and style tags
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
    # Remove HTML tags but keep text
    text = re.sub(r'<[^>]+>', ' ', html)
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_room_count_regex(html: str) -> Optional[int]:
    """Extract room count from HTML using regex. Returns None if not found."""
    # Search through all patterns
    for pattern in ROOM_COUNT_REGEX:
        matches = re.findall(pattern, html)
        for match in matches:
            try:
                count = int(match)
                # Sanity check: 1-2000 rooms
                if 1 <= count <= 2000:
                    return count
            except Exception:
                pass
    return None


def find_all_internal_links(html: str, base_url: str) -> List[str]:
    """Find ALL internal links from HTML."""
    links = []
    base_domain = urlparse(base_url).netloc

    # Find all href attributes
    href_pattern = r'href=["\']([^"\'#]+)["\']'
    for match in re.finditer(href_pattern, html, re.IGNORECASE):
        href = match.group(1)

        # Skip external links, images, files, assets, etc.
        skip_extensions = ['.jpg', '.png', '.pdf', '.css', '.js', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.eot']
        skip_paths = ['/assets/', '/static/', '/wp-content/', '/images/', '/img/', '/css/', '/js/']

        if any(href.lower().endswith(ext) for ext in skip_extensions):
            continue
        if any(skip in href.lower() for skip in skip_paths):
            continue
        if href.startswith('mailto:') or href.startswith('tel:') or href.startswith('javascript:'):
            continue

        # Convert to absolute URL
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        # Only same-domain links
        if parsed.netloc == base_domain:
            # Normalize URL (remove trailing slash, query params)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
            if clean_url not in links and clean_url != base_url.rstrip('/'):
                links.append(clean_url)

    return links


def prioritize_room_links(links: List[str]) -> List[str]:
    """Sort links with room-related keywords first."""
    priority_keywords = ['room', 'accommodation', 'suite', 'lodging', 'stay', 'guest', 'hotel', 'property', 'about']

    def score(url: str) -> int:
        path = urlparse(url).path.lower()
        for i, kw in enumerate(priority_keywords):
            if kw in path:
                return i  # Lower = higher priority
        return 100  # No keyword match

    return sorted(links, key=score)


async def fetch_sitemap_links(client: httpx.AsyncClient, base_url: str) -> List[str]:
    """Try to fetch sitemap.xml and extract URLs."""
    links = []
    sitemap_urls = [
        urljoin(base_url, '/sitemap.xml'),
        urljoin(base_url, '/sitemap_index.xml'),
    ]

    for sitemap_url in sitemap_urls:
        try:
            resp = await client.get(sitemap_url, timeout=5.0)
            if resp.status_code == 200 and 'xml' in resp.headers.get('content-type', ''):
                # Extract URLs from sitemap
                urls = re.findall(r'<loc>([^<]+)</loc>', resp.text)
                links.extend(urls)
                break
        except Exception:
            pass

    return links


async def fetch_and_extract_room_count(client: httpx.AsyncClient, website: str) -> Tuple[Optional[int], str]:
    """
    Fetch hotel website pages and try to extract room count.
    Returns (room_count, all_text) - room_count is None if not found via regex.
    """
    # Normalize URL
    if not website.startswith("http"):
        website = "https://" + website
    base_url = website.rstrip('/')

    all_html = []
    checked_urls = set()
    all_links = []

    # 1. Try homepage first
    homepage_html = await fetch_page_raw(client, website)
    if homepage_html:
        all_html.append(homepage_html)
        checked_urls.add(base_url)
        checked_urls.add(base_url + '/')

        # Try regex on homepage
        count = extract_room_count_regex(homepage_html)
        if count:
            return count, html_to_text(homepage_html)

        # Discover ALL internal links from homepage
        all_links = find_all_internal_links(homepage_html, website)

    # 2. Try sitemap.xml for more pages
    sitemap_links = await fetch_sitemap_links(client, website)
    for link in sitemap_links:
        if link not in all_links:
            all_links.append(link)

    # 3. Prioritize room-related links first
    prioritized_links = prioritize_room_links(all_links)

    # 4. Check prioritized links (limit to avoid hammering server)
    max_pages = 15
    pages_checked = 0

    for link_url in prioritized_links:
        if pages_checked >= max_pages:
            break

        # Normalize URL for comparison
        normalized = link_url.rstrip('/')
        if normalized in checked_urls or normalized + '/' in checked_urls:
            continue
        checked_urls.add(normalized)
        pages_checked += 1

        page_html = await fetch_page_raw(client, link_url)
        if page_html and len(page_html) > 500:
            all_html.append(page_html)
            count = extract_room_count_regex(page_html)
            if count:
                short_path = urlparse(link_url).path or '/'
                log(f"    Found via regex on {short_path}: {count} rooms")
                return count, html_to_text(page_html)

    # No regex match found - return all text for LLM fallback
    combined_text = "\n\n".join(html_to_text(h) for h in all_html if h)
    return None, combined_text


def extract_room_relevant_content(text: str) -> str:
    """Extract sentences/paragraphs that likely contain room count information."""
    # Keywords that often appear near room counts
    keywords = [
        'room', 'suite', 'unit', 'apartment', 'accommodation', 'cabin', 'cottage',
        'guest', 'bedroom', 'lodging', 'property', 'hotel', 'motel', 'inn',
        'featuring', 'offers', 'boasts', 'includes', 'comfortable'
    ]

    # Split into sentences (rough split)
    sentences = re.split(r'[.!?]\s+', text)

    relevant = []
    for sentence in sentences:
        sentence_lower = sentence.lower()
        # Check if sentence contains numbers AND room-related keywords
        has_number = bool(re.search(r'\d+', sentence))
        has_keyword = any(kw in sentence_lower for kw in keywords)

        if has_number and has_keyword:
            # Clean up and add
            clean = sentence.strip()
            if len(clean) > 20 and len(clean) < 500:
                relevant.append(clean)

    # Also search for specific patterns anywhere in text
    patterns = [
        r'.{0,100}\d+\s*(?:guest\s*)?rooms?.{0,100}',
        r'.{0,100}\d+\s*(?:guest\s*)?suites?.{0,100}',
        r'.{0,100}\d+\s*(?:guest\s*)?units?.{0,100}',
        r'.{0,100}\d+\s*accommodations?.{0,100}',
        r'.{0,100}(?:featuring|with|offers?|has|have)\s+\d+.{0,100}',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches[:3]:  # Limit matches per pattern
            clean = match.strip()
            if clean and clean not in relevant and len(clean) > 20:
                relevant.append(clean)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for r in relevant:
        r_lower = r.lower()[:50]  # Use first 50 chars for dedup
        if r_lower not in seen:
            seen.add(r_lower)
            unique.append(r)

    return "\n".join(unique[:20])  # Max 20 relevant excerpts


async def extract_room_count_llm(client: httpx.AsyncClient, hotel_name: str, text: str) -> Optional[int]:
    """Use Groq LLM to estimate room count from website text."""
    if not text or len(text) < 50:
        return None

    # Truncate text to fit in context window (keep most relevant parts)
    # First 6000 chars should capture main content
    truncated_text = text[:6000] if len(text) > 6000 else text

    prompt = f"""Estimate the number of bookable rooms/units at this property based on the website content.

Hotel/Property: {hotel_name}

YOU MUST RETURN A NUMBER. Use these guidelines:
- Single cabin/cottage/house rental = 1
- Small cabin rental company = 5-15 (based on how many properties they mention)
- B&B or small inn = 5-15
- Boutique hotel = 15-50
- Mid-size hotel = 50-150
- Large hotel/resort = 150-500

Look for clues:
- Explicit numbers ("42 rooms", "15 cabins")
- Lists of properties or room types
- Property names that suggest size
- Descriptions of the property

WEBSITE CONTENT:
{truncated_text}

Return ONLY a number (e.g., "12"). You MUST estimate even if unsure:"""

    try:
        resp = await client.post(
            GROQ_API_URL,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 20,
                "temperature": 0.3,  # Slight creativity for estimation
            },
            timeout=30.0,
        )

        if resp.status_code == 429:
            # Rate limited - wait and retry up to 3 times
            for retry in range(3):
                wait_time = (retry + 1) * 5  # 5s, 10s, 15s
                log(f"    Rate limited, waiting {wait_time}s (attempt {retry + 1}/3)")
                await asyncio.sleep(wait_time)

                retry_resp = await client.post(
                    GROQ_API_URL,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 20,
                        "temperature": 0.3,
                    },
                    timeout=30.0,
                )
                if retry_resp.status_code == 200:
                    resp = retry_resp
                    break
                elif retry_resp.status_code != 429:
                    return None
            else:
                log("    Rate limit exceeded after 3 retries")
                return None

        if resp.status_code != 200:
            log(f"    Groq API error: {resp.status_code}")
            return None

        data = resp.json()
        answer = data["choices"][0]["message"]["content"].strip()

        # Extract number from response
        match = re.search(r'\d+', answer)
        if match:
            count = int(match.group())
            # Sanity check - most properties have 1-2000 rooms
            if 1 <= count <= 2000:
                return count

        # LLM didn't return a number - make a default estimate based on property name
        name_lower = hotel_name.lower()
        if any(kw in name_lower for kw in ['cabin', 'cottage', 'house', 'chalet', 'villa']):
            return 1  # Single rental unit
        elif any(kw in name_lower for kw in ['cabins', 'cottages', 'rentals']):
            return 10  # Small rental company
        elif any(kw in name_lower for kw in ['b&b', 'bed and breakfast', 'inn', 'guesthouse']):
            return 8  # Small B&B
        elif any(kw in name_lower for kw in ['motel']):
            return 30  # Typical motel
        elif any(kw in name_lower for kw in ['hotel', 'resort', 'lodge']):
            return 50  # Default hotel
        else:
            return 10  # Generic default

    except Exception as e:
        log(f"  LLM error: {e}")
        return None


async def enrich_hotel_room_count(
    client: httpx.AsyncClient,
    hotel_id: int,
    hotel_name: str,
    website: str,
) -> Tuple[Optional[int], str]:
    """
    Enrich a single hotel with room count.

    Returns (room_count, source) where source is 'regex' or 'groq'.
    Returns (None, '') if no room count could be determined.
    """
    log(f"Processing: {hotel_name}")

    # Fetch website and try regex extraction first
    regex_count, text = await fetch_and_extract_room_count(client, website)

    if regex_count:
        log(f"  Found via regex: {regex_count} rooms")
        return regex_count, "regex"

    if not text:
        log(f"  Could not fetch website")
        return None, ""

    # Fall back to LLM estimation
    count = await extract_room_count_llm(client, hotel_name, text)

    if count:
        log(f"  LLM estimate: ~{count} rooms")
        return count, "groq"
    else:
        log(f"  Could not estimate room count")
        return None, ""


def get_groq_api_key() -> Optional[str]:
    """Get the Groq API key from environment."""
    return GROQ_API_KEY
