"""BIG4 Holiday Parks - Scraper.

Scrapes big4.com.au to extract all park listings and their details.
The site is Next.js SSR so we can extract data from HTML + JSON-LD.
"""

import asyncio
import json
import re
from typing import Optional, List, Dict
from urllib.parse import urljoin

import httpx
from loguru import logger

from lib.big4.models import Big4Park


BASE_URL = "https://www.big4.com.au"
STATE_CODES = ["nsw", "vic", "qld", "wa", "sa", "tas", "nt"]

# Map state codes to full names for the DB
STATE_MAP = {
    "nsw": "NSW",
    "vic": "VIC",
    "qld": "QLD",
    "wa": "WA",
    "sa": "SA",
    "tas": "TAS",
    "nt": "NT",
}


class Big4Scraper:
    """Scrapes BIG4 holiday parks from big4.com.au."""

    def __init__(
        self,
        concurrency: int = 10,
        delay: float = 0.5,
        timeout: float = 30.0,
    ):
        self.concurrency = concurrency
        self.delay = delay
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        self._semaphore = asyncio.Semaphore(self.concurrency)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _fetch(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch URL with retries."""
        for attempt in range(retries):
            try:
                async with self._semaphore:
                    resp = await self._client.get(url)
                    if resp.status_code == 200:
                        return resp.text
                    if resp.status_code == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning(f"Rate limited on {url}, waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    return None
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url}: {e}")
        return None

    async def discover_parks(self) -> List[Dict[str, str]]:
        """Discover all park URLs by scraping state listing pages.

        Returns list of dicts with keys: name, url_path, state, region
        """
        all_parks = []

        tasks = [self._discover_state(code) for code in STATE_CODES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"State discovery failed: {result}")
                continue
            all_parks.extend(result)

        logger.info(f"Discovered {len(all_parks)} parks across {len(STATE_CODES)} states")
        return all_parks

    async def _discover_state(self, state_code: str) -> List[Dict[str, str]]:
        """Discover all parks in a single state."""
        url = f"{BASE_URL}/caravan-parks/{state_code}"
        html = await self._fetch(url)
        if not html:
            logger.error(f"Failed to fetch state page: {state_code}")
            return []

        parks = []
        # Extract park links: /caravan-parks/{state}/{region}/{slug}
        pattern = rf'/caravan-parks/{state_code}/([\w-]+)/([\w-]+)'
        matches = re.findall(pattern, html)
        seen = set()

        for region_slug, park_slug in matches:
            url_path = f"/caravan-parks/{state_code}/{region_slug}/{park_slug}"
            if url_path in seen:
                continue
            seen.add(url_path)

            # Skip non-park paths (subpages like /contact, /facilities etc.)
            # These won't appear because our regex only matches 4-segment paths
            # But filter out known subpage slugs just in case
            if park_slug in ("pet-friendly", "facilities", "contact", "deals",
                             "accommodation", "local-attractions", "reviews"):
                continue

            # Try to extract name from link text
            name_pattern = rf'href="{re.escape(url_path)}"[^>]*>([^<]+)<'
            name_match = re.search(name_pattern, html)
            name = name_match.group(1).strip() if name_match else park_slug.replace("-", " ").title()

            # Clean region name
            region = region_slug.replace("-", " ").title()

            parks.append({
                "name": name,
                "url_path": url_path,
                "slug": park_slug,
                "state": STATE_MAP.get(state_code, state_code.upper()),
                "region": region,
            })

        logger.info(f"  {state_code.upper()}: {len(parks)} parks")
        return parks

    async def scrape_park(self, park_info: Dict[str, str]) -> Optional[Big4Park]:
        """Scrape a single park's detail page for structured data."""
        url = f"{BASE_URL}{park_info['url_path']}"
        html = await self._fetch(url)
        if not html:
            return None

        if self.delay > 0:
            await asyncio.sleep(self.delay)

        park = Big4Park(
            name=park_info["name"],
            slug=park_info["slug"],
            url_path=park_info["url_path"],
            state=park_info.get("state"),
            region=park_info.get("region"),
        )

        # Extract JSON-LD structured data
        json_ld = self._extract_json_ld(html)
        if json_ld:
            self._apply_json_ld(park, json_ld)

        # Extract email from contact page
        contact_html = await self._fetch(park.contact_url)
        if contact_html:
            self._extract_contact_info(park, contact_html)

        return park

    async def scrape_all(self) -> List[Big4Park]:
        """Discover and scrape all parks."""
        park_infos = await self.discover_parks()

        parks = []
        tasks = [self.scrape_park(info) for info in park_infos]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            try:
                park = await coro
                if park:
                    parks.append(park)
            except Exception as e:
                logger.error(f"Error scraping park: {e}")

            if (i + 1) % 20 == 0:
                logger.info(f"  Scraped {i + 1}/{len(park_infos)} parks ({len(parks)} successful)")

        logger.info(f"Scraped {len(parks)} parks total")
        return parks

    def _extract_json_ld(self, html: str) -> Optional[Dict]:
        """Extract JSON-LD structured data from HTML.

        Handles both standard <script type="application/ld+json"> tags
        and Next.js RSC streaming format (self.__next_f.push).
        """
        candidates = []

        # Standard JSON-LD script tags
        pattern = r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        candidates.extend(re.findall(pattern, html, re.DOTALL | re.IGNORECASE))

        # Next.js RSC streaming: JSON-LD embedded in self.__next_f.push([1, "..."])
        for m in re.finditer(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL):
            content = m.group(1)
            if '"@context"' not in content and '@context' not in content:
                continue
            try:
                unescaped = content.encode().decode('unicode_escape')
            except (UnicodeDecodeError, ValueError):
                continue
            json_match = re.search(r'(\{"@context".*)', unescaped)
            if not json_match:
                continue
            raw = json_match.group(1)
            # Find balanced braces to extract complete JSON object
            depth = 0
            for i, c in enumerate(raw):
                if c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                if depth == 0:
                    candidates.append(raw[:i + 1])
                    break

        for candidate in candidates:
            try:
                data = json.loads(candidate.strip())
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") in ("LodgingBusiness", "Hotel", "LocalBusiness"):
                            return item
                elif isinstance(data, dict):
                    if data.get("@type") in ("LodgingBusiness", "Hotel", "LocalBusiness"):
                        return data
                    if "@graph" in data:
                        for item in data["@graph"]:
                            if item.get("@type") in ("LodgingBusiness", "Hotel", "LocalBusiness"):
                                return item
            except json.JSONDecodeError:
                continue
        return None

    def _apply_json_ld(self, park: Big4Park, data: Dict) -> None:
        """Apply JSON-LD structured data to park model."""
        if "name" in data:
            park.name = data["name"].strip()

        if "telephone" in data:
            park.phone = data["telephone"].strip()

        if "email" in data:
            park.email = data["email"].strip()

        if "url" in data and data["url"].startswith("http"):
            park.website = data["url"]

        if "petsAllowed" in data:
            park.pets_allowed = data["petsAllowed"]

        if "description" in data:
            park.description = data["description"][:500]

        # Address
        addr = data.get("address", {})
        if isinstance(addr, dict):
            if "streetAddress" in addr:
                park.address = addr["streetAddress"]
            if "addressLocality" in addr:
                park.city = addr["addressLocality"]
            if "addressRegion" in addr:
                park.state = addr["addressRegion"]
            if "postalCode" in addr:
                park.postcode = addr["postalCode"]

        # Geo coordinates
        geo = data.get("geo", {})
        if isinstance(geo, dict):
            if "latitude" in geo:
                try:
                    park.latitude = float(geo["latitude"])
                except (ValueError, TypeError):
                    pass
            if "longitude" in geo:
                try:
                    park.longitude = float(geo["longitude"])
                except (ValueError, TypeError):
                    pass

        # Rating
        rating = data.get("aggregateRating", {})
        if isinstance(rating, dict):
            if "ratingValue" in rating:
                try:
                    park.rating = float(rating["ratingValue"])
                except (ValueError, TypeError):
                    pass
            if "reviewCount" in rating:
                try:
                    park.review_count = int(rating["reviewCount"])
                except (ValueError, TypeError):
                    pass

    def _decode_rsc_text(self, html: str) -> str:
        """Decode Next.js RSC streaming chunks into plain text."""
        parts = []
        for m in re.finditer(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL):
            try:
                parts.append(m.group(1).encode().decode('unicode_escape'))
            except (UnicodeDecodeError, ValueError):
                continue
        return ''.join(parts)

    def _extract_contact_info(self, park: Big4Park, html: str) -> None:
        """Extract contact info from the contact page HTML."""
        # Decode RSC chunks for searching (site uses Next.js RSC streaming)
        text = html + self._decode_rsc_text(html)

        # Extract email from mailto: links
        if not park.email:
            email_pattern = r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            email_match = re.search(email_pattern, text, re.IGNORECASE)
            if email_match:
                park.email = email_match.group(1).strip()

        # Fallback: find any email address in text
        if not park.email:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(?:com\.au|com|org\.au|net\.au)'
            email_match = re.search(email_pattern, text)
            if email_match:
                park.email = email_match.group(0).strip()

        # Extract phone from tel: links (prefer specific park phone over generic 1800)
        phone_pattern = r'tel:/?/?([\d\s+()-]+)'
        phone_matches = re.findall(phone_pattern, text)
        for phone in phone_matches:
            phone = phone.strip()
            if phone and not phone.startswith("1800"):
                park.phone = phone
                break

        # Fallback: Australian local phone pattern (0X XXXX XXXX) in decoded text
        if not park.phone or park.phone.startswith("1800"):
            local_pattern = r'\(0[2-9]\)\s*\d{4}\s*\d{4}'
            local_matches = re.findall(local_pattern, text)
            for phone in local_matches:
                phone = phone.strip()
                if phone:
                    park.phone = phone
                    break

        # Try to find manager/contact person names
        manager_patterns = [
            r'(?:park\s*manager|manager|managed\s*by|host|hosts?)[:\s]+([A-Z][a-z]+ (?:& )?[A-Z][a-z]+)',
            r'(?:contact|managed by)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
        ]
        for pattern in manager_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                manager_name = match.group(1).strip()
                if park.description:
                    park.description = f"Manager: {manager_name}. {park.description}"
                else:
                    park.description = f"Manager: {manager_name}"
                break
