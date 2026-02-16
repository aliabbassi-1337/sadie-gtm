"""BIG4 Holiday Parks - Scraper.

Fetches all park data from BIG4's Algolia search index, then optionally
enriches with contact info (email/phone) from individual park pages.
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

ALGOLIA_APP_ID = "SD2B0F3EPJ"
ALGOLIA_API_KEY = "c98d8ef4558ee5ea250125c84fc8d7bb"
ALGOLIA_INDEX = "UmbracoParkProdIndex"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries"

STATE_MAP = {
    "New South Wales": "NSW",
    "Victoria": "VIC",
    "Queensland": "QLD",
    "Western Australia": "WA",
    "South Australia": "SA",
    "Tasmania": "TAS",
    "Northern Territory": "NT",
    "Australian Capital Territory": "ACT",
}

# Reverse map: abbreviation -> lowercase URL slug
STATE_URL_SLUG = {
    "NSW": "nsw", "VIC": "vic", "QLD": "qld", "WA": "wa",
    "SA": "sa", "TAS": "tas", "NT": "nt", "ACT": "act",
}

# Brand prefixes stripped from park names in URL slugs
_BRAND_PREFIX_RE = re.compile(
    r'^(big4 |nrma |ingenia holidays |tasman holiday parks - '
    r'|breeze holiday parks - |holiday haven )',
    re.IGNORECASE,
)


def _slugify(name: str) -> str:
    """Convert a park name to a URL-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug.strip("-")


def _url_slugify(name: str) -> str:
    """Slugify a park name for use in big4.com.au URLs (strip brand prefix)."""
    stripped = _BRAND_PREFIX_RE.sub("", name).strip()
    return _slugify(stripped)


def _state_code(state_name: str) -> str:
    """Convert full state name to abbreviation."""
    if not state_name:
        return ""
    if len(state_name) <= 3 and state_name.upper() == state_name:
        return state_name
    return STATE_MAP.get(state_name, state_name)


def _normalize_for_match(name: str) -> str:
    """Normalize a park name for fuzzy matching (strip brand, suffix, punctuation)."""
    n = name.lower().strip()
    # Normalize HTML entities and special chars before stripping
    n = n.replace("&amp;", "&").replace("&#x27;", "'").replace("&#39;", "'")
    n = _BRAND_PREFIX_RE.sub("", n).strip()
    n = re.sub(
        r'\s*(holiday park|caravan park|tourist park|holiday village|holiday resort'
        r'|camping ground|glamping retreat|lifestyle park|holiday parks)$',
        '', n,
    )
    n = re.sub(r'[^a-z0-9\s]', '', n)
    return re.sub(r'\s+', ' ', n).strip()


STATE_CODES = ["nsw", "vic", "qld", "wa", "sa", "tas", "nt"]


class Big4Scraper:
    """Fetches BIG4 parks from their Algolia index + contact pages."""

    def __init__(
        self,
        concurrency: int = 10,
        delay: float = 0.5,
        timeout: float = 30.0,
        enrich_contacts: bool = True,
    ):
        self.concurrency = concurrency
        self.delay = delay
        self.timeout = timeout
        self.enrich_contacts = enrich_contacts
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/json",
            },
        )
        self._semaphore = asyncio.Semaphore(self.concurrency)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def fetch_all_from_algolia(self) -> List[Dict]:
        """Fetch all parks from the Algolia search index."""
        resp = await self._client.post(
            ALGOLIA_URL,
            headers={
                "x-algolia-application-id": ALGOLIA_APP_ID,
                "x-algolia-api-key": ALGOLIA_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "requests": [{
                    "indexName": ALGOLIA_INDEX,
                    "params": "hitsPerPage=1000&page=0",
                }]
            },
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data["results"][0]["hits"]
        logger.info(f"Algolia returned {len(hits)} parks")
        return hits

    async def discover_urls(self) -> Dict[str, List[str]]:
        """Discover real park URLs from state listing pages.

        Returns dict mapping normalized park name -> list of url_paths.
        Multiple URLs per norm name handles same-city parks from different brands.
        """
        url_map: Dict[str, List[str]] = {}

        async def _discover_state(state_code: str):
            url = f"{BASE_URL}/caravan-parks/{state_code}"
            html = await self._fetch(url)
            if not html:
                return
            pattern = rf'/caravan-parks/{state_code}/([\w-]+)/([\w-]+)'
            seen = set()
            for region_slug, park_slug in re.findall(pattern, html):
                url_path = f"/caravan-parks/{state_code}/{region_slug}/{park_slug}"
                if url_path in seen:
                    continue
                seen.add(url_path)
                if park_slug in ("pet-friendly", "facilities", "contact", "deals",
                                 "accommodation", "local-attractions", "reviews",
                                 "whats-local", "facilities-and-activities"):
                    continue
                name_pat = rf'href="{re.escape(url_path)}"[^>]*>([^<]+)<'
                name_match = re.search(name_pat, html)
                link_name = name_match.group(1).strip() if name_match else park_slug.replace("-", " ")
                norm = _normalize_for_match(link_name)
                url_map.setdefault(norm, []).append(url_path)

        tasks = [_discover_state(code) for code in STATE_CODES]
        await asyncio.gather(*tasks, return_exceptions=True)
        total = sum(len(v) for v in url_map.values())
        logger.info(f"Discovered {total} real park URLs from state pages")
        return url_map

    def _hit_to_park(self, hit: Dict, url_map: Optional[Dict[str, List[str]]] = None) -> Optional[Big4Park]:
        """Convert an Algolia hit to a Big4Park model."""
        park_data = hit.get("Park", {})
        name = park_data.get("Name", "").strip()
        if not name:
            return None

        state_full = hit.get("State", "")
        state = _state_code(state_full)
        region = hit.get("Region", "")
        town = hit.get("Town", "")
        slug = _slugify(name)

        # Try to find the real URL from discovered state pages
        norm = _normalize_for_match(name)
        candidates = (url_map or {}).get(norm, [])
        url_path = None
        if len(candidates) == 1:
            url_path = candidates[0]
        elif len(candidates) > 1:
            # Multiple URLs for same normalized name - pick best match by slug similarity
            url_slug = _url_slugify(name)
            for c in candidates:
                if url_slug in c:
                    url_path = c
                    break
            if not url_path:
                url_path = candidates[0]
        if not url_path:
            # Fallback: construct URL from Algolia data
            state_url = STATE_URL_SLUG.get(state, state_full.lower().replace(" ", "-")) if state else ""
            region_slug = _slugify(region) if region else ""
            url_slug = _url_slugify(name)
            url_path = f"/caravan-parks/{state_url}/{region_slug}/{url_slug}" if state_url else f"/parks/{slug}"

        # Coordinates from Park or _geoloc
        lat = park_data.get("Latitude")
        lon = park_data.get("Longitude")
        if lat is None or lon is None:
            geoloc = hit.get("_geoloc", {})
            lat = geoloc.get("lat")
            lon = geoloc.get("lng")

        # Rating
        rating = park_data.get("Rating")
        if rating is not None:
            try:
                rating = float(rating)
                if rating == 0:
                    rating = None
            except (ValueError, TypeError):
                rating = None

        # Pets
        pets = park_data.get("PetsAllowed")

        # Description from About
        description = hit.get("About", "")
        if description:
            # Strip HTML tags
            description = re.sub(r"<[^>]+>", "", description).strip()
            description = description[:500]

        # Website
        website = f"{BASE_URL}{url_path}"

        return Big4Park(
            name=name,
            slug=slug,
            url_path=url_path,
            region=region or None,
            state=state or None,
            city=town or park_data.get("Suburb") or None,
            latitude=float(lat) if lat is not None else None,
            longitude=float(lon) if lon is not None else None,
            rating=rating,
            pets_allowed=pets if isinstance(pets, bool) else None,
            description=description or None,
            website=website,
        )

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
        """Extract contact info from the contact page HTML.

        Parses the <address> block for street/city/postcode,
        tel: links for phone, and mailto: links for email.
        """
        text = html + self._decode_rsc_text(html)

        # Extract structured address from <address> tag
        if not park.address:
            addr_match = re.search(r'<address[^>]*>(.*?)</address>', text, re.DOTALL | re.IGNORECASE)
            if addr_match:
                # Extract <p> children as address lines
                lines = re.findall(r'<p[^>]*>(.*?)</p>', addr_match.group(1), re.DOTALL)
                lines = [re.sub(r'<[^>]+>', '', l).strip() for l in lines]
                lines = [l for l in lines if l]
                if len(lines) >= 3:
                    # Typical: [park_name, street, "City POSTCODE", "STATE"]
                    # Skip the first line if it matches the park name
                    if lines[0].lower().startswith(park.name[:10].lower()):
                        lines = lines[1:]
                    if lines:
                        park.address = lines[0]
                    if len(lines) >= 2:
                        # "Sydney 2101" → city="Sydney", postcode="2101"
                        city_post = re.match(r'^(.+?)\s+(\d{4})$', lines[1])
                        if city_post:
                            if not park.city:
                                park.city = city_post.group(1)
                            park.postcode = city_post.group(2)
                        elif not park.city:
                            park.city = lines[1]

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

        # Extract phone from tel: links (prefer local over 1800)
        phone_pattern = r'tel:/?/?([\d\s+()-]+)'
        phone_matches = re.findall(phone_pattern, text)
        first_1800 = None
        for phone in phone_matches:
            phone = phone.strip()
            if not phone:
                continue
            if phone.startswith("1800"):
                if not first_1800:
                    first_1800 = phone
                continue
            park.phone = phone
            break

        # Fallback: Australian local phone pattern
        if not park.phone:
            local_pattern = r'\(0[2-9]\)\s*\d{4}\s*\d{4}'
            local_matches = re.findall(local_pattern, text)
            for phone in local_matches:
                phone = phone.strip()
                if phone:
                    park.phone = phone
                    break

        # Last resort: use 1800 number
        if not park.phone and first_1800:
            park.phone = first_1800

    def _enrich_from_json_ld(self, park: Big4Park, html: str) -> None:
        """Extract phone/email/address from JSON-LD structured data in HTML."""
        ld = self._extract_json_ld(html)
        if not ld:
            return
        if not park.phone and ld.get("telephone"):
            park.phone = ld["telephone"].strip()
        if not park.email and ld.get("email"):
            park.email = ld["email"].strip()
        addr = ld.get("address", {})
        if isinstance(addr, dict):
            if not park.address and addr.get("streetAddress"):
                park.address = addr["streetAddress"]
            if not park.city and addr.get("addressLocality"):
                park.city = addr["addressLocality"]
            if not park.postcode and addr.get("postalCode"):
                park.postcode = addr["postalCode"]

    async def _enrich_park_contact(self, park: Big4Park) -> None:
        """Fetch the park's contact page and extract email/phone.

        Falls back to the main park page if the contact page 404s.
        """
        html = await self._fetch(park.contact_url)
        if html:
            self._extract_contact_info(park, html)
            self._enrich_from_json_ld(park, html)

        # Fallback: try main park page if contact page failed or had no phone
        if not park.phone:
            main_url = f"{BASE_URL}{park.url_path}"
            if main_url != park.contact_url:
                main_html = await self._fetch(main_url)
                if main_html:
                    self._extract_contact_info(park, main_html)
                    self._enrich_from_json_ld(park, main_html)

        if self.delay > 0:
            await asyncio.sleep(self.delay)

    async def scrape_all(self) -> List[Big4Park]:
        """Fetch all parks from Algolia, optionally enrich with contact info."""
        # Fetch from Algolia + discover real URLs in parallel
        hits = await self.fetch_all_from_algolia()
        url_map: Dict[str, List[str]] = await self.discover_urls() if self.enrich_contacts else {}

        parks = []
        for hit in hits:
            park = self._hit_to_park(hit, url_map)
            if park:
                parks.append(park)

        logger.info(f"Parsed {len(parks)} parks from Algolia")

        if self.enrich_contacts:
            logger.info("Enriching parks with contact info from park pages...")
            tasks = [self._enrich_park_contact(p) for p in parks]
            for i, coro in enumerate(asyncio.as_completed(tasks)):
                try:
                    await coro
                except Exception as e:
                    logger.error(f"Error enriching park contact: {e}")
                if (i + 1) % 50 == 0:
                    logger.info(f"  Enriched {i + 1}/{len(parks)} parks")

        with_email = sum(1 for p in parks if p.email)
        with_phone = sum(1 for p in parks if p.phone)
        with_coords = sum(1 for p in parks if p.has_location())
        logger.info(
            f"Scrape complete: {len(parks)} parks, "
            f"{with_coords} with coords, {with_email} with email, {with_phone} with phone"
        )
        return parks

    # --- Legacy HTML methods kept for tests ---

    def _extract_json_ld(self, html: str) -> Optional[Dict]:
        """Extract JSON-LD structured data from HTML."""
        candidates = []

        pattern = r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        candidates.extend(re.findall(pattern, html, re.DOTALL | re.IGNORECASE))

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
