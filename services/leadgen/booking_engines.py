"""
Booking Engine Reverse Lookup - Enumerate hotels via booking engine APIs.

This module provides multiple strategies to discover hotels using booking engines:

1. TheGuestbook API - Cloudbeds partner directory with 800+ hotels
2. Cloudbeds direct enumeration - Scrape booking pages for property data
3. Google Dorks - Search for booking engine URLs

Key insight: Cloudbeds uses 6-character alphanumeric slugs (e.g., 'cl6l0S') that map
to sequential numeric property IDs (e.g., 317832). The numeric IDs are exposed in:
- Image URLs: h-img*.cloudbeds.com/uploads/{property_id}/
- Analytics: ep.property_id={property_id}
"""

import asyncio
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any
from pydantic import BaseModel
import httpx
from loguru import logger


class CloudbedsProperty(BaseModel):
    """A hotel property discovered from Cloudbeds."""
    name: str
    slug: str  # e.g., 'cl6l0S'
    property_id: Optional[int] = None  # Internal numeric ID, e.g., 317832
    booking_url: str  # Full Cloudbeds booking URL
    
    # Contact info (extracted from booking page)
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    contact_person: Optional[str] = None
    
    # Location
    lat: Optional[float] = None
    lng: Optional[float] = None
    
    # Property info
    website: Optional[str] = None  # Main hotel website
    room_count: Optional[int] = None
    description: Optional[str] = None
    
    source: str = "cloudbeds"  # guestbook, google_dork, etc.


class GuestbookProperty(BaseModel):
    """A hotel from TheGuestbook API."""
    id: int
    name: str
    lat: float
    lng: float
    website: Optional[str] = None
    bei_status: str  # 'automated' = Cloudbeds integrated
    trust_you_score: Optional[float] = None
    review_count: Optional[int] = None


class GuestbookScraper:
    """
    Scrape hotel data from TheGuestbook API.
    
    TheGuestbook is Cloudbeds' rewards program that lists 800+ partner hotels.
    API endpoint: /en/destinations/guestbook/fetch_properties
    
    Data includes: hotel name, coordinates, website, integration status.
    """
    
    BASE_URL = "https://theguestbook.com"
    FETCH_URL = f"{BASE_URL}/en/destinations/guestbook/fetch_properties"
    
    # Default bounding box covering continental US
    US_BBOX = {
        "type": "Polygon",
        "coordinates": [[
            [-125.0, 24.0],  # SW corner
            [-66.0, 24.0],   # SE corner
            [-66.0, 50.0],   # NE corner
            [-125.0, 50.0],  # NW corner
            [-125.0, 24.0],  # Close polygon
        ]]
    }
    
    # Florida-specific bbox
    FLORIDA_BBOX = {
        "type": "Polygon",
        "coordinates": [[
            [-87.6, 24.5],   # SW (Key West area)
            [-80.0, 24.5],   # SE (Miami area)
            [-80.0, 31.0],   # NE (Jacksonville area)
            [-87.6, 31.0],   # NW (Pensacola area)
            [-87.6, 24.5],   # Close polygon
        ]]
    }
    
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    async def fetch_page(
        self,
        page: int = 1,
        bbox: Optional[Dict] = None,
        check_in: str = "2026-02-08",
        check_out: str = "2026-02-11",
    ) -> Tuple[List[GuestbookProperty], int, int]:
        """
        Fetch a single page of properties.
        
        Returns: (properties, current_page, total_pages)
        """
        if bbox is None:
            bbox = self.US_BBOX
        
        import json
        filters = json.dumps({
            "filter_overlay": None,
            "bbox": bbox,
            "center_lat": 0,
            "center_lng": 0,
            "is_nearby_query": False,
        })
        
        params = {
            "check_in": check_in,
            "check_out": check_out,
            "currency_code": "USD",
            "filters": filters,
            "page": page,
            "format": "json",
            "clusters_showing": "false",
            "gopher_installed": "false",
        }
        
        resp = await self._client.get(self.FETCH_URL, params=params)
        resp.raise_for_status()
        
        data = resp.json()
        results = data.get("results", {})
        current_page = data.get("currentPage", 1)
        total_pages = data.get("totalPages", 1)
        total_count = data.get("totalCount", 0)
        
        properties = []
        for prop_id, prop_data in results.items():
            try:
                prop = GuestbookProperty(
                    id=int(prop_data.get("id", prop_id)),
                    name=prop_data.get("name", "Unknown"),
                    lat=float(prop_data.get("lat", 0)),
                    lng=float(prop_data.get("lng", 0)),
                    website=prop_data.get("website"),
                    bei_status=prop_data.get("beiStatus", "unknown"),
                    trust_you_score=prop_data.get("trustYouScore"),
                    review_count=prop_data.get("trustYouReviewCount"),
                )
                properties.append(prop)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse property {prop_id}: {e}")
        
        logger.info(f"Fetched page {current_page}/{total_pages}: {len(properties)} properties (total: {total_count})")
        return properties, current_page, total_pages
    
    async def fetch_all(
        self,
        bbox: Optional[Dict] = None,
        max_pages: Optional[int] = None,
        cloudbeds_only: bool = True,
    ) -> List[GuestbookProperty]:
        """
        Fetch all properties, optionally filtered to Cloudbeds-integrated only.
        
        Args:
            bbox: Bounding box polygon (default: continental US)
            max_pages: Limit number of pages (default: fetch all)
            cloudbeds_only: Only return properties with beiStatus='automated'
        """
        all_properties = []
        page = 1
        total_pages = None
        
        while True:
            properties, current_page, total = await self.fetch_page(page, bbox)
            
            if cloudbeds_only:
                properties = [p for p in properties if p.bei_status == "automated"]
            
            all_properties.extend(properties)
            
            if total_pages is None:
                total_pages = total
            
            if max_pages and page >= max_pages:
                break
            if page >= total_pages:
                break
            
            page += 1
            await asyncio.sleep(0.5)  # Rate limiting
        
        logger.info(f"Fetched {len(all_properties)} properties total")
        return all_properties
    
    async def fetch_florida(self, cloudbeds_only: bool = True) -> List[GuestbookProperty]:
        """Fetch all Florida properties."""
        return await self.fetch_all(bbox=self.FLORIDA_BBOX, cloudbeds_only=cloudbeds_only)
    
    async def fetch_by_state(
        self,
        state_bbox: Dict,
        cloudbeds_only: bool = True,
    ) -> List[GuestbookProperty]:
        """Fetch properties within a state bounding box."""
        return await self.fetch_all(bbox=state_bbox, cloudbeds_only=cloudbeds_only)


class CloudbedsPropertyExtractor:
    """
    Extract detailed property info from Cloudbeds booking pages.
    
    Given a Cloudbeds slug (e.g., 'cl6l0S'), fetches the booking page
    and extracts all available property data via the booking API.
    """
    
    BASE_URL = "https://hotels.cloudbeds.com"
    PROPERTY_INFO_URL = f"{BASE_URL}/booking/property_info"
    ROOMS_URL = f"{BASE_URL}/booking/rooms"
    
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    def slug_from_url(self, url: str) -> Optional[str]:
        """Extract Cloudbeds slug from booking URL."""
        match = re.search(r'cloudbeds\.com/(?:en/)?reservation/(\w+)', url)
        return match.group(1) if match else None
    
    async def fetch_property_info(self, slug: str) -> Optional[CloudbedsProperty]:
        """
        Fetch property info via Cloudbeds booking API.
        
        Note: The booking API requires specific headers and may need
        a session cookie from visiting the page first.
        """
        booking_url = f"{self.BASE_URL}/en/reservation/{slug}"
        
        # First, visit the page to get initial data
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            resp = await self._client.get(booking_url, headers=headers, follow_redirects=True)
            
            if resp.status_code != 200:
                logger.warning(f"Failed to fetch {booking_url}: {resp.status_code}")
                return None
            
            html = resp.text
            
            # Extract property name from title
            name_match = re.search(r'<title>([^<]+)</title>', html)
            name = "Unknown"
            if name_match:
                title = name_match.group(1)
                # Remove suffix like " - City, Country - Best Price Guarantee"
                name = title.split(" - ")[0].strip()
            
            # Extract property ID from image URLs or analytics
            property_id = None
            id_match = re.search(r'uploads/(\d+)/', html)
            if id_match:
                property_id = int(id_match.group(1))
            
            # Try to extract address from structured data or page content
            address = None
            city = None
            state = None
            country = None
            postal_code = None
            phone = None
            email = None
            
            # Look for address in JSON-LD or meta tags
            # (Full implementation would use browser automation)
            
            return CloudbedsProperty(
                name=name,
                slug=slug,
                property_id=property_id,
                booking_url=booking_url,
                source="cloudbeds_direct",
            )
            
        except Exception as e:
            logger.error(f"Error fetching property {slug}: {e}")
            return None
    
    async def batch_fetch(
        self,
        slugs: List[str],
        concurrency: int = 5,
    ) -> List[CloudbedsProperty]:
        """Fetch multiple properties with concurrency control."""
        semaphore = asyncio.Semaphore(concurrency)
        
        async def fetch_with_limit(slug: str) -> Optional[CloudbedsProperty]:
            async with semaphore:
                result = await self.fetch_property_info(slug)
                await asyncio.sleep(0.5)  # Rate limiting
                return result
        
        tasks = [fetch_with_limit(slug) for slug in slugs]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]


# Utility functions for slug analysis

def analyze_cloudbeds_slug(slug: str) -> Dict[str, Any]:
    """
    Analyze a Cloudbeds slug to understand its structure.
    
    Findings from research:
    - Slugs are 6 alphanumeric characters (base62-like)
    - Not a simple encoding of the numeric property ID
    - Likely hash-based or encrypted
    """
    return {
        "slug": slug,
        "length": len(slug),
        "is_alphanumeric": slug.isalnum(),
        "chars": list(slug),
        "lowercase_count": sum(1 for c in slug if c.islower()),
        "uppercase_count": sum(1 for c in slug if c.isupper()),
        "digit_count": sum(1 for c in slug if c.isdigit()),
    }


@dataclass
class CommonCrawlRecord:
    """A record from Common Crawl CDX API with WARC location info."""
    slug: str
    url: str
    timestamp: str
    filename: str  # WARC file path on S3
    offset: int    # Byte offset in WARC file
    length: int    # Length in bytes


class CommonCrawlEnumerator:
    """
    Enumerate Cloudbeds hotels from Common Crawl CDX API and WARC archives.
    
    Common Crawl indexes billions of web pages monthly. We can:
    1. Query CDX API to find all Cloudbeds reservation URLs
    2. Fetch the archived HTML directly from S3 (no rate limits!)
    
    This finds MORE hotels than the sitemap because:
    1. Historical data - hotels removed from sitemap
    2. Direct crawled pages - not dependent on sitemap
    3. Multiple monthly snapshots
    """
    
    COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
    CLOUDBEDS_PATTERN = "hotels.cloudbeds.com/reservation/*"
    CC_S3_BASE = "https://data.commoncrawl.org"
    
    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    async def get_index_list(self, year: Optional[int] = None, limit: Optional[int] = None) -> List[str]:
        """Fetch list of Common Crawl indices."""
        resp = await self._client.get(self.COLLINFO_URL)
        resp.raise_for_status()
        
        indices = []
        for item in resp.json():
            index_id = item.get("id", "")
            if index_id.startswith("CC-MAIN-"):
                if year:
                    if f"CC-MAIN-{year}" in index_id:
                        indices.append(index_id)
                else:
                    indices.append(index_id)
        
        if limit:
            indices = indices[:limit]
        
        return indices
    
    async def query_index(self, index_id: str, max_retries: int = 3) -> List[CommonCrawlRecord]:
        """Query a single Common Crawl index for Cloudbeds records with WARC info."""
        url = f"https://index.commoncrawl.org/{index_id}-index"
        params = {"url": self.CLOUDBEDS_PATTERN, "output": "json"}
        
        records = []
        seen_slugs = set()
        
        for attempt in range(max_retries):
            try:
                resp = await self._client.get(url, params=params)
                
                if resp.status_code == 404:
                    return records
                
                if resp.status_code == 503:
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"  {index_id}: 503, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                resp.raise_for_status()
                
                # Parse JSON lines
                import json
                for line in resp.text.strip().split('\n'):
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        # Extract slug from URL
                        match = re.search(r'reservation/([A-Za-z0-9]{6})', data.get('url', ''))
                        if match:
                            slug = match.group(1).lower()
                            # Skip if we already have this slug (dedup)
                            if slug in seen_slugs:
                                continue
                            seen_slugs.add(slug)
                            
                            records.append(CommonCrawlRecord(
                                slug=slug,
                                url=data.get('url', ''),
                                timestamp=data.get('timestamp', ''),
                                filename=data.get('filename', ''),
                                offset=int(data.get('offset', 0)),
                                length=int(data.get('length', 0)),
                            ))
                    except (json.JSONDecodeError, ValueError):
                        continue
                
                logger.info(f"  {index_id}: {len(records)} records")
                return records
                
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                else:
                    logger.error(f"  {index_id}: {e}")
        
        return records
    
    async def fetch_archived_html(self, record: CommonCrawlRecord) -> Optional[str]:
        """Fetch archived HTML from Common Crawl S3 using byte range."""
        if not record.filename or not record.offset or not record.length:
            return None
        
        url = f"{self.CC_S3_BASE}/{record.filename}"
        end_byte = record.offset + record.length - 1
        headers = {"Range": f"bytes={record.offset}-{end_byte}"}
        
        try:
            resp = await self._client.get(url, headers=headers, timeout=30.0)
            if resp.status_code not in (200, 206):
                return None
            
            # Decompress gzip
            import gzip
            try:
                decompressed = gzip.decompress(resp.content)
                # WARC format: headers, blank line, HTTP response headers, blank line, HTML
                # Find the HTML after the headers
                html_start = decompressed.find(b'<!DOCTYPE')
                if html_start == -1:
                    html_start = decompressed.find(b'<html')
                if html_start == -1:
                    return None
                return decompressed[html_start:].decode('utf-8', errors='ignore')
            except Exception:
                return None
                
        except Exception as e:
            logger.debug(f"Failed to fetch archive for {record.slug}: {e}")
            return None
    
    def extract_hotel_info(self, html: str, slug: str) -> Optional[Dict]:
        """Extract hotel name and details from archived HTML using multiple strategies."""
        if not html:
            return None
        
        name = None
        city = None
        country = None
        website = None
        
        # Strategy 1: OpenGraph title (most reliable)
        og_match = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not og_match:
            og_match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']', html, re.IGNORECASE)
        
        # Strategy 2: Twitter title
        twitter_match = re.search(r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        
        # Strategy 3: H1 tag (often the hotel name)
        h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html, re.IGNORECASE)
        
        # Strategy 4: Title tag (fallback)
        title_match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
        
        # Strategy 5: Schema.org data
        schema_name = re.search(r'"name"\s*:\s*"([^"]+)"', html)
        
        # Pick best name source
        raw_name = None
        if og_match:
            raw_name = og_match.group(1).strip()
        elif twitter_match:
            raw_name = twitter_match.group(1).strip()
        elif h1_match:
            raw_name = h1_match.group(1).strip()
        elif schema_name:
            raw_name = schema_name.group(1).strip()
        elif title_match:
            raw_name = title_match.group(1).strip()
        
        if not raw_name:
            return None
        
        # Clean up name - remove common suffixes
        # "Hotel Name - City, Country - Best Price Guarantee"
        # "Hotel Name | Book Direct"
        parts = re.split(r'\s*[-|–]\s*', raw_name)
        name = parts[0].strip()
        
        # Skip generic/garbage names
        skip_names = ['book now', 'reservation', 'booking', 'home', 'welcome', 
                      'best price guarantee', 'official site', 'book direct']
        if name.lower() in skip_names or len(name) < 3:
            # Try second part if first is generic
            if len(parts) > 1:
                name = parts[1].strip()
            else:
                return None
        
        # Extract location from title parts or meta tags
        if len(parts) >= 2:
            loc_str = parts[1]
            # Handle "City, Country" or "City, State, Country"
            loc_parts = [p.strip() for p in loc_str.split(',')]
            if loc_parts:
                city = loc_parts[0]
                if len(loc_parts) > 1:
                    country = loc_parts[-1]
        
        # Try to find hotel website from page
        # Look for links that aren't social media or booking engines
        website_patterns = [
            r'href=["\']?(https?://(?:www\.)?[a-z0-9][-a-z0-9]*\.[a-z]{2,}(?:\.[a-z]{2,})?)["\'\s>]',
        ]
        skip_domains = ['cloudbeds', 'facebook', 'twitter', 'instagram', 'linkedin', 
                        'youtube', 'tripadvisor', 'booking.com', 'expedia', 'hotels.com',
                        'google', 'bing', 'yahoo', 'pinterest', 'tiktok', 'archive.org']
        
        for pattern in website_patterns:
            for match in re.finditer(pattern, html, re.IGNORECASE):
                url = match.group(1)
                # Check if it's not a skip domain
                if not any(skip in url.lower() for skip in skip_domains):
                    website = url
                    break
            if website:
                break
        
        return {
            "slug": slug,
            "name": name,
            "city": city,
            "country": country,
            "website": website,
            "booking_url": f"https://hotels.cloudbeds.com/reservation/{slug}",
        }
    
    async def enumerate_all(
        self,
        max_indices: Optional[int] = None,
        year: Optional[int] = None,
        concurrency: int = 5,
    ) -> List[str]:
        """
        Query Common Crawl indices for all Cloudbeds slugs (legacy method).
        
        Returns list of unique slugs (lowercase).
        """
        indices = await self.get_index_list(year=year, limit=max_indices)
        logger.info(f"Querying {len(indices)} Common Crawl indices...")
        
        all_slugs: set = set()
        semaphore = asyncio.Semaphore(concurrency)
        
        async def query_with_limit(index_id: str) -> List[CommonCrawlRecord]:
            async with semaphore:
                return await self.query_index(index_id)
        
        tasks = [query_with_limit(idx) for idx in indices]
        results = await asyncio.gather(*tasks)
        
        for records in results:
            for r in records:
                all_slugs.add(r.slug)
        
        logger.info(f"Total unique slugs: {len(all_slugs)}")
        return sorted(all_slugs)
    
    async def enumerate_with_details(
        self,
        max_indices: Optional[int] = None,
        year: Optional[int] = None,
        concurrency: int = 10,
    ) -> List[Dict]:
        """
        Query Common Crawl and fetch hotel details from archived HTML.
        
        This fetches from CC's S3, NOT from Cloudbeds - no rate limits!
        
        Returns list of hotel dicts with name, city, country, booking_url.
        """
        # Step 1: Get all records with WARC info
        indices = await self.get_index_list(year=year, limit=max_indices)
        logger.info(f"Querying {len(indices)} Common Crawl indices...")
        
        all_records: Dict[str, CommonCrawlRecord] = {}  # slug -> record (dedup)
        semaphore = asyncio.Semaphore(concurrency)
        
        async def query_with_limit(index_id: str) -> List[CommonCrawlRecord]:
            async with semaphore:
                return await self.query_index(index_id)
        
        tasks = [query_with_limit(idx) for idx in indices]
        results = await asyncio.gather(*tasks)
        
        for records in results:
            for r in records:
                if r.slug not in all_records:
                    all_records[r.slug] = r
        
        logger.info(f"Found {len(all_records)} unique slugs with WARC info")
        
        # Step 2: Fetch archived HTML and extract details
        logger.info(f"Fetching hotel details from Common Crawl archives...")
        hotels = []
        completed = 0
        
        async def fetch_details(record: CommonCrawlRecord) -> Optional[Dict]:
            nonlocal completed
            async with semaphore:
                html = await self.fetch_archived_html(record)
                completed += 1
                if completed % 100 == 0:
                    logger.info(f"  Fetched {completed}/{len(all_records)}...")
                if html:
                    return self.extract_hotel_info(html, record.slug)
                return None
        
        tasks = [fetch_details(r) for r in all_records.values()]
        results = await asyncio.gather(*tasks)
        
        hotels = [h for h in results if h is not None]
        logger.info(f"Extracted details for {len(hotels)} hotels")
        
        return hotels


    async def lookup_slugs_in_cdx(
        self,
        slugs: List[str],
        concurrency: int = 5,
    ) -> Dict[str, CommonCrawlRecord]:
        """
        Look up specific slugs in Common Crawl CDX to get WARC file info.
        
        This allows us to fetch archived HTML for slugs we already know about
        (e.g., from coworker's crawl file) without re-querying all indices.
        
        Returns dict mapping slug -> CommonCrawlRecord with WARC info.
        """
        import json
        import random
        
        results: Dict[str, CommonCrawlRecord] = {}
        semaphore = asyncio.Semaphore(concurrency)
        rate_limit_hits = 0
        
        # Get latest index only (reduce API calls)
        indices = await self.get_index_list(limit=1)
        if not indices:
            logger.error("No Common Crawl indices available")
            return results
        
        async def lookup_slug(slug: str) -> Optional[CommonCrawlRecord]:
            nonlocal rate_limit_hits
            async with semaphore:
                # Small random delay to spread requests
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
                # Try each index until we find a match
                for index_id in indices:
                    url = f"https://index.commoncrawl.org/{index_id}-index"
                    params = {
                        "url": f"hotels.cloudbeds.com/reservation/{slug}",
                        "output": "json",
                        "limit": 1,
                    }
                    
                    # Retry with exponential backoff for 503 errors
                    for attempt in range(5):
                        try:
                            resp = await self._client.get(url, params=params, timeout=30.0)
                            if resp.status_code == 200 and resp.text.strip():
                                data = json.loads(resp.text.strip().split('\n')[0])
                                return CommonCrawlRecord(
                                    slug=slug.lower(),
                                    url=data.get('url', ''),
                                    timestamp=data.get('timestamp', ''),
                                    filename=data.get('filename', ''),
                                    offset=int(data.get('offset', 0)),
                                    length=int(data.get('length', 0)),
                                )
                            elif resp.status_code == 503:
                                # Rate limited - wait longer and retry
                                rate_limit_hits += 1
                                wait_time = (2 ** attempt) + random.uniform(0, 1)
                                await asyncio.sleep(wait_time)
                                continue
                            elif resp.status_code == 404:
                                break  # Not found in this index
                            else:
                                break  # Other error, try next index
                        except Exception:
                            await asyncio.sleep(1)
                            continue
                
                return None
        
        logger.info(f"Looking up {len(slugs)} slugs in Common Crawl CDX (concurrency={concurrency})...")
        
        # Process in smaller batches for faster DB saves
        batch_size = 50
        for i in range(0, len(slugs), batch_size):
            batch = slugs[i:i + batch_size]
            tasks = [lookup_slug(s) for s in batch]
            batch_results = await asyncio.gather(*tasks)
            
            for slug, record in zip(batch, batch_results):
                if record:
                    results[slug.lower()] = record
            
            found = sum(1 for r in batch_results if r)
            pct = ((i + len(batch)) / len(slugs)) * 100
            logger.info(f"  CDX: {i + len(batch)}/{len(slugs)} ({pct:.1f}%) - found {found}/{len(batch)} - rate_limits: {rate_limit_hits}")
            
            # Longer delay between batches when hitting rate limits
            if rate_limit_hits > 0:
                await asyncio.sleep(3)
                rate_limit_hits = 0  # Reset counter
            else:
                await asyncio.sleep(1)
        
        logger.info(f"Found WARC info for {len(results)}/{len(slugs)} slugs")
        return results


class CrawlIngester:
    """
    High-performance ingester for crawled booking engine URLs.
    
    Improvements over basic ingestion:
    1. Uses Common Crawl S3 archives directly (50+ hotels/sec, no rate limits)
    2. Better name extraction (og:title, h1, meta tags, schema.org)
    3. Extracts actual hotel website when available
    4. Checkpoint/resume for large imports
    5. Fuzzy deduplication support
    """
    
    def __init__(
        self,
        checkpoint_file: Optional[str] = None,
        concurrency: int = 50,
    ):
        self.checkpoint_file = checkpoint_file
        self.concurrency = concurrency
        self._processed_slugs: set = set()
        self._enumerator: Optional[CommonCrawlEnumerator] = None
    
    def _load_checkpoint(self) -> set:
        """Load processed slugs from checkpoint file."""
        if not self.checkpoint_file:
            return set()
        
        from pathlib import Path
        path = Path(self.checkpoint_file)
        if path.exists():
            return set(path.read_text().strip().split('\n'))
        return set()
    
    def _save_checkpoint(self, slugs: List[str]):
        """Append slugs to checkpoint file."""
        if not self.checkpoint_file:
            return
        
        from pathlib import Path
        path = Path(self.checkpoint_file)
        with path.open('a') as f:
            for slug in slugs:
                f.write(f"{slug}\n")
    
    async def ingest_cloudbeds_file(
        self,
        file_path: str,
        use_common_crawl: bool = True,
    ) -> List[Dict]:
        """
        Ingest Cloudbeds slugs from file with hotel details.
        
        Args:
            file_path: Path to text file with one slug per line
            use_common_crawl: If True, fetch from CC archives (fast). If False, use Wayback.
            
        Returns list of hotel dicts with name, city, website, booking_url.
        """
        from pathlib import Path
        
        # Load slugs
        path = Path(file_path)
        all_slugs = [s.strip().lower() for s in path.read_text().strip().split('\n') if s.strip()]
        all_slugs = list(set(all_slugs))  # Dedupe
        
        logger.info(f"Loaded {len(all_slugs)} unique slugs from {path.name}")
        
        # Load checkpoint
        self._processed_slugs = self._load_checkpoint()
        if self._processed_slugs:
            logger.info(f"Resuming: {len(self._processed_slugs)} already processed")
        
        # Filter out already processed
        slugs_to_process = [s for s in all_slugs if s not in self._processed_slugs]
        logger.info(f"Processing {len(slugs_to_process)} slugs...")
        
        if not slugs_to_process:
            logger.info("All slugs already processed!")
            return []
        
        hotels = []
        
        if use_common_crawl:
            # Use Common Crawl archives (fast path)
            async with CommonCrawlEnumerator() as enumerator:
                self._enumerator = enumerator
                
                # Step 1: Look up slugs in CDX to get WARC info
                cdx_records = await enumerator.lookup_slugs_in_cdx(
                    slugs_to_process,
                    concurrency=self.concurrency,
                )
                
                # Step 2: Fetch HTML from S3 and extract info
                logger.info(f"Fetching HTML for {len(cdx_records)} slugs from S3...")
                
                semaphore = asyncio.Semaphore(self.concurrency)
                
                async def fetch_and_extract(slug: str, record: CommonCrawlRecord) -> Optional[Dict]:
                    async with semaphore:
                        html = await enumerator.fetch_archived_html(record)
                        if html:
                            return enumerator.extract_hotel_info(html, slug)
                        return None
                
                # Process in batches
                batch_size = 500
                items = list(cdx_records.items())
                
                for i in range(0, len(items), batch_size):
                    batch = items[i:i + batch_size]
                    tasks = [fetch_and_extract(slug, record) for slug, record in batch]
                    results = await asyncio.gather(*tasks)
                    
                    batch_hotels = [r for r in results if r and r.get('name')]
                    hotels.extend(batch_hotels)
                    
                    # Save checkpoint
                    processed = [slug for slug, _ in batch]
                    self._save_checkpoint(processed)
                    
                    logger.info(f"  {i + len(batch)}/{len(items)}: extracted {len(batch_hotels)} hotels")
                
                # Handle slugs not found in CDX - fallback to Wayback
                missing_slugs = [s for s in slugs_to_process if s not in cdx_records]
                if missing_slugs:
                    logger.info(f"Falling back to Wayback for {len(missing_slugs)} slugs not in CDX...")
                    wayback_hotels = await self._fetch_from_wayback(missing_slugs)
                    hotels.extend(wayback_hotels)
        else:
            # Wayback-only path
            hotels = await self._fetch_from_wayback(slugs_to_process)
        
        logger.info(f"Extracted {len(hotels)} hotels with names")
        return hotels
    
    async def _fetch_from_wayback(self, slugs: List[str]) -> List[Dict]:
        """Fallback: fetch hotel info from Wayback Machine."""
        import httpx
        
        hotels = []
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def fetch_one(client: httpx.AsyncClient, slug: str) -> Optional[Dict]:
            async with semaphore:
                booking_url = f"https://hotels.cloudbeds.com/reservation/{slug}"
                wayback_url = f"https://web.archive.org/web/2024/{booking_url}"
                
                try:
                    resp = await client.get(wayback_url, follow_redirects=True, timeout=15.0)
                    if resp.status_code == 200:
                        # Use the improved extraction
                        if self._enumerator:
                            return self._enumerator.extract_hotel_info(resp.text, slug)
                except Exception:
                    pass
                
                return None
        
        async with httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0 (compatible; HotelBot/1.0)"},
        ) as client:
            batch_size = 100
            for i in range(0, len(slugs), batch_size):
                batch = slugs[i:i + batch_size]
                tasks = [fetch_one(client, s) for s in batch]
                results = await asyncio.gather(*tasks)
                
                batch_hotels = [r for r in results if r and r.get('name')]
                hotels.extend(batch_hotels)
                
                self._save_checkpoint(batch)
                
                logger.info(f"  Wayback {i + len(batch)}/{len(slugs)}: {len(batch_hotels)} hotels")
        
        return hotels


class RMSScanner:
    """
    Scan RMS Cloud for hotel properties by ID enumeration.
    
    RMS uses numeric IDs that are sparse (not sequential).
    We need to scan ranges to find valid properties.
    
    URL patterns:
    - ibe13.rmscloud.com/{id}/3 (newer)
    - bookings12.rmscloud.com/search/index/{id}/3 (older)
    
    RATE LIMITING:
    - Default: 10 concurrent, 0.2s delay = ~50 req/sec
    - Conservative: 5 concurrent, 0.5s delay = ~10 req/sec
    - Distributed: Split ranges across multiple EC2 instances
    
    For 20,000 IDs:
    - Aggressive (100 conc): ~3 min, HIGH ban risk
    - Default (10 conc, 0.2s): ~40 min, medium risk
    - Conservative (5 conc, 0.5s): ~2 hours, low risk
    - Distributed (7 EC2 × 10 conc): ~6 min, low risk
    """
    
    # Known RMS subdomains (from Common Crawl analysis)
    SUBDOMAINS = [
        "ibe13.rmscloud.com",
        "ibe12.rmscloud.com", 
        "ibe14.rmscloud.com",
        "bookings12.rmscloud.com",
        "bookings10.rmscloud.com",
        "bookings8.rmscloud.com",
    ]
    
    def __init__(
        self,
        concurrency: int = 10,  # Conservative default
        timeout: float = 5.0,
        delay: float = 0.2,  # Delay between requests
    ):
        self.concurrency = concurrency
        self.timeout = timeout
        self.delay = delay
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HotelBot/1.0)"},
            follow_redirects=True,
        )
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    async def check_id(self, property_id: int, subdomain: str = "ibe13.rmscloud.com") -> Optional[Dict]:
        """
        Check if a property ID exists and extract hotel info.
        
        Returns dict with id, name, booking_url or None if not found.
        """
        if subdomain.startswith("ibe"):
            url = f"https://{subdomain}/{property_id}/3"
        else:
            url = f"https://{subdomain}/search/index/{property_id}/3"
        
        try:
            # Rate limiting delay
            if self.delay > 0:
                await asyncio.sleep(self.delay)
            
            resp = await self._client.get(url)
            
            # Handle rate limiting
            if resp.status_code == 429:
                logger.warning(f"Rate limited at ID {property_id}, backing off...")
                await asyncio.sleep(5)
                return None
            
            # Valid property returns 200, invalid returns 404 or redirect
            if resp.status_code != 200:
                return None
            
            html = resp.text
            
            # Check for error pages
            if "not found" in html.lower() or "error" in html.lower()[:500]:
                return None
            
            # Extract hotel name
            name = None
            
            # Try multiple extraction methods
            patterns = [
                r'<div[^>]*class="[^"]*prop-name[^"]*"[^>]*>([^<]+)</div>',
                r'<h1[^>]*>([^<]+)</h1>',
                r'<title>([^<]+)</title>',
                r'"propertyName"\s*:\s*"([^"]+)"',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    raw_name = match.group(1).strip()
                    # Clean up
                    raw_name = re.split(r'\s*[-|–]\s*', raw_name)[0].strip()
                    if raw_name and raw_name.lower() not in ['search', 'booking', 'rms']:
                        name = raw_name
                        break
            
            if not name:
                return None
            
            return {
                "id": property_id,
                "name": name,
                "booking_url": url,
                "subdomain": subdomain,
            }
            
        except Exception:
            return None
    
    async def scan_range(
        self,
        start_id: int,
        end_id: int,
        subdomain: str = "ibe13.rmscloud.com",
        on_found: Optional[callable] = None,
    ) -> List[Dict]:
        """
        Scan a range of IDs for valid properties.
        
        Args:
            start_id: First ID to check
            end_id: Last ID to check (inclusive)
            subdomain: RMS subdomain to scan
            on_found: Optional callback(hotel_dict) for incremental processing
            
        Returns list of found hotels.
        """
        found = []
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def check_with_limit(property_id: int) -> Optional[Dict]:
            async with semaphore:
                return await self.check_id(property_id, subdomain)
        
        total = end_id - start_id + 1
        batch_size = 1000
        
        for batch_start in range(start_id, end_id + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_id)
            tasks = [check_with_limit(i) for i in range(batch_start, batch_end + 1)]
            results = await asyncio.gather(*tasks)
            
            batch_found = [r for r in results if r]
            found.extend(batch_found)
            
            # Callback for incremental processing
            if on_found:
                for hotel in batch_found:
                    await on_found(hotel)
            
            processed = batch_end - start_id + 1
            logger.info(f"  Scanned {processed}/{total} IDs, found {len(found)} properties")
        
        return found
    
    async def scan_all_subdomains(
        self,
        start_id: int = 1,
        end_id: int = 20000,
        on_found: Optional[callable] = None,
    ) -> List[Dict]:
        """
        Scan all known RMS subdomains for properties.
        
        Default range 1-20000 covers most properties (~30 min at 100 concurrency).
        """
        all_found = []
        seen_names = set()  # Dedupe by name
        
        for subdomain in self.SUBDOMAINS:
            logger.info(f"Scanning {subdomain} ({start_id}-{end_id})...")
            
            found = await self.scan_range(
                start_id, end_id, subdomain, on_found
            )
            
            # Dedupe
            for hotel in found:
                name_key = hotel["name"].lower().strip()
                if name_key not in seen_names:
                    seen_names.add(name_key)
                    all_found.append(hotel)
        
        logger.info(f"Total unique properties: {len(all_found)}")
        return all_found


# Known Cloudbeds slugs for reference/testing
KNOWN_SLUGS = {
    "cl6l0S": {"name": "The Kendall", "property_id": 317832, "city": "Boerne", "state": "TX"},
    "UxSswi": {"name": "7 Seas Hotel", "property_id": 202743, "city": "Miami", "state": "FL"},
    "sEhTC1": {"name": "St Augustine Hotel", "city": "Miami Beach", "state": "FL"},
    "TxCgVr": {"name": "Casa Ocean", "city": "Miami Beach", "state": "FL"},
    "iocJE7": {"name": "Sebastian Gardens Inn & Suites", "city": "Sebastian", "state": "FL"},
    "UpukGL": {"name": "Up Midtown", "city": "Miami", "state": "FL"},
}
