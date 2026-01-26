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


class CommonCrawlEnumerator:
    """
    Enumerate Cloudbeds slugs from Common Crawl CDX API.
    
    Common Crawl indexes billions of web pages monthly. We can query their
    CDX API to find all indexed Cloudbeds reservation URLs.
    
    This finds MORE hotels than the sitemap because:
    1. Historical data - hotels removed from sitemap
    2. Direct crawled pages - not dependent on sitemap
    3. Multiple monthly snapshots
    """
    
    COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
    CLOUDBEDS_PATTERN = "hotels.cloudbeds.com/reservation/*"
    
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
    
    async def query_index(self, index_id: str, max_retries: int = 3) -> set:
        """Query a single Common Crawl index for Cloudbeds slugs."""
        url = f"https://index.commoncrawl.org/{index_id}-index"
        params = {"url": self.CLOUDBEDS_PATTERN, "output": "json"}
        
        slugs = set()
        
        for attempt in range(max_retries):
            try:
                resp = await self._client.get(url, params=params)
                
                if resp.status_code == 404:
                    return slugs
                
                if resp.status_code == 503:
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"  {index_id}: 503, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                resp.raise_for_status()
                
                # Extract 6-char slugs (lowercase for dedup)
                for match in re.finditer(r'reservation/([A-Za-z0-9]{6})', resp.text):
                    slugs.add(match.group(1).lower())
                
                logger.info(f"  {index_id}: {len(slugs)} slugs")
                return slugs
                
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                else:
                    logger.error(f"  {index_id}: {e}")
        
        return slugs
    
    async def enumerate_all(
        self,
        max_indices: Optional[int] = None,
        year: Optional[int] = None,
        concurrency: int = 5,
    ) -> List[str]:
        """
        Query Common Crawl indices for all Cloudbeds slugs.
        
        Args:
            max_indices: Limit number of indices to query
            year: Only query indices from specific year
            concurrency: Concurrent requests
            
        Returns list of unique slugs (lowercase).
        """
        indices = await self.get_index_list(year=year, limit=max_indices)
        logger.info(f"Querying {len(indices)} Common Crawl indices...")
        
        all_slugs: set = set()
        semaphore = asyncio.Semaphore(concurrency)
        
        async def query_with_limit(index_id: str) -> set:
            async with semaphore:
                return await self.query_index(index_id)
        
        tasks = [query_with_limit(idx) for idx in indices]
        results = await asyncio.gather(*tasks)
        
        for slugs in results:
            all_slugs.update(slugs)
        
        logger.info(f"Total unique slugs: {len(all_slugs)}")
        return sorted(all_slugs)


# Known Cloudbeds slugs for reference/testing
KNOWN_SLUGS = {
    "cl6l0S": {"name": "The Kendall", "property_id": 317832, "city": "Boerne", "state": "TX"},
    "UxSswi": {"name": "7 Seas Hotel", "property_id": 202743, "city": "Miami", "state": "FL"},
    "sEhTC1": {"name": "St Augustine Hotel", "city": "Miami Beach", "state": "FL"},
    "TxCgVr": {"name": "Casa Ocean", "city": "Miami Beach", "state": "FL"},
    "iocJE7": {"name": "Sebastian Gardens Inn & Suites", "city": "Sebastian", "state": "FL"},
    "UpukGL": {"name": "Up Midtown", "city": "Miami", "state": "FL"},
}
