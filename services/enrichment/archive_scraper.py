"""Archive Scraper - Fetch and extract hotel data from web archives.

Fetches HTML from live pages with fallback to Common Crawl and Wayback Machine
for 404 URLs. Extracts hotel name, address, city, country from the HTML.

Usage:
    scraper = ArchiveScraper(httpx_client)
    data = await scraper.extract(booking_url)
"""

import gzip
import json
import re
from typing import Optional, Dict, Any

import httpx
from pydantic import BaseModel


class ExtractedBookingData(BaseModel):
    """Data extracted from a booking page."""
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    zip_code: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    contact_name: Optional[str] = None


class ArchiveScraper:
    """Fetches HTML from live pages with archive fallback for 404s.
    
    Sources (in order):
    1. Live page - fastest if available
    2. Common Crawl - fast, no rate limits, good for 2019-2022 data
    3. Wayback Machine - largest archive, rate limited
    """
    
    # Older indexes have dead URLs that are now 404
    ARCHIVE_INDEXES = [
        "CC-MAIN-2020-34",
        "CC-MAIN-2021-04", 
        "CC-MAIN-2019-51",
        "CC-MAIN-2022-05",
        "CC-MAIN-2020-16",
    ]
    
    GARBAGE_INDICATORS = [
        "soluções online",
        "oops! something went wrong",
        "page not found",
        "404",
    ]
    
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    
    # =========================================================================
    # PUBLIC API
    # =========================================================================
    
    async def fetch(self, url: str, use_archives: bool = True) -> Optional[str]:
        """Fetch HTML from URL, with optional archive fallback.
        
        Returns HTML content or None if not found.
        """
        html = await self._fetch_live(url)
        
        if not html and use_archives:
            html = await self._fetch_common_crawl(url)
        
        if not html and use_archives:
            html = await self._fetch_wayback(url)
        
        return html
    
    async def extract(self, url: str, use_archives: bool = True) -> Optional[ExtractedBookingData]:
        """Fetch and extract hotel data from URL.
        
        Returns ExtractedBookingData or None if not found.
        """
        html = await self.fetch(url, use_archives=use_archives)
        if not html:
            return None
        return self.extract_from_html(html)
    
    # =========================================================================
    # FETCHING
    # =========================================================================
    
    async def _fetch_live(self, url: str) -> Optional[str]:
        """Try to fetch from live URL."""
        try:
            resp = await self.client.get(url, headers=self.headers, follow_redirects=True, timeout=30.0)
            if resp.status_code == 200:
                html = resp.text
                # Check for garbage (redirected to error page)
                if any(ind in html.lower() for ind in self.GARBAGE_INDICATORS):
                    return None
                return html
        except Exception:
            pass
        return None
    
    async def _fetch_common_crawl(self, url: str) -> Optional[str]:
        """Try to fetch from Common Crawl archives."""
        for crawl_id in self.ARCHIVE_INDEXES:
            try:
                resp = await self.client.get(
                    f"https://index.commoncrawl.org/{crawl_id}-index",
                    params={"url": url, "output": "json"},
                    timeout=15,
                )
                if resp.status_code != 200 or not resp.text.strip():
                    continue
                
                data = json.loads(resp.text.strip().split("\n")[0])
                if data.get("status") != "200":
                    continue
                
                warc_url = f"https://data.commoncrawl.org/{data['filename']}"
                offset = int(data["offset"])
                length = int(data["length"])
                warc_headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
                
                resp2 = await self.client.get(warc_url, headers=warc_headers, timeout=60)
                if resp2.status_code == 206:
                    content = gzip.decompress(resp2.content)
                    return content.decode("utf-8", errors="ignore")
            except Exception:
                continue
        return None
    
    async def _fetch_wayback(self, url: str) -> Optional[str]:
        """Try to fetch from Wayback Machine."""
        try:
            resp = await self.client.get(
                "https://archive.org/wayback/available",
                params={"url": url},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                wayback_url = data.get("archived_snapshots", {}).get("closest", {}).get("url")
                if wayback_url:
                    resp2 = await self.client.get(wayback_url, timeout=30, follow_redirects=True)
                    if resp2.status_code == 200:
                        return resp2.text
        except Exception:
            pass
        return None
    
    # =========================================================================
    # EXTRACTION
    # =========================================================================
    
    def extract_from_html(self, html: str) -> Optional[ExtractedBookingData]:
        """Extract hotel data from HTML (live or archived)."""
        # Try Cloudbeds-specific extraction first (modern format)
        cloudbeds_data = self._extract_cloudbeds_modern(html)
        if cloudbeds_data and (cloudbeds_data.city or cloudbeds_data.address):
            # Get name from other sources
            name = None
            json_ld = self._extract_json_ld(html)
            if json_ld and json_ld.get("name"):
                name = json_ld["name"].strip()
            if not name:
                meta_data = self._extract_meta_tags(html)
                name = meta_data.name
            cloudbeds_data.name = name
            return cloudbeds_data
        
        # Try older Cloudbeds format (archives from 2019-2022)
        older_data = self._extract_cloudbeds_archive(html)
        if older_data and (older_data.city or older_data.address):
            return older_data
        
        # Try JSON-LD
        json_ld = self._extract_json_ld(html)
        if json_ld:
            data = self._parse_json_ld(json_ld)
            if data.city or data.address:
                return data
        
        # Fall back to meta tags
        data = self._extract_meta_tags(html)
        if data.city or data.address:
            return data
        
        # Return whatever we have (even just name)
        return older_data or data
    
    def _extract_cloudbeds_modern(self, html: str) -> Optional[ExtractedBookingData]:
        """Extract from modern Cloudbeds pages with data-be-text elements."""
        if 'data-testid="property-address-and-contact"' not in html and 'cb-address-and-contact' not in html:
            return None
        
        # Extract all text lines from the address container
        text_pattern = r'<p[^>]*data-be-text="true"[^>]*>([^<]*(?:<a[^>]*>([^<]*)</a>[^<]*)?)</p>'
        matches = re.findall(text_pattern, html, re.IGNORECASE | re.DOTALL)
        
        if not matches:
            return None
        
        lines = []
        for match in matches:
            text = match[1].strip() if match[1] else match[0].strip()
            if text:
                lines.append(text)
        
        if len(lines) < 3:
            return None
        
        # Parse address lines
        address = lines[0] if lines else None
        city = lines[1] if len(lines) > 1 else None
        
        # Third line often "State Country"
        state = None
        country = None
        if len(lines) > 2:
            state_country = lines[2]
            parts = state_country.split()
            if len(parts) >= 2:
                state = parts[0]
                country = " ".join(parts[1:])
            else:
                country = state_country
        
        # Extract email
        email = None
        email_match = re.search(r'href="mailto:([^"]+)"', html)
        if email_match:
            email = email_match.group(1)
        
        # Extract phone
        phone = None
        phone_match = re.search(r'href="tel:([^"]+)"', html)
        if phone_match:
            phone = phone_match.group(1)
        
        return ExtractedBookingData(
            address=address,
            city=city,
            state=state,
            country=country,
            email=email,
            phone=phone,
        )
    
    def _extract_cloudbeds_archive(self, html: str) -> Optional[ExtractedBookingData]:
        """Extract from older Cloudbeds HTML format (pre-2022 archives)."""
        name = None
        city = None
        country = None
        address = None
        
        # Title: "Hotel Name - City, Country - Best Price Guarantee"
        title_match = re.search(r'<title>([^<]+)</title>', html)
        if title_match:
            title = title_match.group(1).strip()
            if 'cloudbeds' not in title.lower() and 'soluções' not in title.lower():
                title = re.sub(r'\s*-\s*Best Price Guarantee.*$', '', title, flags=re.I)
                parts = title.split(' - ')
                if len(parts) >= 2:
                    name = parts[0].strip()
                    loc = parts[1].strip()
                    loc_parts = loc.split(',')
                    if len(loc_parts) >= 2:
                        city = loc_parts[0].strip()
                        country = loc_parts[-1].strip()
                    else:
                        city = loc
        
        # Address: "Address 1:</span> Street Address</p>"
        addr_match = re.search(r'Address\s*\d?:</span>\s*([^<]+)</p>', html)
        if addr_match:
            address = addr_match.group(1).strip()
        
        # City from older format
        city_match = re.search(r'City\s*:</span>\s*([^<]+)</p>', html)
        if city_match and not city:
            city = city_match.group(1).strip().split(' - ')[0].strip()
        
        if name or city or address:
            return ExtractedBookingData(
                name=name,
                address=address,
                city=city,
                country=country,
            )
        return None
    
    def _extract_json_ld(self, html: str) -> Optional[Dict[str, Any]]:
        """Extract JSON-LD structured data from HTML."""
        try:
            pattern = r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
            matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
            
            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, list):
                        for item in data:
                            if item.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                                return item
                    elif data.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                        return data
                    elif "@graph" in data:
                        for item in data["@graph"]:
                            if item.get("@type") in ["Hotel", "LodgingBusiness", "LocalBusiness", "Organization"]:
                                return item
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass
        return None
    
    def _parse_json_ld(self, json_ld: Dict[str, Any]) -> ExtractedBookingData:
        """Parse address from JSON-LD structured data."""
        name = json_ld.get("name", "").strip() if json_ld.get("name") else None
        address = None
        city = None
        state = None
        country = None
        
        addr_data = json_ld.get("address", {})
        if isinstance(addr_data, str):
            address = addr_data
        elif isinstance(addr_data, dict):
            address = addr_data.get("streetAddress")
            city = addr_data.get("addressLocality")
            state = addr_data.get("addressRegion")
            country = addr_data.get("addressCountry")
            if isinstance(country, dict):
                country = country.get("name")
        
        return ExtractedBookingData(
            name=name,
            address=address,
            city=city,
            state=state,
            country=country,
        )
    
    def _extract_meta_tags(self, html: str) -> ExtractedBookingData:
        """Extract from OpenGraph and Twitter meta tags."""
        name = None
        
        # OpenGraph title
        og_match = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        if not og_match:
            og_match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']', html, re.I)
        
        # Twitter title
        twitter_match = re.search(r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        
        # Title tag fallback
        title_match = re.search(r'<title>([^<]+)</title>', html, re.I)
        
        if og_match:
            name = og_match.group(1).strip()
        elif twitter_match:
            name = twitter_match.group(1).strip()
        elif title_match:
            name = title_match.group(1).strip()
        
        # Clean up name
        if name:
            parts = re.split(r'\s*[-|–]\s*', name)
            name = parts[0].strip()
        
        return ExtractedBookingData(name=name)
