"""RMS API Client.

Fast API-based extraction without Playwright.
Falls back to HTML parsing, then Playwright scraper.
"""

import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel

from lib.rms.models import ExtractedRMSData
from lib.rms.utils import normalize_country, decode_cloudflare_email


# API timeout
API_TIMEOUT = 10.0

# RMS API base URLs by server
API_SERVERS = [
    "bookings.rmscloud.com",
    "bookings12.rmscloud.com",
    "bookings10.rmscloud.com",
    "bookings8.rmscloud.com",
]

# IBE servers for OnlineApi (have richer data)
IBE_SERVERS = [
    "ibe12.rmscloud.com",
    "ibe13.rmscloud.com",
    "ibe14.rmscloud.com",
    "betaibe12.rmscloud.com",
]


class RMSApiResponse(BaseModel):
    """Combined response from RMS APIs."""
    
    # From /api/Property
    property_name: Optional[str] = None
    property_description: Optional[str] = None
    client_id: Optional[str] = None
    
    # From /api/Details
    business_facilities: Optional[str] = None
    features: Optional[str] = None
    travel_directions: Optional[str] = None
    redirect_url: Optional[str] = None
    
    # From /OnlineApi/GetSearchOptions (richer data)
    property_address: Optional[str] = None
    property_phone: Optional[str] = None
    property_email: Optional[str] = None


class RMSApiClient:
    """Fast RMS data extraction via API (no browser needed)."""
    
    def __init__(self, timeout: float = API_TIMEOUT):
        self.timeout = timeout
    
    async def extract(self, slug: str, server: str = "bookings12.rmscloud.com") -> Optional[ExtractedRMSData]:
        """Extract hotel data from RMS API.
        
        Args:
            slug: The client ID (numeric or hex)
            server: RMS server to use
            
        Returns:
            ExtractedRMSData if successful, None if API fails
        """
        try:
            api_data = await self._fetch_api_data(slug, server)
            if not api_data or not api_data.property_name:
                return None
            
            # Build extracted data
            data = ExtractedRMSData(
                slug=slug,
                booking_url=f"https://{server}/Search/Index/{slug}/90/",
                name=api_data.property_name,
            )
            
            # Use OnlineApi data first (most reliable)
            if api_data.property_phone:
                data.phone = api_data.property_phone
            if api_data.property_email:
                data.email = api_data.property_email
            if api_data.property_address:
                data.address = api_data.property_address
                # Parse city/state/country from address
                parsed = self._parse_address_string(api_data.property_address)
                data.city = parsed.get("city")
                data.state = parsed.get("state")
                data.country = parsed.get("country")
            
            # Use redirect URL as website
            if api_data.redirect_url:
                data.website = api_data.redirect_url
            
            # Fill in missing fields from structured description
            if api_data.property_description:
                parsed = self._parse_structured_description(api_data.property_description)
                if not data.phone and parsed["phone"]:
                    data.phone = parsed["phone"]
                if not data.email and parsed["email"]:
                    data.email = parsed["email"]
                if not data.address and parsed["address"]:
                    data.address = parsed["address"]
                if not data.city and parsed["city"]:
                    data.city = parsed["city"]
                if not data.state and parsed["state"]:
                    data.state = parsed["state"]
                if not data.country and parsed["country"]:
                    data.country = parsed["country"]
            
            # Combine all text for fallback extraction
            all_text = " ".join(filter(None, [
                api_data.property_description,
                api_data.business_facilities,
                api_data.features,
                api_data.travel_directions,
            ]))
            
            # Fill in missing fields with regex extraction
            if not data.phone:
                data.phone = self._extract_phone(all_text)
            if not data.email:
                data.email = self._extract_email(all_text)
            if not data.website:
                data.website = self._extract_website(all_text)
            
            # Try to extract address from travel directions if not found
            if not data.address and api_data.travel_directions:
                data.address = self._extract_address(api_data.travel_directions)
            
            # Parse location from text if not found
            if not data.city and not data.state and not data.country:
                data.city, data.state, data.country = self._extract_location(all_text)
            
            return data if data.has_data() else None
            
        except Exception as e:
            logger.debug(f"RMS API error for {slug}: {e}")
            return None
    
    def _parse_address_string(self, address: str) -> dict:
        """Parse full address string to extract city, state, country.
        
        Handles formats like:
        - "215 Pacific Highway, Coffs Harbour NSW 2450, Australia"
        - "850 Main Neerim Road, Drouin West VIC 3818 , Australia"
        """
        result = {"city": None, "state": None, "country": None}
        
        if not address:
            return result
        
        # Clean up extra whitespace
        address = re.sub(r'\s+', ' ', address.strip())
        
        # Check for country at the end
        if re.search(r',?\s*Australia\s*$', address, re.IGNORECASE):
            result["country"] = "AU"
            address = re.sub(r',?\s*Australia\s*$', '', address, flags=re.IGNORECASE)
        elif re.search(r',?\s*New Zealand\s*$', address, re.IGNORECASE):
            result["country"] = "NZ"
            address = re.sub(r',?\s*New Zealand\s*$', '', address, flags=re.IGNORECASE)
        
        # Look for Australian state + postcode pattern: "City STATE Postcode"
        au_match = re.search(
            r'([A-Za-z\s\-\']+)\s+(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)\s+(\d{4})\s*$',
            address,
            re.IGNORECASE
        )
        if au_match:
            result["city"] = au_match.group(1).strip().rstrip(',')
            result["state"] = au_match.group(2).upper()
            if not result["country"]:
                result["country"] = "AU"
        
        # US state + ZIP pattern
        us_states = 'AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC'
        us_match = re.search(
            rf'([A-Za-z\s\-\']+),?\s+({us_states})\s+(\d{{5}}(?:-\d{{4}})?)\s*$',
            address,
            re.IGNORECASE
        )
        if us_match:
            result["city"] = us_match.group(1).strip().rstrip(',')
            result["state"] = us_match.group(2).upper()
            result["country"] = "USA"
        
        return result
    
    async def _fetch_api_data(self, slug: str, server: str) -> Optional[RMSApiResponse]:
        """Fetch data from RMS API endpoints."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = RMSApiResponse()
            
            # 1. First try OnlineApi/GetSearchOptions (richest data - has address, phone, email)
            # Try multiple IBE servers
            ibe_servers = IBE_SERVERS if server.startswith("bookings") else [server] + IBE_SERVERS
            for ibe_server in ibe_servers:
                try:
                    online_resp = await client.get(
                        f"https://{ibe_server}/OnlineApi/GetSearchOptions",
                        params={"clientId": slug, "agentId": "90"},
                    )
                    if online_resp.status_code == 200:
                        data = online_resp.json()
                        prop_opts = data.get("propertyOptions", {})
                        if prop_opts.get("propertyName"):
                            response.property_name = prop_opts.get("propertyName")
                            response.property_address = prop_opts.get("propertyAddress", "").strip()
                            response.property_phone = prop_opts.get("propertyPhoneBH")
                            response.property_email = prop_opts.get("propertyEmail")
                            response.property_description = prop_opts.get("propertyDescription")
                            logger.debug(f"OnlineApi success for {slug} via {ibe_server}")
                            break
                except Exception as e:
                    logger.debug(f"OnlineApi failed for {ibe_server}: {e}")
            
            # 2. Try /api/Property for name/description if not found
            if not response.property_name:
                try:
                    prop_resp = await client.get(
                        f"https://{server}/api/Property",
                        params={"clientId": slug, "languageId": "0"},
                    )
                    if prop_resp.status_code == 200:
                        data = prop_resp.json()
                        response.property_name = data.get("sPropertyName")
                        response.property_description = data.get("sPropertyDescription")
                        response.client_id = str(data.get("nClientId", ""))
                except Exception as e:
                    logger.debug(f"Property API failed: {e}")
            
            # 3. Fetch /api/Details for additional info (travel directions, redirect URL)
            try:
                details_resp = await client.get(
                    f"https://{server}/api/Details",
                    params={"clientId": slug, "agentId": "1", "useCache": "true"},
                )
                if details_resp.status_code == 200:
                    data = details_resp.json()
                    response.business_facilities = data.get("BusinessFacilities")
                    response.features = data.get("Features")
                    response.travel_directions = data.get("TravelDirections")
                    response.redirect_url = data.get("RedirectURLAfterBooking")
                    # Use numeric client ID if we got it
                    if data.get("ClientId"):
                        response.client_id = data.get("ClientId")
            except Exception as e:
                logger.debug(f"Details API failed: {e}")
            
            return response if response.property_name else None
    
    def _parse_structured_description(self, text: str) -> dict:
        """Parse structured property descriptions.
        
        Many RMS properties have descriptions formatted like:
        Property Name
        123 Street Address
        City STATE
        Country Postcode
        Phone: (XX) XXXX XXXX
        Email: xxx@example.com
        """
        result = {
            "address": None,
            "city": None,
            "state": None,
            "country": None,
            "postcode": None,
            "phone": None,
            "email": None,
        }
        
        if not text:
            return result
        
        # Clean HTML
        text = re.sub(r'<[^>]+>', '\n', text)
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        # Look for Phone: line
        for line in lines:
            if line.lower().startswith('phone:'):
                result["phone"] = line.split(':', 1)[1].strip()
                break
        
        # Look for Email: line
        for line in lines:
            if line.lower().startswith('email:'):
                result["email"] = line.split(':', 1)[1].strip()
                break
        
        # Look for Australian address pattern: "City STATE" or "City STATE Country Postcode"
        au_states = r'(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)'
        for i, line in enumerate(lines):
            # Match "City VIC" or "Collingwood VIC"
            state_match = re.search(rf'^([A-Za-z\s\-\']+)\s+{au_states}$', line)
            if state_match:
                result["city"] = state_match.group(1).strip()
                result["state"] = state_match.group(2).upper()
                # Check next line for "Australia Postcode"
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    country_match = re.match(r'(Australia|New Zealand|USA|Canada)\s*(\d{4,5})?', next_line, re.IGNORECASE)
                    if country_match:
                        result["country"] = self._normalize_country(country_match.group(1))
                        if country_match.group(2):
                            result["postcode"] = country_match.group(2)
                # Check previous line for street address
                if i > 0:
                    prev_line = lines[i - 1]
                    if re.search(r'\d+\s+[A-Za-z]', prev_line):  # Looks like street address
                        result["address"] = prev_line
                break
        
        # Look for US address pattern: "City, STATE ZIP"
        us_states = 'AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC'
        for line in lines:
            us_match = re.search(rf'([A-Za-z\s\-\']+),?\s+({us_states})\s+(\d{{5}}(?:-\d{{4}})?)', line)
            if us_match:
                result["city"] = us_match.group(1).strip().rstrip(',')
                result["state"] = us_match.group(2).upper()
                result["postcode"] = us_match.group(3)
                result["country"] = "USA"
                break
        
        return result
    
    def _normalize_country(self, country: str) -> str:
        """Normalize country name to code."""
        if not country:
            return ""
        country_lower = country.lower()
        if "australia" in country_lower:
            return "AU"
        if "new zealand" in country_lower:
            return "NZ"
        if "usa" in country_lower or "united states" in country_lower:
            return "USA"
        if "canada" in country_lower:
            return "CA"
        if "uk" in country_lower or "united kingdom" in country_lower:
            return "UK"
        return country
    
    async def extract_from_html(self, slug: str, server: str = "bookings.rmscloud.com") -> Optional[ExtractedRMSData]:
        """Extract hotel data from HTML page (no JavaScript rendering needed).
        
        The RMS booking pages contain all data in the initial HTML response.
        """
        url = f"https://{server}/Search/Index/{slug}/90/"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # First check the redirect without following
                resp = await client.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
                    follow_redirects=False,
                )
                
                # Check if redirecting to error page
                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("location", "")
                    if "message=" in location or "error" in location.lower():
                        logger.debug(f"Redirect to error page for {slug}: {location}")
                        return None
                
                # Now follow redirects
                resp = await client.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
                    follow_redirects=True,
                )
                
                if resp.status_code != 200:
                    return None
                
                html = resp.text
                if len(html) < 1000:
                    return None
                
                return self._parse_html(html, slug, url)
                
        except Exception as e:
            logger.debug(f"HTML fetch error for {slug}: {e}")
            return None
    
    def _parse_html(self, html: str, slug: str, url: str) -> Optional[ExtractedRMSData]:
        """Parse hotel data from RMS HTML page."""
        # Check for error page indicators
        html_lower = html.lower()
        error_patterns = [
            "object reference not set",
            "<title>error</title>",
            "application issues",
            "page not found",
            "does not exist",
            "no longer available",
        ]
        for pattern in error_patterns:
            if pattern in html_lower:
                logger.debug(f"Error page detected for {slug}: {pattern}")
                return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        data = ExtractedRMSData(slug=slug, booking_url=url)
        
        # Extract name from multiple sources
        # 1. Hidden input field (most reliable)
        name_input = soup.find("input", {"id": "propertyName"})
        if name_input and name_input.get("value"):
            data.name = name_input.get("value").strip()
        
        # 2. H1 tag
        if not data.name:
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(strip=True)
                if name and len(name) > 2 and len(name) < 100:
                    data.name = name
        
        # 3. Hidden P input
        if not data.name:
            p_input = soup.find("input", {"id": "P"})
            if p_input and p_input.get("value"):
                data.name = p_input.get("value").strip()
        
        if not data.name:
            return None
        
        # Extract email (Cloudflare protected)
        cf_email = soup.find("a", {"class": "__cf_email__"})
        if cf_email and cf_email.get("data-cfemail"):
            data.email = decode_cloudflare_email(cf_email.get("data-cfemail"))
        
        # Extract phone from icon
        phone_icon = soup.find("i", {"class": "fa-phone"})
        if phone_icon and phone_icon.parent:
            parent_text = phone_icon.parent.get_text(strip=True)
            phone_match = re.search(r'[\+\d][\d\s\-\(\)]{7,20}', parent_text)
            if phone_match:
                data.phone = phone_match.group(0).strip()
        
        # Get body text for additional extraction
        body_text = soup.get_text(separator="\n")
        
        # Parse structured description if present
        parsed = self._parse_structured_description(body_text)
        if not data.phone and parsed["phone"]:
            data.phone = parsed["phone"]
        if not data.email and parsed["email"]:
            data.email = parsed["email"]
        if parsed["address"]:
            data.address = parsed["address"]
        if parsed["city"]:
            data.city = parsed["city"]
        if parsed["state"]:
            data.state = parsed["state"]
        if parsed["country"]:
            data.country = parsed["country"]
        
        # Extract website from links
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if href.startswith("http") and "rmscloud" not in href and "google" not in href:
                if any(x in href.lower() for x in [".com", ".com.au", ".co.nz", ".co.uk"]):
                    data.website = href
                    break
        
        return data if data.has_data() else None
    
    def _extract_phone(self, text: str) -> Optional[str]:
        """Extract phone number from text."""
        if not text:
            return None
        
        # Clean HTML
        text = re.sub(r'<[^>]+>', ' ', text)
        
        patterns = [
            # Labeled phone (tel, phone, call, locally, international)
            r'(?:tel|phone|call|locally|international)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            # International format with country code (+XX X XXXX XXXX)
            r'(\+\d{1,3}[\s\-]?\d{1,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
            # Australian format (XX XXXX XXXX)
            r'(\d{2}[\s\-]?\d{4}[\s\-]?\d{4})',
            # US format ((XXX) XXX-XXXX)
            r'(\(\d{3}\)[\s\-]?\d{3}[\s\-]?\d{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                phone = match.group(1).strip()
                digits = re.sub(r'\D', '', phone)
                if 7 <= len(digits) <= 15:
                    return phone
        return None
    
    def _extract_email(self, text: str) -> Optional[str]:
        """Extract email from text."""
        if not text:
            return None
        
        text = re.sub(r'<[^>]+>', ' ', text)
        match = re.search(r'[\w\.\-+]+@[\w\.-]+\.\w{2,}', text)
        if match:
            email = match.group(0)
            if not any(x in email.lower() for x in ['rmscloud', 'example', 'test', 'noreply']):
                return email
        return None
    
    def _extract_website(self, text: str) -> Optional[str]:
        """Extract website URL from text."""
        if not text:
            return None
        
        match = re.search(r'(?:www\.|https?://)[\w\.-]+\.\w{2,}[^\s<>"]*', text, re.IGNORECASE)
        if match:
            url = match.group(0)
            if 'rmscloud' not in url.lower():
                if not url.startswith('http'):
                    url = 'https://' + url
                return url
        return None
    
    def _extract_address(self, text: str) -> Optional[str]:
        """Extract address from travel directions."""
        if not text:
            return None
        
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Look for street address patterns
        patterns = [
            r'(\d+\s+[A-Za-z]+\s+(?:St|Street|Rd|Road|Ave|Avenue|Dr|Drive|Blvd|Boulevard)[^\n<]{0,50})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_location(self, text: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract city, state, country from text."""
        city = None
        state = None
        country = None
        
        if not text:
            return city, state, country
        
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Australian states
        au_match = re.search(
            r'(?:in|near|from)\s+([A-Za-z\s\-\']+?)(?:\s+|,\s*)(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)',
            text, re.IGNORECASE
        )
        if au_match:
            city = au_match.group(1).strip()
            state = au_match.group(2).upper()
            country = 'AU'
            return city, state, country
        
        # Check for country mentions
        if re.search(r'Australia', text, re.IGNORECASE):
            country = 'AU'
        elif re.search(r'New Zealand', text, re.IGNORECASE):
            country = 'NZ'
        elif re.search(r'United States|USA', text, re.IGNORECASE):
            country = 'USA'
        elif re.search(r'Canada', text, re.IGNORECASE):
            country = 'CA'
        
        return city, state, country


async def extract_with_fallback(
    slug: str,
    scraper=None,
    server: str = "bookings.rmscloud.com",
) -> tuple[Optional[ExtractedRMSData], str]:
    """Try API -> HTML -> Playwright scraper.
    
    Args:
        slug: RMS client ID
        scraper: Optional RMSScraper instance for fallback
        server: RMS server to use
        
    Returns:
        Tuple of (ExtractedRMSData, method_used) or (None, "none")
        method_used is one of: "api", "html", "scraper", "none"
    """
    api_client = RMSApiClient()
    
    # 1. Try API first (fastest, ~100ms)
    data = await api_client.extract(slug, server)
    if data and data.name:
        has_contact = data.email or data.phone
        has_location = data.city or data.state or data.country
        if has_contact or has_location:
            logger.debug(f"API success for {slug}: {data.name}")
            return data, "api"
    
    # 2. Try HTML parsing (fast, ~500ms, no JS needed)
    html_data = await api_client.extract_from_html(slug, server)
    if html_data and html_data.name:
        logger.debug(f"HTML success for {slug}: {html_data.name}")
        # Merge with API data if we got partial API results
        if data and data.name:
            if not html_data.email and data.email:
                html_data.email = data.email
            if not html_data.phone and data.phone:
                html_data.phone = data.phone
        return html_data, "html"
    
    # 3. Fall back to Playwright scraper (slowest, ~5-10s)
    if scraper:
        logger.debug(f"API+HTML failed for {slug}, falling back to Playwright")
        url = f"https://{server}/Search/Index/{slug}/90/"
        scraped = await scraper.extract(url, slug)
        if scraped:
            return scraped, "scraper"
    
    return None, "none"
