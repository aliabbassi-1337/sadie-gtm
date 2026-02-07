"""Mews API Client for extracting hotel data.

Uses a hybrid approach:
1. Get session token via Playwright (once)
2. Use fast httpx API calls for all hotels
3. Refresh session when expired

Supports optional Brightdata proxy integration:
    client = MewsApiClient(use_brightdata=True)

The API endpoint is: https://api.mews.com/api/bookingEngine/v1/configurations/get
"""

import asyncio
import os
import time
import httpx
from typing import Optional
from pydantic import BaseModel
from loguru import logger


def _get_brightdata_proxy(prefer_cheap: bool = True) -> Optional[str]:
    """Build Brightdata proxy URL if credentials are available.
    
    Args:
        prefer_cheap: If True, prefer datacenter (~$0.11/GB) > residential (~$5.5/GB) > unlocker
    """
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    if not customer_id:
        return None
    
    if prefer_cheap:
        # Try datacenter first (cheapest ~$0.11/GB)
        dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
        dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
        if dc_zone and dc_password:
            username = f"brd-customer-{customer_id}-zone-{dc_zone}"
            return f"http://{username}:{dc_password}@brd.superproxy.io:33335"
        
        # Fall back to residential (~$5.5/GB)
        res_zone = os.getenv("BRIGHTDATA_RES_ZONE", "")
        res_password = os.getenv("BRIGHTDATA_RES_PASSWORD", "")
        if res_zone and res_password:
            username = f"brd-customer-{customer_id}-zone-{res_zone}"
            return f"http://{username}:{res_password}@brd.superproxy.io:33335"
    
    # Fall back to unlocker zone
    zone = os.getenv("BRIGHTDATA_ZONE", "")
    password = os.getenv("BRIGHTDATA_ZONE_PASSWORD", "")
    
    if zone and password:
        username = f"brd-customer-{customer_id}-zone-{zone}"
        return f"http://{username}:{password}@brd.superproxy.io:33335"
    return None

# Common country code to full name mapping
_COUNTRY_CODES = {
    "US": "United States", "CA": "Canada", "GB": "United Kingdom", "UK": "United Kingdom",
    "AU": "Australia", "NZ": "New Zealand", "IE": "Ireland",
    "DE": "Germany", "FR": "France", "ES": "Spain", "IT": "Italy", "PT": "Portugal",
    "NL": "Netherlands", "BE": "Belgium", "AT": "Austria", "CH": "Switzerland",
    "SE": "Sweden", "NO": "Norway", "DK": "Denmark", "FI": "Finland",
    "PL": "Poland", "CZ": "Czech Republic", "HU": "Hungary", "RO": "Romania",
    "GR": "Greece", "HR": "Croatia", "BG": "Bulgaria", "SK": "Slovakia",
    "SI": "Slovenia", "EE": "Estonia", "LV": "Latvia", "LT": "Lithuania",
    "MX": "Mexico", "BR": "Brazil", "AR": "Argentina", "CL": "Chile", "CO": "Colombia",
    "PE": "Peru", "EC": "Ecuador", "CR": "Costa Rica", "PA": "Panama",
    "JP": "Japan", "KR": "South Korea", "CN": "China", "TW": "Taiwan",
    "TH": "Thailand", "VN": "Vietnam", "MY": "Malaysia", "SG": "Singapore",
    "ID": "Indonesia", "PH": "Philippines", "IN": "India", "LK": "Sri Lanka",
    "ZA": "South Africa", "KE": "Kenya", "MA": "Morocco", "EG": "Egypt",
    "AE": "United Arab Emirates", "SA": "Saudi Arabia", "IL": "Israel", "TR": "Turkey",
    "RU": "Russia", "UA": "Ukraine", "IS": "Iceland", "MT": "Malta", "CY": "Cyprus",
    "LU": "Luxembourg", "MC": "Monaco", "LI": "Liechtenstein",
    "CW": "Curacao", "GE": "Georgia", "BS": "Bahamas", "BQ": "Bonaire",
    "VU": "Vanuatu", "JE": "Jersey", "RE": "Reunion", "SJ": "Svalbard",
    "GI": "Gibraltar", "FO": "Faroe Islands", "GP": "Guadeloupe", "MQ": "Martinique",
    "AW": "Aruba", "BM": "Bermuda", "KY": "Cayman Islands", "VI": "Virgin Islands",
    "PR": "Puerto Rico", "GU": "Guam", "TT": "Trinidad and Tobago", "JM": "Jamaica",
    "BB": "Barbados", "DO": "Dominican Republic", "HN": "Honduras", "GT": "Guatemala",
    "SV": "El Salvador", "NI": "Nicaragua", "BZ": "Belize", "UY": "Uruguay",
    "PY": "Paraguay", "BO": "Bolivia", "VE": "Venezuela", "GY": "Guyana",
    "TZ": "Tanzania", "NG": "Nigeria", "GH": "Ghana", "UG": "Uganda",
    "ET": "Ethiopia", "SN": "Senegal", "MU": "Mauritius", "MZ": "Mozambique",
    "NA": "Namibia", "BW": "Botswana", "RW": "Rwanda", "TN": "Tunisia",
    "MM": "Myanmar", "KH": "Cambodia", "LA": "Laos", "NP": "Nepal",
    "BD": "Bangladesh", "PK": "Pakistan", "QA": "Qatar", "KW": "Kuwait",
    "BH": "Bahrain", "OM": "Oman", "JO": "Jordan", "LB": "Lebanon",
    "FJ": "Fiji", "PF": "French Polynesia", "NC": "New Caledonia", "WS": "Samoa",
    "RS": "Serbia", "ME": "Montenegro", "BA": "Bosnia and Herzegovina",
    "MK": "North Macedonia", "AL": "Albania", "MD": "Moldova", "XK": "Kosovo",
}


def _normalize_country_code(code: str) -> str:
    """Convert 2-letter country code to full name. Returns code if not found."""
    if not code:
        return code
    return _COUNTRY_CODES.get(code.upper().strip(), code)


# ISO 3166-2 subdivision code -> human-readable state/province name
_US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}
_AU_STATES = {
    "ACT": "Australian Capital Territory", "NSW": "New South Wales",
    "NT": "Northern Territory", "QLD": "Queensland", "SA": "South Australia",
    "TAS": "Tasmania", "VIC": "Victoria", "WA": "Western Australia",
}
_CA_PROVINCES = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba", "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador", "NS": "Nova Scotia", "NT": "Northwest Territories",
    "NU": "Nunavut", "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
    "SK": "Saskatchewan", "YT": "Yukon",
}
_UK_NATIONS = {
    "ENG": "England", "NIR": "Northern Ireland", "SCT": "Scotland", "WLS": "Wales",
}
_SUBDIVISION_MAPS = {"US": _US_STATES, "AU": _AU_STATES, "CA": _CA_PROVINCES, "GB": _UK_NATIONS}


def _normalize_subdivision_code(country_code: str, subdivision_code: str) -> str:
    """Convert ISO 3166-2 subdivision code to human-readable state/province name.
    
    Args:
        country_code: 2-letter ISO country code (e.g. "US", "AU")
        subdivision_code: Full ISO 3166-2 code (e.g. "US-CA") or just the subdivision part ("CA")
    
    Returns:
        Human-readable name (e.g. "California") or the raw subdivision part if no mapping exists.
    """
    if not subdivision_code:
        return ""
    # Strip the country prefix if present (e.g. "US-CA" -> "CA")
    sub = subdivision_code.strip()
    if "-" in sub:
        sub = sub.split("-", 1)[1]
    cc = (country_code or "").upper().strip()
    mapping = _SUBDIVISION_MAPS.get(cc, {})
    return mapping.get(sub.upper(), sub)


# Rate limiting - use semaphore for concurrency control instead of sequential
_api_semaphore = None
MAX_CONCURRENT_REQUESTS = 3  # Conservative to avoid rate limits
BASE_RETRY_DELAY = 1.5  # Base delay for exponential backoff on 429
MAX_RETRY_DELAY = 10.0  # Cap backoff at 10s to prevent stalling


class MewsHotelData(BaseModel):
    """Extracted hotel data from Mews API."""
    
    slug: str
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    chain_name: Optional[str] = None
    website: Optional[str] = None
    booking_url: str = ""
    
    @property
    def is_valid(self) -> bool:
        """Check if we got meaningful data."""
        return bool(self.name and self.name != "Unknown")


# Session token cache (shared across instances)
_session_cache = {
    "session": None,
    "client": None,
    "obtained_at": 0,
    "lock": None,
}

# Session expires after 30 minutes (conservative estimate)
SESSION_TTL = 30 * 60


class MewsApiClient:
    """Client for Mews booking engine API.
    
    Uses httpx for all API calls. Session tokens are obtained on-demand
    via a throwaway Playwright browser (launched, token grabbed, closed
    immediately). The token is cached for SESSION_TTL seconds.
    
    Usage:
        client = MewsApiClient(use_brightdata=True)
        await client.initialize()
        data = await client.extract(slug)
        await client.close()
    """
    
    BOOKING_URL_TEMPLATE = "https://app.mews.com/distributor/{slug}"
    API_URL = "https://api.mews.com/api/bookingEngine/v1/configurations/get"
    # Known working hotel UUID used to bootstrap session tokens
    _SEED_HOTEL = "cb6072cc-1e03-45cc-a6e8-ab0d00ea7979"
    
    def __init__(self, timeout: float = 20.0, use_brightdata: bool = False):
        self.timeout = timeout
        self.use_brightdata = use_brightdata
        self._http_client: Optional[httpx.AsyncClient] = None
        self._proxy_url: Optional[str] = None
    
    async def initialize(self):
        """Initialize HTTP client."""
        if self._http_client is None:
            if self.use_brightdata:
                self._proxy_url = _get_brightdata_proxy()
                if self._proxy_url:
                    logger.debug("Mews client using Brightdata proxy")
                    self._http_client = httpx.AsyncClient(
                        timeout=self.timeout,
                        proxy=self._proxy_url,
                        verify=False,
                    )
                else:
                    logger.warning("Brightdata requested but credentials not found")
                    self._http_client = httpx.AsyncClient(timeout=self.timeout)
            else:
                self._http_client = httpx.AsyncClient(timeout=self.timeout)
        if _session_cache["lock"] is None:
            _session_cache["lock"] = asyncio.Lock()
    
    async def close(self):
        """Close HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
    
    async def _get_session(self) -> tuple[Optional[str], Optional[str]]:
        """Get or refresh session token. Thread-safe with lock."""
        now = time.time()
        
        # Fast path: cached session still valid
        if (_session_cache["session"] 
            and now - _session_cache["obtained_at"] < SESSION_TTL):
            return _session_cache["session"], _session_cache["client"]
        
        # Acquire lock so only one coroutine refreshes at a time
        async with _session_cache["lock"]:
            # Double-check after lock
            now = time.time()
            if (_session_cache["session"] 
                and now - _session_cache["obtained_at"] < SESSION_TTL):
                return _session_cache["session"], _session_cache["client"]
            
            logger.info("Obtaining Mews session token (one-shot browser)...")
            session, client = await self._obtain_session_token()
            
            if session and client:
                _session_cache["session"] = session
                _session_cache["client"] = client
                _session_cache["obtained_at"] = time.time()
                logger.info(f"Got Mews session (client: {client})")
            else:
                logger.error("Failed to obtain Mews session token")
            
            return _session_cache["session"], _session_cache["client"]
    
    @staticmethod
    async def _obtain_session_token() -> tuple[Optional[str], Optional[str]]:
        """Launch a throwaway browser, intercept the session token, close browser.
        
        The Mews distributor widget makes a POST to configurations/get on load.
        We intercept that request to capture the session + client tokens, then
        immediately tear down the browser. Total lifetime: ~3-5 seconds.
        """
        from playwright.async_api import async_playwright
        import json
        
        captured = {}
        pw = None
        browser = None
        
        try:
            pw = await async_playwright().start()
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            async def intercept(route):
                req = route.request
                if "configurations/get" in req.url and req.post_data:
                    captured["body"] = req.post_data
                await route.continue_()
            
            await page.route("**/*", intercept)
            
            await page.goto(
                f"https://app.mews.com/distributor/{MewsApiClient._SEED_HOTEL}",
                wait_until="commit",
                timeout=30000,
            )
            
            # Wait up to 10s for the API call to fire
            for _ in range(20):
                if captured.get("body"):
                    break
                await asyncio.sleep(0.5)
            
            if captured.get("body"):
                data = json.loads(captured["body"])
                return data.get("session"), data.get("client")
            
            logger.warning("Browser loaded but no configurations/get call intercepted")
            return None, None
            
        except Exception as e:
            logger.warning(f"Failed to obtain Mews session: {e}")
            return None, None
        finally:
            # Always tear down browser immediately
            if browser:
                await browser.close()
            if pw:
                await pw.stop()
    
    async def extract(self, slug: str) -> Optional[MewsHotelData]:
        """
        Extract hotel data from Mews using fast API call.
        
        Args:
            slug: The Mews enterprise UUID
        
        Returns:
            MewsHotelData if successful, None if failed
            
        Raises:
            httpx.TimeoutException: If all retries timed out (caller should NOT
                treat this as "no data" -- the hotel may be valid but slow).
        """
        await self.initialize()
        
        try:
            # Try API call - returns (data, needs_session_refresh)
            data, needs_refresh = await self._fetch_via_api(slug)
            
            if data:
                return self._parse_response(slug, data)
            
            # Only retry with new session if we got a session error
            if needs_refresh:
                _session_cache["obtained_at"] = 0
                data, _ = await self._fetch_via_api(slug)
                if data:
                    return self._parse_response(slug, data)
            
            return None
            
        except httpx.TimeoutException:
            raise  # Let caller handle -- this is NOT "no data"
        except Exception as e:
            logger.debug(f"Mews extraction error for {slug}: {e}")
            return None
    
    async def _fetch_via_api(self, slug: str, retry_count: int = 0) -> tuple[Optional[dict], bool]:
        """Fetch data via direct API call with concurrency control.
        
        Returns:
            Tuple of (data, needs_session_refresh). If data is None and needs_refresh
            is True, caller should refresh session and retry.
        """
        global _api_semaphore
        
        if _api_semaphore is None:
            _api_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        session, client = await self._get_session()
        
        if not session or not client:
            logger.warning("No Mews session available")
            return None, True  # No session - need refresh
        
        payload = {
            "ids": [slug],
            "primaryId": slug,
            "client": client,
            "session": session,
        }
        
        # Use semaphore to limit concurrent requests
        async with _api_semaphore:
            try:
                resp = await self._http_client.post(
                    self.API_URL,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Origin": "https://app.mews.com",
                        "Referer": "https://app.mews.com/",
                    },
                )
                
                if resp.status_code == 200:
                    return resp.json(), False
                elif resp.status_code == 429:
                    # Rate limited - exponential backoff with cap, then retry
                    if retry_count < 5:
                        wait = min(BASE_RETRY_DELAY * (2 ** retry_count), MAX_RETRY_DELAY)
                        logger.debug(f"Rate limited, waiting {wait:.1f}s (attempt {retry_count + 1}/5)...")
                        await asyncio.sleep(wait)
                        return await self._fetch_via_api(slug, retry_count + 1)
                    logger.warning(f"Mews rate limit exceeded for {slug} after 5 retries")
                    return None, False  # Rate limit - don't refresh session
                elif resp.status_code == 400:
                    text = resp.text
                    if "session" in text.lower():
                        # Session expired, need refresh
                        logger.debug(f"Mews session expired for {slug}")
                        return None, True
                    # Invalid hotel ID - don't retry or refresh
                    logger.debug(f"Invalid Mews ID {slug}: {text[:50]}")
                    return None, False
                else:
                    logger.debug(f"Mews API error for {slug}: HTTP {resp.status_code}")
                    return None, False
                    
            except httpx.TimeoutException:
                # Timeout is transient -- retry with backoff, not a permanent failure
                if retry_count < 3:
                    wait = min(BASE_RETRY_DELAY * (2 ** retry_count), MAX_RETRY_DELAY)
                    logger.warning(f"Mews API timeout for {slug}, retrying in {wait:.1f}s (attempt {retry_count + 1}/3)...")
                    await asyncio.sleep(wait)
                    return await self._fetch_via_api(slug, retry_count + 1)
                logger.warning(f"Mews API timeout for {slug} after 3 retries")
                raise  # Propagate so caller knows this was a timeout, not "no data"
            except Exception as e:
                logger.debug(f"Mews API request failed for {slug}: {e}")
                return None, False
    
    def _parse_response(self, slug: str, data: dict) -> MewsHotelData:
        """Parse Mews API response into hotel data."""
        result = MewsHotelData(
            slug=slug,
            booking_url=self.BOOKING_URL_TEMPLATE.format(slug=slug),
        )
        
        # Get enterprise (property) data - keys can be camelCase or PascalCase
        enterprises = data.get("enterprises") or data.get("Enterprises", [])
        if enterprises:
            enterprise = enterprises[0]
            
            # Name - can be dict with language codes
            name = enterprise.get("name") or enterprise.get("Name")
            if isinstance(name, dict):
                # Prefer English, fall back to first available
                result.name = name.get("en-US") or name.get("en-GB") or next(iter(name.values()), None)
            elif isinstance(name, str):
                result.name = name
            
            # Address
            address = enterprise.get("address") or enterprise.get("Address", {})
            if address:
                line1 = address.get("line1") or address.get("Line1")
                line2 = address.get("line2") or address.get("Line2")
                result.address = line1
                if line2:
                    result.address = f"{result.address}, {line2}"
                result.city = address.get("city") or address.get("City")
                result.postal_code = address.get("postalCode") or address.get("PostalCode")
                country_code = address.get("countryCode") or address.get("CountryCode")
                result.country = _normalize_country_code(country_code) if country_code else None
                # Extract state/province from ISO 3166-2 subdivision code
                subdivision = address.get("countrySubdivisionCode") or address.get("CountrySubdivisionCode")
                if subdivision and country_code:
                    result.state = _normalize_subdivision_code(country_code, subdivision)
                result.lat = address.get("latitude") or address.get("Latitude")
                result.lon = address.get("longitude") or address.get("Longitude")
            
            # Contact info
            result.email = enterprise.get("email") or enterprise.get("Email")
            result.phone = enterprise.get("telephone") or enterprise.get("Telephone")
        
        # Get chain name
        chains = data.get("chains") or data.get("Chains", [])
        if chains:
            result.chain_name = chains[0].get("name") or chains[0].get("Name")
            # If no enterprise name, use chain name
            if not result.name and result.chain_name:
                result.name = result.chain_name
        
        return result


async def extract_mews_hotel(slug: str, timeout: float = 45.0) -> Optional[MewsHotelData]:
    """
    Convenience function to extract hotel data from Mews.
    
    Args:
        slug: Mews enterprise UUID
        timeout: Request timeout in seconds
    
    Returns:
        MewsHotelData if successful, None if failed
    """
    client = MewsApiClient(timeout=timeout)
    return await client.extract(slug)
