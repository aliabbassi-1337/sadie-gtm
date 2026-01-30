"""RMS API Client.

Fast API-based extraction without Playwright.
Falls back to scraper for missing data.
"""

import re
from typing import Optional

import httpx
from loguru import logger
from pydantic import BaseModel

from lib.rms.models import ExtractedRMSData
from lib.rms.utils import normalize_country


# API timeout
API_TIMEOUT = 10.0

# RMS API base URLs by server
API_SERVERS = [
    "bookings.rmscloud.com",
    "bookings12.rmscloud.com",
    "bookings10.rmscloud.com",
    "bookings8.rmscloud.com",
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
            
            # Extract phone from description/features
            all_text = " ".join(filter(None, [
                api_data.property_description,
                api_data.business_facilities,
                api_data.features,
                api_data.travel_directions,
            ]))
            
            data.phone = self._extract_phone(all_text)
            data.email = self._extract_email(all_text)
            data.website = api_data.redirect_url or self._extract_website(all_text)
            
            # Try to extract address from travel directions
            if api_data.travel_directions:
                data.address = self._extract_address(api_data.travel_directions)
            
            # Parse location from description
            if all_text:
                data.city, data.state, data.country = self._extract_location(all_text)
            
            return data if data.has_data() else None
            
        except Exception as e:
            logger.debug(f"RMS API error for {slug}: {e}")
            return None
    
    async def _fetch_api_data(self, slug: str, server: str) -> Optional[RMSApiResponse]:
        """Fetch data from RMS API endpoints."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = RMSApiResponse()
            
            # Fetch Property API
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
            
            # Fetch Details API
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
    
    def _extract_phone(self, text: str) -> Optional[str]:
        """Extract phone number from text."""
        if not text:
            return None
        
        # Clean HTML
        text = re.sub(r'<[^>]+>', ' ', text)
        
        patterns = [
            # International format with country code
            r'(?:tel|phone|call|locally)[:\s]*([+\d][\d\s\-\(\)]{7,20})',
            r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
            # Australian format
            r'(\d{2}[\s\-]?\d{4}[\s\-]?\d{4})',
            # US format
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
    server: str = "bookings12.rmscloud.com",
) -> Optional[ExtractedRMSData]:
    """Try API first, fall back to Playwright scraper if needed.
    
    Args:
        slug: RMS client ID
        scraper: Optional RMSScraper instance for fallback
        server: RMS server to use
        
    Returns:
        ExtractedRMSData or None
    """
    # Try API first (fast, no browser)
    api_client = RMSApiClient()
    data = await api_client.extract(slug, server)
    
    if data and data.name:
        logger.debug(f"API success for {slug}: {data.name}")
        
        # Check if we have enough data or need fallback
        has_contact = data.email or data.phone
        has_location = data.city or data.state or data.country
        
        if has_contact and has_location:
            return data
        
        # If we have name but missing contact/location, use scraper to fill gaps
        if scraper:
            logger.debug(f"API partial for {slug}, trying scraper for more data")
            url = f"https://{server}/Search/Index/{slug}/90/"
            scraped = await scraper.extract(url, slug)
            if scraped:
                # Merge: keep API name, fill in missing from scraper
                if not data.email and scraped.email:
                    data.email = scraped.email
                if not data.phone and scraped.phone:
                    data.phone = scraped.phone
                if not data.address and scraped.address:
                    data.address = scraped.address
                if not data.city and scraped.city:
                    data.city = scraped.city
                if not data.state and scraped.state:
                    data.state = scraped.state
                if not data.country and scraped.country:
                    data.country = scraped.country
                if not data.website and scraped.website:
                    data.website = scraped.website
        
        return data
    
    # API failed, fall back to scraper
    if scraper:
        logger.debug(f"API failed for {slug}, falling back to scraper")
        url = f"https://{server}/Search/Index/{slug}/90/"
        return await scraper.extract(url, slug)
    
    return None
