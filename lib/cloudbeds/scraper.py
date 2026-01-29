"""Cloudbeds Data Scraper.

Extracts hotel data from Cloudbeds booking pages.
"""

import asyncio
import re
from typing import Optional, Dict, Any, Protocol, runtime_checkable

from loguru import logger
from playwright.async_api import Page
from pydantic import BaseModel


class ExtractedCloudbedsData(BaseModel):
    """Extracted data from a Cloudbeds booking page."""
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    
    def has_data(self) -> bool:
        """Check if any meaningful data was extracted."""
        return bool(self.name or self.city or self.email or self.phone)


@runtime_checkable
class ICloudbedsScaper(Protocol):
    """Cloudbeds Scraper interface."""
    async def extract(self, url: str) -> Optional[ExtractedCloudbedsData]: ...


class CloudbedsScraper(ICloudbedsScaper):
    """Extracts hotel data from Cloudbeds booking pages."""
    
    def __init__(self, page: Page):
        self._page = page
    
    async def extract(self, url: str) -> Optional[ExtractedCloudbedsData]:
        """Extract hotel data from Cloudbeds booking page."""
        try:
            response = await self._page.goto(url, timeout=30000, wait_until="domcontentloaded")
            
            # Check for 404
            if response and response.status == 404:
                return None
            
            await asyncio.sleep(4)  # Wait for React to render
            
            data = await self._extract_from_page()
            
            if not data or not data.has_data():
                return None
            
            # Check for garbage data (redirected to Cloudbeds homepage)
            if data.name and data.name.lower() in ['cloudbeds.com', 'cloudbeds', 'book now', 'reservation']:
                return None
            if data.city and 'soluções online' in data.city.lower():
                return None
            
            return data
            
        except Exception as e:
            logger.debug(f"Error extracting {url}: {e}")
            return None
    
    async def _extract_from_page(self) -> ExtractedCloudbedsData:
        """Extract name and address/contact from Cloudbeds page."""
        result = ExtractedCloudbedsData()
        
        # Extract from title tag: "Hotel Name - City, Country - Best Price Guarantee"
        try:
            title_data = await self._page.evaluate("""
                () => {
                    const title = document.querySelector('title');
                    if (!title) return null;
                    
                    const text = title.textContent.trim();
                    const parts = text.split(/\\s*-\\s*/);
                    
                    if (parts.length >= 2) {
                        const name = parts[0].trim();
                        const locationPart = parts[1].trim();
                        const locParts = locationPart.split(',').map(p => p.trim());
                        
                        return {
                            name: name,
                            city: locParts[0] || null,
                            state: locParts.length === 3 ? locParts[1] : null,
                            country: locParts[locParts.length - 1] || null
                        };
                    }
                    
                    return { name: parts[0].trim() };
                }
            """)
            if title_data:
                if title_data.get('name') and title_data['name'] not in ['Book Now', 'Reservation', 'Booking', 'Home']:
                    result.name = title_data['name']
                if title_data.get('city'):
                    result.city = title_data['city']
                if title_data.get('state'):
                    result.state = title_data['state']
                if title_data.get('country'):
                    country = title_data['country']
                    if country in ['United States of America', 'United States', 'US', 'USA']:
                        result.country = 'USA'
                    else:
                        result.country = country
        except Exception:
            pass
        
        # Extract from "Address and Contact" section
        try:
            widget_data = await self._page.evaluate("""
                () => {
                    // Strategy 1: Find by data-testid or class
                    let container = document.querySelector('[data-testid="property-address-and-contact"]') 
                                 || document.querySelector('.cb-address-and-contact');
                    
                    // Strategy 2: Find by heading text "Address and Contact"
                    if (!container) {
                        const headings = Array.from(document.querySelectorAll('h3'));
                        const addressHeading = headings.find(h => 
                            h.textContent?.toLowerCase().includes('address and contact')
                        );
                        if (addressHeading) {
                            container = addressHeading.closest('div')?.parentElement || 
                                        addressHeading.parentElement;
                        }
                    }
                    
                    if (!container) return null;
                    
                    // Get all paragraph text (try multiple selectors)
                    let paragraphs = Array.from(container.querySelectorAll('p[data-be-text="true"]'));
                    if (paragraphs.length === 0) {
                        paragraphs = Array.from(container.querySelectorAll('p'));
                    }
                    
                    const lines = paragraphs
                        .map(p => p.textContent?.trim())
                        .filter(t => t && t.length > 0 && t.length < 100);
                    
                    const mailtoLink = container.querySelector('a[href^="mailto:"]');
                    const email = mailtoLink ? mailtoLink.href.replace('mailto:', '').split('?')[0] : '';
                    
                    const telLink = container.querySelector('a[href^="tel:"]');
                    const phone = telLink ? telLink.href.replace('tel:', '').replace(/[^0-9+()-]/g, '') : '';
                    
                    return { lines, email, phone };
                }
            """)
            
            if widget_data and widget_data.get('lines') and len(widget_data['lines']) >= 2:
                lines = widget_data['lines']
                
                if len(lines) > 0:
                    result.address = lines[0]
                if len(lines) > 1:
                    result.city = lines[1]
                
                # Look for "State Country" pattern
                state_country_pattern = re.compile(
                    r'^([A-Za-z\s]+)\s+(US|USA|AU|UK|CA|NZ|GB|IE|MX|AR|PR|CO|IT|ES|FR|DE|PT|BR|CL|PE|CR|PA)$',
                    re.IGNORECASE
                )
                for line in lines[2:6]:
                    match = state_country_pattern.match(line.strip())
                    if match:
                        result.state = match.group(1).strip()
                        country = match.group(2).strip().upper()
                        result.country = 'USA' if country in ['US', 'USA'] else country
                        break
                
                # Fallback: original rsplit approach
                if not result.state and len(lines) > 2:
                    state_country = lines[2].strip()
                    parts = state_country.rsplit(' ', 1)
                    if len(parts) == 2 and len(parts[1]) <= 3:
                        result.state = parts[0].strip()
                        country = parts[1].strip().upper()
                        result.country = 'USA' if country in ['US', 'USA'] else country
                
                # Phone from tel link or pattern match
                if widget_data.get('phone') and len(widget_data['phone']) >= 10:
                    result.phone = widget_data['phone']
                else:
                    phone_pattern = re.compile(r'^[\d\-\(\)\s\+\.]{7,20}$')
                    for line in lines[3:]:
                        if phone_pattern.match(line) and not result.phone:
                            result.phone = line
                            break
                
                if widget_data.get('email'):
                    result.email = widget_data['email']
        except Exception:
            pass
        
        # Fallback: phone from tel: links anywhere
        if not result.phone:
            try:
                phone = await self._page.evaluate("""
                    () => {
                        const tel = document.querySelector('a[href^="tel:"]');
                        if (tel) return tel.href.replace('tel:', '').replace(/[^0-9+()-]/g, '');
                        return null;
                    }
                """)
                if phone and len(phone) >= 10:
                    result.phone = phone
            except Exception:
                pass
        
        # Fallback: email from mailto: links anywhere
        if not result.email:
            try:
                email = await self._page.evaluate("""
                    () => {
                        const mailto = document.querySelector('a[href^="mailto:"]');
                        if (mailto) return mailto.href.replace('mailto:', '').split('?')[0];
                        return null;
                    }
                """)
                if email and '@' in email:
                    result.email = email
            except Exception:
                pass
        
        return result
