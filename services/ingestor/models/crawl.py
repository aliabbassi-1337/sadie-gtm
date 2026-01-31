"""
Crawled Booking Engine Hotel Model.

Represents hotels discovered via crawling booking engine URLs (Common Crawl, etc.)
"""

from typing import Optional
from pydantic import Field

from services.ingestor.models.base import BaseRecord


# URL patterns for each booking engine
URL_PATTERNS = {
    "cloudbeds": "https://hotels.cloudbeds.com/reservation/{slug}",
    "mews": "https://app.mews.com/distributor/{slug}",
    "rms": "https://bookings.rmscloud.com/{slug}",
    "siteminder": "https://{slug}.book-onlinenow.net/",
    "ipms247": "https://live.ipms247.com/booking/book-rooms-{slug}",
}


class CrawledHotel(BaseRecord):
    """
    Hotel record from crawled booking engine URLs.
    
    These records typically have:
    - A booking URL/slug
    - A booking engine type
    - Optionally a name (scraped from the page)
    
    Hotels without names get placeholder names like "Unknown (slug)"
    and are enriched later via SQS workers.
    """
    
    # Booking engine info
    slug: str
    booking_url: Optional[str] = None
    booking_engine: str  # cloudbeds, mews, rms, siteminder
    booking_engine_id: Optional[int] = None
    
    # Detection metadata
    detection_method: str = "crawl_import"
    
    @classmethod
    def from_slug(
        cls,
        slug: str,
        booking_engine: str,
        name: Optional[str] = None,
        source: Optional[str] = None,
    ) -> "CrawledHotel":
        """
        Create a CrawledHotel from a slug.
        
        Args:
            slug: The booking engine property ID/slug
            booking_engine: Engine name (cloudbeds, mews, rms, siteminder)
            name: Optional hotel name (if scraped)
            source: Source tag for tracking
        """
        # Build booking URL from pattern
        url_pattern = URL_PATTERNS.get(booking_engine.lower(), "")
        booking_url = url_pattern.replace("{slug}", slug) if url_pattern else None
        
        # Use placeholder name if none provided
        if not name:
            name = f"Unknown ({slug})"
        
        # Build external ID
        external_id = slug
        external_id_type = f"{booking_engine.lower()}_crawl"
        source_tag = source or external_id_type
        
        return cls(
            name=name,
            slug=slug,
            booking_url=booking_url,
            booking_engine=booking_engine.lower(),
            external_id=external_id,
            external_id_type=external_id_type,
            source=source_tag,
        )
    
    def to_db_tuple(self) -> tuple:
        """
        Convert to tuple for batch insert.
        
        Format: (name, source, status, address, city, state, country, phone, category, external_id, lat, lon)
        """
        return (
            self.name,
            self.source,
            0,  # HOTEL_STATUS_PENDING
            self.address,
            self.city,
            self.state,
            self.country,
            self.phone,
            self.category,
            self.external_id,
            self.lat,
            self.lon,
        )
