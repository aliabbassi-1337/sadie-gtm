"""BIG4 Holiday Parks - Data models."""

from typing import Optional, List
from pydantic import BaseModel


class Big4Park(BaseModel):
    """A BIG4 holiday park scraped from big4.com.au."""

    name: str
    slug: str  # URL path slug e.g. "sydney-lakeside-holiday-park"
    url_path: str  # Full path e.g. "/caravan-parks/nsw/greater-sydney/sydney-lakeside-holiday-park"
    region: Optional[str] = None  # e.g. "Greater Sydney"
    state: Optional[str] = None  # e.g. "NSW"

    # Contact
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None

    # Location
    address: Optional[str] = None
    city: Optional[str] = None
    postcode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Metadata
    rating: Optional[float] = None
    review_count: Optional[int] = None
    pets_allowed: Optional[bool] = None
    description: Optional[str] = None

    @property
    def full_url(self) -> str:
        return f"https://www.big4.com.au{self.url_path}"

    @property
    def contact_url(self) -> str:
        return f"https://www.big4.com.au{self.url_path}/contact"

    @property
    def external_id(self) -> str:
        return f"big4_{self.slug}"

    def has_location(self) -> bool:
        return self.latitude is not None and self.longitude is not None

    def to_insert_tuple(self) -> tuple:
        """Convert to tuple for BATCH_INSERT_HOTELS.

        Format: (name, source, status, address, city, state, country, phone,
                 category, external_id, lat, lon)
        """
        return (
            self.name,
            "big4_scrape",
            1,  # status=1 (active/enriched)
            self.address,
            self.city,
            self.state,
            "Australia",
            self.phone,
            "holiday_park",
            self.external_id,
            self.latitude,
            self.longitude,
        )


class Big4ScrapeResult(BaseModel):
    """Result of a BIG4 scrape run."""

    parks_discovered: int = 0
    parks_scraped: int = 0
    parks_with_contact: int = 0
    parks_failed: int = 0
    parks: List[Big4Park] = []
