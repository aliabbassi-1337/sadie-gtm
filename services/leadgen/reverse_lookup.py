"""
Reverse Lookup - Find hotels by their booking engine software.

Instead of searching for hotels and detecting their booking engine,
we search for booking engine URLs directly. These are pre-qualified leads.

## Supported Engines and URL Patterns

- Cloudbeds: hotels.cloudbeds.com/reservation/{slug}
- Guesty: *.guestybookings.com
- Little Hotelier: *.littlehotelier.com
- WebRezPro: "powered by webrezpro"
- Lodgify: *.lodgify.com
- Hostaway: *.hostaway.com

## Cloudbeds ID Structure (Research Findings)

Cloudbeds uses two types of identifiers:

1. **Public Slug** (6 alphanumeric chars): e.g., 'cl6l0S', 'UxSswi'
   - Used in booking URLs: hotels.cloudbeds.com/reservation/{slug}
   - Not a simple encoding of the numeric ID
   - Likely hash-based or encrypted

2. **Internal Numeric ID**: e.g., 317832, 202743
   - Sequential integers (currently ~300,000+ properties)
   - Exposed in image URLs: h-img*.cloudbeds.com/uploads/{property_id}/
   - Exposed in analytics: ep.property_id={property_id}
   - Cannot be used directly in booking URLs

## Enumeration Strategies

1. **Google Dorks** - Search for booking engine URLs (implemented below)
2. **TheGuestbook API** - Cloudbeds partner directory with 800+ hotels
   - See: services/leadgen/booking_engines.py
3. **Certificate Transparency** - Search crt.sh for *.cloudbeds.com subdomains

## Known Mappings

| Hotel               | Slug    | Numeric ID |
|---------------------|---------|------------|
| The Kendall         | cl6l0S  | 317832     |
| 7 Seas Hotel        | UxSswi  | 202743     |
| St Augustine Hotel  | sEhTC1  | -          |
| Casa Ocean          | TxCgVr  | -          |
"""

from typing import Optional
from pydantic import BaseModel


# Booking engine dork patterns
# Each tuple: (engine_name, dork_template, url_pattern_regex)
BOOKING_ENGINE_DORKS = [
    # Cloudbeds - Major PMS
    ("cloudbeds", 'site:hotels.cloudbeds.com {location}', r'hotels\.cloudbeds\.com/(?:en/)?reservation/(\w+)'),
    ("cloudbeds", '"powered by cloudbeds" {location} hotel', r'cloudbeds'),
    ("cloudbeds", 'inurl:cloudbeds.com/reservation {location}', r'cloudbeds\.com'),

    # Guesty - Vacation rental focused
    ("guesty", 'site:guestybookings.com {location}', r'guestybookings\.com'),
    ("guesty", 'inurl:guesty {location} hotel', r'guesty'),

    # Little Hotelier - Small hotels
    ("little_hotelier", 'site:littlehotelier.com {location}', r'littlehotelier\.com'),
    ("little_hotelier", 'inurl:littlehotelier {location}', r'littlehotelier'),

    # WebRezPro
    ("webrezpro", '"powered by webrezpro" {location}', r'webrezpro'),
    ("webrezpro", 'inurl:webrezpro {location} hotel', r'webrezpro'),

    # Lodgify - Vacation rentals
    ("lodgify", 'site:lodgify.com {location}', r'lodgify\.com'),
    ("lodgify", 'inurl:lodgify {location} hotel', r'lodgify'),

    # Hostaway
    ("hostaway", 'site:hostaway.com {location}', r'hostaway\.com'),

    # RMS Cloud
    ("rms_cloud", '"powered by rms cloud" {location} hotel', r'rms'),
    ("rms_cloud", 'inurl:rmscloud {location}', r'rmscloud'),

    # innRoad
    ("innroad", '"powered by innroad" {location}', r'innroad'),
    ("innroad", 'inurl:innroad {location} hotel', r'innroad'),

    # ResNexus
    ("resnexus", 'site:resnexus.com {location}', r'resnexus\.com'),
    ("resnexus", 'inurl:resnexus {location}', r'resnexus'),

    # SiteMinder
    ("siteminder", '"powered by siteminder" {location} hotel', r'siteminder'),
    ("siteminder", 'inurl:siteminder {location}', r'siteminder'),

    # Mews
    ("mews", 'inurl:mews.com {location} hotel', r'mews\.com'),
    ("mews", '"powered by mews" {location}', r'mews'),

    # Clock PMS
    ("clock_pms", 'inurl:clock-software {location} hotel', r'clock'),

    # eviivo
    ("eviivo", 'inurl:eviivo {location} hotel', r'eviivo'),
    ("eviivo", '"powered by eviivo" {location}', r'eviivo'),

    # Beds24
    ("beds24", 'inurl:beds24 {location}', r'beds24'),

    # Sirvoy
    ("sirvoy", 'inurl:sirvoy {location} hotel', r'sirvoy'),
    ("sirvoy", '"book now" sirvoy {location}', r'sirvoy'),

    # ThinkReservations
    ("thinkreservations", 'inurl:thinkreservations {location}', r'thinkreservations'),

    # Direct booking patterns (fallback - needs manual engine detection)
    ("unknown", 'intitle:"book direct" "independent hotel" {location}', None),
    ("unknown", 'intitle:"official site" hotel {location} "book now"', None),
]


class ReverseLookupResult(BaseModel):
    """A hotel found via reverse lookup."""
    name: str
    booking_url: str
    booking_engine: str
    website: Optional[str] = None  # Main website (if different from booking URL)
    snippet: Optional[str] = None  # Search result snippet
    source_dork: str  # The dork that found this


class ReverseLookupStats(BaseModel):
    """Stats from a reverse lookup run."""
    dorks_run: int = 0
    api_calls: int = 0
    results_found: int = 0
    unique_results: int = 0
    by_engine: dict = {}
