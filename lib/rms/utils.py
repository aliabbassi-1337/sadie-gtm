"""RMS helper functions."""

import re
from typing import Optional, Tuple


def extract_coordinates_from_google_maps_url(url: str) -> Tuple[Optional[float], Optional[float]]:
    """Extract latitude and longitude from a Google Maps URL.
    
    Handles formats like:
    - https://www.google.com/maps/place/.../@-37.7309159,144.7393793,17z/...
    - https://maps.google.com/?q=-37.7309159,144.7393793
    - https://goo.gl/maps/... (won't work without redirect)
    
    Returns (latitude, longitude) or (None, None) if not found.
    """
    if not url:
        return None, None
    
    # Pattern 1: @lat,lon,zoom in URL path (most common)
    # e.g., /@-37.7309159,144.7393793,17z
    match = re.search(r'@(-?\d+\.?\d*),(-?\d+\.?\d*)', url)
    if match:
        try:
            lat = float(match.group(1))
            lon = float(match.group(2))
            # Validate reasonable lat/lon ranges
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except ValueError:
            pass
    
    # Pattern 2: !3d<lat>!4d<lon> format (in data params)
    # e.g., !3d-37.7309202!4d144.7419542
    match = re.search(r'!3d(-?\d+\.?\d*)!4d(-?\d+\.?\d*)', url)
    if match:
        try:
            lat = float(match.group(1))
            lon = float(match.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except ValueError:
            pass
    
    # Pattern 3: ?q=lat,lon or ?ll=lat,lon
    match = re.search(r'[?&](?:q|ll)=(-?\d+\.?\d*),(-?\d+\.?\d*)', url)
    if match:
        try:
            lat = float(match.group(1))
            lon = float(match.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except ValueError:
            pass
    
    return None, None


def extract_country_from_flag_url(flag_url: str) -> Optional[str]:
    """Extract country code from an RMS flag image URL.
    
    RMS uses flag images like: /images/flags/us.gif or /flags/AU.png
    
    Returns full country name or None.
    """
    if not flag_url:
        return None
    
    # Extract country code from flag URL
    match = re.search(r'/flags?/([a-zA-Z]{2})\.(?:gif|png|jpg|svg)', flag_url, re.IGNORECASE)
    if match:
        code = match.group(1).upper()
        return normalize_country(code)
    
    return None


def decode_cloudflare_email(encoded: str) -> str:
    """Decode Cloudflare-obfuscated email addresses."""
    try:
        r = int(encoded[:2], 16)
        return ''.join(chr(int(encoded[i:i+2], 16) ^ r) for i in range(2, len(encoded), 2))
    except Exception:
        return ""


def normalize_country(country: str) -> str:
    """Normalize country name to full name."""
    if not country:
        return ""
    country_map = {
        "united states": "United States", "us": "United States", "usa": "United States",
        "australia": "Australia", "canada": "Canada", "new zealand": "New Zealand",
        "united kingdom": "United Kingdom", "uk": "United Kingdom", "gb": "United Kingdom",
        "mexico": "Mexico",
    }
    return country_map.get(country.lower().strip(), country)
