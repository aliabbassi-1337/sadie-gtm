"""RMS helper functions."""


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
