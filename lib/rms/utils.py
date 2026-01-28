"""RMS helper functions."""


def decode_cloudflare_email(encoded: str) -> str:
    """Decode Cloudflare-obfuscated email addresses."""
    try:
        r = int(encoded[:2], 16)
        return ''.join(chr(int(encoded[i:i+2], 16) ^ r) for i in range(2, len(encoded), 2))
    except Exception:
        return ""


def normalize_country(country: str) -> str:
    """Normalize country name to 2-letter code."""
    if not country:
        return ""
    country_map = {
        "united states": "USA", "us": "USA", "usa": "USA",
        "australia": "AU", "canada": "CA", "new zealand": "NZ",
        "united kingdom": "GB", "uk": "GB", "mexico": "MX",
    }
    return country_map.get(country.lower().strip(), country.upper()[:2])
