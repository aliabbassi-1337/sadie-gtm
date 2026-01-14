"""
Customer Proximity Enricher
============================
Finds nearest existing Sadie customer to each hotel.

This is an internal helper module - only service.py can call repo functions.
Uses PostGIS for efficient spatial queries when available.
"""

import math
from datetime import datetime
from typing import Optional, Dict, Any


def log(msg: str) -> None:
    """Print timestamped log message."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in kilometers using Haversine formula.

    This is a fallback method if PostGIS is not available.
    PostGIS ST_Distance is more accurate and efficient for database queries.
    """
    R = 6371  # Earth's radius in km

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c


def format_customer_info(
    customer_name: str,
    distance_km: float,
    gm_name: Optional[str] = None,
    phone: Optional[str] = None,
) -> str:
    """Format customer info for display.

    Example output: "Nearest: Grand Hotel (15.3km) | GM: John Smith | Phone: +1-555-0100"
    """
    parts = []

    # Name and distance
    name_part = f"Nearest: {customer_name} ({distance_km}km)"
    parts.append(name_part)

    # GM if available
    if gm_name:
        parts.append(f"GM: {gm_name}")

    # Phone if available
    if phone:
        parts.append(f"Phone: {phone}")

    return " | ".join(parts)


def validate_coordinates(latitude: Optional[float], longitude: Optional[float]) -> bool:
    """Validate that coordinates are within valid ranges."""
    if latitude is None or longitude is None:
        return False

    # Valid latitude: -90 to 90
    # Valid longitude: -180 to 180
    if not (-90 <= latitude <= 90):
        return False
    if not (-180 <= longitude <= 180):
        return False

    return True
