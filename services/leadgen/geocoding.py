"""Geocoding helper - fetches coordinates from external APIs.

This is an internal helper module. It does NOT access the database.
Only the service layer should call this and handle caching.
"""

from typing import Optional
import httpx
from pydantic import BaseModel
from loguru import logger


class CityLocation(BaseModel):
    """City with coordinates."""
    name: str
    state: str
    lat: float
    lng: float
    population: Optional[int] = None
    display_name: Optional[str] = None
    radius_km: float = 12.0  # Default scrape radius


async def geocode_city(city: str, state: str) -> CityLocation:
    """
    Fetch coordinates for a city from OpenStreetMap Nominatim API.
    
    This is a free API with rate limits (1 req/sec).
    Results should be cached by the caller.
    
    Args:
        city: City name (e.g., "Miami")
        state: State code (e.g., "FL")
        
    Returns:
        CityLocation with coordinates
        
    Raises:
        ValueError: If city not found
        httpx.HTTPError: On API errors
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": f"{city}, {state}, USA",
                "format": "json",
                "limit": 1,
            },
            headers={"User-Agent": "sadie-gtm/1.0"},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
        
        if not data:
            raise ValueError(f"City not found: {city}, {state}")
        
        result = data[0]
        return CityLocation(
            name=city,
            state=state,
            lat=float(result["lat"]),
            lng=float(result["lon"]),
            display_name=result.get("display_name"),
            radius_km=_suggest_radius(city),
        )


def _suggest_radius(city_name: str) -> float:
    """Suggest scrape radius based on city name."""
    major_metros = {"miami", "orlando", "tampa", "jacksonville", "houston", "dallas", "los angeles", "new york"}
    medium_cities = {"fort lauderdale", "west palm beach", "sarasota", "fort myers", "tallahassee", "pensacola"}
    
    name_lower = city_name.lower()
    if name_lower in major_metros:
        return 25.0
    if name_lower in medium_cities:
        return 15.0
    return 12.0
