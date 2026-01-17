"""Geocoding helper - fetches coordinates from external APIs.

This is an internal helper module. It does NOT access the database.
Only the service layer should call this and handle caching.
"""

import json
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
    display_name: Optional[str] = None
    radius_km: float = 12.0  # Default scrape radius (from Nominatim importance)


class CityBoundary(BaseModel):
    """City with actual boundary polygon from OpenStreetMap."""
    name: str
    state: str
    lat: float  # Center
    lng: float  # Center
    polygon_geojson: str  # GeoJSON Polygon or MultiPolygon
    display_name: Optional[str] = None
    osm_type: Optional[str] = None  # relation, way, node
    osm_id: Optional[int] = None


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
        importance = float(result.get("importance", 0.3))
        
        return CityLocation(
            name=city,
            state=state,
            lat=float(result["lat"]),
            lng=float(result["lon"]),
            display_name=result.get("display_name"),
            radius_km=_suggest_radius_from_importance(importance),
        )


def _suggest_radius_from_importance(importance: float) -> float:
    """
    Suggest scrape radius based on Nominatim importance score.
    
    Nominatim returns an 'importance' field (0-1) based on:
    - Wikipedia article length/links
    - OSM node connections
    - Population data when available
    
    Importance ranges observed:
    - 0.7+: Major metros (NYC, LA, Chicago)
    - 0.5-0.7: Large cities (Orlando, Tampa)
    - 0.3-0.5: Medium cities (regional centers)
    - <0.3: Small towns
    """
    if importance >= 0.7:
        return 25.0  # Major metros
    if importance >= 0.5:
        return 20.0  # Large cities
    if importance >= 0.3:
        return 15.0  # Medium cities
    return 12.0  # Default


async def fetch_city_boundary(city: str, state: str) -> Optional[CityBoundary]:
    """
    Fetch the actual city boundary polygon from OpenStreetMap Nominatim.
    
    This returns the real administrative boundary - not a circle.
    Much more efficient for coastal cities, islands, etc.
    
    Args:
        city: City name (e.g., "Miami Beach")
        state: State code (e.g., "FL")
        
    Returns:
        CityBoundary with GeoJSON polygon, or None if no boundary found
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": f"{city}, {state}, USA",
                "format": "json",
                "polygon_geojson": 1,  # Request the boundary polygon
                "limit": 1,
            },
            headers={"User-Agent": "sadie-gtm/1.0"},
            timeout=15.0,
        )
        resp.raise_for_status()
        data = resp.json()
        
        if not data:
            logger.warning(f"City not found: {city}, {state}")
            return None
        
        result = data[0]
        geojson = result.get("geojson")
        
        if not geojson:
            logger.warning(f"No boundary polygon for: {city}, {state}")
            return None
        
        # Only accept Polygon or MultiPolygon
        if geojson.get("type") not in ("Polygon", "MultiPolygon"):
            logger.warning(f"Unexpected geometry type for {city}: {geojson.get('type')}")
            return None
        
        return CityBoundary(
            name=city,
            state=state,
            lat=float(result["lat"]),
            lng=float(result["lon"]),
            polygon_geojson=json.dumps(geojson),
            display_name=result.get("display_name"),
            osm_type=result.get("osm_type"),
            osm_id=int(result["osm_id"]) if result.get("osm_id") else None,
        )
