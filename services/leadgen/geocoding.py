"""Geocoding helper - fetches coordinates from external APIs.

This is an internal helper module. It does NOT access the database.
Only the service layer should call this and handle caching.
"""

import asyncio
import json
from typing import Optional
import httpx
from pydantic import BaseModel
from loguru import logger


class ReverseGeocodingResult(BaseModel):
    """Result from reverse geocoding (coordinates to address)."""
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    display_name: Optional[str] = None


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


async def reverse_geocode(lat: float, lng: float) -> Optional[ReverseGeocodingResult]:
    """
    Fetch address components from coordinates using OpenStreetMap Nominatim API.

    This is a free API with rate limits (1 req/sec).

    Args:
        lat: Latitude
        lng: Longitude

    Returns:
        ReverseGeocodingResult with address components, or None if not found
    """
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={
                    "lat": lat,
                    "lon": lng,
                    "format": "json",
                    "addressdetails": 1,
                },
                headers={"User-Agent": "sadie-gtm/1.0"},
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                logger.warning(f"Reverse geocoding failed for ({lat}, {lng}): {data['error']}")
                return None

            address = data.get("address", {})

            # Extract city - Nominatim uses different keys depending on location type
            city = (
                address.get("city") or
                address.get("town") or
                address.get("village") or
                address.get("municipality") or
                address.get("hamlet")
            )

            # Extract state
            state = address.get("state")
            # Convert full state name to abbreviation for US states
            if state and address.get("country_code") == "us":
                state = _state_to_abbrev(state)

            # Build street address
            street_parts = []
            if address.get("house_number"):
                street_parts.append(address["house_number"])
            if address.get("road"):
                street_parts.append(address["road"])
            street_address = " ".join(street_parts) if street_parts else None

            return ReverseGeocodingResult(
                address=street_address,
                city=city,
                state=state,
                country=address.get("country"),
                display_name=data.get("display_name"),
            )

        except httpx.HTTPError as e:
            logger.error(f"Reverse geocoding HTTP error for ({lat}, {lng}): {e}")
            return None


def _state_to_abbrev(state_name: str) -> str:
    """Convert US state name to abbreviation."""
    state_map = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
        "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
        "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
        "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
        "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
        "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
        "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
        "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
        "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
        "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
        "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
        "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
    }
    return state_map.get(state_name, state_name)
