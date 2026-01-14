"""
Grid Scraper - Adaptive grid-based hotel scraping.

Converted from scripts/scrapers/grid.py into proper service code.
Uses adaptive subdivision: starts with coarse grid, subdivides dense cells.
"""

import os
import math
import asyncio
from typing import List, Optional, Set, Tuple

import httpx
from pydantic import BaseModel
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SERPER_MAPS_URL = "https://google.serper.dev/maps"

# City coordinates for quick lookups
CITY_COORDINATES = {
    "miami_beach": (25.7907, -80.1300),
    "miami": (25.7617, -80.1918),
    "orlando": (28.5383, -81.3792),
    "tampa": (27.9506, -82.4572),
    "los_angeles": (34.0522, -118.2437),
    "san_francisco": (37.7749, -122.4194),
    "new_york": (40.7128, -74.0060),
    "las_vegas": (36.1699, -115.1398),
}

# Adaptive subdivision settings (from context/grid_scraper_adaptive.md)
DEFAULT_CELL_SIZE_KM = 2.0   # Default cell size (2km works for most areas)
MIN_CELL_SIZE_KM = 0.5       # Don't subdivide below 500m
API_RESULT_LIMIT = 20        # Serper returns max 20 results - subdivide if hit

# Zoom levels by cell size (must cover the cell area)
ZOOM_BY_CELL_SIZE = {
    0.5: 16,   # 500m cell -> 16z
    1.0: 15,   # 1km cell -> 15z
    2.0: 14,   # 2km cell -> 14z
    5.0: 13,   # 5km cell -> 13z
    10.0: 12,  # 10km cell -> 12z
}

# Concurrency settings - stay under Serper rate limits
MAX_CONCURRENT_CELLS = 2     # Process up to 2 cells concurrently
MAX_CONCURRENT_REQUESTS = 4  # Stay under 5 qps rate limit (free/basic plan)

# State bounding boxes from scripts/scrapers/grid.py
STATE_BOUNDS = {
    "florida": (24.396308, 31.000968, -87.634896, -79.974307),
    "california": (32.528832, 42.009503, -124.482003, -114.131211),
    "texas": (25.837377, 36.500704, -106.645646, -93.508039),
    "new_york": (40.477399, 45.015851, -79.762418, -71.777491),
    "tennessee": (34.982924, 36.678118, -90.310298, -81.6469),
    "north_carolina": (33.752878, 36.588117, -84.321869, -75.460621),
    "georgia": (30.355644, 35.000659, -85.605165, -80.839729),
    "arizona": (31.332177, 37.004260, -114.818269, -109.045223),
    "nevada": (35.001857, 42.002207, -120.005746, -114.039648),
    "colorado": (36.992426, 41.003444, -109.060253, -102.041524),
}

# Search types - diverse terms to surface different properties (from original)
SEARCH_TYPES = [
    "hotel",
    "motel",
    "resort",
    "boutique hotel",
    "inn",
    "lodge",
    "guest house",
    "vacation rental",
    "extended stay",
    "suites",
    "apart hotel",
]

# Modifiers to get niche results (rotated per cell)
SEARCH_MODIFIERS = [
    "",  # Plain search
    "small",
    "family",
    "cheap",
    "budget",
    "local",
    "independent",
    "boutique",
    "cozy",
    "beachfront",
    "waterfront",
    "downtown",
]

# Chain filter - names to skip
SKIP_CHAINS = [
    "marriott", "hilton", "hyatt", "sheraton", "westin", "w hotel",
    "intercontinental", "holiday inn", "crowne plaza", "ihg",
    "best western", "choice hotels", "comfort inn", "quality inn",
    "radisson", "wyndham", "ramada", "days inn", "super 8", "motel 6",
    "la quinta", "travelodge", "ibis", "novotel", "mercure", "accor",
    "four seasons", "ritz-carlton", "st. regis", "fairmont",
]

# Website domains to skip (big chains, aggregators, social media, junk)
SKIP_DOMAINS = [
    # Big chains
    "marriott.com", "hilton.com", "hyatt.com", "ihg.com",
    "wyndham.com", "wyndhamhotels.com", "choicehotels.com", "bestwestern.com",
    "radissonhotels.com", "accor.com", "fourseasons.com",
    "ritzcarlton.com", "starwoodhotels.com",
    # OTAs and aggregators
    "booking.com", "expedia.com", "hotels.com", "trivago.com",
    "tripadvisor.com", "kayak.com", "priceline.com", "agoda.com",
    "airbnb.com", "vrbo.com",
    # Social media
    "facebook.com", "instagram.com", "twitter.com", "youtube.com",
    "tiktok.com", "linkedin.com", "yelp.com",
    # Other junk
    "google.com",
    # Government/education (not hotels)
    ".gov", ".edu", ".mil",
    "dnr.", "parks.", "recreation.",
]


class GridCell(BaseModel):
    """A grid cell for searching."""
    lat_min: float
    lat_max: float
    lng_min: float
    lng_max: float
    index: int = 0  # Cell index for rotating search terms

    @property
    def center_lat(self) -> float:
        return (self.lat_min + self.lat_max) / 2

    @property
    def center_lng(self) -> float:
        return (self.lng_min + self.lng_max) / 2

    @property
    def size_km(self) -> float:
        """Approximate cell size in km (average of width/height)."""
        height = (self.lat_max - self.lat_min) * 111.0
        width = (self.lng_max - self.lng_min) * 111.0 * math.cos(math.radians(self.center_lat))
        return (height + width) / 2

    def subdivide(self) -> List["GridCell"]:
        """Split into 4 smaller cells."""
        mid_lat = self.center_lat
        mid_lng = self.center_lng
        base_idx = self.index * 4
        return [
            GridCell(lat_min=self.lat_min, lat_max=mid_lat, lng_min=self.lng_min, lng_max=mid_lng, index=base_idx),
            GridCell(lat_min=self.lat_min, lat_max=mid_lat, lng_min=mid_lng, lng_max=self.lng_max, index=base_idx + 1),
            GridCell(lat_min=mid_lat, lat_max=self.lat_max, lng_min=self.lng_min, lng_max=mid_lng, index=base_idx + 2),
            GridCell(lat_min=mid_lat, lat_max=self.lat_max, lng_min=mid_lng, lng_max=self.lng_max, index=base_idx + 3),
        ]


class ScrapedHotel(BaseModel):
    """Hotel data from scraper."""
    name: str
    website: Optional[str] = None
    phone: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None


class ScrapeStats(BaseModel):
    """Scrape run statistics."""
    hotels_found: int = 0
    api_calls: int = 0
    cells_searched: int = 0
    cells_subdivided: int = 0
    cells_skipped: int = 0  # Cells with existing coverage
    cells_reduced: int = 0  # Cells with partial coverage (fewer queries)
    duplicates_skipped: int = 0
    chains_skipped: int = 0
    out_of_bounds: int = 0  # Hotels outside scrape region


class ScrapeEstimate(BaseModel):
    """Cost estimate for a scrape run."""
    initial_cells: int = 0
    estimated_cells_after_subdivision: int = 0
    avg_queries_per_cell: float = 4.0  # Adaptive: sparse=2, medium=6, dense=12
    estimated_api_calls: int = 0
    estimated_cost_usd: float = 0.0
    estimated_hotels: int = 0
    region_size_km2: float = 0.0


class GridScraper:
    """Adaptive grid-based hotel scraper using Serper Maps API."""

    def __init__(self, api_key: Optional[str] = None, cell_size_km: float = DEFAULT_CELL_SIZE_KM):
        self.api_key = api_key or os.environ.get("SERPER_API_KEY", "")
        if not self.api_key:
            raise ValueError("No Serper API key. Set SERPER_API_KEY env var or pass api_key.")

        self.cell_size_km = cell_size_km
        # Pick zoom level that covers the cell
        self.zoom_level = 14  # default
        for size, zoom in sorted(ZOOM_BY_CELL_SIZE.items()):
            if cell_size_km <= size:
                self.zoom_level = zoom
                break

        self._seen: Set[str] = set()
        self._seen_locations: Set[Tuple[float, float]] = set()  # (lat, lng) rounded to ~100m
        self._stats = ScrapeStats()
        self._out_of_credits = False
        # Scrape bounds for filtering out-of-region results
        self._bounds: Optional[Tuple[float, float, float, float]] = None  # (lat_min, lat_max, lng_min, lng_max)

    async def scrape_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float,
        on_batch_complete: Optional[callable] = None,
    ) -> Tuple[List[ScrapedHotel], ScrapeStats]:
        """Scrape hotels in a circular region using adaptive grid."""
        # Convert center+radius to bounding box
        lat_deg = radius_km / 111.0
        lng_deg = radius_km / (111.0 * math.cos(math.radians(center_lat)))

        return await self._scrape_bounds(
            lat_min=center_lat - lat_deg,
            lat_max=center_lat + lat_deg,
            lng_min=center_lng - lng_deg,
            lng_max=center_lng + lng_deg,
            on_batch_complete=on_batch_complete,
        )

    async def scrape_state(
        self,
        state: str,
        on_batch_complete: Optional[callable] = None,
    ) -> Tuple[List[ScrapedHotel], ScrapeStats]:
        """Scrape hotels in a state using adaptive grid."""
        state_key = state.lower().replace(" ", "_")
        if state_key not in STATE_BOUNDS:
            raise ValueError(f"Unknown state: {state}. Available: {list(STATE_BOUNDS.keys())}")

        lat_min, lat_max, lng_min, lng_max = STATE_BOUNDS[state_key]
        return await self._scrape_bounds(lat_min, lat_max, lng_min, lng_max, on_batch_complete=on_batch_complete)

    def estimate_region(
        self,
        center_lat: float,
        center_lng: float,
        radius_km: float,
    ) -> ScrapeEstimate:
        """Estimate cost for scraping a circular region."""
        lat_deg = radius_km / 111.0
        lng_deg = radius_km / (111.0 * math.cos(math.radians(center_lat)))

        return self._estimate_bounds(
            lat_min=center_lat - lat_deg,
            lat_max=center_lat + lat_deg,
            lng_min=center_lng - lng_deg,
            lng_max=center_lng + lng_deg,
        )

    def estimate_state(self, state: str) -> ScrapeEstimate:
        """Estimate cost for scraping a state."""
        state_key = state.lower().replace(" ", "_")
        if state_key not in STATE_BOUNDS:
            raise ValueError(f"Unknown state: {state}. Available: {list(STATE_BOUNDS.keys())}")

        lat_min, lat_max, lng_min, lng_max = STATE_BOUNDS[state_key]
        return self._estimate_bounds(lat_min, lat_max, lng_min, lng_max)

    def _estimate_bounds(
        self,
        lat_min: float,
        lat_max: float,
        lng_min: float,
        lng_max: float,
    ) -> ScrapeEstimate:
        """Calculate cost estimate for a bounding box."""
        # Calculate region size
        center_lat = (lat_min + lat_max) / 2
        height_km = (lat_max - lat_min) * 111.0
        width_km = (lng_max - lng_min) * 111.0 * math.cos(math.radians(center_lat))
        region_size_km2 = height_km * width_km

        # Count cells with configured cell size
        cells = self._generate_grid(lat_min, lat_max, lng_min, lng_max, self.cell_size_km)
        initial_cells = len(cells)

        # For small cells (dense mode), no subdivision expected
        # For large cells, ~25% subdivide
        if self.cell_size_km <= 2.0:
            subdivision_rate = 0.0
        else:
            subdivision_rate = 0.25
        subdivided_cells = int(initial_cells * subdivision_rate * 4)
        estimated_total_cells = initial_cells + subdivided_cells

        # Query count depends on cell size:
        # - Small cells (≤2km, dense mode): 3 diverse queries per cell
        # - Large cells (>2km): ~4 queries avg (early exit)
        if self.cell_size_km <= 2.0:
            avg_queries_per_cell = 3.0
        else:
            avg_queries_per_cell = 4.0
        estimated_api_calls = int(estimated_total_cells * avg_queries_per_cell)

        # Cost: $1 per 1000 credits ($50 plan = 50k credits)
        cost_per_credit = 0.001
        estimated_cost = estimated_api_calls * cost_per_credit

        # Estimate hotels: ~8-15 unique hotels per cell after dedup/filtering
        # Conservative estimate of 10 per cell
        hotels_per_cell = 10
        estimated_hotels = estimated_total_cells * hotels_per_cell

        return ScrapeEstimate(
            initial_cells=initial_cells,
            estimated_cells_after_subdivision=estimated_total_cells,
            avg_queries_per_cell=avg_queries_per_cell,
            estimated_api_calls=estimated_api_calls,
            estimated_cost_usd=round(estimated_cost, 2),
            estimated_hotels=estimated_hotels,
            region_size_km2=round(region_size_km2, 1),
        )

    async def _scrape_bounds(
        self,
        lat_min: float,
        lat_max: float,
        lng_min: float,
        lng_max: float,
        on_batch_complete: Optional[callable] = None,
    ) -> Tuple[List[ScrapedHotel], ScrapeStats]:
        """Scrape with adaptive subdivision using concurrent cell processing.

        Args:
            on_batch_complete: Optional callback called after each batch with list of hotels found.
                               Use for incremental saving.
        """
        self._seen = set()
        self._seen_locations = set()
        self._stats = ScrapeStats()
        self._out_of_credits = False
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        # Store bounds for filtering out-of-region results (with 10% buffer)
        lat_buffer = (lat_max - lat_min) * 0.1
        lng_buffer = (lng_max - lng_min) * 0.1
        self._bounds = (lat_min - lat_buffer, lat_max + lat_buffer, lng_min - lng_buffer, lng_max + lng_buffer)

        hotels: List[ScrapedHotel] = []

        # Generate grid with configured cell size
        cells = self._generate_grid(lat_min, lat_max, lng_min, lng_max, self.cell_size_km)
        logger.info(f"Starting scrape: {len(cells)} cells ({self.cell_size_km}km, zoom {self.zoom_level}z)")

        async with httpx.AsyncClient(timeout=30.0) as client:
            self._client = client

            while cells and not self._out_of_credits:
                # Process cells in batches concurrently
                batch = []
                for _ in range(min(MAX_CONCURRENT_CELLS, len(cells))):
                    if cells:
                        batch.append(cells.pop(0))

                # Run batch concurrently
                results = await asyncio.gather(*[self._process_cell(cell) for cell in batch])

                # Collect results and handle subdivision
                batch_hotels = []
                for cell, (cell_hotels, hit_limit) in zip(batch, results):
                    self._stats.cells_searched += 1
                    hotels.extend(cell_hotels)
                    batch_hotels.extend(cell_hotels)

                    # Adaptive subdivision: if we hit API limit and cell is large enough
                    if hit_limit and cell.size_km > MIN_CELL_SIZE_KM * 2:
                        subcells = cell.subdivide()
                        cells.extend(subcells)
                        self._stats.cells_subdivided += 1
                        logger.debug(f"Subdivided cell at ({cell.center_lat:.3f}, {cell.center_lng:.3f})")

                # Incremental save callback
                if on_batch_complete and batch_hotels:
                    await on_batch_complete(batch_hotels)
                    logger.info(f"Saved {len(batch_hotels)} hotels ({self._stats.cells_searched}/{len(cells) + self._stats.cells_searched} cells)")

        self._stats.hotels_found = len(hotels)
        logger.info(f"Scrape done: {len(hotels)} hotels, {self._stats.api_calls} API calls")

        return hotels, self._stats

    async def _process_cell(self, cell: GridCell) -> Tuple[List[ScrapedHotel], bool]:
        """Process a single cell (wrapper for concurrent execution)."""
        return await self._search_cell(cell)

    def _generate_grid(
        self,
        lat_min: float,
        lat_max: float,
        lng_min: float,
        lng_max: float,
        cell_size_km: float,
    ) -> List[GridCell]:
        """Generate grid cells covering bounding box."""
        center_lat = (lat_min + lat_max) / 2

        height_km = (lat_max - lat_min) * 111.0
        width_km = (lng_max - lng_min) * 111.0 * math.cos(math.radians(center_lat))

        n_lat = max(1, int(math.ceil(height_km / cell_size_km)))
        n_lng = max(1, int(math.ceil(width_km / cell_size_km)))

        lat_step = (lat_max - lat_min) / n_lat
        lng_step = (lng_max - lng_min) / n_lng

        cells = []
        idx = 0
        for i in range(n_lat):
            for j in range(n_lng):
                cells.append(GridCell(
                    lat_min=lat_min + i * lat_step,
                    lat_max=lat_min + (i + 1) * lat_step,
                    lng_min=lng_min + j * lng_step,
                    lng_max=lng_min + (j + 1) * lng_step,
                    index=idx,
                ))
                idx += 1
        return cells

    def _get_cell_coverage(self, cell: GridCell) -> int:
        """Count how many already-seen hotels are within this cell."""
        count = 0
        for lat, lng in self._seen_locations:
            if cell.lat_min <= lat <= cell.lat_max and cell.lng_min <= lng <= cell.lng_max:
                count += 1
        return count

    async def _search_cell(
        self,
        cell: GridCell,
    ) -> Tuple[List[ScrapedHotel], bool]:
        """Search a cell with adaptive query count based on density.

        For small cells (≤2km, dense mode): run all 12 queries
        For large cells: early exit based on scout query results

        Skip cells that already have good coverage from adjacent cells.
        """
        hotels: List[ScrapedHotel] = []
        hit_limit = False

        # Check if cell already has coverage from adjacent cells
        existing_coverage = self._get_cell_coverage(cell)
        if existing_coverage >= 5:
            # Cell already has 5+ hotels from adjacent cell queries - skip entirely
            self._stats.cells_skipped += 1
            logger.debug(f"SKIP cell ({cell.center_lat:.3f}, {cell.center_lng:.3f}) - already has {existing_coverage} hotels from adjacent cells")
            return hotels, False
        elif existing_coverage >= 2:
            # Cell has some coverage - run reduced queries (just 1)
            self._stats.cells_reduced += 1
            logger.debug(f"REDUCED queries for cell ({cell.center_lat:.3f}, {cell.center_lng:.3f}) - has {existing_coverage} hotels")
            results = await self._search_serper("hotel", cell.center_lat, cell.center_lng)
            for place in results:
                hotel = self._process_place(place)
                if hotel:
                    hotels.append(hotel)
            return hotels, len(results) >= API_RESULT_LIMIT

        # Pick 4 types for this cell (rotate through them based on cell index)
        num_types = len(SEARCH_TYPES)
        types_for_cell = [
            SEARCH_TYPES[cell.index % num_types],
            SEARCH_TYPES[(cell.index + 3) % num_types],
            SEARCH_TYPES[(cell.index + 6) % num_types],
            SEARCH_TYPES[(cell.index + 9) % num_types],
        ]

        # Pick 3 modifiers for this cell (rotate through them)
        num_mods = len(SEARCH_MODIFIERS)
        modifiers_for_cell = [
            SEARCH_MODIFIERS[cell.index % num_mods],
            SEARCH_MODIFIERS[(cell.index + 4) % num_mods],
            SEARCH_MODIFIERS[(cell.index + 8) % num_mods],
        ]

        # Build all queries for this cell
        all_queries = []
        for search_type in types_for_cell:
            for modifier in modifiers_for_cell:
                query = f"{modifier} {search_type}".strip() if modifier else search_type
                all_queries.append(query)

        # Dense mode (small cells ≤2km): run 3 diverse queries instead of 12
        # This reduces duplicates significantly while still getting good coverage
        if self.cell_size_km <= 2.0:
            # Pick 3 diverse search types (hotel, motel, inn cover most cases)
            diverse_queries = [all_queries[0], all_queries[3], all_queries[6]]
            results = await asyncio.gather(*[
                self._search_serper(query, cell.center_lat, cell.center_lng)
                for query in diverse_queries
            ])
            for places in results:
                if len(places) >= API_RESULT_LIMIT:
                    hit_limit = True
                for place in places:
                    hotel = self._process_place(place)
                    if hotel:
                        hotels.append(hotel)
            return hotels, hit_limit

        # Sparse mode (large cells): scout first, early exit if sparse
        scout_results = await self._search_serper(all_queries[0], cell.center_lat, cell.center_lng)
        scout_count = len(scout_results)

        # Process scout results
        if scout_count >= API_RESULT_LIMIT:
            hit_limit = True
        for place in scout_results:
            hotel = self._process_place(place)
            if hotel:
                hotels.append(hotel)

        # Determine how many more queries based on density
        if scout_count <= 5:
            remaining_queries = all_queries[1:2]  # 2 total
        elif scout_count <= 14:
            remaining_queries = all_queries[1:6]  # 6 total
        else:
            remaining_queries = all_queries[1:]   # 12 total

        # Execute remaining queries concurrently
        if remaining_queries:
            results = await asyncio.gather(*[
                self._search_serper(query, cell.center_lat, cell.center_lng)
                for query in remaining_queries
            ])

            for places in results:
                if len(places) >= API_RESULT_LIMIT:
                    hit_limit = True
                for place in places:
                    hotel = self._process_place(place)
                    if hotel:
                        hotels.append(hotel)

        return hotels, hit_limit

    async def _search_serper(
        self,
        query: str,
        lat: float,
        lng: float,
    ) -> List[dict]:
        """Call Serper Maps API with semaphore for rate limiting."""
        if self._out_of_credits:
            return []

        async with self._semaphore:
            self._stats.api_calls += 1

            try:
                resp = await self._client.post(
                    SERPER_MAPS_URL,
                    headers={"X-API-KEY": self.api_key, "Content-Type": "application/json"},
                    json={"q": query, "num": 100, "ll": f"@{lat},{lng},{self.zoom_level}z"},
                )

                if resp.status_code == 400 and "credits" in resp.text.lower():
                    logger.warning("Out of Serper credits")
                    self._out_of_credits = True
                    return []

                if resp.status_code != 200:
                    logger.error(f"Serper error {resp.status_code}: {resp.text[:100]}")
                    return []

                return resp.json().get("places", [])
            except Exception as e:
                logger.error(f"Serper request failed: {e}")
                return []

    def _process_place(self, place: dict) -> Optional[ScrapedHotel]:
        """Process place into ScrapedHotel, filtering chains/duplicates."""
        name = place.get("title", "").strip()
        if not name:
            return None

        name_lower = name.lower()
        website = place.get("website", "") or ""

        # Skip duplicates
        if name_lower in self._seen:
            self._stats.duplicates_skipped += 1
            logger.debug(f"SKIP duplicate: {name}")
            return None
        self._seen.add(name_lower)

        # Track location for cell coverage analysis (round to ~100m grid)
        lat = place.get("latitude")
        lng = place.get("longitude")
        if lat and lng:
            loc_key = (round(lat, 3), round(lng, 3))  # ~111m precision
            self._seen_locations.add(loc_key)

            # Filter out-of-bounds results (Paris hotels when scraping Miami)
            if self._bounds:
                lat_min, lat_max, lng_min, lng_max = self._bounds
                if not (lat_min <= lat <= lat_max and lng_min <= lng <= lng_max):
                    self._stats.out_of_bounds += 1
                    logger.debug(f"SKIP out-of-bounds: {name} at ({lat:.4f}, {lng:.4f})")
                    return None

        # Skip chains by name
        for chain in SKIP_CHAINS:
            if chain in name_lower:
                self._stats.chains_skipped += 1
                logger.debug(f"SKIP chain '{chain}': {name}")
                return None

        # Skip chains/aggregators by website domain
        website_lower = website.lower()
        for domain in SKIP_DOMAINS:
            if domain in website_lower:
                self._stats.chains_skipped += 1
                logger.debug(f"SKIP domain '{domain}': {name} -> {website}")
                return None

        # Parse city/state from address
        address = place.get("address", "")
        city, state = self._parse_address(address)

        return ScrapedHotel(
            name=name,
            website=place.get("website"),
            phone=place.get("phoneNumber"),
            latitude=place.get("latitude"),
            longitude=place.get("longitude"),
            address=address or None,
            city=city,
            state=state,
            rating=place.get("rating"),
            review_count=place.get("reviews"),
        )

    def _parse_address(self, address: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract city and state from address string."""
        if not address:
            return None, None

        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 2:
            # Last part: "FL 33139" -> state = "FL"
            last = parts[-1].split()
            state = last[0] if last and len(last[0]) == 2 else None
            city = parts[-2] if len(parts) >= 2 else None
            return city, state

        return None, None
