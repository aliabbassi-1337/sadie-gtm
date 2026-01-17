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

# City coordinates are now loaded from database via service layer
# This empty dict is a fallback - service should pass city_coords to GridScraper
_DEFAULT_CITY_COORDS: List[Tuple[float, float]] = []

# Hybrid mode settings (defaults - can be overridden via constructor)
HYBRID_DENSE_RADIUS_KM = 30.0  # Use small cells within this distance of a city
HYBRID_DENSE_CELL_SIZE_KM = 2.0  # Cell size for dense areas
HYBRID_SPARSE_CELL_SIZE_KM = 10.0  # Cell size for sparse areas

# Aggressive hybrid mode (lower cost, slightly less coverage)
HYBRID_AGGRESSIVE_DENSE_RADIUS_KM = 20.0
HYBRID_AGGRESSIVE_SPARSE_CELL_SIZE_KM = 15.0

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

# Search types - diverse terms to surface different properties
SEARCH_TYPES = [
    "hotel",
    "motel",
    "resort",
    "inn",
    "lodge",
    "guest house",
    "bed and breakfast",
    "vacation rental",
    "extended stay",
    "suites",
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

# Non-hotel businesses to skip by name keywords
SKIP_NON_HOTELS = [
    # Healthcare
    "pharmacy", "hospital", "clinic", "medical", "urgent care", "emergency",
    "dental", "dentist", "doctor", "physician", "health center", "healthcare",
    "veterinary", "vet clinic", "animal hospital", "laboratory",
    # Retail
    "publix", "walmart", "target", "costco", "kroger", "cvs", "walgreens",
    "home depot", "lowe's", "menards", "staples", "office depot",
    "dollar general", "dollar tree", "family dollar", "best buy", "apple store",
    "warby parker", "eyewear", "optical", "mattress",
    # Restaurants (generic food terms)
    "restaurant", "grill", "sushi", "pizza", "taco", "burrito", "bbq", "barbecue",
    "steakhouse", "seafood", "buffet", "diner", "bakery", "deli", "cafe",
    "bistro", "eatery", "cantina", "tavern", "pub", "brewery", "bar & grill",
    "ramen", "noodle", "pho", "wings", "wingstop", "wing stop", "hot pot",
    "korean bbq", "hibachi", "teriyaki", "shawarma", "falafel", "kebab",
    # Restaurants (chains)
    "mcdonald", "burger king", "wendy's", "taco bell", "chick-fil-a",
    "starbucks", "dunkin", "subway", "pizza hut", "domino's", "papa john",
    "chipotle", "panera", "olive garden", "applebee", "chili's", "ihop",
    "denny's", "waffle house", "cracker barrel", "outback", "longhorn",
    "red lobster", "texas roadhouse", "buffalo wild wings", "hooters",
    "carrabba", "bonefish", "cheesecake factory", "pf chang", "benihana",
    "sonic drive", "arby's", "popeyes", "five guys", "shake shack",
    "in-n-out", "whataburger", "jack in the box", "hardee", "carl's jr",
    "krispy kreme", "baskin", "cold stone", "dairy queen", "culver's",
    # Banks/Finance
    "bank of america", "chase bank", "wells fargo", "citibank", "td bank",
    "credit union", "atm", "western union", "moneygram", "payday loan",
    # Gas stations
    "gas station", "shell", "chevron", "exxon", "bp ", "speedway", "wawa",
    "sheetz", "racetrac", "quiktrip", "circle k", "7-eleven", "7 eleven",
    # Religious/Education
    "church", "temple", "mosque", "synagogue", "chapel",
    "school", "university", "college", "library", "academy",
    # Government/Services
    "police", "fire station", "post office", "ups store", "fedex", "usps",
    "dmv", "courthouse", "city hall",
    # Storage/Moving
    "storage", "self storage", "u-haul", "public storage", "extra space",
    # Fitness
    "gym", "fitness", "planet fitness", "la fitness", "ymca", "crossfit",
    "anytime fitness", "orangetheory", "equinox",
    # Personal services
    "salon", "barber", "nail", "spa ", "tattoo", "piercing",
    # Pet services
    "pet", "grooming", "doggy", "veterinar", "animal clinic",
    # Childcare
    "daycare", "childcare", "preschool", "kindergarten", "learning center",
    # Entertainment (not hotels)
    "cinema", "theater", "theatre", "bowling", "arcade", "laser tag",
    "escape room", "trampoline", "skating rink", "mini golf",
    # Car rental
    "sixt", "hertz", "avis", "enterprise rent", "budget car", "national car",
    "rent a car", "car rental",
    # Apartments/Senior Living
    "apartment", "the palace", "senior living", "assisted living", "nursing home",
    "retirement", "memory care", "eldercare",
    # Construction/Services
    "exteriors", "roofing", "plumbing", "electric", "hvac", "landscaping",
    "construction", "contractor", "remodeling", "renovation",
    # Coffee/Food misc
    "coffee", "bagel", "donut", "smoothie", "juice bar", "ice cream",
    "frozen yogurt", "cupcake", "cookie",
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
    # Non-hotels (retail, pharmacy, healthcare, restaurants, etc.)
    "publix.com", "cvs.com", "walgreens.com", "walmart.com", "target.com",
    "costco.com", "kroger.com", "albertsons.com", "safeway.com",
    "mcdonalds.com", "starbucks.com", "dunkindonuts.com", "subway.com",
    "chipotle.com", "tacobell.com", "wendys.com", "burgerking.com",
    "chick-fil-a.com", "dominos.com", "pizzahut.com", "papajohns.com",
    "bankofamerica.com", "chase.com", "wellsfargo.com", "citibank.com",
    "ups.com", "fedex.com", "usps.com",
    "homedepot.com", "lowes.com", "menards.com",
    "staples.com", "officedepot.com",
    # Government/education (not hotels)
    ".gov", ".edu", ".mil",
    "dnr.", "parks.", "recreation.",
]


def _distance_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate approximate distance in km between two points (Haversine simplified)."""
    avg_lat = (lat1 + lat2) / 2
    dlat = (lat2 - lat1) * 111.0
    dlng = (lng2 - lng1) * 111.0 * math.cos(math.radians(avg_lat))
    return math.sqrt(dlat * dlat + dlng * dlng)


def _distance_to_nearest_city(lat: float, lng: float, city_coords: List[Tuple[float, float]]) -> float:
    """Calculate distance to nearest city in the provided coordinates list."""
    if not city_coords:
        return float('inf')  # No cities = treat as sparse
    min_dist = float('inf')
    for city_lat, city_lng in city_coords:
        dist = _distance_km(lat, lng, city_lat, city_lng)
        if dist < min_dist:
            min_dist = dist
    return min_dist


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
    google_place_id: Optional[str] = None


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

    def __init__(
        self,
        api_key: Optional[str] = None,
        cell_size_km: float = DEFAULT_CELL_SIZE_KM,
        hybrid: bool = False,
        aggressive: bool = False,
        city_coords: Optional[List[Tuple[float, float]]] = None,
    ):
        self.api_key = api_key or os.environ.get("SERPER_API_KEY", "")
        if not self.api_key:
            raise ValueError("No Serper API key. Set SERPER_API_KEY env var or pass api_key.")

        self.cell_size_km = cell_size_km
        self.hybrid = hybrid  # Use variable cell sizes based on proximity to cities
        self.aggressive = aggressive  # Use more aggressive (cheaper) hybrid settings
        
        # City coordinates for hybrid mode density detection (passed from service)
        self.city_coords = city_coords or _DEFAULT_CITY_COORDS
        
        # Set hybrid parameters based on mode
        if aggressive:
            self.dense_radius_km = HYBRID_AGGRESSIVE_DENSE_RADIUS_KM
            self.sparse_cell_size_km = HYBRID_AGGRESSIVE_SPARSE_CELL_SIZE_KM
        else:
            self.dense_radius_km = HYBRID_DENSE_RADIUS_KM
            self.sparse_cell_size_km = HYBRID_SPARSE_CELL_SIZE_KM
        
        # Pick zoom level that covers the cell
        self.zoom_level = 14  # default
        for size, zoom in sorted(ZOOM_BY_CELL_SIZE.items()):
            if cell_size_km <= size:
                self.zoom_level = zoom
                break

        self._seen: Set[str] = set()  # Name-based dedup (fallback)
        self._seen_place_ids: Set[str] = set()  # Google Place ID dedup (primary)
        self._seen_locations: Set[Tuple[float, float]] = set()  # Location dedup (secondary)
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

        # Generate cells - use hybrid if enabled
        if self.hybrid:
            cells = self._generate_hybrid_grid(lat_min, lat_max, lng_min, lng_max)
            initial_cells = len(cells)
            
            # Count dense vs sparse cells for accurate estimate
            dense_cells = sum(1 for c in cells if c.size_km <= HYBRID_DENSE_CELL_SIZE_KM + 1.0)
            sparse_cells = initial_cells - dense_cells
            
            # Dense cells: 3 queries, no subdivision
            # Sparse cells: 4 queries, ~25% subdivision
            dense_api_calls = dense_cells * 3
            sparse_subdivided = int(sparse_cells * 0.25 * 4)
            sparse_api_calls = (sparse_cells + sparse_subdivided) * 4
            
            estimated_api_calls = dense_api_calls + sparse_api_calls
            estimated_total_cells = initial_cells + sparse_subdivided
            avg_queries_per_cell = estimated_api_calls / max(estimated_total_cells, 1)
        else:
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

            # Query count depends on cell size
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
            avg_queries_per_cell=round(avg_queries_per_cell, 1),
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
        self._seen_place_ids = set()
        self._seen_locations = set()
        self._stats = ScrapeStats()
        self._out_of_credits = False
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        # Store bounds for filtering out-of-region results (with 10% buffer)
        lat_buffer = (lat_max - lat_min) * 0.1
        lng_buffer = (lng_max - lng_min) * 0.1
        self._bounds = (lat_min - lat_buffer, lat_max + lat_buffer, lng_min - lng_buffer, lng_max + lng_buffer)

        hotels: List[ScrapedHotel] = []

        # Generate grid - use hybrid if enabled, otherwise uniform cell size
        if self.hybrid:
            cells = self._generate_hybrid_grid(lat_min, lat_max, lng_min, lng_max)
            logger.info(f"Starting hybrid scrape: {len(cells)} cells")
        else:
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

    def _generate_hybrid_grid(
        self,
        lat_min: float,
        lat_max: float,
        lng_min: float,
        lng_max: float,
    ) -> List[GridCell]:
        """Generate grid with variable cell sizes based on proximity to cities.
        
        - Near cities (within dense_radius_km): use small cells (2km)
        - Far from cities: use large cells (sparse_cell_size_km)
        
        This optimizes cost by using dense coverage only where hotels are likely.
        """
        # First pass: generate coarse grid to classify areas
        coarse_cells = self._generate_grid(lat_min, lat_max, lng_min, lng_max, self.sparse_cell_size_km)
        
        final_cells = []
        idx = 0
        
        for coarse_cell in coarse_cells:
            center_lat = coarse_cell.center_lat
            center_lng = coarse_cell.center_lng
            
            # Check distance to nearest city
            dist = _distance_to_nearest_city(center_lat, center_lng, self.city_coords)
            
            if dist <= self.dense_radius_km:
                # Dense area: subdivide into small cells
                small_cells = self._generate_grid(
                    coarse_cell.lat_min, coarse_cell.lat_max,
                    coarse_cell.lng_min, coarse_cell.lng_max,
                    HYBRID_DENSE_CELL_SIZE_KM
                )
                for cell in small_cells:
                    cell.index = idx
                    idx += 1
                final_cells.extend(small_cells)
            else:
                # Sparse area: keep coarse cell
                coarse_cell.index = idx
                idx += 1
                final_cells.append(coarse_cell)
        
        # Log hybrid grid stats
        dense_count = sum(1 for c in final_cells if c.size_km <= HYBRID_DENSE_CELL_SIZE_KM + 0.5)
        sparse_count = len(final_cells) - dense_count
        mode = "aggressive" if self.aggressive else "standard"
        logger.info(f"Hybrid grid ({mode}): {len(final_cells)} cells ({dense_count} dense @ {HYBRID_DENSE_CELL_SIZE_KM}km, {sparse_count} sparse @ {self.sparse_cell_size_km}km)")
        
        return final_cells

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
        
        # Get zoom level for this cell (important for hybrid mode with variable cell sizes)
        cell_zoom = self._get_zoom_for_cell_size(cell.size_km)

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
            results = await self._search_serper("hotel", cell.center_lat, cell.center_lng, cell_zoom)
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
        # Use cell.size_km for hybrid mode where cells have different sizes
        if cell.size_km <= 2.5:
            # Pick 3 diverse search types (hotel, motel, inn cover most cases)
            diverse_queries = [all_queries[0], all_queries[3], all_queries[6]]
            results = await asyncio.gather(*[
                self._search_serper(query, cell.center_lat, cell.center_lng, cell_zoom)
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
        scout_results = await self._search_serper(all_queries[0], cell.center_lat, cell.center_lng, cell_zoom)
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
                self._search_serper(query, cell.center_lat, cell.center_lng, cell_zoom)
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

    def _get_zoom_for_cell_size(self, cell_size_km: float) -> int:
        """Get appropriate zoom level for a cell size."""
        for size, zoom in sorted(ZOOM_BY_CELL_SIZE.items()):
            if cell_size_km <= size:
                return zoom
        return 12  # Default for large cells

    async def _search_serper(
        self,
        query: str,
        lat: float,
        lng: float,
        zoom_level: Optional[int] = None,
    ) -> List[dict]:
        """Call Serper Maps API with semaphore for rate limiting."""
        if self._out_of_credits:
            return []

        zoom = zoom_level or self.zoom_level

        async with self._semaphore:
            self._stats.api_calls += 1

            try:
                resp = await self._client.post(
                    SERPER_MAPS_URL,
                    headers={"X-API-KEY": self.api_key, "Content-Type": "application/json"},
                    json={"q": query, "num": 100, "ll": f"@{lat},{lng},{zoom}z"},
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

        # Filter by place type - only keep lodging types
        place_type = (place.get("type") or "").lower()
        valid_lodging_types = ["hotel", "motel", "inn", "resort", "lodge", "hostel", "guest house", "bed & breakfast", "b&b", "suites", "extended stay"]
        is_lodging = any(t in place_type for t in valid_lodging_types)
        
        if place_type and not is_lodging:
            self._stats.chains_skipped += 1
            logger.debug(f"SKIP non-lodging type '{place_type}': {name}")
            return None

        name_lower = name.lower()
        website = place.get("website", "") or ""
        place_id = place.get("placeId")  # Google Place ID - most reliable dedup key
        lat = place.get("latitude")
        lng = place.get("longitude")

        # 3-tier deduplication: placeId → location → name
        # Primary: Google Place ID (globally unique, stable)
        if place_id:
            if place_id in self._seen_place_ids:
                self._stats.duplicates_skipped += 1
                logger.debug(f"SKIP duplicate (placeId): {name}")
                return None
            self._seen_place_ids.add(place_id)
        elif lat and lng:
            # Secondary: Location (~11m precision)
            loc_key = (round(lat, 4), round(lng, 4))
            if loc_key in self._seen_locations:
                self._stats.duplicates_skipped += 1
                logger.debug(f"SKIP duplicate (location): {name} at ({lat:.4f}, {lng:.4f})")
                return None
            self._seen_locations.add(loc_key)
        else:
            # Tertiary: Name-based (least reliable)
            if name_lower in self._seen:
                self._stats.duplicates_skipped += 1
                logger.debug(f"SKIP duplicate (name): {name}")
                return None
            self._seen.add(name_lower)

        # Track location for cell coverage analysis
        if lat and lng:
            coverage_key = (round(lat, 3), round(lng, 3))  # ~111m precision for coverage
            self._seen_locations.add(coverage_key)

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

        # Skip non-hotel businesses by name
        for keyword in SKIP_NON_HOTELS:
            if keyword in name_lower:
                self._stats.chains_skipped += 1
                logger.debug(f"SKIP non-hotel '{keyword}': {name}")
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
            google_place_id=place.get("placeId") or place.get("cid"),
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
