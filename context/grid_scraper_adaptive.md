# Grid Scraper with Adaptive Subdivision

## Goal
Scrape all hotels in a region using coordinates for maximum accuracy and credit efficiency.

## Why Grid Scraper?
- Uses `ll` parameter with structured coordinates (proper API usage)
- More accurate than query string hacks ("hotels in 33139")
- Google returns precise lat/lng for each hotel
- Systematic coverage of entire region

## Adaptive Subdivision Strategy

### Problem
- Large grid cells (10km) = cheap but miss hotels due to Google's 20-result limit
- Small grid cells (2.5km) = complete coverage but burns credits in sparse areas

### Solution: Adaptive Subdivision
1. **Start with coarse grid** (10km cells)
2. **Search each cell** with `ll` parameter: `{"q": "hotels", "ll": "@25.7617,-80.1918,14z"}`
3. **If cell returns 20 results** (API max) → Cell is dense, subdivide into 4 smaller cells
4. **If cell returns < 20 results** → Got everything, move on
5. **Repeat** until all cells have < 20 results

### Credit Efficiency
- **Rural areas**: 1 search per 10km cell
- **Dense cities**: Auto-subdivides to 2.5km or smaller
- Only burn credits where hotels are actually dense

## Implementation Steps

### 1. Update Grid Scraper
- Modify `scripts/scrapers/grid.py` to support adaptive subdivision
- Add logic to detect when cell hits 20-result limit
- Recursively subdivide dense cells

### 2. Database Integration
- Take target region as input (center point + radius, or bounding box)
- Store results in `hotels` table with city/state as TEXT
- Normalize city/state in scraper (title case, trim)

### 3. Workflow
```bash
# Run scraper workflow with coordinates
uv run main.py scraper --center-lat 25.7617 --center-lng -80.1918 --radius-km 50

# Or with bounding box
uv run main.py scraper --state florida

# Scraper will:
# - Generate grid for the region
# - Search each cell adaptively
# - Insert hotels with normalized city/state text
```

## Grid Parameters
- **Initial cell size**: 10km × 10km
- **Subdivision threshold**: 20 results (API max)
- **Minimum cell size**: 2.5km × 2.5km (prevents infinite subdivision)
- **Zoom level**: `17z` (neighborhood level)

## Search Terms
Rotate through search types per cell to get diverse results:
- "hotels"
- "motels"
- "resorts"
- "boutique hotel"
- "inns"
- "lodges"

## Expected Credits Usage
**Example: Miami Metro Area**
- Area: ~100km × 100km
- Initial grid: 10×10 = 100 cells
- Dense areas (20% subdivide): ~80 cells
- Total cells: ~180 cells
- Search terms: 5 types
- **Total API calls**: ~900 searches

Compare to zip code approach (900 zip codes × 5 terms = 4,500 searches).

## Next Steps
1. Implement adaptive subdivision logic in grid.py
2. Add database integration to write to hotels table
3. Add workflow to main.py
4. Test with single region (Miami Beach (small and dense)) before scaling to states
