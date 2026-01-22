# Task: Grid Scraping Optimization - Quadtree & Density-Based Approaches

## Problem Statement

The current uniform grid approach in `grid_scraper.py` faces a fundamental tradeoff:
- **Dense areas** (Palm Beach, Miami Beach) get under-sampled even at 2km cells
- **Sparse areas** (rural, ocean, swamps) waste API calls on empty cells
- **Cost scales with grid size**, not hotel density
- Creating 2,000+ cells for Palm Beach requires expensive discovery of which areas are actually dense

### Current Performance
- Palm Beach: ~550 cells @ 2km = 2,750 API calls ($2.75)
- 30-40% of cells are sparse/empty (wasted cost)
- Dense downtown areas still hit 20-result limit (incomplete coverage)

## Root Cause Analysis

The grid approach **starts uniformly and adapts reactively**:
1. Generate thousands of uniform cells
2. Run scout queries to discover density
3. Subdivide cells that hit limits
4. Skip cells that are empty

**The inefficiency:** You pay to discover density instead of knowing it upfront.

## Recommended Solutions

### **Solution 1: True Quadtree (Best ROI)**

Replace uniform grid with recursive subdivision based on actual results.

#### How It Works
```
1. Start with ONE large cell per metro area (e.g., 50km)
2. Run scout query → if >15 results, subdivide into 4 quadrants
3. Recursively subdivide until:
   - Cell has <20 results (fits in one query)
   - Cell reaches minimum size (0.5km)
4. Only dense areas get fine subdivision
```

#### Expected Performance (Palm Beach Example)
| Area Type | Cell Size | Cells | Queries/Cell | API Calls |
|-----------|-----------|-------|--------------|-----------|
| Downtown dense | 0.5km | 800 | 5 | 4,000 |
| Suburban | 5km | 60 | 5 | 300 |
| Sparse/rural | 10km | 11 | 2 | 22 |
| **TOTAL** | - | **871** | - | **4,322** |

**vs Current:** Same coverage in dense areas, 50-70% cost reduction in sparse areas.

#### Implementation
```python
def _generate_quadtree_cells(
    self,
    lat_min: float, lat_max: float,
    lng_min: float, lng_max: float,
    depth: int = 0,
    max_depth: int = 6
) -> List[GridCell]:
    """Generate cells via recursive subdivision."""
    cell = GridCell(lat_min, lat_max, lng_min, lng_max)

    # Run scout query
    scout_results = await self._search_serper("hotel", cell.center_lat, cell.center_lng, zoom)

    # Base cases
    if len(scout_results) < 15:  # Sparse - no subdivision needed
        return [cell]
    if cell.size_km < MIN_CELL_SIZE_KM:  # Too small - stop
        return [cell]
    if depth >= max_depth:  # Max depth - stop
        return [cell]

    # Recursive case - subdivide into 4 quadrants
    cells = []
    for subcell in cell.subdivide():
        cells.extend(await self._generate_quadtree_cells(
            subcell.lat_min, subcell.lat_max,
            subcell.lng_min, subcell.lng_max,
            depth + 1, max_depth
        ))

    return cells
```

#### References
- [Spatial Indexing with Quadtrees](https://medium.com/@waleoyediran/spatial-indexing-with-quadtrees-b998ae49336) - Core algorithm explanation
- [Quadtree: The Secret Behind Sub-Millisecond Location Searches](https://pratikpandey.substack.com/p/quad-tree-the-secret-behind-sub-millisecond) - Real-world applications
- [Damn Cool Algorithms: Spatial Indexing with Quadtrees](http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves) - Classic deep dive

### **Solution 2: Density Pre-Scouting + Targeted Grid (Most Accurate)**

Run a cheap coarse grid first, cluster the results, then apply fine grids only where needed.

#### How It Works
```
Phase 1 - Density Mapping (200 API calls):
  - Run 10km grid with single "hotel" query per cell
  - ~200 cells for all of Florida
  - Cost: $0.20

Phase 2 - Density Clustering:
  - Use DBSCAN to identify "hotel density zones"
  - Cluster results into high/medium/low density regions
  - No API calls

Phase 3 - Targeted Dense Grid:
  - Apply 0.5-2km fine grid ONLY to high-density zones
  - Use measured density, not proximity to cities

Phase 4 - Sparse Sampling:
  - Use 5-10km grid for low-density areas
```

#### Expected Performance (Palm Beach)
| Phase | Cells | API Calls | Cost |
|-------|-------|-----------|------|
| Scout (10km grid) | 20 | 20 | $0.02 |
| Dense zones (0.5km) | 1,000 | 5,000 | $5.00 |
| Sparse zones (5km) | 50 | 100 | $0.10 |
| **TOTAL** | **1,070** | **5,120** | **$5.12** |

**Result:** Near-complete coverage for ~2x current cost, but minimal waste.

#### Implementation Steps
1. Add `scout_phase()` method to run coarse grid
2. Integrate sklearn DBSCAN for clustering
3. Generate heatmap of density (visualize before spending)
4. Apply variable cell sizes based on clusters

#### References
- [DBSCAN Density-Based Clustering](https://www.geeksforgeeks.org/machine-learning/dbscan-clustering-in-ml-density-based-clustering/) - Clustering algorithm
- [How Density-based Clustering Works (ArcGIS)](https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-statistics/how-density-based-clustering-works.htm) - Spatial applications
- [ADBSCAN: Adaptive Density-Based Clustering](https://arxiv.org/pdf/1809.06189) - Adaptive approach for variable density

### **Solution 3: Improved Hybrid Mode (Lowest Effort)**

Keep current grid approach but replace city proximity with actual density measurement.

#### Current Problem
Your hybrid mode (lines 581-631 in `grid_scraper.py`) uses proximity to cities:
```python
dist = _distance_to_nearest_city(center_lat, center_lng, self.city_coords)
if dist <= self.dense_radius_km:
    # Use small cells
```

**Issues:**
- Cities aren't perfect proxy for hotel density
- Tourist areas (beaches, theme parks) are dense but not "cities"
- City centers might have fewer hotels than suburbs
- Airport corridors can be extremely dense

#### Improvement
```python
# Phase 1: Run coarse scout
density_map = await self._measure_density(lat_min, lat_max, lng_min, lng_max)

# Phase 2: Generate adaptive grid
for coarse_cell in coarse_cells:
    measured_density = density_map.get(coarse_cell)
    if measured_density > 15:  # Dense
        cell_size = 0.5
    elif measured_density > 8:  # Medium
        cell_size = 2.0
    else:  # Sparse
        cell_size = 10.0
```

**Cost:** +200 scout API calls upfront, saves 1,000+ wasted calls in sparse areas.

## Pagination Optimization

### Current Implementation (Good)
Your zoom level strategy (lines 43-51) is already excellent:
```python
ZOOM_BY_CELL_SIZE = {
    0.5: 15,   # 500m cell -> 15z (1km radius) - overlap catches more
    2.0: 14,   # 2km cell -> 14z
    10.0: 12,  # 10km cell -> 12z
}
```

**Key insight from your code:** "Wider zoom + smaller cells = more unique results due to overlap"

This is brilliant - at zoom 15 with 0.5km cells, adjacent cells have overlapping search radii, catching hotels that get buried in single larger searches.

### Recommended Enhancements

1. **Lean into overlap for dense areas**
   - Use zoom 16 (500m radius) with 0.3km cells in downtown cores
   - Overlap is a feature, not a bug (similar to hexagonal grid coverage)

2. **Optimize pagination strategy**
   - According to [SerpAPI pagination research](https://serpapi.com/blog/how-we-reverse-engineered-google-maps-pagination/), beyond page 6 results get duplicated
   - Your current `max_pages` parameter is good
   - Recommendation: `max_pages=2` for most cells, `max_pages=3` only in proven dense cells

3. **Smart page fetching**
   ```python
   # Stop pagination early if results are mostly duplicates
   if page > 1 and new_unique_results < 5:
       break  # Diminishing returns
   ```

### References
- [How SerpApi Reverse-Engineered Google Maps Pagination](https://serpapi.com/blog/how-we-reverse-engineered-google-maps-pagination/) - Pagination limits and strategies
- [Building a Google Places Extraction Tool That Scales](https://dev.to/domharvest/building-a-google-places-extraction-tool-that-actually-scales-1i1j) - Deduplication best practices

## Grid Shape Optimization

### Rectangular vs Hexagonal Grids

Your current rectangular grid is simple but has coverage gaps at edges. Research shows hexagonal grids are superior for certain geographic patterns.

#### When to Use Hexagonal
- **Coastal areas** (Palm Beach, Miami Beach) - [Research shows](https://www.researchgate.net/publication/222692566_Rectangular_and_hexagonal_grids_used_for_observation_experiment_and_simulation_in_ecology) hexagonal grids "better capture the contours of coastlines"
- **Radial patterns** - Hotels clustered around a central point (airports, attractions)
- **Minimizing edge effects** - Hexagons have more uniform neighbor distances

#### When to Keep Rectangular
- **Simple urban grids** - Matches city street patterns
- **Simplicity** - Easier to implement and debug
- **GeoJSON compatibility** - Most mapping tools assume rectangular

#### Implementation Note
Hexagonal grids require more complex coordinate math but libraries like `h3` (Uber's hexagonal hierarchical spatial index) make it straightforward.

### References
- [Rectangular and Hexagonal Grids in Ecology](https://www.researchgate.net/publication/222692566_Rectangular_and_hexagonal_grids_used_for_observation_experiment_and_simulation_in_ecology) - Comparison study
- [Hexagonal vs Rectangular Coverage](https://www.researchgate.net/figure/Hexagonal-cell-layout-a-and-idealized-circular-coverage-areas-b-c_fig2_221912601) - Visual comparison

## Cost Optimization Strategies

### 1. Caching Results
While Serper-specific guidance is limited, general API optimization applies:
- Cache place IDs and locations (you're already doing this with `preload_existing()`)
- Consider caching scout query results for 30 days
- Share cache across different scrape runs

### 2. Rate Limiting Best Practices
```python
MAX_CONCURRENT_REQUESTS = 4  # Current setting - good for free/basic plan
```

[Serper documentation](https://serper.dev/) indicates:
- Free tier: 2,500 queries
- Rate limits: 5 QPS (queries per second) for free/basic
- Your setting of 4 concurrent requests is optimal

### 3. Query Reduction Techniques
Already implemented in your code:
- ✅ Coverage tracking (skip already-covered cells)
- ✅ Sparse skipping (skip empty cells)
- ✅ Duplicate skipping (skip cells with all known hotels)
- ✅ Adaptive query count (2-8 queries based on scout results)

**Additional opportunity:**
- Extend sparse skip radius - if cell has 0 results, mark 8 surrounding cells as "likely sparse"

### 4. Smart Query Selection
Your current approach rotates through 16 search types and 13 modifiers. Consider:
- Track which query types yield best results per region
- Use machine learning to predict best queries for each cell type
- Skip low-performing query types in sparse areas

### References
- [Serper API Pricing](https://serper.dev/) - Cost and rate limits
- [Best SERP APIs Comparison 2026](https://www.scrapingdog.com/blog/best-serp-apis/) - Alternative providers
- [Optimizing Google Maps Geocoding at Scale](https://sanborn.com/blog/optimizing-google-maps-geocoding-api-at-scale-balancing-cost-and-performance/) - Caching strategies

## Deduplication Best Practices

### Current Implementation (Excellent)
Your 3-tier deduplication (lines 967-989) is textbook correct:
1. Google Place ID (primary, globally unique)
2. Location coordinates (secondary, ~11m precision)
3. Name (tertiary, fallback)

### Validation from Research
[Google Places extraction research](https://dev.to/domharvest/building-a-google-places-extraction-tool-that-actually-scales-1i1j) confirms:
> "Deduplication is not a cleanup step but part of the extraction loop"

You're doing this correctly with in-memory sets during scraping.

### Coverage Tracking Enhancement
Your coverage tracking (lines 633-682) is smart. Consider extending it:

```python
# Current: track at ~111m precision
coverage_key = (round(lat, 3), round(lng, 3))

# Enhancement: use spatial index for faster lookup
from rtree import index
coverage_index = index.Index()  # R-tree for O(log n) spatial queries

# Check coverage by radius
nearby = coverage_index.intersection((lat-0.01, lng-0.01, lat+0.01, lng+0.01))
```

### References
- [Building Scalable Google Places Extraction](https://dev.to/domharvest/building-a-google-places-extraction-tool-that-actually-scales-1i1j) - In-loop deduplication

## Comparison: Grid vs Quadtree vs Density-Scout

| Approach | Upfront Cost | Coverage | Total Cost (Palm Beach) | Complexity |
|----------|--------------|----------|-------------------------|------------|
| **Current Grid (2km)** | $0 | Medium | $2.75 | Low |
| **Quadtree** | $0 | High | $4.30 | Medium |
| **Density Scout** | $0.20 | Very High | $5.10 | High |
| **Improved Hybrid** | $0.20 | High | $3.50 | Low |

### Recommendation: **Improved Hybrid**
- Lowest effort (modify existing code)
- Moderate cost increase (27%)
- Significant coverage improvement (40-60%)
- Clear migration path to quadtree later

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 days)
1. ✅ Add sparse skip radius (mark surrounding cells)
2. ✅ Extend duplicate skip threshold (currently only if 100% overlap)
3. ✅ Implement smart pagination stop (stop if <5 new results per page)
4. ✅ Log density heatmap data for analysis

### Phase 2: Density Scouting (3-5 days)
1. Add `scout_phase()` method to run coarse 10km grid
2. Integrate sklearn DBSCAN for clustering
3. Generate density heatmap JSON/visualization
4. Modify `_generate_grid()` to use measured density
5. A/B test vs current approach on small metro

### Phase 3: Quadtree Migration (1-2 weeks)
1. Implement `_generate_quadtree_cells()` recursive method
2. Add quadtree depth/size constraints
3. Test on single metro area (e.g., Southwest FL)
4. Compare results vs grid approach
5. Roll out to all metros if successful

### Phase 4: Advanced Optimizations (ongoing)
1. Hexagonal grid for coastal metros
2. Query performance ML (predict best queries per cell)
3. Multi-resolution scraping (coarse first, then fine where needed)
4. Distributed scraping across multiple API keys

## Key References & Research

### Spatial Indexing & Algorithms
- [Spatial Indexing with Quadtrees](https://medium.com/@waleoyediran/spatial-indexing-with-quadtrees-b998ae49336) - Core quadtree concepts
- [Quadtree vs Geohash Comparison](https://medium.com/@namasricharan/geohash-vs-quadtree-choosing-the-right-spatial-index-for-location-services-0c957c4f8a1c) - When to use each
- [Damn Cool Algorithms: Quadtrees & Hilbert Curves](http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves) - Classic deep dive
- [Understanding Efficient Spatial Indexing](https://www.geeksforgeeks.org/dsa/understanding-efficient-spatial-indexing/) - General overview

### Density-Based Clustering
- [DBSCAN Clustering in ML](https://www.geeksforgeeks.org/machine-learning/dbscan-clustering-in-ml-density-based-clustering/) - Algorithm explanation
- [How Density-based Clustering Works (ArcGIS)](https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-statistics/how-density-based-clustering-works.htm) - Spatial statistics
- [ADBSCAN: Adaptive Density Clustering](https://arxiv.org/pdf/1809.06189) - Adaptive approach for variable density

### Google Maps Scraping
- [Building Google Places Extraction That Scales](https://dev.to/domharvest/building-a-google-places-extraction-tool-that-actually-scales-1i1j) - Best practices, deduplication
- [How SerpApi Reverse-Engineered Google Maps Pagination](https://serpapi.com/blog/how-we-reverse-engineered-google-maps-pagination/) - Pagination limits
- [Google Maps Scraping Complete Guide 2025](https://scrap.io/google-maps-scraping-complete-guide-business-data-leads-2025) - Comprehensive overview

### Grid Coverage Optimization
- [Rectangular vs Hexagonal Grids in Ecology](https://www.researchgate.net/publication/222692566_Rectangular_and_hexagonal_grids_used_for_observation_experiment_and_simulation_in_ecology) - Grid shape comparison
- [Hexagonal Cell Coverage](https://www.researchgate.net/figure/Hexagonal-cell-layout-a-and-idealized-circular-coverage-areas-b-c_fig2_221912601) - Visual comparison
- [Google Maps Zoom Levels](https://wiki.openstreetmap.org/wiki/Zoom_levels) - Zoom level scales and coverage

### API Optimization
- [Serper API Documentation](https://serper.dev/) - Pricing and rate limits
- [Best SERP APIs 2026](https://www.scrapingdog.com/blog/best-serp-apis/) - Alternative providers
- [Optimizing Google Maps Geocoding at Scale](https://sanborn.com/blog/optimizing-google-maps-geocoding-api-at-scale-balancing-cost-and-performance/) - Caching and batching

## Success Metrics

Track these to measure optimization impact:

### Coverage Metrics
- Hotels found per API call (current: ~1.3)
- Coverage gaps (areas with missed hotels)
- Duplicate rate (current: good with place ID dedup)

### Cost Metrics
- API calls per km² (varies by density)
- Cost per hotel found (current: ~$0.001)
- Wasted calls in sparse areas (target: <10%)

### Quality Metrics
- Chain filter accuracy (how many chains slip through)
- Non-hotel filter accuracy (restaurants, gas stations, etc.)
- Website availability (% of results with websites)

## Next Steps

1. **Immediate (This Week):**
   - Add detailed logging of cell density and query performance
   - Generate density heatmap for Palm Beach using existing data
   - Identify top 5 highest-density cells for manual validation

2. **Short-term (Next 2 Weeks):**
   - Implement density scouting phase
   - Test improved hybrid mode on single metro
   - A/B test vs current approach

3. **Long-term (Next Month):**
   - Migrate to quadtree if testing shows significant improvement
   - Consider hexagonal grid for coastal metros
   - Implement ML-based query selection

---

**Bottom Line:** Your grid approach works, but quadtree or density-scouting would be 2-3x more efficient for the same coverage. Start with improved hybrid (lowest effort, moderate gains), then migrate to quadtree if you need more efficiency.
