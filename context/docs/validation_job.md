# Location Validation Job

## Purpose
Validate hotel coordinates and catch obvious errors.

## What to Validate

1. **Missing coordinates**:
   ```sql
   SELECT id, name, city, state, address
   FROM hotels
   WHERE location IS NULL;
   ```
   - Needs geocoding using address or city/state

2. **Coordinates outside USA** (basic sanity check):
   ```sql
   SELECT id, name, city, state,
          ST_Y(location::geometry) as lat,
          ST_X(location::geometry) as lng
   FROM hotels
   WHERE ST_Y(location::geometry) NOT BETWEEN 24 AND 50  -- Not in USA latitude
      OR ST_X(location::geometry) NOT BETWEEN -125 AND -65;  -- Not in USA longitude
   ```

3. **City/state text normalization**:
   - Ensure consistent format: "Miami" not "miami" or "MIAMI"
   - Ensure state abbreviations: "FL" not "Florida"

## What NOT to Validate

❌ **Distance from city center** - Cities vary wildly in size (Manhattan vs LA)
❌ **City boundaries** - Fuzzy and overlapping, not worth the complexity

## Implementation

Run as a workflow:
```bash
uv run main.py validation
```

## When to Run
- After bulk scraping (data quality check)
- Manually when CEO reports bad location data
- Weekly scheduled job (low priority)
