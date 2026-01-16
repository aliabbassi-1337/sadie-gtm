-- Scrape regions - GeoJSON polygons defining areas to scrape
-- Enables polygon-based scraping instead of bounding box + cell grid

CREATE TABLE IF NOT EXISTS scrape_regions (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,                    -- e.g., "Miami Metro", "Orlando Theme Parks"
    state TEXT NOT NULL,                   -- State code (e.g., "FL")
    region_type TEXT DEFAULT 'city',       -- city, corridor, custom
    polygon GEOGRAPHY(POLYGON, 4326),      -- GeoJSON polygon as PostGIS geography
    center_lat DOUBLE PRECISION,           -- Center point for reference
    center_lng DOUBLE PRECISION,
    radius_km DOUBLE PRECISION,            -- If generated from city buffer
    cell_size_km DOUBLE PRECISION DEFAULT 2.0,  -- Recommended cell size for this region
    priority INTEGER DEFAULT 0,            -- Higher = scrape first
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(name, state)
);

CREATE INDEX IF NOT EXISTS idx_scrape_regions_state ON scrape_regions(state);
CREATE INDEX IF NOT EXISTS idx_scrape_regions_polygon ON scrape_regions USING GIST(polygon);

-- Also add to schema
COMMENT ON TABLE scrape_regions IS 'Polygon regions for targeted scraping - only scrape within these areas';
