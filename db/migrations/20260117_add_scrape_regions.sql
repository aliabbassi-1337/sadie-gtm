-- Scrape regions - GeoJSON polygons defining areas to scrape
-- Enables polygon-based scraping instead of bounding box + cell grid

-- Ensure search_path includes public for PostGIS types
SET search_path TO sadie_gtm, public;

CREATE TABLE IF NOT EXISTS sadie_gtm.scrape_regions (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,                    -- e.g., "Miami Metro", "Orlando Theme Parks"
    state TEXT NOT NULL,                   -- State code (e.g., "FL")
    region_type TEXT DEFAULT 'city',       -- city, corridor, custom, boundary
    polygon GEOGRAPHY,                     -- GeoJSON Polygon or MultiPolygon as PostGIS geography
    center_lat DOUBLE PRECISION,           -- Center point for reference
    center_lng DOUBLE PRECISION,
    radius_km DOUBLE PRECISION,            -- If generated from city buffer
    cell_size_km DOUBLE PRECISION DEFAULT 2.0,  -- Recommended cell size for this region
    priority INTEGER DEFAULT 0,            -- Higher = scrape first
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(name, state)
);

CREATE INDEX IF NOT EXISTS idx_scrape_regions_state ON sadie_gtm.scrape_regions(state);
CREATE INDEX IF NOT EXISTS idx_scrape_regions_polygon ON sadie_gtm.scrape_regions USING GIST(polygon);

COMMENT ON TABLE sadie_gtm.scrape_regions IS 'Polygon regions for targeted scraping - only scrape within these areas';
