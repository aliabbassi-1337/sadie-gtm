-- Scrape target cities - cached coordinates for city scraping workflow
CREATE TABLE IF NOT EXISTS scrape_target_cities (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    population INTEGER,
    display_name TEXT,
    source TEXT DEFAULT 'nominatim',  -- 'nominatim', 'simplemaps', 'manual'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(name, state)
);

CREATE INDEX IF NOT EXISTS idx_scrape_target_cities_state ON scrape_target_cities(state);
