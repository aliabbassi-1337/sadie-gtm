-- Scrape target cities - cities to scrape for hotels
CREATE TABLE IF NOT EXISTS scrape_target_cities (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    radius_km DOUBLE PRECISION DEFAULT 12.0,
    display_name TEXT,
    source TEXT DEFAULT 'nominatim',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(name, state)
);

CREATE INDEX IF NOT EXISTS idx_scrape_target_cities_state ON scrape_target_cities(state);
