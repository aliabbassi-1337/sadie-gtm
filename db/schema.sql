-- Sadie GTM Database Schema
-- PostgreSQL / Aurora PostgreSQL
--
-- Usage: psql -h <host> -U <user> -d <db> -f schema.sql
--
-- DESIGN DECISIONS:
-- 1. Normalized structure: hotel_booking_engines is a separate table
--    - Only creates rows for hotels WITH booking engines (saves storage)
--    - Booking engine metadata (tier, domains) stored once in booking_engines
-- 2. INT status codes instead of TEXT for storage optimization
--    - hotels.status: 0=scraped, 1=detected, 2=enriching, 3=enriched, 4=scored, 5=live, 6=exported, 99=no_booking_engine
--    - hotels.location_status: 0=pending, 1=validated, 99=out_of_market
--    - jobs.status: 0=pending, 1=running, 2=completed, 3=failed, 4=retrying
--    - pipeline_runs.status: 0=pending, 1=running, 2=completed, 3=failed
-- 3. No conflicts between scraper and detector:
--    - Scraper: INSERT INTO hotels
--    - Detector: UPDATE hotels.status + INSERT INTO hotel_booking_engines + UPDATE website_content_s3_path
-- 4. Website content stored in S3 (not DB):
--    - Stripped HTML truncated to ~50KB per hotel
--    - S3 path stored in hotels.website_content_s3_path
--    - Pattern: s3://sadie-gtm-data/website-content/{state}/{city}/{hotel_id}.txt
-- 5. PostGIS for location data:
--    - GEOGRAPHY(POINT, 4326) for accurate distance calculations
--    - GiST indexes for fast proximity queries
--    - Text fields (city, state) kept for display and grouping

-- Enable PostGIS extension (must be created before schema to make geography type available)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create schema
CREATE SCHEMA IF NOT EXISTS sadie_gtm;
SET search_path TO sadie_gtm, public;

-- ============================================================================
-- STATUS ENUM (using INT for storage optimization)
-- ============================================================================
-- hotels.status:
--   0 = scraped
--   1 = detected
--   2 = enriching
--   3 = enriched
--   4 = scored
--   5 = live
--   6 = exported
--   99 = no_booking_engine (dead end)

-- ============================================================================
-- HOTELS: Core hotel data from scraping
-- ============================================================================
CREATE TABLE IF NOT EXISTS hotels (
    id SERIAL PRIMARY KEY,

    -- Core identifiers
    name TEXT NOT NULL,
    google_place_id TEXT,  -- Google Place ID for deduplication
    website TEXT,

    -- Contact info
    phone_google TEXT,
    phone_website TEXT,
    email TEXT,

    -- Location
    location GEOGRAPHY(POINT, 4326),
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT DEFAULT 'USA',

    -- Ratings
    rating DOUBLE PRECISION,
    review_count INTEGER,

    -- Pipeline status
    status SMALLINT DEFAULT 0,  -- See STATUS ENUM above

    -- Metadata
    source TEXT,  -- serper, grid, zipcode
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotels_location ON hotels USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_hotels_city_state ON hotels(city, state);
CREATE INDEX IF NOT EXISTS idx_hotels_website ON hotels(website);
CREATE INDEX IF NOT EXISTS idx_hotels_status ON hotels(status);
CREATE INDEX IF NOT EXISTS idx_hotels_google_place_id ON hotels(google_place_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotels_name_website_unique ON hotels(name, COALESCE(website, ''));
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotels_name_city_unique ON hotels(lower(name), lower(COALESCE(city, '')));
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotels_google_place_id_unique ON hotels(google_place_id) WHERE google_place_id IS NOT NULL;

-- ============================================================================
-- BOOKING_ENGINES: Reference table for known engines (must be created before hotel_booking_engines)
-- ============================================================================
CREATE TABLE IF NOT EXISTS booking_engines (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    domains TEXT[],  -- Array of domains
    tier INTEGER DEFAULT 1,  -- 1 = known, 2 = unknown
    is_active BOOLEAN DEFAULT TRUE
);

-- ============================================================================
-- EXISTING CUSTOMERS: Existing Sadie customers (must be created before hotel_customer_proximity)
-- ============================================================================
CREATE TABLE IF NOT EXISTS existing_customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    sadie_hotel_id TEXT,

    -- Location
    location GEOGRAPHY(POINT, 4326),
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT DEFAULT 'USA',

    -- Status
    status TEXT DEFAULT 'active',  -- active, churned, trial
    go_live_date DATE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_existing_customers_location ON existing_customers USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_existing_customers_city_state ON existing_customers(city, state);

-- ============================================================================
-- HOTEL_BOOKING_ENGINES: Detected booking engines (only for hotels WITH engines)
-- ============================================================================
CREATE TABLE IF NOT EXISTS hotel_booking_engines (
    hotel_id INTEGER PRIMARY KEY REFERENCES hotels(id) ON DELETE CASCADE,
    booking_engine_id INTEGER REFERENCES booking_engines(id),

    -- Detection metadata
    booking_url TEXT,
    detection_method TEXT,  -- playwright, regex, manual

    -- Timestamps
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_engine_id ON hotel_booking_engines(booking_engine_id);

-- ============================================================================
-- ENRICHMENT TABLES (separate for each enrichment type)
-- ============================================================================

-- Room count (Groq/Google enrichment job)
CREATE TABLE IF NOT EXISTS hotel_room_count (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL UNIQUE REFERENCES hotels(id) ON DELETE CASCADE,
    room_count INTEGER NOT NULL,
    source TEXT,  -- groq, google, manual
    confidence DECIMAL(3,2),  -- 0.00 to 1.00
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotel_room_count_hotel ON hotel_room_count(hotel_id);

-- Nearest existing Sadie customer
CREATE TABLE IF NOT EXISTS hotel_customer_proximity (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL UNIQUE REFERENCES hotels(id) ON DELETE CASCADE,
    existing_customer_id INTEGER NOT NULL REFERENCES existing_customers(id),
    distance_km DECIMAL(6,1) NOT NULL,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotel_customer_proximity_hotel ON hotel_customer_proximity(hotel_id);
CREATE INDEX IF NOT EXISTS idx_hotel_customer_proximity_customer ON hotel_customer_proximity(existing_customer_id);
CREATE INDEX IF NOT EXISTS idx_hotel_customer_proximity_distance ON hotel_customer_proximity(distance_km);

-- ============================================================================
-- JOB STATUS ENUM (using INT for storage optimization)
-- ============================================================================
-- 0 = pending
-- 1 = running
-- 2 = completed
-- 3 = failed
-- 4 = retrying

-- ============================================================================
-- JOBS: Track individual job executions (queue workers)
-- ============================================================================
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,

    -- Job identification
    job_type TEXT NOT NULL,  -- scrape, detect, enrich-room, enrich-proximity, enrich-research, score, export
    hotel_id INTEGER REFERENCES hotels(id),

    -- For export jobs
    city TEXT,
    state TEXT,
    export_type TEXT,  -- city, state

    -- Queue info
    queue_name TEXT,  -- scrape-queue, detect-queue, etc.
    message_id TEXT,  -- SQS message ID
    attempt_number INTEGER DEFAULT 1,

    -- Execution
    worker_id TEXT,  -- hostname or container ID
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    duration_ms INTEGER,

    -- Status
    status SMALLINT DEFAULT 1,  -- See JOB STATUS ENUM above
    error_message TEXT,
    error_stack TEXT,

    -- Metadata
    input_params JSONB,  -- Job input parameters
    output_data JSONB,  -- Job output/results

    -- S3 log reference
    s3_log_path TEXT  -- s3://bucket/logs/jobs/{job_type}/{date}/{job_id}.log
);

CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_hotel_id ON jobs(hotel_id);
CREATE INDEX IF NOT EXISTS idx_jobs_started_at ON jobs(started_at);
CREATE INDEX IF NOT EXISTS idx_jobs_city_state ON jobs(city, state);

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Update timestamp trigger
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER hotels_updated_at
    BEFORE UPDATE ON hotels
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER hotel_booking_engines_updated_at
    BEFORE UPDATE ON hotel_booking_engines
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- DETECTION_ERRORS: Track detection failures for debugging
-- ============================================================================
CREATE TABLE IF NOT EXISTS detection_errors (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    error_type TEXT NOT NULL,  -- precheck_failed, timeout, location_mismatch, junk_domain, etc.
    error_message TEXT,        -- Full error details
    detected_location TEXT,    -- What location was detected (for location_mismatch)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_detection_errors_hotel_id ON detection_errors(hotel_id);
CREATE INDEX IF NOT EXISTS idx_detection_errors_error_type ON detection_errors(error_type);
CREATE INDEX IF NOT EXISTS idx_detection_errors_created_at ON detection_errors(created_at);

-- ============================================================================
-- SCRAPE_TARGET_CITIES: Cities to scrape for hotels
-- ============================================================================
CREATE TABLE IF NOT EXISTS scrape_target_cities (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    radius_km DOUBLE PRECISION DEFAULT 12.0,  -- Suggested scrape radius (from Nominatim importance)
    display_name TEXT,
    source TEXT DEFAULT 'nominatim',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(name, state)
);

CREATE INDEX IF NOT EXISTS idx_scrape_target_cities_state ON scrape_target_cities(state);

-- ============================================================================
-- SCRAPE_REGIONS: Polygon regions for targeted scraping
-- ============================================================================
-- Instead of scraping entire states with uniform grids, define specific
-- polygon regions around cities/tourist areas. Each region can have its
-- own cell size, optimizing cost by using smaller cells in dense areas.
CREATE TABLE IF NOT EXISTS scrape_regions (
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

CREATE INDEX IF NOT EXISTS idx_scrape_regions_state ON scrape_regions(state);
CREATE INDEX IF NOT EXISTS idx_scrape_regions_polygon ON scrape_regions USING GIST(polygon);
