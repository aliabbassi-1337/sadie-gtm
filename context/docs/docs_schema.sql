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
-- 3. Separate enrichment tables for better normalization:
--    - hotel_room_count (1:1)
--    - hotel_research (1:many with research_type)
--    - hotel_customer_proximity (1:1)
--    - hotel_score (1:1)
-- 4. No conflicts between scraper and detector:
--    - Scraper: INSERT INTO hotels
--    - Detector: UPDATE hotels.status + INSERT INTO hotel_booking_engines + UPDATE website_content_s3_path
-- 5. Website content stored in S3 (not DB):
--    - Stripped HTML truncated to ~50KB per hotel
--    - S3 path stored in hotels.website_content_s3_path
--    - Pattern: s3://sadie-gtm-data/website-content/{state}/{city}/{hotel_id}.txt

-- Create schema
CREATE SCHEMA IF NOT EXISTS sadie_gtm;
SET search_path TO sadie_gtm;

-- ============================================================================
-- STATUS ENUM (using INT for storage optimization)
-- ============================================================================
-- 0 = scraped
-- 1 = detected
-- 2 = enriching
-- 3 = enriched
-- 4 = scored
-- 5 = live
-- 6 = exported
-- 99 = no_booking_engine (dead end)

-- ============================================================================
-- HOTELS: Core hotel data from scraping
-- ============================================================================
CREATE TABLE IF NOT EXISTS hotels (
    id SERIAL PRIMARY KEY,

    -- Core identifiers
    name TEXT NOT NULL,
    website TEXT,
    place_id TEXT,  -- Google Place ID

    -- Contact info
    phone_google TEXT,
    phone_website TEXT,
    email TEXT,

    -- Location
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    city TEXT,
    state TEXT,
    country TEXT DEFAULT 'USA',

    -- Ratings
    rating DOUBLE PRECISION,
    review_count INTEGER,

    -- Pipeline status
    status SMALLINT DEFAULT 0,  -- See STATUS ENUM above
    error TEXT,

    -- Website content (stripped HTML, saved during detection)
    website_content_s3_path TEXT,  -- s3://bucket/website-content/{state}/{city}/{hotel_id}.txt (truncated to ~50KB before save)

    -- Metadata
    source TEXT,  -- osm, serper, grid, zipcode
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Deduplication
    UNIQUE(name, COALESCE(website, ''))
);

CREATE INDEX IF NOT EXISTS idx_hotels_city_state ON hotels(city, state);
CREATE INDEX IF NOT EXISTS idx_hotels_state ON hotels(state);
CREATE INDEX IF NOT EXISTS idx_hotels_website ON hotels(website);
CREATE INDEX IF NOT EXISTS idx_hotels_status ON hotels(status);
CREATE INDEX IF NOT EXISTS idx_hotels_website_content_s3_path ON hotels(website_content_s3_path) WHERE website_content_s3_path IS NOT NULL;

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

-- AI agent research output (1:many, multiple research types per hotel)
-- research_type:
--   1 = website_analysis  → {"title": "...", "description": "...", "amenities": [...]}
--   2 = company_news      → {"articles": [...], "funding": "...", "expansion": "..."}
--   3 = social_presence   → {"instagram": "...", "facebook_rating": 4.5}
--   4 = competitor_intel  → {"nearby_hotels": [...], "market_position": "..."}
--   5 = pain_points       → {"challenges": [...], "opportunities": [...]}
CREATE TABLE IF NOT EXISTS hotel_research (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    research_type SMALLINT NOT NULL,
    content JSONB NOT NULL,
    researched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (hotel_id, research_type)
);

CREATE INDEX IF NOT EXISTS idx_hotel_research_hotel ON hotel_research(hotel_id);
CREATE INDEX IF NOT EXISTS idx_hotel_research_type ON hotel_research(hotel_id, research_type);
CREATE INDEX IF NOT EXISTS idx_hotel_research_content ON hotel_research USING GIN(content);

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

-- Scoring and prioritization
CREATE TABLE IF NOT EXISTS hotel_score (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL UNIQUE REFERENCES hotels(id) ON DELETE CASCADE,

    -- Individual scores (0-100)
    overall SMALLINT CHECK (overall BETWEEN 0 AND 100),
    size_score SMALLINT CHECK (size_score BETWEEN 0 AND 100),
    tier_score SMALLINT CHECK (tier_score BETWEEN 0 AND 100),
    proximity_score SMALLINT CHECK (proximity_score BETWEEN 0 AND 100),
    engagement_score SMALLINT CHECK (engagement_score BETWEEN 0 AND 100),

    score_details JSONB,  -- Breakdown of scoring factors
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotel_score_hotel ON hotel_score(hotel_id);
CREATE INDEX IF NOT EXISTS idx_hotel_score_overall ON hotel_score(overall DESC);

-- ============================================================================
-- REFERENCE TABLES
-- ============================================================================

-- Existing Sadie customers (for proximity scoring)
CREATE TABLE IF NOT EXISTS existing_customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    sadie_hotel_id TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    status TEXT DEFAULT 'active',  -- active, churned, trial
    go_live_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_existing_customers_state ON existing_customers(state);

-- Existing customer location (geocoded)
CREATE TABLE IF NOT EXISTS existing_customer_location (
    id SERIAL PRIMARY KEY,
    existing_customer_id INTEGER NOT NULL UNIQUE REFERENCES existing_customers(id) ON DELETE CASCADE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    source TEXT,  -- serper, google, manual
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_existing_customer_location_customer ON existing_customer_location(existing_customer_id);

-- ============================================================================
-- BOOKING_ENGINES: Reference table for known engines
-- ============================================================================
CREATE TABLE IF NOT EXISTS booking_engines (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    domains TEXT[],  -- Array of domains
    tier INTEGER DEFAULT 1,  -- 1 = tier 1 (will be enriched), 2 = tier 2 (skip enrichment)
    category TEXT,  -- pms, channel_manager, crs, ota_widget
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert known engines
INSERT INTO booking_engines (name, domains, tier, category) VALUES
    ('Cloudbeds', ARRAY['cloudbeds.com'], 1, 'pms'),
    ('SynXis / TravelClick', ARRAY['synxis.com', 'travelclick.com'], 1, 'crs'),
    ('SiteMinder', ARRAY['siteminder.com', 'thebookingbutton.com'], 1, 'channel_manager'),
    ('Mews', ARRAY['mews.com', 'mews.li'], 1, 'pms'),
    ('RMS Cloud', ARRAY['rmscloud.com'], 1, 'pms'),
    ('ResNexus', ARRAY['resnexus.com'], 1, 'pms'),
    ('ThinkReservations', ARRAY['thinkreservations.com'], 1, 'pms'),
    ('InnRoad', ARRAY['innroad.com'], 1, 'pms'),
    ('WebRez', ARRAY['webrez.com', 'webrezpro.com'], 1, 'pms'),
    ('Guesty', ARRAY['guesty.com'], 1, 'pms'),
    ('Lodgify', ARRAY['lodgify.com'], 1, 'pms'),
    ('Little Hotelier', ARRAY['littlehotelier.com'], 1, 'pms'),
    ('eviivo', ARRAY['eviivo.com'], 1, 'pms'),
    ('Beds24', ARRAY['beds24.com'], 1, 'channel_manager'),
    ('Hostaway', ARRAY['hostaway.com'], 1, 'pms'),
    ('RoomRaccoon', ARRAY['roomraccoon.com'], 1, 'pms'),
    ('Booking.com Widget', ARRAY['booking.com'], 2, 'ota_widget'),
    ('Expedia Widget', ARRAY['expedia.com'], 2, 'ota_widget'),
    ('TripAdvisor Widget', ARRAY['tripadvisor.com'], 2, 'ota_widget'),
    ('Unknown', ARRAY[], 2, 'unknown')
ON CONFLICT (name) DO NOTHING;

-- ============================================================================
-- VIEWS: Useful aggregations
-- ============================================================================

-- Full hotel view with all enrichments
CREATE OR REPLACE VIEW v_hotels AS
SELECT
    h.id,
    h.name,
    h.website,
    h.place_id,
    h.address,
    h.city,
    h.state,
    h.country,
    h.latitude,
    h.longitude,
    h.phone_google,
    h.phone_website,
    h.email,
    h.rating,
    h.review_count,
    h.status,
    h.source,
    h.website_content_s3_path,
    -- Booking engine
    be.name AS booking_engine,
    be.tier,
    be.category AS booking_engine_category,
    hbe.booking_url,
    hbe.detection_method,
    hbe.detected_at,
    -- Room count
    hrc.room_count,
    hrc.source AS room_count_source,
    hrc.confidence AS room_count_confidence,
    -- Customer proximity
    ec.name AS nearest_customer,
    hcp.distance_km AS customer_distance_km,
    -- Score
    hs.overall AS score,
    hs.size_score,
    hs.tier_score,
    hs.proximity_score,
    hs.engagement_score,
    -- Timestamps
    h.created_at,
    h.updated_at
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN existing_customers ec ON hcp.existing_customer_id = ec.id
LEFT JOIN hotel_score hs ON h.id = hs.hotel_id;

-- Hotels by city with stats
CREATE OR REPLACE VIEW hotels_by_city AS
SELECT
    h.city,
    h.state,
    COUNT(*) as total_hotels,
    COUNT(hbe.hotel_id) as with_booking_engine,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) as tier1,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) as tier2,
    COUNT(hrc.room_count) as with_room_count,
    COUNT(h.email) as with_email
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
GROUP BY h.city, h.state
ORDER BY total_hotels DESC;

-- State stats
CREATE OR REPLACE VIEW v_state_stats AS
SELECT
    h.state,
    COUNT(*) AS total_hotels,
    COUNT(hbe.hotel_id) AS detected,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier1,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier2,
    COUNT(hrc.hotel_id) AS with_room_count,
    COUNT(hcp.hotel_id) AS with_customer_proximity
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
GROUP BY h.state
ORDER BY total_hotels DESC;

-- Booking engine distribution
CREATE OR REPLACE VIEW engine_distribution AS
SELECT
    be.name as booking_engine,
    be.tier,
    be.category,
    COUNT(hbe.hotel_id) as count,
    ROUND(COUNT(hbe.hotel_id) * 100.0 / NULLIF(SUM(COUNT(hbe.hotel_id)) OVER (), 0), 2) as percentage
FROM booking_engines be
LEFT JOIN hotel_booking_engines hbe ON be.id = hbe.booking_engine_id
GROUP BY be.id, be.name, be.tier, be.category
ORDER BY count DESC;

-- Pipeline status summary (INT status)
CREATE OR REPLACE VIEW pipeline_status AS
SELECT
    h.status,
    CASE h.status
        WHEN 0 THEN 'scraped'
        WHEN 1 THEN 'detected'
        WHEN 2 THEN 'enriching'
        WHEN 3 THEN 'enriched'
        WHEN 4 THEN 'scored'
        WHEN 5 THEN 'live'
        WHEN 6 THEN 'exported'
        WHEN 99 THEN 'no_booking_engine'
        ELSE 'unknown'
    END as status_name,
    COUNT(*) as count,
    ROUND(AVG(hs.overall), 2) as avg_score
FROM hotels h
LEFT JOIN hotel_score hs ON h.id = hs.hotel_id
GROUP BY h.status
ORDER BY h.status;

-- Hotels needing room count enrichment (tier 1, detected, no room count yet)
CREATE OR REPLACE VIEW v_needs_room_count AS
SELECT
    h.id,
    h.name,
    h.website,
    be.name AS booking_engine
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.status = 1  -- detected
  AND be.tier = 1
  AND hrc.id IS NULL;

-- Hotels needing scoring (enriched but not scored)
CREATE OR REPLACE VIEW v_needs_scoring AS
SELECT
    h.id,
    h.name,
    h.website
FROM hotels h
LEFT JOIN hotel_score hs ON h.id = hs.hotel_id
WHERE h.status = 3  -- enriched
  AND hs.id IS NULL;

-- Hotels ready for launch (tier 1 only, scored and ready to go live)
CREATE OR REPLACE VIEW hotels_ready_for_launch AS
SELECT
    h.*,
    be.name as booking_engine_name,
    be.tier as booking_engine_tier,
    hrc.room_count,
    hs.overall as score,
    CASE
        WHEN hs.overall >= 80 THEN 'high_priority'
        WHEN hs.overall >= 60 THEN 'medium_priority'
        ELSE 'low_priority'
    END as priority
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN hotel_score hs ON h.id = hs.hotel_id
WHERE h.status = 4  -- scored
  AND be.tier = 1
  AND hs.overall IS NOT NULL
ORDER BY hs.overall DESC;

-- Booking engine stats
CREATE OR REPLACE VIEW v_booking_engine_stats AS
SELECT
    be.name AS booking_engine,
    be.tier,
    be.category,
    COUNT(hbe.hotel_id) AS hotel_count
FROM booking_engines be
LEFT JOIN hotel_booking_engines hbe ON be.id = hbe.booking_engine_id
GROUP BY be.id, be.name, be.tier, be.category
ORDER BY hotel_count DESC;

-- ============================================================================
-- FUNCTIONS: Useful utilities
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

CREATE TRIGGER hotel_score_updated_at
    BEFORE UPDATE ON hotel_score
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
