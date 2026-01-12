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
--    - jobs.status: 0=pending, 1=running, 2=completed, 3=failed, 4=retrying
--    - pipeline_runs.status: 0=pending, 1=running, 2=completed, 3=failed
-- 3. No conflicts between scraper and detector:
--    - Scraper: INSERT INTO hotels
--    - Detector: UPDATE hotels.status + INSERT INTO hotel_booking_engines + UPDATE website_content_s3_path
-- 4. Website content stored in S3 (not DB):
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
CREATE INDEX IF NOT EXISTS idx_hotels_website ON hotels(website);
CREATE INDEX IF NOT EXISTS idx_hotels_status ON hotels(status);
CREATE INDEX IF NOT EXISTS idx_hotels_score ON hotels(score);
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
-- AUDIT_LOG: Track all status transitions for hotels
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,

    -- What changed
    entity_type TEXT NOT NULL,  -- hotel
    entity_id INTEGER NOT NULL,

    -- State transition (INT status)
    old_status SMALLINT,
    new_status SMALLINT NOT NULL,

    -- Context
    changed_by TEXT,  -- worker_id, user_id, or 'system'
    change_reason TEXT,  -- e.g., 'enrichment_completed', 'launcher_approved', 'manual_override'
    job_id INTEGER REFERENCES jobs(id),

    -- Additional changes
    fields_changed JSONB,  -- e.g., {"score": 85, "room_count": 45}

    -- Timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_new_status ON audit_log(new_status);

-- ============================================================================
-- PIPELINE RUN STATUS ENUM (using INT for storage optimization)
-- ============================================================================
-- 0 = pending
-- 1 = running
-- 2 = completed
-- 3 = failed

-- ============================================================================
-- PIPELINE_RUNS: Track pipeline executions
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,

    run_type TEXT NOT NULL,  -- scrape, process, score, launch, export
    state TEXT,
    city TEXT,

    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,

    -- Metrics
    hotels_processed INTEGER DEFAULT 0,
    leads_found INTEGER DEFAULT 0,
    jobs_created INTEGER DEFAULT 0,
    jobs_completed INTEGER DEFAULT 0,
    jobs_failed INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,

    status SMALLINT DEFAULT 1,  -- See PIPELINE RUN STATUS ENUM above
    error_message TEXT,

    -- S3 log reference
    s3_log_path TEXT  -- s3://bucket/logs/runs/{run_type}/{date}/{run_id}.log
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_type ON pipeline_runs(run_type);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at);

-- ============================================================================
-- BOOKING_ENGINES: Reference table for known engines
-- ============================================================================
CREATE TABLE IF NOT EXISTS booking_engines (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    domains TEXT[],  -- Array of domains
    tier INTEGER DEFAULT 1,  -- 1 = known, 2 = unknown
    is_active BOOLEAN DEFAULT TRUE
);

-- Insert known engines
INSERT INTO booking_engines (name, domains, tier) VALUES
    ('Cloudbeds', ARRAY['cloudbeds.com'], 1),
    ('SynXis / TravelClick', ARRAY['synxis.com', 'travelclick.com'], 1),
    ('SiteMinder', ARRAY['siteminder.com', 'thebookingbutton.com'], 1),
    ('Mews', ARRAY['mews.com', 'mews.li'], 1),
    ('RMS Cloud', ARRAY['rmscloud.com'], 1),
    ('ResNexus', ARRAY['resnexus.com'], 1),
    ('ThinkReservations', ARRAY['thinkreservations.com'], 1),
    ('InnRoad', ARRAY['innroad.com'], 1),
    ('WebRez', ARRAY['webrez.com', 'webrezpro.com'], 1),
    ('Guesty', ARRAY['guesty.com'], 1),
    ('Lodgify', ARRAY['lodgify.com'], 1),
    ('Little Hotelier', ARRAY['littlehotelier.com'], 1),
    ('eviivo', ARRAY['eviivo.com'], 1),
    ('Beds24', ARRAY['beds24.com'], 1),
    ('Hostaway', ARRAY['hostaway.com'], 1)
ON CONFLICT (name) DO NOTHING;

-- ============================================================================
-- VIEWS: Useful aggregations
-- ============================================================================

-- Hotels by city with stats
CREATE OR REPLACE VIEW hotels_by_city AS
SELECT
    h.city,
    h.state,
    COUNT(*) as total_hotels,
    COUNT(hbe.hotel_id) as with_booking_engine,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) as tier1,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) as tier2,
    COUNT(h.room_count) as with_room_count,
    COUNT(h.email) as with_email
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
GROUP BY h.city, h.state
ORDER BY total_hotels DESC;

-- Booking engine distribution
CREATE OR REPLACE VIEW engine_distribution AS
SELECT
    be.name as booking_engine,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM hotel_booking_engines hbe
JOIN booking_engines be ON hbe.booking_engine_id = be.id
GROUP BY be.name
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
    ROUND(AVG(h.score), 2) as avg_score
FROM hotels h
GROUP BY h.status
ORDER BY h.status;

-- Job performance metrics
CREATE OR REPLACE VIEW job_metrics AS
SELECT
    job_type,
    DATE(started_at) as date,
    COUNT(*) as total_jobs,
    COUNT(CASE WHEN status = 2 THEN 1 END) as completed,  -- 2 = completed
    COUNT(CASE WHEN status = 3 THEN 1 END) as failed,     -- 3 = failed
    ROUND(AVG(duration_ms)::numeric, 2) as avg_duration_ms,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms)::numeric, 2) as median_duration_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::numeric, 2) as p95_duration_ms
FROM jobs
WHERE completed_at IS NOT NULL
GROUP BY job_type, DATE(started_at)
ORDER BY date DESC, job_type;

-- Full hotel view with all enrichments
CREATE OR REPLACE VIEW v_hotels AS
SELECT
    h.id,
    h.name,
    h.website,
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

-- Export queue status
CREATE OR REPLACE VIEW export_status AS
SELECT
    state,
    city,
    export_type,
    COUNT(*) as pending_exports,
    MAX(j.started_at) as last_attempt
FROM jobs j
WHERE job_type = 'export'
  AND status IN (1, 4)  -- 1 = running, 4 = retrying
GROUP BY state, city, export_type
ORDER BY state, city;

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

-- Auto-create audit log entries on status changes
CREATE OR REPLACE FUNCTION create_audit_log_on_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status) THEN
        INSERT INTO audit_log (
            entity_type,
            entity_id,
            old_status,
            new_status,
            changed_by,
            change_reason,
            fields_changed
        ) VALUES (
            'hotel',
            NEW.id,
            OLD.status,
            NEW.status,
            'system',
            'status_transition',
            jsonb_build_object(
                'score', NEW.score,
                'room_count', NEW.room_count
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_hotels_status_change
    AFTER UPDATE ON hotels
    FOR EACH ROW
    EXECUTE FUNCTION create_audit_log_on_status_change();

-- ============================================================================
-- LOGGING STRATEGY
-- ============================================================================
--
-- HYBRID APPROACH: Database + S3
--
-- DATABASE LOGS (PostgreSQL):
-- ✓ State transitions & audit trail (audit_log table)
-- ✓ Job metadata & metrics (jobs table)
-- ✓ Pipeline run summaries (pipeline_runs table)
-- ✓ Queryable, structured data for dashboards and monitoring
-- ✓ Fast queries for "what's the status of hotel X?"
-- ✓ Retention: 90 days (then archive to S3)
--
-- S3 LOGS (Object Storage):
-- ✓ Application logs (stdout/stderr from workers)
-- ✓ Error stack traces and debug output
-- ✓ API responses (Claude, Groq, Serper raw outputs)
-- ✓ Website content (stripped HTML, truncated to ~50KB)
-- ✓ Retention: 1 year
--
-- S3 BUCKET STRUCTURE:
-- s3://sadie-gtm-data/
--   ├── website-content/
--   │   └── {state}/{city}/{hotel_id}.txt  -- Stripped HTML, truncated to ~50KB
--   ├── workers/
--   │   ├── scrape/{date}/{worker_id}/{timestamp}.log
--   │   ├── detect/{date}/{worker_id}/{timestamp}.log
--   │   ├── enrich-room/{date}/{worker_id}/{timestamp}.log
--   │   ├── enrich-proximity/{date}/{worker_id}/{timestamp}.log
--   │   ├── enrich-research/{date}/{worker_id}/{timestamp}.log
--   │   ├── score/{date}/{worker_id}/{timestamp}.log
--   │   └── export/{date}/{worker_id}/{timestamp}.log
--   ├── jobs/
--   │   ├── scrape/{date}/{job_id}.log
--   │   ├── detect/{date}/{job_id}.log
--   │   ├── enrich-room/{date}/{job_id}.log
--   │   ├── enrich-proximity/{date}/{job_id}.log
--   │   ├── enrich-research/{date}/{job_id}.log
--   │   ├── score/{date}/{job_id}.log
--   │   └── export/{date}/{job_id}.log
--   ├── runs/
--   │   ├── scrape/{date}/{run_id}.log
--   │   ├── process/{date}/{run_id}.log
--   │   ├── score/{date}/{run_id}.log
--   │   ├── launch/{date}/{run_id}.log
--   │   └── export/{date}/{run_id}.log
--   └── api-responses/
--       ├── claude/{date}/{hotel_id}.json
--       ├── groq/{date}/{hotel_id}.json
--       └── serper/{date}/{city}_{state}.json
--
-- WORKFLOW:
-- 1. Worker starts job → INSERT into jobs table with status='running'
-- 2. Worker writes logs to S3 (streaming or at completion)
-- 3. Worker updates job with s3_log_path
-- 4. Worker completes → UPDATE jobs set status='completed', completed_at, duration_ms
-- 5. On status change → audit_log trigger automatically creates audit entry
-- 6. On error → UPDATE jobs set status='failed', error_message, error_stack
--
-- QUERIES:
-- - "Show me all failed jobs today": SELECT * FROM jobs WHERE status='failed' AND DATE(started_at) = CURRENT_DATE
-- - "What's the status of hotel 123?": SELECT * FROM audit_log WHERE entity_id=123 ORDER BY created_at DESC
-- - "Average enrichment time": SELECT AVG(duration_ms) FROM jobs WHERE job_type='enrich-room'
-- - "Get logs for job 456": SELECT s3_log_path FROM jobs WHERE id=456
--
