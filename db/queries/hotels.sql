-- name: get_hotel_by_id^
-- Get single hotel by ID with location coordinates
SELECT
    id,
    name,
    external_id,
    external_id_type,
    website,
    phone_google,
    phone_website,
    email,
    city,
    state,
    country,
    address,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude,
    rating,
    review_count,
    status,
    source,
    created_at,
    updated_at
FROM sadie_gtm.hotels
WHERE id = :hotel_id;

-- name: insert_hotel<!
-- Insert a new hotel and return the ID
INSERT INTO sadie_gtm.hotels (
    name,
    external_id,
    external_id_type,
    website,
    phone_google,
    phone_website,
    email,
    location,
    address,
    city,
    state,
    country,
    rating,
    review_count,
    status,
    source
) VALUES (
    :name,
    :external_id,
    :external_id_type,
    :website,
    :phone_google,
    :phone_website,
    :email,
    ST_Point(:longitude, :latitude)::geography,
    :address,
    :city,
    :state,
    :country,
    :rating,
    :review_count,
    :status,
    :source
)
ON CONFLICT (name, COALESCE(website, ''))
DO UPDATE SET
    external_id = COALESCE(EXCLUDED.external_id, hotels.external_id),
    external_id_type = COALESCE(EXCLUDED.external_id_type, hotels.external_id_type),
    phone_google = EXCLUDED.phone_google,
    phone_website = EXCLUDED.phone_website,
    email = EXCLUDED.email,
    location = EXCLUDED.location,
    address = EXCLUDED.address,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    country = EXCLUDED.country,
    rating = EXCLUDED.rating,
    review_count = EXCLUDED.review_count,
    status = EXCLUDED.status,
    source = EXCLUDED.source,
    updated_at = CURRENT_TIMESTAMP
RETURNING id;

-- name: delete_hotel!
-- Delete a hotel by ID
DELETE FROM sadie_gtm.hotels
WHERE id = :hotel_id;

-- name: get_hotels_pending_detection
-- Get hotels that need booking engine detection
-- Criteria: status=0 (pending), has website, not a big chain, no booking engine detected yet
SELECT
    h.id,
    h.name,
    h.external_id,
    h.external_id_type,
    h.website,
    h.phone_google,
    h.phone_website,
    h.email,
    h.city,
    h.state,
    h.country,
    h.address,
    ST_Y(h.location::geometry) AS latitude,
    ST_X(h.location::geometry) AS longitude,
    h.rating,
    h.review_count,
    h.status,
    h.source,
    h.created_at,
    h.updated_at
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE h.status = 0
  AND hbe.hotel_id IS NULL
  AND h.website IS NOT NULL
  AND h.website != ''
LIMIT :limit;

-- name: get_hotels_pending_detection_by_categories
-- Get hotels that need booking engine detection, filtered by categories
-- Criteria: status=0 (pending), has website, not a big chain, no booking engine detected yet, in categories list
SELECT
    h.id,
    h.name,
    h.external_id,
    h.external_id_type,
    h.website,
    h.phone_google,
    h.phone_website,
    h.email,
    h.city,
    h.state,
    h.country,
    h.address,
    ST_Y(h.location::geometry) AS latitude,
    ST_X(h.location::geometry) AS longitude,
    h.rating,
    h.review_count,
    h.status,
    h.source,
    h.created_at,
    h.updated_at
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE h.status = 0
  AND hbe.hotel_id IS NULL
  AND h.website IS NOT NULL
  AND h.website != ''
  AND h.category = ANY(:categories)
LIMIT :limit;

-- name: update_hotel_status!
-- Update hotel status after detection
UPDATE sadie_gtm.hotels
SET status = :status,
    phone_website = COALESCE(:phone_website, phone_website),
    email = COALESCE(:email, email),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: update_hotel_contact_info!
-- Update hotel contact info without changing status
UPDATE sadie_gtm.hotels
SET phone_website = COALESCE(:phone_website, phone_website),
    email = COALESCE(:email, email),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: get_hotels_by_ids
-- Get hotels by list of IDs (for worker to fetch batch)
SELECT
    id,
    name,
    external_id,
    external_id_type,
    website,
    phone_google,
    phone_website,
    email,
    city,
    state,
    country,
    address,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude,
    rating,
    review_count,
    status,
    source,
    created_at,
    updated_at
FROM sadie_gtm.hotels
WHERE id = ANY(:hotel_ids);

-- name: update_hotels_status_batch!
-- Update status for multiple hotels at once (for enqueue job)
UPDATE sadie_gtm.hotels
SET status = :status, updated_at = CURRENT_TIMESTAMP
WHERE id = ANY(:hotel_ids);

-- ============================================================================
-- REPORTING QUERIES
-- ============================================================================

-- name: get_leads_for_city
-- Get hotel leads for a city with booking engine, room count, and nearest customer
-- Only returns launched hotels (status=1)
SELECT
    h.id,
    h.name AS hotel_name,
    h.website,
    h.phone_google,
    h.phone_website,
    h.email,
    h.address,
    h.city,
    h.state,
    h.country,
    h.rating,
    h.review_count,
    be.name AS booking_engine_name,
    be.tier AS booking_engine_tier,
    hrc.room_count,
    ec.name AS nearest_customer_name,
    hcp.distance_km AS nearest_customer_distance_km
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN sadie_gtm.existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.city = :city
  AND h.state = :state
  AND h.status = 1;

-- name: get_leads_for_state
-- Get hotel leads for an entire state with booking engine, room count, and nearest customer
-- Only returns launched hotels (status=1)
SELECT
    h.id,
    h.name AS hotel_name,
    h.category,
    h.website,
    h.phone_google,
    h.phone_website,
    h.email,
    h.address,
    h.city,
    h.state,
    h.country,
    h.rating,
    h.review_count,
    be.name AS booking_engine_name,
    be.tier AS booking_engine_tier,
    hrc.room_count,
    ec.name AS nearest_customer_name,
    hcp.distance_km AS nearest_customer_distance_km
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN sadie_gtm.existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.state = :state
  AND h.status = 1;

-- name: get_leads_for_state_by_source
-- Get hotel leads for a state filtered by source pattern
-- Only returns launched hotels (status=1)
SELECT
    h.id,
    h.name AS hotel_name,
    h.category,
    h.website,
    h.phone_google,
    h.phone_website,
    h.email,
    h.address,
    h.city,
    h.state,
    h.country,
    h.rating,
    h.review_count,
    be.name AS booking_engine_name,
    be.tier AS booking_engine_tier,
    hrc.room_count,
    ec.name AS nearest_customer_name,
    hcp.distance_km AS nearest_customer_distance_km
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN sadie_gtm.existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.state = :state
  AND h.status = 1
  AND h.source LIKE :source_pattern;

-- name: get_city_stats^
-- Get stats for a city (for analytics tab)
SELECT
    COUNT(*) AS total_scraped,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL AND hbe.status = 1 THEN 1 END) AS booking_found,
    COUNT(CASE WHEN h.phone_google IS NOT NULL OR h.phone_website IS NOT NULL THEN 1 END) AS with_phone,
    COUNT(CASE WHEN h.email IS NOT NULL THEN 1 END) AS with_email,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier_1_count,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier_2_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.city = :city
  AND h.state = :state;

-- name: get_state_stats^
-- Get stats for a state (for analytics tab)
SELECT
    COUNT(*) AS total_scraped,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL AND hbe.status = 1 THEN 1 END) AS booking_found,
    COUNT(CASE WHEN h.phone_google IS NOT NULL OR h.phone_website IS NOT NULL THEN 1 END) AS with_phone,
    COUNT(CASE WHEN h.email IS NOT NULL THEN 1 END) AS with_email,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier_1_count,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier_2_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state;

-- name: get_state_stats_by_source^
-- Get stats for a state filtered by source pattern (for analytics tab)
SELECT
    COUNT(*) AS total_scraped,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL AND hbe.status = 1 THEN 1 END) AS booking_found,
    COUNT(CASE WHEN h.phone_google IS NOT NULL OR h.phone_website IS NOT NULL THEN 1 END) AS with_phone,
    COUNT(CASE WHEN h.email IS NOT NULL THEN 1 END) AS with_email,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier_1_count,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier_2_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state
  AND h.source LIKE :source_pattern;

-- name: get_top_engines_for_city
-- Get top booking engines for a city (launched hotels only)
SELECT
    be.name AS engine_name,
    COUNT(*) AS hotel_count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.city = :city
  AND h.state = :state
  AND h.status = 1
GROUP BY be.name;

-- name: get_top_engines_for_state
-- Get top booking engines for a state (launched hotels only)
SELECT
    be.name AS engine_name,
    COUNT(*) AS hotel_count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state
  AND h.status = 1
GROUP BY be.name;

-- name: get_top_engines_for_state_by_source
-- Get top booking engines for a state filtered by source (launched hotels only)
SELECT
    be.name AS engine_name,
    COUNT(*) AS hotel_count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state
  AND h.status = 1
  AND h.source LIKE :source_pattern
GROUP BY be.name;

-- name: get_detection_funnel^
-- Get comprehensive detection funnel metrics for a state
-- Used for Stats sheet breakdown
WITH base AS (
    SELECT
        COUNT(*) as total_hotels,
        COUNT(CASE WHEN website IS NOT NULL AND website != '' THEN 1 END) as with_website,
        COUNT(CASE WHEN status = 1 THEN 1 END) as launched
    FROM sadie_gtm.hotels
    WHERE state = :state
),
detection AS (
    SELECT
        COUNT(DISTINCT h.id) as detection_attempted,
        COUNT(DISTINCT CASE WHEN hbe.status = 1 AND hbe.booking_engine_id IS NOT NULL AND be.tier > 0 THEN h.id END) as engine_found,
        COUNT(DISTINCT CASE WHEN hbe.status = 1 AND hbe.booking_engine_id IS NOT NULL AND be.tier = 0 THEN h.id END) as ota_found,
        COUNT(DISTINCT CASE WHEN hbe.status != 1 THEN h.id END) as no_engine_found
    FROM sadie_gtm.hotels h
    JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
    LEFT JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
    WHERE h.state = :state
),
pending AS (
    -- Hotels truly pending: status=0, has website, no HBE record yet
    SELECT COUNT(*) as pending_detection
    FROM sadie_gtm.hotels h
    WHERE h.state = :state
    AND h.status = 0
    AND h.website IS NOT NULL AND h.website != ''
    AND NOT EXISTS (SELECT 1 FROM sadie_gtm.hotel_booking_engines hbe WHERE hbe.hotel_id = h.id)
),
failures AS (
    SELECT
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: HTTP 403%') as http_403,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: HTTP 429%') as http_429,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:junk_booking_url%') as junk_url,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:junk_domain%') as junk_domain,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:non_hotel_name%') as non_hotel_name,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: timeout%') as timeout_err,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: HTTP 5%') as server_5xx,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:exception%') as browser_err
    FROM sadie_gtm.hotel_booking_engines hbe
    JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
    WHERE h.state = :state
)
SELECT
    b.total_hotels,
    b.with_website,
    b.launched,
    d.detection_attempted,
    d.engine_found,
    d.ota_found,
    d.no_engine_found,
    p.pending_detection,
    f.http_403,
    f.http_429,
    f.junk_url,
    f.junk_domain,
    f.non_hotel_name,
    f.timeout_err,
    f.server_5xx,
    f.browser_err
FROM base b, detection d, pending p, failures f;

-- name: get_detection_funnel_by_source^
-- Get comprehensive detection funnel metrics for a state filtered by source
WITH base AS (
    SELECT
        COUNT(*) as total_hotels,
        COUNT(CASE WHEN website IS NOT NULL AND website != '' THEN 1 END) as with_website,
        COUNT(CASE WHEN status = 1 THEN 1 END) as launched
    FROM sadie_gtm.hotels
    WHERE state = :state AND source LIKE :source_pattern
),
detection AS (
    SELECT
        COUNT(DISTINCT h.id) as detection_attempted,
        COUNT(DISTINCT CASE WHEN hbe.status = 1 AND hbe.booking_engine_id IS NOT NULL AND be.tier > 0 THEN h.id END) as engine_found,
        COUNT(DISTINCT CASE WHEN hbe.status = 1 AND hbe.booking_engine_id IS NOT NULL AND be.tier = 0 THEN h.id END) as ota_found,
        COUNT(DISTINCT CASE WHEN hbe.status != 1 THEN h.id END) as no_engine_found
    FROM sadie_gtm.hotels h
    JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
    LEFT JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
    WHERE h.state = :state AND h.source LIKE :source_pattern
),
pending AS (
    -- Hotels truly pending: status=0, has website, no HBE record yet
    SELECT COUNT(*) as pending_detection
    FROM sadie_gtm.hotels h
    WHERE h.state = :state AND h.source LIKE :source_pattern
    AND h.status = 0
    AND h.website IS NOT NULL AND h.website != ''
    AND NOT EXISTS (SELECT 1 FROM sadie_gtm.hotel_booking_engines hbe WHERE hbe.hotel_id = h.id)
),
failures AS (
    SELECT
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: HTTP 403%') as http_403,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: HTTP 429%') as http_429,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:junk_booking_url%') as junk_url,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:junk_domain%') as junk_domain,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:non_hotel_name%') as non_hotel_name,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: timeout%') as timeout_err,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:precheck_failed: HTTP 5%') as server_5xx,
        COUNT(*) FILTER (WHERE detection_method LIKE 'error:exception%') as browser_err
    FROM sadie_gtm.hotel_booking_engines hbe
    JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
    WHERE h.state = :state AND h.source LIKE :source_pattern
)
SELECT
    b.total_hotels,
    b.with_website,
    b.launched,
    d.detection_attempted,
    d.engine_found,
    d.ota_found,
    d.no_engine_found,
    p.pending_detection,
    f.http_403,
    f.http_429,
    f.junk_url,
    f.junk_domain,
    f.non_hotel_name,
    f.timeout_err,
    f.server_5xx,
    f.browser_err
FROM base b, detection d, pending p, failures f;

-- name: get_cities_in_state
-- Get all cities in a state that have launched hotels
SELECT DISTINCT city
FROM sadie_gtm.hotels
WHERE state = :state
  AND city IS NOT NULL
  AND status = 1;

-- ============================================================================
-- LAUNCHER QUERIES
-- ============================================================================
-- Status values:
--   -2 = Location mismatch (rejected)
--   -1 = No booking engine found (rejected)
--    0 = Pending/Not ready
--    1 = Launched and live

-- name: get_launchable_hotels
-- Get hotels ready to be launched
-- Criteria: status=0 (pending), has booking engine with successful detection
-- Room count and proximity are optional (LEFT JOIN)
SELECT
    h.id,
    h.name AS hotel_name,
    h.website,
    h.city,
    h.state,
    be.name AS booking_engine_name,
    be.tier AS booking_engine_tier,
    hrc.room_count,
    ec.name AS nearest_customer_name,
    hcp.distance_km AS nearest_customer_distance_km
FROM sadie_gtm.hotels h
INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
INNER JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN sadie_gtm.existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.status = 0
LIMIT :limit;

-- name: get_launchable_count^
-- Count hotels ready to be launched (status=0, has successful detection)
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
WHERE h.status = 0;

-- name: launch_hotels
-- Atomically claim and launch hotels (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple EC2 instances can run concurrently
-- Returns launched hotel IDs for logging/tracking
WITH claimed AS (
    SELECT h.id
    FROM sadie_gtm.hotels h
    INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
    WHERE h.status = 0
      AND h.id = ANY(:hotel_ids)
    FOR UPDATE OF h SKIP LOCKED
)
UPDATE sadie_gtm.hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE id IN (SELECT id FROM claimed)
RETURNING id;

-- name: launch_ready_hotels
-- Atomically claim and launch up to :limit ready hotels (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple EC2 instances can run concurrently
-- Returns launched hotel IDs for logging/tracking
WITH claimed AS (
    SELECT h.id
    FROM sadie_gtm.hotels h
    INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
    WHERE h.status = 0
    FOR UPDATE OF h SKIP LOCKED
    LIMIT :limit
)
UPDATE sadie_gtm.hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE id IN (SELECT id FROM claimed)
RETURNING id;

-- name: get_launched_count^
-- Count hotels that have been launched (status=1)
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE status = 1;

-- ============================================================================
-- LOCATION ENRICHMENT QUERIES
-- ============================================================================

-- name: get_hotels_pending_location_enrichment
-- Get hotels with coordinates but missing city
SELECT
    id,
    name,
    address,
    city,
    state,
    country,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude
FROM sadie_gtm.hotels
WHERE location IS NOT NULL
  AND (city IS NULL OR city = '')
LIMIT :limit;

-- name: get_pending_location_enrichment_count^
-- Count hotels needing location enrichment
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE location IS NOT NULL
  AND (city IS NULL OR city = '');

-- name: update_hotel_location!
-- Update hotel location fields from reverse geocoding
UPDATE sadie_gtm.hotels
SET address = COALESCE(:address, address),
    city = COALESCE(:city, city),
    state = COALESCE(:state, state),
    country = COALESCE(:country, country),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: get_hotels_pending_coordinate_enrichment
-- Get hotels with coordinates but no website (parcel data needing Places API lookup)
SELECT
    id,
    name,
    address,
    city,
    state,
    category,
    source,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude
FROM sadie_gtm.hotels
WHERE location IS NOT NULL
  AND (website IS NULL OR website = '')
  AND source IN ('sf_assessor', 'md_sdat_cama')
ORDER BY id
LIMIT :limit;

-- name: get_pending_coordinate_enrichment_count^
-- Count hotels needing coordinate-based enrichment
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE location IS NOT NULL
  AND (website IS NULL OR website = '')
  AND source IN ('sf_assessor', 'md_sdat_cama');

-- name: update_hotel_from_places!
-- Update hotel with data from Places API (name, website, phone)
UPDATE sadie_gtm.hotels
SET name = COALESCE(:name, name),
    website = COALESCE(:website, website),
    phone_google = COALESCE(:phone, phone_google),
    rating = COALESCE(:rating, rating),
    address = COALESCE(:address, address),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- ============================================================================
-- INGESTOR QUERIES
-- ============================================================================

-- name: get_hotel_by_name_city^
-- Check if hotel exists by name and city (for dedup)
SELECT id, category
FROM sadie_gtm.hotels
WHERE LOWER(name) = LOWER(:name)
  AND LOWER(COALESCE(city, '')) = LOWER(COALESCE(:city, ''))
LIMIT 1;

-- name: get_hotel_by_source^
-- Check if hotel exists by source (for sources with unique IDs like texas_hot:12345:00001)
SELECT id, category
FROM sadie_gtm.hotels
WHERE source = :source
LIMIT 1;

-- name: update_hotel_category!
-- Update hotel category
UPDATE sadie_gtm.hotels
SET category = :category, updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: update_hotel_from_ingestor!
-- Update hotel with data from ingestor (DBPR etc)
-- Uses COALESCE to not overwrite existing data with NULL
UPDATE sadie_gtm.hotels
SET category = COALESCE(:category, category),
    address = COALESCE(:address, address),
    phone_google = COALESCE(:phone, phone_google),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: insert_hotel_with_category<!
-- Insert a new hotel with category and return the ID
INSERT INTO sadie_gtm.hotels (
    name, website, source, status, address, city, state, country, phone_google, category
) VALUES (
    :name, :website, :source, :status, :address, :city, :state, :country, :phone, :category
)
RETURNING id;

-- name: update_hotel_website!
-- Update hotel website
UPDATE sadie_gtm.hotels
SET website = :website, updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: update_hotel_location_point!
-- Update hotel location from lat/lng
-- SRID 4326 = WGS84 (standard GPS coordinate system)
UPDATE sadie_gtm.hotels
SET location = ST_SetSRID(ST_MakePoint(:lng, :lat), 4326),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: update_hotel_location_point_if_null!
-- Update hotel location from lat/lng ONLY if location is currently NULL
-- Prevents overwriting existing location data
UPDATE sadie_gtm.hotels
SET location = ST_SetSRID(ST_MakePoint(:lng, :lat), 4326),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id
  AND location IS NULL;

-- ============================================================================
-- RETRY QUERIES
-- ============================================================================

-- name: get_hotels_for_retry
-- Get hotels with retryable errors (timeout, 5xx, browser exceptions)
SELECT
    h.id,
    h.name,
    h.website,
    h.city,
    h.state,
    hbe.detection_method
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE h.state = :state
  AND (
    hbe.detection_method LIKE 'error:precheck_failed: timeout%'
    OR hbe.detection_method LIKE 'error:precheck_failed: HTTP 5%'
    OR hbe.detection_method LIKE 'error:exception%'
  )
LIMIT :limit;

-- name: get_hotels_for_retry_by_source
-- Get hotels with retryable errors, filtered by source
SELECT
    h.id,
    h.name,
    h.website,
    h.city,
    h.state,
    hbe.detection_method
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE h.state = :state
  AND h.source LIKE :source_pattern
  AND (
    hbe.detection_method LIKE 'error:precheck_failed: timeout%'
    OR hbe.detection_method LIKE 'error:precheck_failed: HTTP 5%'
    OR hbe.detection_method LIKE 'error:exception%'
  )
LIMIT :limit;

-- name: delete_hbe_for_retry!
-- Delete HBE record to allow retry
DELETE FROM sadie_gtm.hotel_booking_engines
WHERE hotel_id = :hotel_id;

-- name: delete_hbe_batch_for_retry*!
-- Delete HBE records for batch retry
DELETE FROM sadie_gtm.hotel_booking_engines
WHERE hotel_id = ANY(:hotel_ids);

-- name: reset_hotels_for_retry*!
-- Reset hotel status to 0 (pending) for retry
UPDATE sadie_gtm.hotels
SET status = 0, updated_at = CURRENT_TIMESTAMP
WHERE id = ANY(:hotel_ids);

-- ============================================================================
-- EXTERNAL ID QUERIES
-- ============================================================================

-- name: get_hotel_by_external_id^
-- Look up hotel by external ID
SELECT id FROM sadie_gtm.hotels
WHERE external_id_type = :external_id_type AND external_id = :external_id;

-- name: get_hotels_by_external_ids
-- Batch lookup hotels by external IDs
SELECT id, external_id FROM sadie_gtm.hotels
WHERE external_id_type = :external_id_type AND external_id = ANY(:external_ids);

-- name: update_hotel_external_id!
-- Set external_id on existing hotel (only if not already set)
UPDATE sadie_gtm.hotels
SET external_id = :external_id,
    external_id_type = :external_id_type,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id AND external_id IS NULL;

-- name: insert_hotel_with_external_id<!
-- Insert a new hotel with external_id and return the ID
INSERT INTO sadie_gtm.hotels (
    name, website, source, status, address, city, state, country, phone_google, category, external_id, external_id_type
) VALUES (
    :name, :website, :source, :status, :address, :city, :state, :country, :phone, :category, :external_id, :external_id_type
)
RETURNING id;

-- name: get_hotels_in_bbox
-- Get hotels within a bounding box for dedup during scraping
SELECT
    external_id,
    external_id_type,
    ST_Y(location::geometry) as lat,
    ST_X(location::geometry) as lng
FROM sadie_gtm.hotels
WHERE location IS NOT NULL
AND ST_Within(
    location::geometry,
    ST_MakeEnvelope(:lng_min, :lat_min, :lng_max, :lat_max, 4326)
);
