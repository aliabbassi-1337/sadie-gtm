-- name: get_hotel_by_id^
-- Get single hotel by ID with location coordinates
SELECT
    id,
    name,
    google_place_id,
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
    google_place_id,
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
    :google_place_id,
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
    google_place_id = COALESCE(EXCLUDED.google_place_id, hotels.google_place_id),
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
    h.google_place_id,
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
  AND LOWER(h.website) NOT LIKE '%marriott.com%'
  AND LOWER(h.website) NOT LIKE '%hilton.com%'
  AND LOWER(h.website) NOT LIKE '%ihg.com%'
  AND LOWER(h.website) NOT LIKE '%hyatt.com%'
  AND LOWER(h.website) NOT LIKE '%wyndham.com%'
  AND LOWER(h.website) NOT LIKE '%choicehotels.com%'
  AND LOWER(h.website) NOT LIKE '%bestwestern.com%'
  AND LOWER(h.website) NOT LIKE '%radissonhotels.com%'
  AND LOWER(h.website) NOT LIKE '%accor.com%'
LIMIT :limit;

-- name: get_hotels_pending_detection_by_categories
-- Get hotels that need booking engine detection, filtered by categories
-- Criteria: status=0 (pending), has website, not a big chain, no booking engine detected yet, in categories list
SELECT
    h.id,
    h.name,
    h.google_place_id,
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
  AND LOWER(h.website) NOT LIKE '%marriott.com%'
  AND LOWER(h.website) NOT LIKE '%hilton.com%'
  AND LOWER(h.website) NOT LIKE '%ihg.com%'
  AND LOWER(h.website) NOT LIKE '%hyatt.com%'
  AND LOWER(h.website) NOT LIKE '%wyndham.com%'
  AND LOWER(h.website) NOT LIKE '%choicehotels.com%'
  AND LOWER(h.website) NOT LIKE '%bestwestern.com%'
  AND LOWER(h.website) NOT LIKE '%radissonhotels.com%'
  AND LOWER(h.website) NOT LIKE '%accor.com%'
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
    google_place_id,
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

-- name: get_city_stats^
-- Get stats for a city (for analytics tab)
SELECT
    COUNT(*) AS total_scraped,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL THEN 1 END) AS booking_found,
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
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL THEN 1 END) AS booking_found,
    COUNT(CASE WHEN h.phone_google IS NOT NULL OR h.phone_website IS NOT NULL THEN 1 END) AS with_phone,
    COUNT(CASE WHEN h.email IS NOT NULL THEN 1 END) AS with_email,
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier_1_count,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier_2_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state;

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
UPDATE sadie_gtm.hotels
SET location = ST_SetSRID(ST_MakePoint(:lng, :lat), 4326),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

