-- name: get_hotel_by_id^
-- Get single hotel by ID with location coordinates
SELECT
    id,
    name,
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
FROM hotels
WHERE id = :hotel_id;

-- name: insert_hotel<!
-- Insert a new hotel and return the ID
INSERT INTO hotels (
    name,
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
DELETE FROM hotels
WHERE id = :hotel_id;

-- name: get_hotels_pending_detection
-- Get hotels that need booking engine detection
-- Criteria: status=0 (pending), has website, not a big chain, no booking engine detected yet
SELECT
    h.id,
    h.name,
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
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
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

-- name: claim_hotels_for_detection
-- Atomically claim hotels for processing (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple workers grab different rows
-- Updates updated_at to mark as being processed
UPDATE hotels
SET updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT h.id FROM hotels h
    LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
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
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
)
RETURNING
    id,
    name,
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
    updated_at;

-- name: update_hotel_status!
-- Update hotel status after detection
UPDATE hotels
SET status = :status,
    phone_website = COALESCE(:phone_website, phone_website),
    email = COALESCE(:email, email),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: get_booking_engine_by_name^
-- Get booking engine by name
SELECT id, name, domains, tier, is_active
FROM booking_engines
WHERE name = :name;

-- name: get_all_booking_engines
-- Get all active booking engines with their domain patterns
SELECT id, name, domains, tier
FROM booking_engines
WHERE is_active = TRUE
  AND domains IS NOT NULL
  AND array_length(domains, 1) > 0;

-- name: insert_booking_engine<!
-- Insert a new booking engine (tier 2 = unknown/discovered)
INSERT INTO booking_engines (name, domains, tier)
VALUES (:name, :domains, :tier)
ON CONFLICT (name) DO UPDATE SET
    domains = COALESCE(EXCLUDED.domains, booking_engines.domains)
RETURNING id;

-- name: insert_hotel_booking_engine!
-- Link hotel to detected booking engine
INSERT INTO hotel_booking_engines (
    hotel_id,
    booking_engine_id,
    booking_url,
    detection_method,
    detected_at,
    updated_at
) VALUES (
    :hotel_id,
    :booking_engine_id,
    :booking_url,
    :detection_method,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (hotel_id) DO UPDATE SET
    booking_engine_id = EXCLUDED.booking_engine_id,
    booking_url = EXCLUDED.booking_url,
    detection_method = EXCLUDED.detection_method,
    updated_at = CURRENT_TIMESTAMP;

-- name: get_hotels_by_ids
-- Get hotels by list of IDs (for worker to fetch batch)
SELECT
    id,
    name,
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
FROM hotels
WHERE id = ANY(:hotel_ids);

-- name: update_hotels_status_batch!
-- Update status for multiple hotels at once (for enqueue job)
UPDATE hotels
SET status = :status, updated_at = CURRENT_TIMESTAMP
WHERE id = ANY(:hotel_ids);

-- name: insert_detection_error!
-- Log a detection error for debugging
INSERT INTO detection_errors (hotel_id, error_type, error_message, detected_location)
VALUES (:hotel_id, :error_type, :error_message, :detected_location);

-- name: get_detection_errors_by_type
-- Get detection errors by type for analysis
SELECT id, hotel_id, error_type, error_message, detected_location, created_at
FROM detection_errors
WHERE error_type = :error_type
ORDER BY created_at DESC
LIMIT :limit;

-- name: get_detection_errors_summary
-- Get count of errors by type
SELECT error_type, COUNT(*) as count
FROM detection_errors
GROUP BY error_type
ORDER BY count DESC;

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
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.city = :city
  AND h.state = :state
  AND h.status = 1
ORDER BY h.name;

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
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.state = :state
  AND h.status = 1
ORDER BY h.city, h.name;

-- name: get_city_stats^
-- Get stats for a city (for analytics tab)
SELECT
    -- Total counts
    COUNT(*) AS total_scraped,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL THEN 1 END) AS booking_found,
    -- Contact info
    COUNT(CASE WHEN h.phone_google IS NOT NULL OR h.phone_website IS NOT NULL THEN 1 END) AS with_phone,
    COUNT(CASE WHEN h.email IS NOT NULL THEN 1 END) AS with_email,
    -- Tier breakdown (of hotels with booking engine)
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier_1_count,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier_2_count
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.city = :city
  AND h.state = :state;

-- name: get_state_stats^
-- Get stats for a state (for analytics tab)
SELECT
    -- Total counts
    COUNT(*) AS total_scraped,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL THEN 1 END) AS booking_found,
    -- Contact info
    COUNT(CASE WHEN h.phone_google IS NOT NULL OR h.phone_website IS NOT NULL THEN 1 END) AS with_phone,
    COUNT(CASE WHEN h.email IS NOT NULL THEN 1 END) AS with_email,
    -- Tier breakdown (of hotels with booking engine)
    COUNT(CASE WHEN be.tier = 1 THEN 1 END) AS tier_1_count,
    COUNT(CASE WHEN be.tier = 2 THEN 1 END) AS tier_2_count
FROM hotels h
LEFT JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state;

-- name: get_top_engines_for_city
-- Get top booking engines for a city (launched hotels only)
SELECT
    be.name AS engine_name,
    COUNT(*) AS hotel_count
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.city = :city
  AND h.state = :state
  AND h.status = 1
GROUP BY be.name
ORDER BY hotel_count DESC;

-- name: get_top_engines_for_state
-- Get top booking engines for a state (launched hotels only)
SELECT
    be.name AS engine_name,
    COUNT(*) AS hotel_count
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.state = :state
  AND h.status = 1
GROUP BY be.name
ORDER BY hotel_count DESC;

-- name: get_cities_in_state
-- Get all cities in a state that have launched hotels
SELECT DISTINCT city
FROM hotels
WHERE state = :state
  AND city IS NOT NULL
  AND status = 1
ORDER BY city;

-- ============================================================================
-- LAUNCHER QUERIES
-- ============================================================================
-- Status values:
--   -2 = Location mismatch (rejected)
--   -1 = No booking engine found (rejected)
--    0 = Pending/Not ready
--    1 = Launched and live

-- name: get_launchable_hotels
-- Get hotels ready to be launched (fully enriched with all data)
-- Criteria: status=0 (pending), has booking engine, has successful room count (status=1), has proximity
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
FROM hotels h
INNER JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
INNER JOIN booking_engines be ON hbe.booking_engine_id = be.id
INNER JOIN hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
INNER JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
INNER JOIN existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE h.status = 0
ORDER BY h.state, h.city, h.name
LIMIT :limit;

-- name: get_launchable_count^
-- Count hotels ready to be launched (status=0 with all enrichment data)
SELECT COUNT(*) AS count
FROM hotels h
INNER JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
INNER JOIN hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
INNER JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.status = 0;

-- name: launch_hotels!
-- Mark hotels as launched (status=1)
-- Only launches hotels that meet all criteria
UPDATE hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT h.id
    FROM hotels h
    INNER JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
    INNER JOIN hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
    INNER JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
    WHERE h.status = 0
      AND h.id = ANY(:hotel_ids)
);

-- name: launch_all_ready_hotels!
-- Mark ALL ready hotels as launched (status=1)
UPDATE hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT h.id
    FROM hotels h
    INNER JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
    INNER JOIN hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
    INNER JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
    WHERE h.status = 0
);

-- name: get_launched_count^
-- Count hotels that have been launched (status=1)
SELECT COUNT(*) AS count
FROM hotels
WHERE status = 1;
