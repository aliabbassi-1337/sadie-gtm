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
-- Criteria: status < DETECTED (30), has website, no booking engine detected yet
-- Includes: INGESTED (0), HAS_WEBSITE (10), HAS_LOCATION (20)
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
WHERE h.status >= 0 AND h.status < 30  -- INGESTED, HAS_WEBSITE, HAS_LOCATION
  AND hbe.hotel_id IS NULL
  AND h.website IS NOT NULL
  AND h.website != ''
LIMIT :limit;

-- name: get_hotels_pending_detection_by_categories
-- Get hotels that need booking engine detection, filtered by categories
-- Criteria: status < DETECTED (30), has website, no booking engine detected yet, in categories list
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
WHERE h.status >= 0 AND h.status < 30  -- INGESTED, HAS_WEBSITE, HAS_LOCATION
  AND hbe.hotel_id IS NULL
  AND h.website IS NOT NULL
  AND h.website != ''
  AND h.category ILIKE ANY(ARRAY(SELECT '%' || unnest(:categories) || '%'))
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

-- name: update_hotel_scraped_address!
-- Update hotel address scraped from booking page (Cloudbeds).
-- Only updates if current value is null/empty to avoid overwriting authoritative data.
UPDATE sadie_gtm.hotels
SET address = CASE WHEN (address IS NULL OR address = '') THEN :address ELSE address END,
    city = CASE WHEN (city IS NULL OR city = '') THEN :city ELSE city END,
    state = CASE WHEN (state IS NULL OR state = '') THEN :state ELSE state END,
    country = CASE WHEN (country IS NULL OR country = '') THEN :country ELSE country END,
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
  AND h.country = :country
  AND h.status = 1;

-- name: get_leads_for_country
-- Get all hotel leads for a country with booking engine, room count, and nearest customer
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
WHERE h.country = :country
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
  AND h.country = :country
  AND h.status = 1
  AND h.source LIKE :source_pattern;

-- name: get_leads_by_booking_engine
-- Get hotel leads by booking engine name and source pattern
-- Only exports launched hotels with active booking engine status
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
    hbe.booking_url,
    hbe.engine_property_id,
    hrc.room_count,
    ec.name AS nearest_customer_name,
    hcp.distance_km AS nearest_customer_distance_km
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
LEFT JOIN sadie_gtm.existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE be.name = :booking_engine
  AND h.status = 1
  AND hbe.status = 1
ORDER BY h.country, h.state, h.city, h.name;

-- name: get_leads_by_source
-- Get hotel leads by source pattern (for IPMS247, etc.)
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
    hbe.booking_url,
    hbe.engine_property_id,
    NULL::integer AS room_count,
    NULL::text AS nearest_customer_name,
    NULL::numeric AS nearest_customer_distance_km
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.source LIKE :source_pattern
ORDER BY h.country, h.city, h.name;

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
WHERE h.state = :state
  AND h.country = :country;

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
  AND h.country = :country
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
  AND h.country = :country
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
  AND h.country = :country
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
    WHERE state = :state AND country = :country
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
    WHERE h.state = :state AND h.country = :country
),
pending AS (
    -- Hotels truly pending: status=0, has website, no HBE record yet
    SELECT COUNT(*) as pending_detection
    FROM sadie_gtm.hotels h
    WHERE h.state = :state AND h.country = :country
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
    WHERE h.state = :state AND h.country = :country
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
    WHERE state = :state AND country = :country AND source LIKE :source_pattern
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
    WHERE h.state = :state AND h.country = :country AND h.source LIKE :source_pattern
),
pending AS (
    -- Hotels truly pending: status=0, has website, no HBE record yet
    SELECT COUNT(*) as pending_detection
    FROM sadie_gtm.hotels h
    WHERE h.state = :state AND h.country = :country AND h.source LIKE :source_pattern
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
    WHERE h.state = :state AND h.country = :country AND h.source LIKE :source_pattern
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
  AND country = 'United States'
  AND city IS NOT NULL
  AND status = 1;

-- ============================================================================
-- LAUNCHER QUERIES
-- ============================================================================
-- Status values:
--   -1 = Error/rejected
--    0 = Pending/Not ready
--    1 = Launched (fully enriched)
--
-- Launch criteria (ALL required):
--   - status = 0 (pending)
--   - name (not null, not empty, not 'Unknown')
--   - email OR phone (at least one)
--   - state, country (city optional)
--   - booking engine detected (hbe.status = 1)
-- NOT required (optional, displayed if available):
--   - room_count
--   - customer proximity

-- name: get_launchable_hotels
-- Get hotels ready to be launched (valid name + country + BE, state optional)
SELECT
    h.id,
    h.name AS hotel_name,
    h.website,
    h.email,
    h.phone_website,
    h.city,
    h.state,
    h.country,
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
  -- Location requirements (country required, state optional)
  AND h.country IS NOT NULL AND h.country != ''
  -- Valid name requirements (filter out junk/test/system names)
  AND h.name IS NOT NULL AND h.name != '' AND h.name != ' '
  AND LENGTH(h.name) > 2
  AND h.name NOT IN ('&#65279;', '-', '--', '---', '.', '..', 'Error', 'Online Bookings', 'Search', 'Book Now', 'Booking Engine', 'Hotel Booking Engine', 'Reservation', 'Reservations', 'View or Change a Reservation', 'My reservations', 'Modify/Cancel reservation', 'Book Now Pay on Check-in', 'DEACTIVATED ACCOUNT DO NO BOOK', 'Rates', 'Hotel')
  AND h.name NOT ILIKE '%test%'
  AND h.name NOT ILIKE '%demo%'
  AND h.name NOT ILIKE '%sandbox%'
  AND h.name NOT ILIKE '%sample%'
  AND h.name NOT ILIKE 'unknown%'
  AND h.name NOT ILIKE '%internal%server%error%'
  AND h.name NOT ILIKE '%check availability%'
  AND h.name NOT ILIKE '%booking engine%'
  AND h.name NOT LIKE '% RMS %'
  AND h.name NOT LIKE 'RMS %'
  AND h.name NOT LIKE '% RMS'
  AND h.name !~ '^[0-9-]+$'
LIMIT :limit;

-- name: get_launchable_count^
-- Count hotels ready to be launched (valid name + country + BE, state optional)
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
WHERE h.status = 0
  -- Location requirements (country required, state optional)
  AND h.country IS NOT NULL AND h.country != ''
  -- Valid name requirements (filter out junk/test/system names)
  AND h.name IS NOT NULL AND h.name != '' AND h.name != ' '
  AND LENGTH(h.name) > 2
  AND h.name NOT IN ('&#65279;', '-', '--', '---', '.', '..', 'Error', 'Online Bookings', 'Search', 'Book Now', 'Booking Engine', 'Hotel Booking Engine', 'Reservation', 'Reservations', 'View or Change a Reservation', 'My reservations', 'Modify/Cancel reservation', 'Book Now Pay on Check-in', 'DEACTIVATED ACCOUNT DO NO BOOK', 'Rates', 'Hotel')
  AND h.name NOT ILIKE '%test%'
  AND h.name NOT ILIKE '%demo%'
  AND h.name NOT ILIKE '%sandbox%'
  AND h.name NOT ILIKE '%sample%'
  AND h.name NOT ILIKE 'unknown%'
  AND h.name NOT ILIKE '%internal%server%error%'
  AND h.name NOT ILIKE '%check availability%'
  AND h.name NOT ILIKE '%booking engine%'
  AND h.name NOT LIKE '% RMS %'
  AND h.name NOT LIKE 'RMS %'
  AND h.name NOT LIKE '% RMS'
  AND h.name !~ '^[0-9-]+$';

-- name: launch_hotels
-- Atomically claim and launch hotels (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple EC2 instances can run concurrently
-- Launches hotels with valid names (email not required)
-- Returns launched hotel IDs for logging/tracking
WITH claimed AS (
    SELECT h.id
    FROM sadie_gtm.hotels h
    INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
    WHERE h.status = 0
      AND h.id = ANY(:hotel_ids)
      -- Location requirements (country required, state optional)
      AND h.country IS NOT NULL AND h.country != ''
      -- Valid name requirements (filter out junk/test/system names)
      AND h.name IS NOT NULL AND h.name != '' AND h.name != ' '
      AND LENGTH(h.name) > 2
      AND h.name NOT IN ('&#65279;', '-', '--', '---', '.', '..', 'Error', 'Online Bookings', 'Search', 'Book Now', 'Booking Engine', 'Hotel Booking Engine', 'Reservation', 'Reservations', 'View or Change a Reservation', 'My reservations', 'Modify/Cancel reservation', 'Book Now Pay on Check-in', 'DEACTIVATED ACCOUNT DO NO BOOK', 'Rates', 'Hotel')
      AND h.name NOT ILIKE '%test%'
      AND h.name NOT ILIKE '%demo%'
      AND h.name NOT ILIKE '%sandbox%'
      AND h.name NOT ILIKE '%sample%'
      AND h.name NOT ILIKE 'unknown%'
      AND h.name NOT ILIKE '%internal%server%error%'
      AND h.name NOT ILIKE '%check availability%'
      AND h.name NOT ILIKE '%booking engine%'
      AND h.name NOT LIKE '% RMS %'
      AND h.name NOT LIKE 'RMS %'
      AND h.name NOT LIKE '% RMS'
      AND h.name !~ '^[0-9-]+$'
    FOR UPDATE OF h SKIP LOCKED
)
UPDATE sadie_gtm.hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE id IN (SELECT id FROM claimed)
RETURNING id;

-- name: launch_ready_hotels
-- Atomically claim and launch up to :limit ready hotels (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple EC2 instances can run concurrently
-- Launches hotels with valid names (country required, state optional)
-- Returns launched hotel IDs for logging/tracking
WITH claimed AS (
    SELECT h.id
    FROM sadie_gtm.hotels h
    INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
    WHERE h.status = 0
      -- Location requirements (country required, state optional)
      AND h.country IS NOT NULL AND h.country != ''
      -- Valid name requirements (filter out junk/test/system names)
      AND h.name IS NOT NULL AND h.name != '' AND h.name != ' '
      AND LENGTH(h.name) > 2
      AND h.name NOT IN ('&#65279;', '-', '--', '---', '.', '..', 'Error', 'Online Bookings', 'Search', 'Book Now', 'Booking Engine', 'Hotel Booking Engine', 'Reservation', 'Reservations', 'View or Change a Reservation', 'My reservations', 'Modify/Cancel reservation', 'Book Now Pay on Check-in', 'DEACTIVATED ACCOUNT DO NO BOOK', 'Rates', 'Hotel')
      AND h.name NOT ILIKE '%test%'
      AND h.name NOT ILIKE '%demo%'
      AND h.name NOT ILIKE '%sandbox%'
      AND h.name NOT ILIKE '%sample%'
      AND h.name NOT ILIKE 'unknown%'
      AND h.name NOT ILIKE '%internal%server%error%'
      AND h.name NOT ILIKE '%check availability%'
      AND h.name NOT ILIKE '%booking engine%'
      AND h.name NOT LIKE '% RMS %'
      AND h.name NOT LIKE 'RMS %'
      AND h.name NOT LIKE '% RMS'
      AND h.name !~ '^[0-9-]+$'
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
-- Get USA leads with coordinates but missing state (for reverse geocoding)
SELECT DISTINCT
    h.id,
    h.name,
    h.address,
    h.city,
    h.state,
    h.country,
    ST_Y(h.location::geometry) AS latitude,
    ST_X(h.location::geometry) AS longitude
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE h.location IS NOT NULL
  AND h.status != -1
  AND h.country = 'United States'
  AND (h.state IS NULL OR h.state = '')
LIMIT :limit;

-- name: get_pending_location_enrichment_count^
-- Count USA leads needing location enrichment (have coords, missing state)
SELECT COUNT(DISTINCT h.id) AS count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE h.location IS NOT NULL
  AND h.status != -1
  AND h.country = 'United States'
  AND (h.state IS NULL OR h.state = '');

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
-- sources: optional array of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
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
  AND (:sources::text[] IS NULL OR source = ANY(:sources))
ORDER BY id
LIMIT :limit;

-- name: get_pending_coordinate_enrichment_count^
-- Count hotels needing coordinate-based enrichment
-- sources: optional array of source names (e.g., ['sf_assessor', 'md_sdat_cama'])
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE location IS NOT NULL
  AND (website IS NULL OR website = '')
  AND (:sources::text[] IS NULL OR source = ANY(:sources));

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

-- ============================================================================
-- PIPELINE STATE MACHINE QUERIES
-- ============================================================================

-- name: get_pipeline_summary
-- Get count of hotels at each pipeline stage
SELECT 
    status,
    COUNT(*) AS count
FROM sadie_gtm.hotels
GROUP BY status
ORDER BY status DESC;

-- name: get_pipeline_by_source
-- Get status breakdown by source
-- Status: 0=pending, 1=live, -1=error
SELECT 
    source,
    SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS live,
    SUM(CASE WHEN status = -1 THEN 1 ELSE 0 END) AS error,
    COUNT(*) AS total
FROM sadie_gtm.hotels
GROUP BY source
ORDER BY total DESC;

-- name: get_pipeline_by_source_name
-- Get pipeline breakdown for a specific source
SELECT 
    status,
    COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE source = :source
GROUP BY status
ORDER BY status DESC;

-- name: get_hotels_by_status
-- Get hotels at a specific status
-- Status: 0=pending, 1=live, -1=error
SELECT
    id, name, website, city, state, source,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude
FROM sadie_gtm.hotels
WHERE status = :status
ORDER BY id
LIMIT :limit;

-- name: get_pending_hotels
-- Get hotels needing processing (status=0)
SELECT
    id, name, website, city, state, source,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude
FROM sadie_gtm.hotels
WHERE status = 0
ORDER BY id
LIMIT :limit;

-- name: set_hotel_live!
-- Mark hotel as live (status=1)
UPDATE sadie_gtm.hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: set_hotel_error!
-- Mark hotel as error (status=-1)
UPDATE sadie_gtm.hotels
SET status = -1, updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- ============================================================================
-- COMMON CRAWL / REVERSE LOOKUP QUERIES
-- ============================================================================

-- name: find_hotel_by_name^
-- Find hotel by normalized name (case-insensitive, trimmed)
-- Used for matching Common Crawl hotels to existing records
SELECT
    id,
    name,
    website,
    source,
    city,
    state
FROM sadie_gtm.hotels
WHERE LOWER(TRIM(name)) = LOWER(TRIM(:name))
LIMIT 1;

-- name: find_hotel_by_name_and_city^
-- Find hotel by name and city (more precise matching)
SELECT
    id,
    name,
    website,
    source,
    city,
    state
FROM sadie_gtm.hotels
WHERE LOWER(TRIM(name)) = LOWER(TRIM(:name))
  AND LOWER(TRIM(city)) = LOWER(TRIM(:city))
LIMIT 1;

-- name: update_hotel_source!
-- Append source to existing hotel (e.g., dbpr -> dbpr::commoncrawl)
UPDATE sadie_gtm.hotels
SET source = CASE 
    WHEN source IS NULL OR source = '' THEN :new_source
    WHEN source LIKE '%' || :new_source || '%' THEN source  -- already has this source
    ELSE source || '::' || :new_source
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: upsert_commoncrawl_hotel<!
-- Upsert hotel from Common Crawl with source appending
-- If hotel exists (by external_id), append source; otherwise insert
INSERT INTO sadie_gtm.hotels (
    name, 
    city, 
    country,
    source, 
    status, 
    external_id, 
    external_id_type
) VALUES (
    :name,
    :city,
    :country,
    :source,
    0,
    :external_id,
    :external_id_type
)
ON CONFLICT (external_id_type, external_id) WHERE external_id IS NOT NULL
DO UPDATE SET
    source = CASE 
        WHEN hotels.source IS NULL OR hotels.source = '' THEN EXCLUDED.source
        WHEN hotels.source LIKE '%' || EXCLUDED.source || '%' THEN hotels.source
        ELSE hotels.source || '::' || EXCLUDED.source
    END,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, 
    (xmax = 0) AS inserted;  -- True if inserted, False if updated


-- name: get_distinct_states
-- Get all distinct US states that have hotels (50 states + DC + territories)
SELECT DISTINCT state
FROM sadie_gtm.hotels
WHERE state IS NOT NULL AND state != ''
  AND country = 'United States'
  AND state IN (
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
    'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois',
    'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts',
    'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
    'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota',
    'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia',
    'Wisconsin', 'Wyoming', 'Puerto Rico'
  )
ORDER BY state;


-- ============================================================================
-- GEOCODING QUERIES (Serper Places enrichment for crawl data)
-- ============================================================================

-- name: get_hotels_needing_geocoding
-- Get hotels with names but missing location data (for Serper Places geocoding)
-- Targets hotels that have been name-enriched but need location (city or state)
-- Uses same name validation as launcher to exclude junk names (see launch_conditions.py)
-- Use :engine parameter to filter by booking engine (e.g., 'Cloudbeds', 'RMS Cloud')
-- When :engine is NULL, excludes Cloudbeds by default (handled by booking page enrichment)
-- Note: Hotels with valid 2-letter state abbreviations (CA, TX) don't need geocoding - just normalization
SELECT 
    h.id,
    h.name,
    h.address,
    h.city,
    h.state,
    h.country,
    h.source,
    hbe.booking_url,
    be.name as engine_name
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE 
  -- Not rejected
  h.status >= 0
  -- Missing location (need geocoding) - empty state, not abbreviations
  AND ((h.city IS NULL OR h.city = '') OR (h.state IS NULL OR h.state = ''))
  -- Engine filter
  AND (
    CAST(:engine AS TEXT) IS NOT NULL AND be.name = :engine  -- Filter by specific engine
    OR (CAST(:engine AS TEXT) IS NULL AND (be.name IS NULL OR be.name != 'Cloudbeds'))  -- Default: exclude Cloudbeds
  )
  AND (CAST(:source AS TEXT) IS NULL OR h.source LIKE :source)
  AND (CAST(:country AS TEXT) IS NULL OR h.country = :country)
  -- Name validation (same as launcher - see launch_conditions.py)
  AND h.name IS NOT NULL
  AND h.name != ''
  AND h.name != ' '
  AND LENGTH(h.name) > 2
  AND h.name NOT IN ('&#65279;', '-', '--', '---', '.', '..', 'Book Now', 'Book Now Pay on Check-in', 
      'Booking Engine', 'DEACTIVATED ACCOUNT DO NO BOOK', 'Error', 'Hotel', 'Hotel Booking Engine',
      'Modify/Cancel reservation', 'My reservations', 'Online Bookings', 'Rates', 'Reservation', 
      'Reservations', 'Search', 'View or Change a Reservation')
  AND h.name NOT ILIKE '%test%'
  AND h.name NOT ILIKE '%demo%'
  AND h.name NOT ILIKE '%sandbox%'
  AND h.name NOT ILIKE '%sample%'
  AND h.name NOT ILIKE 'unknown%'
  AND h.name NOT ILIKE '%internal%server%error%'
  AND h.name NOT ILIKE '%check availability%'
  AND h.name NOT ILIKE '%booking engine%'
  AND h.name NOT LIKE '% RMS %'
  AND h.name NOT LIKE 'RMS %'
  AND h.name NOT LIKE '% RMS'
  AND h.name !~ '^[0-9-]+$'
ORDER BY h.id
LIMIT :limit;


-- name: get_hotels_needing_geocoding_count^
-- Count hotels needing geocoding (uses same filters as above)
SELECT COUNT(*) as count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE 
  -- Not rejected
  h.status >= 0
  -- Missing location (need geocoding) - empty state, not abbreviations
  AND ((h.city IS NULL OR h.city = '') OR (h.state IS NULL OR h.state = ''))
  -- Engine filter
  AND (
    CAST(:engine AS TEXT) IS NOT NULL AND be.name = :engine  -- Filter by specific engine
    OR (CAST(:engine AS TEXT) IS NULL AND (be.name IS NULL OR be.name != 'Cloudbeds'))  -- Default: exclude Cloudbeds
  )
  AND (CAST(:source AS TEXT) IS NULL OR h.source LIKE :source)
  AND (CAST(:country AS TEXT) IS NULL OR h.country = :country)
  -- Name validation (same as launcher - see launch_conditions.py)
  AND h.name IS NOT NULL
  AND h.name != ''
  AND h.name != ' '
  AND LENGTH(h.name) > 2
  AND h.name NOT IN ('&#65279;', '-', '--', '---', '.', '..', 'Book Now', 'Book Now Pay on Check-in', 
      'Booking Engine', 'DEACTIVATED ACCOUNT DO NO BOOK', 'Error', 'Hotel', 'Hotel Booking Engine',
      'Modify/Cancel reservation', 'My reservations', 'Online Bookings', 'Rates', 'Reservation', 
      'Reservations', 'Search', 'View or Change a Reservation')
  AND h.name NOT ILIKE '%test%'
  AND h.name NOT ILIKE '%demo%'
  AND h.name NOT ILIKE '%sandbox%'
  AND h.name NOT ILIKE '%sample%'
  AND h.name NOT ILIKE 'unknown%'
  AND h.name NOT ILIKE '%internal%server%error%'
  AND h.name NOT ILIKE '%check availability%'
  AND h.name NOT ILIKE '%booking engine%'
  AND h.name NOT LIKE '% RMS %'
  AND h.name NOT LIKE 'RMS %'
  AND h.name NOT LIKE '% RMS'
  AND h.name !~ '^[0-9-]+$';


-- name: update_hotel_geocoding!
-- Update hotel with geocoding results from Serper Places
-- Updates location, contact info, and coordinates
UPDATE sadie_gtm.hotels
SET
    address = COALESCE(CAST(:address AS TEXT), address),
    city = COALESCE(CAST(:city AS TEXT), city),
    state = COALESCE(CAST(:state AS TEXT), state),
    country = COALESCE(CAST(:country AS TEXT), country),
    location = CASE
        WHEN CAST(:latitude AS FLOAT) IS NOT NULL AND CAST(:longitude AS FLOAT) IS NOT NULL
        THEN ST_SetSRID(ST_MakePoint(CAST(:longitude AS FLOAT), CAST(:latitude AS FLOAT)), 4326)::geography
        ELSE location
    END,
    phone_google = COALESCE(CAST(:phone AS TEXT), phone_google),
    email = COALESCE(CAST(:email AS TEXT), email),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;


-- ============================================================================
-- ENRICHMENT STATS QUERIES
-- ============================================================================

-- name: get_enrichment_stats_by_engine
-- Get enrichment stats grouped by booking engine
-- Shows data completeness metrics for target engines (Cloudbeds, Mews, RMS, SiteMinder)
SELECT
    bes.name AS engine_name,
    COUNT(*) AS total_hotels,
    COUNT(CASE WHEN h.status = 1 THEN 1 END) AS live,
    COUNT(CASE WHEN h.status = 0 THEN 1 END) AS pending,
    COUNT(CASE WHEN h.status = -1 THEN 1 END) AS error,
    COUNT(CASE WHEN h.name IS NOT NULL AND h.name != '' AND h.name NOT LIKE 'Unknown%' THEN 1 END) AS has_name,
    COUNT(CASE WHEN h.email IS NOT NULL AND h.email != '' THEN 1 END) AS has_email,
    COUNT(CASE WHEN h.phone_website IS NOT NULL AND h.phone_website != '' THEN 1 END) AS has_phone,
    COUNT(CASE WHEN (h.email IS NOT NULL AND h.email != '') OR (h.phone_website IS NOT NULL AND h.phone_website != '') THEN 1 END) AS has_contact,
    COUNT(CASE WHEN h.city IS NOT NULL AND h.city != '' THEN 1 END) AS has_city,
    COUNT(CASE WHEN h.state IS NOT NULL AND h.state != '' THEN 1 END) AS has_state,
    COUNT(CASE WHEN h.country IS NOT NULL AND h.country != '' THEN 1 END) AS has_country,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS has_website,
    COUNT(CASE WHEN h.address IS NOT NULL AND h.address != '' THEN 1 END) AS has_address,
    COUNT(CASE WHEN h.location IS NOT NULL THEN 1 END) AS has_coordinates,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL THEN 1 END) AS has_booking_engine,
    COUNT(CASE WHEN hrc.hotel_id IS NOT NULL THEN 1 END) AS has_room_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
WHERE hbe.booking_engine_id IN (3, 4, 12, 14)  -- Cloudbeds, Mews, RMS Cloud, SiteMinder
  AND h.country = 'United States'
GROUP BY bes.name
ORDER BY total_hotels DESC;

-- name: get_enrichment_stats_by_engine_source
-- Get enrichment stats grouped by booking engine, filtered by source pattern
SELECT
    bes.name AS engine_name,
    COUNT(*) AS total_hotels,
    COUNT(CASE WHEN h.status = 1 THEN 1 END) AS live,
    COUNT(CASE WHEN h.status = 0 THEN 1 END) AS pending,
    COUNT(CASE WHEN h.status = -1 THEN 1 END) AS error,
    COUNT(CASE WHEN h.name IS NOT NULL AND h.name != '' AND h.name NOT LIKE 'Unknown%' THEN 1 END) AS has_name,
    COUNT(CASE WHEN h.email IS NOT NULL AND h.email != '' THEN 1 END) AS has_email,
    COUNT(CASE WHEN h.phone_website IS NOT NULL AND h.phone_website != '' THEN 1 END) AS has_phone,
    COUNT(CASE WHEN (h.email IS NOT NULL AND h.email != '') OR (h.phone_website IS NOT NULL AND h.phone_website != '') THEN 1 END) AS has_contact,
    COUNT(CASE WHEN h.city IS NOT NULL AND h.city != '' THEN 1 END) AS has_city,
    COUNT(CASE WHEN h.state IS NOT NULL AND h.state != '' THEN 1 END) AS has_state,
    COUNT(CASE WHEN h.country IS NOT NULL AND h.country != '' THEN 1 END) AS has_country,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS has_website,
    COUNT(CASE WHEN h.address IS NOT NULL AND h.address != '' THEN 1 END) AS has_address,
    COUNT(CASE WHEN h.location IS NOT NULL THEN 1 END) AS has_coordinates,
    COUNT(CASE WHEN hbe.hotel_id IS NOT NULL THEN 1 END) AS has_booking_engine,
    COUNT(CASE WHEN hrc.hotel_id IS NOT NULL THEN 1 END) AS has_room_count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
WHERE hbe.booking_engine_id IN (3, 4, 12, 14)  -- Cloudbeds, Mews, RMS Cloud, SiteMinder
  AND h.country = 'United States'
  AND h.source LIKE :source_pattern
GROUP BY bes.name
ORDER BY total_hotels DESC;


-- ============================================================================
-- LOCATION NORMALIZATION QUERIES
-- ============================================================================

-- name: get_normalization_status^
-- Get counts of data needing location normalization
SELECT
    (SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country IN ('USA', 'US', 'AU', 'UK', 'GB', 'NZ', 'DE', 'FR', 'ES', 'IT', 'MX', 'JP', 'CN', 'BR')) AS countries_to_normalize,
    (SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country IN ('USA', 'United States') AND state IN ('VIC', 'NSW', 'QLD', 'TAS', 'ACT', 'SA', 'NT')) AS australian_in_usa,
    (SELECT COUNT(*) FROM sadie_gtm.hotels WHERE country IN ('USA', 'United States') AND state ~ '^[A-Z]{2}$' AND state NOT IN ('VIC', 'NSW', 'QLD', 'TAS', 'ACT', 'NT')) AS us_state_codes,
    (SELECT COUNT(*) FROM sadie_gtm.hotels WHERE state ~ '[0-9]') AS states_with_zips;

-- name: normalize_country!
-- Normalize a country code to full name
UPDATE sadie_gtm.hotels 
SET country = :new_country, updated_at = NOW() 
WHERE country = :old_country;

-- name: normalize_us_state!
-- Normalize a US state code to full name
UPDATE sadie_gtm.hotels 
SET state = :new_state, updated_at = NOW() 
WHERE state = :old_state AND country IN ('USA', 'United States');

-- name: fix_australian_state!
-- Fix Australian state incorrectly in USA - update both country and state
UPDATE sadie_gtm.hotels 
SET country = 'Australia', state = :new_state, updated_at = NOW()
WHERE country IN ('USA', 'United States') AND state = :old_state;

-- name: fix_state_with_zip!
-- Fix state that has zip code attached (e.g., "WY 83012" -> "Wyoming")
UPDATE sadie_gtm.hotels 
SET state = :new_state, updated_at = NOW() 
WHERE state = :old_state;

-- name: get_states_with_zips
-- Get distinct states that have zip codes attached
SELECT DISTINCT state 
FROM sadie_gtm.hotels 
WHERE state ~ '^[A-Z]{2} [0-9]+$';

-- name: get_country_counts
-- Get counts of each country code that needs normalization
SELECT country, COUNT(*) as cnt 
FROM sadie_gtm.hotels 
WHERE country IN ('USA', 'US', 'AU', 'UK', 'GB', 'NZ', 'DE', 'FR', 'ES', 'IT', 'MX', 'JP', 'CN', 'BR', 'IN', 'AR', 'CL', 'CO', 'PE', 'ZA', 'EG', 'MA', 'KE', 'TH', 'VN', 'ID', 'MY', 'SG', 'PH', 'KR', 'TW', 'HK', 'AE', 'IL', 'TR', 'GR', 'PT', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'PL', 'CZ', 'HU', 'RO', 'IE', 'PR')
GROUP BY country 
ORDER BY cnt DESC;

-- ============================================================================
-- STATE EXTRACTION QUERIES
-- ============================================================================

-- name: get_us_hotels_missing_state
-- Get US hotels that have address but no state (for state extraction from address)
SELECT id, name, address, city, country
FROM sadie_gtm.hotels
WHERE (state IS NULL OR state = '')
  AND address IS NOT NULL
  AND address != ''
  AND (country = 'United States' OR country = 'USA' OR country = 'US' OR country IS NULL)
ORDER BY id
LIMIT :limit;

-- name: batch_update_extracted_states!
-- Batch update hotels with extracted state data
UPDATE sadie_gtm.hotels h
SET state = v.state,
    country = CASE WHEN h.country IS NULL THEN 'United States' ELSE h.country END,
    updated_at = NOW()
FROM (
    SELECT * FROM unnest(:hotel_ids::int[], :states::text[]) AS t(hotel_id, state)
) v
WHERE h.id = v.hotel_id;
