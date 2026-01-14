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
-- Criteria: status=0 (scraped), website not null, not a big chain
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
WHERE status = 0
  AND website IS NOT NULL
  AND website != ''
  AND LOWER(website) NOT LIKE '%marriott.com%'
  AND LOWER(website) NOT LIKE '%hilton.com%'
  AND LOWER(website) NOT LIKE '%ihg.com%'
  AND LOWER(website) NOT LIKE '%hyatt.com%'
  AND LOWER(website) NOT LIKE '%wyndham.com%'
  AND LOWER(website) NOT LIKE '%choicehotels.com%'
  AND LOWER(website) NOT LIKE '%bestwestern.com%'
  AND LOWER(website) NOT LIKE '%radissonhotels.com%'
  AND LOWER(website) NOT LIKE '%accor.com%'
LIMIT :limit;

-- name: claim_hotels_for_detection
-- Atomically claim hotels for processing (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple workers grab different rows
-- Sets status=10 (processing) to mark as claimed
UPDATE hotels
SET status = 10, updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT id FROM hotels
    WHERE status = 0
      AND website IS NOT NULL
      AND website != ''
      AND LOWER(website) NOT LIKE '%marriott.com%'
      AND LOWER(website) NOT LIKE '%hilton.com%'
      AND LOWER(website) NOT LIKE '%ihg.com%'
      AND LOWER(website) NOT LIKE '%hyatt.com%'
      AND LOWER(website) NOT LIKE '%wyndham.com%'
      AND LOWER(website) NOT LIKE '%choicehotels.com%'
      AND LOWER(website) NOT LIKE '%bestwestern.com%'
      AND LOWER(website) NOT LIKE '%radissonhotels.com%'
      AND LOWER(website) NOT LIKE '%accor.com%'
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

-- name: reset_stale_processing_hotels!
-- Reset hotels stuck in processing state (status=10) for more than N minutes
-- Run this periodically to recover from crashed workers
UPDATE hotels
SET status = 0, updated_at = CURRENT_TIMESTAMP
WHERE status = 10
  AND updated_at < NOW() - INTERVAL '30 minutes';

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
