-- name: get_hotel_by_booking_url^
-- Find hotel by booking URL - returns hotel_id if this booking URL already exists
-- Used for deduplication when ingesting crawled booking engine URLs
SELECT 
    hbe.hotel_id,
    hbe.booking_engine_id,
    hbe.booking_url,
    hbe.detection_method,
    h.name,
    h.website,
    h.status
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.hotels h ON h.id = hbe.hotel_id
WHERE hbe.booking_url = :booking_url
LIMIT 1;

-- name: insert_hotel_booking_engine!
-- Link hotel to detected booking engine
-- status: -1=failed (non-retriable), 1=success
INSERT INTO sadie_gtm.hotel_booking_engines (
    hotel_id,
    booking_engine_id,
    booking_url,
    engine_property_id,
    detection_method,
    status,
    detected_at,
    updated_at
) VALUES (
    :hotel_id,
    :booking_engine_id,
    :booking_url,
    :engine_property_id,
    :detection_method,
    :status,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (hotel_id) DO UPDATE SET
    booking_engine_id = COALESCE(EXCLUDED.booking_engine_id, hotel_booking_engines.booking_engine_id),
    booking_url = COALESCE(EXCLUDED.booking_url, hotel_booking_engines.booking_url),
    engine_property_id = COALESCE(EXCLUDED.engine_property_id, hotel_booking_engines.engine_property_id),
    detection_method = COALESCE(EXCLUDED.detection_method, hotel_booking_engines.detection_method),
    status = EXCLUDED.status,
    updated_at = CURRENT_TIMESTAMP;

-- name: get_hotel_by_engine_property_id^
-- Look up hotel by booking engine property ID (slug/UUID/numeric ID)
SELECT h.id, h.name, h.website
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE hbe.booking_engine_id = :booking_engine_id
  AND hbe.engine_property_id = :engine_property_id;

-- name: get_hotels_needing_names
-- Get hotels with booking URLs but missing/placeholder names
-- Used by name enrichment workers to scrape hotel names from booking pages
SELECT 
    h.id,
    h.name,
    hbe.booking_url,
    hbe.engine_property_id as slug,
    be.name as engine_name
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%')
  AND hbe.booking_url IS NOT NULL
  AND hbe.booking_url != ''
ORDER BY h.id
LIMIT :limit;

-- name: update_hotel_name!
-- Update hotel name after scraping from booking page
UPDATE sadie_gtm.hotels
SET name = :name, updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: get_existing_booking_urls
-- Bulk check which booking URLs already exist
-- Used for fast deduplication during crawl ingestion
-- Note: Uses ANY() for array parameter - call with booking_urls=list
SELECT booking_url 
FROM sadie_gtm.hotel_booking_engines 
WHERE booking_url = ANY(:booking_urls);

-- name: get_hotels_needing_addresses
-- Get hotels with booking URLs but missing location data
-- Used by address enrichment workers to scrape location from booking pages
SELECT 
    h.id,
    h.name,
    h.city,
    h.state,
    h.country,
    hbe.booking_url,
    hbe.engine_property_id as slug,
    be.name as engine_name
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE (h.city IS NULL OR h.city = '' OR h.state IS NULL OR h.state = '')
  AND hbe.booking_url IS NOT NULL
  AND hbe.booking_url != ''
ORDER BY h.id
LIMIT :limit;

-- name: get_hotels_needing_enrichment
-- Get hotels needing either name or address enrichment
-- type param: 'names' = missing names, 'addresses' = missing location, 'both' = either
-- NOTE: Excludes Cloudbeds hotels - they have their own dedicated enrichment queue
SELECT 
    h.id,
    h.name,
    h.city,
    h.state,
    h.country,
    hbe.booking_url,
    hbe.engine_property_id as slug,
    be.name as engine_name,
    CASE WHEN (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%') THEN true ELSE false END as needs_name,
    CASE WHEN (h.city IS NULL OR h.city = '' OR h.state IS NULL OR h.state = '') THEN true ELSE false END as needs_address
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE hbe.booking_url IS NOT NULL
  AND hbe.booking_url != ''
  AND be.name != 'Cloudbeds'  -- Cloudbeds has dedicated queue
  AND (
    (:enrich_type = 'names' AND (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%'))
    OR (:enrich_type = 'addresses' AND (h.city IS NULL OR h.city = '' OR h.state IS NULL OR h.state = ''))
    OR (:enrich_type = 'both' AND (
      (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%')
      OR (h.city IS NULL OR h.city = '' OR h.state IS NULL OR h.state = '')
    ))
  )
ORDER BY h.id
LIMIT :limit;

-- name: update_hotel_location!
-- Update hotel location after scraping from booking page
-- Only updates fields that are provided (non-null)
UPDATE sadie_gtm.hotels
SET 
    address = COALESCE(:address, address),
    city = COALESCE(:city, city),
    state = COALESCE(:state, state),
    country = COALESCE(:country, country),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- name: update_hotel_name_and_location!
-- Update both name and location in one call
-- Only updates fields that are provided (non-null)
UPDATE sadie_gtm.hotels
SET 
    name = CASE WHEN :name IS NOT NULL AND :name != '' THEN :name ELSE name END,
    address = COALESCE(:address, address),
    city = COALESCE(:city, city),
    state = COALESCE(:state, state),
    country = COALESCE(:country, country),
    phone_website = COALESCE(:phone, phone_website),
    email = COALESCE(:email, email),
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- ============================================================================
-- CLOUDBEDS ENRICHMENT QUERIES
-- ============================================================================

-- name: get_cloudbeds_hotels_needing_enrichment
-- Get hotels with Cloudbeds booking URLs that need name or location enrichment
SELECT h.id, h.name, h.city, h.state, h.country, h.address,
       hbe.booking_url, hbe.engine_property_id as slug
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name ILIKE '%cloudbeds%'
  AND hbe.booking_url IS NOT NULL
  AND hbe.booking_url != ''
  AND (
      (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%')
      OR (h.city IS NULL OR h.city = '')
  )
ORDER BY h.id
LIMIT :limit;

-- name: get_cloudbeds_hotels_needing_enrichment_count^
-- Count Cloudbeds hotels needing enrichment
SELECT COUNT(*)
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name ILIKE '%cloudbeds%'
  AND hbe.booking_url IS NOT NULL
  AND hbe.booking_url != ''
  AND (
      (h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%')
      OR (h.city IS NULL OR h.city = '')
  );

-- name: get_cloudbeds_hotels_total_count^
-- Count total Cloudbeds hotels
SELECT COUNT(*)
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name ILIKE '%cloudbeds%';
