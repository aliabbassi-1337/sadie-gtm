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
