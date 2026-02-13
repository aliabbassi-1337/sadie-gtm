-- RMS Availability Enrichment Queries
-- Used to check if Australia RMS hotels have availability via ibe12 API

-- name: get_rms_hotels_pending_availability
-- Get Australia RMS hotels that need availability check (has_availability IS NULL)
SELECT
    h.id AS hotel_id,
    h.name,
    h.city,
    h.state,
    hbe.booking_url
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia')
  AND h.status = 1
  AND hbe.status = 1
  AND hbe.has_availability IS NULL
ORDER BY h.id
LIMIT :limit;

-- name: get_rms_hotels_all_for_recheck
-- Get ALL Australia RMS hotels for force re-check
SELECT
    h.id AS hotel_id,
    h.name,
    h.city,
    h.state,
    hbe.booking_url
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia')
  AND h.status = 1
  AND hbe.status = 1
ORDER BY h.id
LIMIT :limit;

-- name: reset_rms_availability!
-- Reset all Australia RMS availability results to NULL
UPDATE sadie_gtm.hotel_booking_engines AS hbe
SET has_availability = NULL,
    availability_checked_at = NULL
FROM sadie_gtm.hotels h
JOIN sadie_gtm.booking_engines be ON be.name = 'RMS Cloud'
WHERE hbe.hotel_id = h.id
  AND hbe.booking_engine_id = be.id
  AND h.country IN ('AU', 'Australia')
  AND h.status = 1
  AND hbe.status = 1
  AND hbe.has_availability IS NOT NULL;

-- name: get_rms_availability_stats^
-- Get availability check statistics for Australia RMS hotels
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE hbe.has_availability IS NULL) AS pending,
    COUNT(*) FILTER (WHERE hbe.has_availability = TRUE) AS has_availability,
    COUNT(*) FILTER (WHERE hbe.has_availability = FALSE) AS no_availability
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.hotels h ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia')
  AND h.status = 1
  AND hbe.status = 1;

-- name: get_rms_available_sample
-- Get random sample of hotels marked as available (for verification)
SELECT h.id AS hotel_id, h.name, hbe.booking_url
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.hotels h ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia')
  AND h.status = 1 AND hbe.status = 1
  AND hbe.has_availability = TRUE
ORDER BY RANDOM()
LIMIT :limit;

-- name: get_rms_no_availability_sample
-- Get random sample of hotels marked as no availability (for verification)
SELECT h.id AS hotel_id, h.name, hbe.booking_url
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.hotels h ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia')
  AND h.status = 1 AND hbe.status = 1
  AND hbe.has_availability = FALSE
ORDER BY RANDOM()
LIMIT :limit;
