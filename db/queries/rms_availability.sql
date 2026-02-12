-- RMS Availability Enrichment Queries
-- Used to check if Australia RMS hotels have availability

-- name: get_australia_rms_hotels_needing_availability_check
-- Get Australia RMS hotels that need availability check
-- RMS Cloud booking_engine_id = 12, Australia country codes
SELECT 
    h.id AS hotel_id,
    h.name,
    h.city,
    h.state,
    hbe.booking_url,
    be.id AS booking_engine_id
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND hbe.has_availability IS NULL
  AND h.country IN ('AU', 'Australia')
ORDER BY h.id
LIMIT :limit;

-- name: get_australia_rms_hotels_all
-- Get ALL Australia RMS hotels (for force re-check)
SELECT 
    h.id AS hotel_id,
    h.name,
    h.city,
    h.state,
    hbe.booking_url,
    be.id AS booking_engine_id
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia')
ORDER BY h.id
LIMIT :limit;

-- name: update_rms_availability_status!
-- Update availability status for a hotel
UPDATE sadie_gtm.hotel_booking_engines
SET has_availability = :has_availability,
    availability_checked_at = NOW()
WHERE hotel_id = :hotel_id;

-- name: count_australia_rms_pending_availability^
-- Count Australia RMS hotels pending availability check
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND hbe.has_availability IS NULL
  AND h.country IN ('AU', 'Australia');

-- name: get_rms_availability_stats^
-- Get availability check statistics for Australia RMS hotels
SELECT 
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE has_availability IS NULL) AS pending,
    COUNT(*) FILTER (WHERE has_availability = TRUE) AS has_availability,
    COUNT(*) FILTER (WHERE has_availability = FALSE) AS no_availability
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.hotels h ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE be.name = 'RMS Cloud'
  AND h.country IN ('AU', 'Australia');
