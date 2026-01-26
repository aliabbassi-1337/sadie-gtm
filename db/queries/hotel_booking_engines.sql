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
    detection_method,
    status,
    detected_at,
    updated_at
) VALUES (
    :hotel_id,
    :booking_engine_id,
    :booking_url,
    :detection_method,
    :status,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (hotel_id) DO UPDATE SET
    booking_engine_id = COALESCE(EXCLUDED.booking_engine_id, hotel_booking_engines.booking_engine_id),
    booking_url = COALESCE(EXCLUDED.booking_url, hotel_booking_engines.booking_url),
    detection_method = COALESCE(EXCLUDED.detection_method, hotel_booking_engines.detection_method),
    status = EXCLUDED.status,
    updated_at = CURRENT_TIMESTAMP;
