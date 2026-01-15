-- name: insert_hotel_booking_engine!
-- Link hotel to detected booking engine
-- status: -1=failed (non-retriable), 1=success
INSERT INTO hotel_booking_engines (
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
