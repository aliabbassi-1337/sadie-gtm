-- name: insert_hotel_booking_engine!
-- Link hotel to detected booking engine or OTA
-- status: -1=failed (non-retriable), 1=success
INSERT INTO sadie_gtm.hotel_booking_engines (
    hotel_id,
    booking_engine_id,
    booking_url,
    detection_method,
    status,
    ota_name,
    detected_at,
    updated_at
) VALUES (
    :hotel_id,
    :booking_engine_id,
    :booking_url,
    :detection_method,
    :status,
    :ota_name,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (hotel_id) DO UPDATE SET
    booking_engine_id = COALESCE(EXCLUDED.booking_engine_id, hotel_booking_engines.booking_engine_id),
    booking_url = COALESCE(EXCLUDED.booking_url, hotel_booking_engines.booking_url),
    detection_method = COALESCE(EXCLUDED.detection_method, hotel_booking_engines.detection_method),
    status = EXCLUDED.status,
    ota_name = COALESCE(EXCLUDED.ota_name, hotel_booking_engines.ota_name),
    updated_at = CURRENT_TIMESTAMP;
