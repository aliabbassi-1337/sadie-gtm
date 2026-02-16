-- Deep-link URL generation queries

-- name: get_hotel_booking_info^
-- Get the primary booking URL for a hotel
SELECT hbe.booking_url, hbe.engine_property_id, be.name as engine_name
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE hbe.hotel_id = :hotel_id AND hbe.booking_url IS NOT NULL AND hbe.status = 1
LIMIT 1;
