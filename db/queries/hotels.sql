-- name: get_hotel_by_id^
-- Get single hotel by ID with location coordinates
SELECT
    id,
    name,
    website,
    city,
    state,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude,
    location_status,
    status,
    created_at
FROM hotels
WHERE id = $1;
