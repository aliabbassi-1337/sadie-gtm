-- name: get_hotels_pending_proximity
-- Get hotels that need customer proximity calculation (read-only, for status display)
-- Criteria: status=0 (pending), successfully detected (hbe.status=1), has location, not in hotel_customer_proximity
-- Note: Does NOT depend on room count - proximity runs in parallel with room count enrichment
SELECT
    h.id,
    h.name,
    ST_Y(h.location::geometry) AS latitude,
    ST_X(h.location::geometry) AS longitude,
    h.created_at,
    h.updated_at
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.status = 0
  AND h.location IS NOT NULL
  AND hcp.id IS NULL
LIMIT :limit;

-- name: get_pending_proximity_count^
-- Count hotels waiting for proximity calculation (status=0, successfully detected, has location, not in hotel_customer_proximity)
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.status = 0
  AND h.location IS NOT NULL
  AND hcp.id IS NULL;

-- name: insert_customer_proximity<!
-- Insert customer proximity for a hotel
INSERT INTO sadie_gtm.hotel_customer_proximity (hotel_id, existing_customer_id, distance_km)
VALUES (:hotel_id, :existing_customer_id, :distance_km)
ON CONFLICT (hotel_id) DO UPDATE SET
    existing_customer_id = EXCLUDED.existing_customer_id,
    distance_km = EXCLUDED.distance_km,
    computed_at = CURRENT_TIMESTAMP
RETURNING id;

-- name: get_customer_proximity_by_hotel_id^
-- Get customer proximity for a specific hotel
SELECT
    hcp.id,
    hcp.hotel_id,
    hcp.existing_customer_id,
    hcp.distance_km,
    hcp.computed_at,
    ec.name AS customer_name
FROM sadie_gtm.hotel_customer_proximity hcp
JOIN sadie_gtm.existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE hcp.hotel_id = :hotel_id;

-- name: delete_customer_proximity!
-- Delete customer proximity for a hotel (for testing)
DELETE FROM sadie_gtm.hotel_customer_proximity
WHERE hotel_id = :hotel_id;
