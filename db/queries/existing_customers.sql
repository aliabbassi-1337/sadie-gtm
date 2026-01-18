-- name: get_all_existing_customers
-- Get all existing customers with location for proximity calculation
SELECT
    id,
    name,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude,
    status,
    created_at
FROM sadie_gtm.existing_customers
WHERE location IS NOT NULL
  AND status = 'active';

-- name: find_nearest_customer^
-- Find the nearest existing customer to a hotel within max_distance_km
-- Uses PostGIS ST_DWithin for efficient spatial query and ST_Distance for exact distance
SELECT
    ec.id AS existing_customer_id,
    ec.name AS customer_name,
    ST_Distance(h.location, ec.location) / 1000 AS distance_km
FROM sadie_gtm.hotels h
CROSS JOIN sadie_gtm.existing_customers ec
WHERE h.id = :hotel_id
  AND h.location IS NOT NULL
  AND ec.location IS NOT NULL
  AND ec.status = 'active'
  AND ST_DWithin(h.location, ec.location, :max_distance_meters)
LIMIT 1;
