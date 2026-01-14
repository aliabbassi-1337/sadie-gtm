-- name: get_hotels_pending_enrichment
-- Get hotels that need room count enrichment
-- Criteria: status=1 (detected), has website, not already enriched
-- Only select columns needed for enrichment
SELECT
    h.id,
    h.name,
    h.website,
    h.status,
    h.created_at,
    h.updated_at
FROM hotels h
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.status = 1
  AND h.website IS NOT NULL
  AND h.website != ''
  AND hrc.id IS NULL
ORDER BY h.updated_at DESC
LIMIT :limit;

-- name: claim_hotels_for_enrichment
-- Atomically claim hotels for enrichment (multi-worker safe)
-- Uses FOR UPDATE SKIP LOCKED so multiple workers grab different rows
-- Sets status=2 (enriching) to mark as claimed
-- Only select columns needed for enrichment
UPDATE hotels
SET status = 2, updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT h.id FROM hotels h
    WHERE h.status = 1
      AND h.website IS NOT NULL
      AND h.website != ''
      AND NOT EXISTS (
          SELECT 1 FROM hotel_room_count hrc WHERE hrc.hotel_id = h.id
      )
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
)
RETURNING
    id,
    name,
    website,
    status,
    created_at,
    updated_at;

-- name: reset_stale_enriching_hotels!
-- Reset hotels stuck in enriching state (status=2) for more than N minutes
-- Run this periodically to recover from crashed workers
UPDATE hotels
SET status = 1, updated_at = CURRENT_TIMESTAMP
WHERE status = 2
  AND updated_at < NOW() - INTERVAL '30 minutes';

-- name: get_pending_enrichment_count^
-- Count hotels waiting for enrichment (status=1, not yet enriched)
SELECT COUNT(*) AS count
FROM hotels h
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.status = 1
  AND h.website IS NOT NULL
  AND h.website != ''
  AND hrc.id IS NULL;

-- name: insert_room_count<!
-- Insert room count for a hotel
INSERT INTO hotel_room_count (hotel_id, room_count, source, confidence)
VALUES (:hotel_id, :room_count, :source, :confidence)
ON CONFLICT (hotel_id) DO UPDATE SET
    room_count = EXCLUDED.room_count,
    source = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    enriched_at = CURRENT_TIMESTAMP
RETURNING id;

-- name: get_room_count_by_hotel_id^
-- Get room count for a specific hotel
SELECT id, hotel_id, room_count, source, confidence, enriched_at
FROM hotel_room_count
WHERE hotel_id = :hotel_id;

-- name: delete_room_count!
-- Delete room count for a hotel (for testing)
DELETE FROM hotel_room_count
WHERE hotel_id = :hotel_id;

-- name: update_hotel_enrichment_status!
-- Update hotel status after enrichment
UPDATE hotels
SET status = :status, updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;

-- ============================================================================
-- CUSTOMER PROXIMITY QUERIES
-- ============================================================================

-- name: get_hotels_pending_proximity
-- Get hotels that need customer proximity calculation
-- Criteria: has location, not already in hotel_customer_proximity
-- Only select columns needed for proximity calculation
SELECT
    h.id,
    h.name,
    ST_Y(h.location::geometry) AS latitude,
    ST_X(h.location::geometry) AS longitude,
    h.status,
    h.created_at,
    h.updated_at
FROM hotels h
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.location IS NOT NULL
  AND hcp.id IS NULL
ORDER BY h.updated_at DESC
LIMIT :limit;

-- name: get_all_existing_customers
-- Get all existing customers with location for proximity calculation
-- Only select columns needed
SELECT
    id,
    name,
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude,
    status,
    created_at
FROM existing_customers
WHERE location IS NOT NULL
  AND status = 'active';

-- name: find_nearest_customer^
-- Find the nearest existing customer to a hotel within max_distance_km
-- Uses PostGIS ST_DWithin for efficient spatial query and ST_Distance for exact distance
SELECT
    ec.id AS existing_customer_id,
    ec.name AS customer_name,
    ST_Distance(h.location, ec.location) / 1000 AS distance_km
FROM hotels h
CROSS JOIN existing_customers ec
WHERE h.id = :hotel_id
  AND h.location IS NOT NULL
  AND ec.location IS NOT NULL
  AND ec.status = 'active'
  AND ST_DWithin(h.location, ec.location, :max_distance_meters)
ORDER BY ST_Distance(h.location, ec.location)
LIMIT 1;

-- name: insert_customer_proximity<!
-- Insert customer proximity for a hotel
INSERT INTO hotel_customer_proximity (hotel_id, existing_customer_id, distance_km)
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
FROM hotel_customer_proximity hcp
JOIN existing_customers ec ON hcp.existing_customer_id = ec.id
WHERE hcp.hotel_id = :hotel_id;

-- name: delete_customer_proximity!
-- Delete customer proximity for a hotel (for testing)
DELETE FROM hotel_customer_proximity
WHERE hotel_id = :hotel_id;

-- name: get_pending_proximity_count^
-- Count hotels waiting for proximity calculation (has location)
SELECT COUNT(*) AS count
FROM hotels h
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.location IS NOT NULL
  AND hcp.id IS NULL;
