-- ============================================================================
-- ROOM COUNT ENRICHMENT QUERIES
-- ============================================================================
-- Status values for hotels.status:
--   -2 = Location mismatch (rejected)
--   -1 = No booking engine found (rejected)
--    0 = Pending/Not ready
--    1 = Launched and live
--
-- Status values for hotel_room_count.status:
--   -1 = Processing (claimed by worker)
--    0 = Failed
--    1 = Success

-- name: get_hotels_pending_enrichment
-- Get hotels that need room count enrichment (read-only, for status display)
-- Criteria: status=0 (pending), detected (in hotel_booking_engines), has website, not in hotel_room_count
SELECT
    h.id,
    h.name,
    h.website,
    h.created_at,
    h.updated_at
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.status = 0
  AND h.website IS NOT NULL
  AND h.website != ''
  AND hrc.id IS NULL
ORDER BY h.updated_at DESC
LIMIT :limit;

-- name: claim_hotels_for_enrichment
-- Atomically claim hotels for enrichment (multi-worker safe)
-- Inserts status=-1 (processing) records, returns claimed hotel IDs
-- Uses ON CONFLICT DO NOTHING so only one worker claims each hotel
WITH pending AS (
    SELECT h.id, h.name, h.website, h.created_at, h.updated_at
    FROM hotels h
    JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
    LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
    WHERE h.status = 0
      AND h.website IS NOT NULL
      AND h.website != ''
      AND hrc.id IS NULL
    ORDER BY h.updated_at DESC
    LIMIT :limit
),
claimed AS (
    INSERT INTO hotel_room_count (hotel_id, status)
    SELECT id, -1 FROM pending
    ON CONFLICT (hotel_id) DO NOTHING
    RETURNING hotel_id
)
SELECT p.id, p.name, p.website, p.created_at, p.updated_at
FROM pending p
JOIN claimed c ON p.id = c.hotel_id;

-- name: reset_stale_enrichment_claims!
-- Reset claims stuck in processing state (status=-1) for more than N minutes
-- Run this periodically to recover from crashed workers
DELETE FROM hotel_room_count
WHERE status = -1
  AND enriched_at < NOW() - INTERVAL '30 minutes';

-- name: get_pending_enrichment_count^
-- Count hotels waiting for enrichment (status=0, detected, has website, not in hotel_room_count)
SELECT COUNT(*) AS count
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
LEFT JOIN hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.status = 0
  AND h.website IS NOT NULL
  AND h.website != ''
  AND hrc.id IS NULL;

-- name: insert_room_count<!
-- Insert/update room count for a hotel
-- status: -1=processing, 0=failed, 1=success
INSERT INTO hotel_room_count (hotel_id, room_count, source, confidence, status)
VALUES (:hotel_id, :room_count, :source, :confidence, :status)
ON CONFLICT (hotel_id) DO UPDATE SET
    room_count = EXCLUDED.room_count,
    source = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    status = EXCLUDED.status,
    enriched_at = CURRENT_TIMESTAMP
RETURNING id;

-- name: get_room_count_by_hotel_id^
-- Get room count for a specific hotel
SELECT id, hotel_id, room_count, source, confidence, status, enriched_at
FROM hotel_room_count
WHERE hotel_id = :hotel_id;

-- name: delete_room_count!
-- Delete room count for a hotel (for testing)
DELETE FROM hotel_room_count
WHERE hotel_id = :hotel_id;

-- ============================================================================
-- CUSTOMER PROXIMITY QUERIES
-- ============================================================================
-- Only process hotels that have been detected and enriched

-- name: get_hotels_pending_proximity
-- Get hotels that need customer proximity calculation (read-only, for status display)
-- Criteria: status=0 (pending), detected (in hotel_booking_engines), has room count (status=1), has location, not in hotel_customer_proximity
SELECT
    h.id,
    h.name,
    ST_Y(h.location::geometry) AS latitude,
    ST_X(h.location::geometry) AS longitude,
    h.created_at,
    h.updated_at
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.status = 0
  AND h.location IS NOT NULL
  AND hcp.id IS NULL
ORDER BY h.updated_at DESC
LIMIT :limit;

-- name: get_pending_proximity_count^
-- Count hotels waiting for proximity calculation (status=0, detected, has room count, has location, not in hotel_customer_proximity)
SELECT COUNT(*) AS count
FROM hotels h
JOIN hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN hotel_room_count hrc ON h.id = hrc.hotel_id AND hrc.status = 1
LEFT JOIN hotel_customer_proximity hcp ON h.id = hcp.hotel_id
WHERE h.status = 0
  AND h.location IS NOT NULL
  AND hcp.id IS NULL;

-- name: get_all_existing_customers
-- Get all existing customers with location for proximity calculation
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
