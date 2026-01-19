-- Status values for hotel_room_count.status:
--   -1 = Processing (claimed by worker)
--    0 = Failed
--    1 = Success

-- name: get_hotels_pending_enrichment
-- Get hotels that need room count enrichment (read-only, for status display)
-- Criteria: successfully detected (hbe.status=1), has website, not in hotel_room_count
-- Note: No status filter - can enrich hotels at any stage
SELECT
    h.id,
    h.name,
    h.website,
    h.created_at,
    h.updated_at
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.website IS NOT NULL
  AND h.website != ''
  AND hrc.id IS NULL
LIMIT :limit;

-- name: claim_hotels_for_enrichment
-- Atomically claim hotels for enrichment (multi-worker safe)
-- Inserts status=-1 (processing) records, returns claimed hotel IDs
-- Uses ON CONFLICT DO NOTHING so only one worker claims each hotel
-- Note: No status filter - can enrich hotels at any stage
-- Note: No tier filter - enriches hotels with any booking engine
WITH pending AS (
    SELECT h.id, h.name, h.website, h.created_at, h.updated_at
    FROM sadie_gtm.hotels h
    JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
    JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
    LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
    WHERE h.website IS NOT NULL
      AND h.website != ''
      AND hrc.id IS NULL
    LIMIT :limit
),
claimed AS (
    INSERT INTO sadie_gtm.hotel_room_count (hotel_id, status)
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
DELETE FROM sadie_gtm.hotel_room_count
WHERE status = -1
  AND enriched_at < NOW() - INTERVAL '30 minutes';

-- name: get_pending_enrichment_count^
-- Count hotels waiting for enrichment (successfully detected, has website, not in hotel_room_count)
-- Note: No status filter - can enrich hotels at any stage
-- Note: No tier filter - counts hotels with any booking engine
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
LEFT JOIN sadie_gtm.hotel_room_count hrc ON h.id = hrc.hotel_id
WHERE h.website IS NOT NULL
  AND h.website != ''
  AND hrc.id IS NULL;

-- name: insert_room_count<!
-- Insert/update room count for a hotel
-- status: -1=processing, 0=failed, 1=success
INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, confidence, status)
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
FROM sadie_gtm.hotel_room_count
WHERE hotel_id = :hotel_id;

-- name: delete_room_count!
-- Delete room count for a hotel (for testing)
DELETE FROM sadie_gtm.hotel_room_count
WHERE hotel_id = :hotel_id;
