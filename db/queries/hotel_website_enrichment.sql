-- Status values for hotel_website_enrichment.status:
--   -1 = Processing (claimed by worker)
--    0 = Failed
--    1 = Success

-- name: get_hotels_pending_website_enrichment
-- Get hotels that need website enrichment (read-only, for status display)
-- Criteria: no website, has name and city, not in hotel_website_enrichment
SELECT
    h.id,
    h.name,
    h.city,
    h.state,
    h.address,
    h.source,
    h.created_at,
    h.updated_at
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_website_enrichment hwe ON h.id = hwe.hotel_id
WHERE (h.website IS NULL OR h.website = '')
  AND h.name IS NOT NULL
  AND h.city IS NOT NULL
  AND hwe.id IS NULL
LIMIT :limit;

-- name: claim_hotels_for_website_enrichment
-- Atomically claim hotels for website enrichment (multi-worker safe)
-- Inserts status=-1 (processing) records, returns claimed hotel IDs
-- Uses ON CONFLICT DO NOTHING so only one worker claims each hotel
WITH pending AS (
    SELECT h.id, h.name, h.city, h.state, h.address, h.source, h.created_at, h.updated_at
    FROM sadie_gtm.hotels h
    LEFT JOIN sadie_gtm.hotel_website_enrichment hwe ON h.id = hwe.hotel_id
    WHERE (h.website IS NULL OR h.website = '')
      AND h.name IS NOT NULL
      AND h.city IS NOT NULL
      AND hwe.id IS NULL
    LIMIT :limit
),
claimed AS (
    INSERT INTO sadie_gtm.hotel_website_enrichment (hotel_id, status)
    SELECT id, -1 FROM pending
    ON CONFLICT (hotel_id) DO NOTHING
    RETURNING hotel_id
)
SELECT p.id, p.name, p.city, p.state, p.address, p.source, p.created_at, p.updated_at
FROM pending p
JOIN claimed c ON p.id = c.hotel_id;

-- name: claim_hotels_for_website_enrichment_filtered
-- Atomically claim hotels for website enrichment with optional source/state filters
-- Uses ON CONFLICT DO NOTHING so only one worker claims each hotel
WITH pending AS (
    SELECT h.id, h.name, h.city, h.state, h.address, h.source, h.created_at, h.updated_at
    FROM sadie_gtm.hotels h
    LEFT JOIN sadie_gtm.hotel_website_enrichment hwe ON h.id = hwe.hotel_id
    WHERE (h.website IS NULL OR h.website = '')
      AND h.name IS NOT NULL
      AND h.city IS NOT NULL
      AND hwe.id IS NULL
      AND (CAST(:source_filter AS TEXT) IS NULL OR h.source LIKE :source_filter)
      AND (CAST(:state_filter AS TEXT) IS NULL OR h.state = :state_filter)
    ORDER BY h.created_at DESC
    LIMIT :limit
),
claimed AS (
    INSERT INTO sadie_gtm.hotel_website_enrichment (hotel_id, status)
    SELECT id, -1 FROM pending
    ON CONFLICT (hotel_id) DO NOTHING
    RETURNING hotel_id
)
SELECT p.id, p.name, p.city, p.state, p.address, p.source, p.created_at, p.updated_at
FROM pending p
JOIN claimed c ON p.id = c.hotel_id;

-- name: reset_stale_website_enrichment_claims!
-- Reset claims stuck in processing state (status=-1) for more than N minutes
-- Run this periodically to recover from crashed workers
DELETE FROM sadie_gtm.hotel_website_enrichment
WHERE status = -1
  AND enriched_at < NOW() - INTERVAL '30 minutes';

-- name: get_pending_website_enrichment_count^
-- Count hotels waiting for website enrichment
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_website_enrichment hwe ON h.id = hwe.hotel_id
WHERE (h.website IS NULL OR h.website = '')
  AND h.name IS NOT NULL
  AND h.city IS NOT NULL
  AND hwe.id IS NULL;

-- name: update_website_enrichment_status!
-- Update website enrichment status after processing
UPDATE sadie_gtm.hotel_website_enrichment
SET status = :status,
    source = :source,
    enriched_at = CURRENT_TIMESTAMP
WHERE hotel_id = :hotel_id;

-- name: get_website_enrichment_stats^
-- Get stats for website enrichment progress
SELECT
    COUNT(*) AS total,
    COUNT(CASE WHEN h.website IS NOT NULL AND h.website != '' THEN 1 END) AS with_website,
    COUNT(CASE WHEN h.website IS NULL OR h.website = '' THEN 1 END) AS without_website,
    COUNT(CASE WHEN hwe.status = 1 THEN 1 END) AS enriched_success,
    COUNT(CASE WHEN hwe.status = 0 THEN 1 END) AS enriched_failed,
    COUNT(CASE WHEN hwe.status = -1 THEN 1 END) AS in_progress
FROM sadie_gtm.hotels h
LEFT JOIN sadie_gtm.hotel_website_enrichment hwe ON h.id = hwe.hotel_id
WHERE :source_prefix IS NULL OR h.source LIKE :source_prefix;

-- name: get_website_enrichment_by_hotel_id^
-- Get website enrichment status for a specific hotel
SELECT id, hotel_id, status, source, enriched_at
FROM sadie_gtm.hotel_website_enrichment
WHERE hotel_id = :hotel_id;

-- name: delete_website_enrichment!
-- Delete website enrichment record for a hotel (for testing)
DELETE FROM sadie_gtm.hotel_website_enrichment
WHERE hotel_id = :hotel_id;
