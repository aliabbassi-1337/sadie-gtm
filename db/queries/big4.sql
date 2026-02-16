-- BIG4 Holiday Parks queries

-- name: get_big4_count^
-- Count BIG4 parks in the database.
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE external_id_type = 'big4';

-- name: get_existing_au_hotels
-- Get all Australian hotels for cross-source dedup matching.
SELECT id, name, state, city, email, website, address,
       external_id, external_id_type
FROM sadie_gtm.hotels
WHERE country IN ('Australia', 'AU')
  AND status >= 0;

-- name: get_big4_dedup_stats^
-- Get stats about BIG4 parks and overlap with other sources.
SELECT
    COUNT(*) FILTER (WHERE external_id_type = 'big4') AS big4_only,
    COUNT(*) FILTER (WHERE external_id_type != 'big4'
                     AND country IN ('Australia', 'AU')) AS other_au,
    COUNT(*) FILTER (WHERE email IS NOT NULL AND email != ''
                     AND country IN ('Australia', 'AU')) AS with_email
FROM sadie_gtm.hotels
WHERE status >= 0;
