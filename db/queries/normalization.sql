-- ============================================================================
-- NORMALIZATION QUERIES
-- Country and state normalization for hotel data
-- ============================================================================


-- name: count_hotels_by_country_value^
-- Count hotels with a specific country value (for dry-run reporting)
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE country = :old_value AND status != -1;


-- name: update_country_value!
-- Normalize a country value (e.g., 'AU' -> 'Australia', 'USA' -> 'United States')
UPDATE sadie_gtm.hotels
SET country = :new_value, updated_at = NOW()
WHERE country = :old_value AND status != -1;


-- name: null_country_value!
-- NULL out a garbage country value (e.g., '--', 'XX')
UPDATE sadie_gtm.hotels
SET country = NULL, updated_at = NOW()
WHERE country = :old_value AND status != -1;


-- name: get_state_counts_for_country
-- Get unique state values and their counts for a country
SELECT state, COUNT(*) AS cnt
FROM sadie_gtm.hotels
WHERE status != -1
  AND country = :country
  AND state IS NOT NULL
GROUP BY state
ORDER BY cnt DESC;


-- name: update_state_value!
-- Normalize a state value (e.g., 'CA' -> 'California')
UPDATE sadie_gtm.hotels
SET state = :new_state, updated_at = CURRENT_TIMESTAMP
WHERE status != -1 AND country = :country AND state = :old_state;


-- name: null_state_value!
-- NULL out a junk state value (e.g., '-', '90210')
UPDATE sadie_gtm.hotels
SET state = NULL, updated_at = CURRENT_TIMESTAMP
WHERE status != -1 AND country = :country AND state = :old_state;
