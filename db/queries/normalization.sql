-- ============================================================================
-- NORMALIZATION QUERIES
-- Country and state normalization for hotel data
-- Runs on ALL hotels regardless of status
-- ============================================================================


-- name: count_hotels_by_country_value^
-- Count hotels with a specific country value (for dry-run reporting)
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE country = :old_value;


-- name: update_country_value!
-- Normalize a country value (e.g., 'AU' -> 'Australia', 'USA' -> 'United States')
UPDATE sadie_gtm.hotels
SET country = :new_value, updated_at = NOW()
WHERE country = :old_value;


-- name: null_country_value!
-- NULL out a garbage country value (e.g., '--', 'XX')
UPDATE sadie_gtm.hotels
SET country = NULL, updated_at = NOW()
WHERE country = :old_value;


-- name: get_state_counts_for_country
-- Get unique state values and their counts for a country
SELECT state, COUNT(*) AS cnt
FROM sadie_gtm.hotels
WHERE country = :country
  AND state IS NOT NULL
GROUP BY state
ORDER BY cnt DESC;


-- name: update_state_value!
-- Normalize a state value (e.g., 'CA' -> 'California')
UPDATE sadie_gtm.hotels
SET state = :new_state, updated_at = CURRENT_TIMESTAMP
WHERE country = :country AND state = :old_state;


-- name: null_state_value!
-- NULL out a junk state value (e.g., '-', '90210')
UPDATE sadie_gtm.hotels
SET state = NULL, updated_at = CURRENT_TIMESTAMP
WHERE country = :country AND state = :old_state;


-- name: get_hotels_for_location_inference
-- Get hotels that may need country/state inference (misclassified or missing)
-- Fetches hotels with at least one signal (website, phone, or address)
SELECT id, name, website, phone_google, phone_website, address, city, state, country
FROM sadie_gtm.hotels
WHERE (
    country = :country
    OR (:include_null AND country IS NULL)
  )
  AND (website IS NOT NULL OR phone_google IS NOT NULL OR phone_website IS NOT NULL OR address IS NOT NULL)
ORDER BY id;


-- name: fix_hotel_country_and_state!
-- Update a hotel's country and state based on inference
UPDATE sadie_gtm.hotels
SET country = :country,
    state = :state,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;


-- name: fix_hotel_country_only!
-- Update only the country (leave state unchanged)
UPDATE sadie_gtm.hotels
SET country = :country,
    updated_at = CURRENT_TIMESTAMP
WHERE id = :hotel_id;


-- ============================================================================
-- BATCH OPERATIONS
-- These use unnest/ANY for bulk updates in a single SQL statement
-- ============================================================================


-- name: count_hotels_by_country_values
-- Batch count hotels matching any of the given country values (for dry-run)
SELECT country AS old_value, COUNT(*) AS count
FROM sadie_gtm.hotels
WHERE country = ANY(:old_values::text[])
GROUP BY country;


-- name: batch_update_country_values!
-- Batch normalize multiple country values in one UPDATE
UPDATE sadie_gtm.hotels h
SET country = m.new_value, updated_at = NOW()
FROM unnest(:old_values::text[], :new_values::text[]) AS m(old_value, new_value)
WHERE h.country = m.old_value;


-- name: batch_null_country_values!
-- Batch NULL out multiple garbage country values in one UPDATE
UPDATE sadie_gtm.hotels
SET country = NULL, updated_at = NOW()
WHERE country = ANY(:old_values::text[]);


-- name: batch_update_state_values!
-- Batch normalize multiple state values for a country in one UPDATE
UPDATE sadie_gtm.hotels h
SET state = m.new_state, updated_at = NOW()
FROM unnest(:old_states::text[], :new_states::text[]) AS m(old_state, new_state)
WHERE h.country = :country AND h.state = m.old_state;


-- name: batch_null_state_values!
-- Batch NULL out multiple junk state values for a country in one UPDATE
UPDATE sadie_gtm.hotels
SET state = NULL, updated_at = NOW()
WHERE country = :country AND state = ANY(:old_states::text[]);


-- name: batch_fix_hotel_locations!
-- Batch update country and optionally state for multiple hotels by ID
-- Pass NULL for state entries that should keep their existing value
UPDATE sadie_gtm.hotels h
SET country = m.country,
    state = COALESCE(m.state, h.state),
    updated_at = NOW()
FROM unnest(:ids::bigint[], :countries::text[], :states::text[]) AS m(id, country, state)
WHERE h.id = m.id;


-- ============================================================================
-- ADDRESS ENRICHMENT
-- Extract state/city from address text
-- ============================================================================


-- name: get_hotels_for_address_enrichment
-- Get hotels with address but missing state or city for a country
SELECT id, address, city, state, country
FROM sadie_gtm.hotels
WHERE address IS NOT NULL
  AND country = :country
  AND (state IS NULL OR city IS NULL)
ORDER BY id;


-- name: batch_enrich_hotel_state_city!
-- Batch set state and city from address parsing
-- Uses COALESCE to only fill in NULL fields (never overwrite existing values)
UPDATE sadie_gtm.hotels h
SET state = COALESCE(h.state, m.state),
    city = COALESCE(h.city, m.city),
    updated_at = NOW()
FROM unnest(:ids::bigint[], :states::text[], :cities::text[]) AS m(id, state, city)
WHERE h.id = m.id
  AND (h.state IS NULL OR h.city IS NULL);
