-- name: get_booking_engine_by_name^
-- Get booking engine by name (case-insensitive)
SELECT id, name, domains, tier, is_active
FROM sadie_gtm.booking_engines
WHERE LOWER(name) = LOWER(:name);

-- name: get_all_booking_engines
-- Get all active booking engines with their domain patterns
SELECT id, name, domains, tier
FROM sadie_gtm.booking_engines
WHERE is_active = TRUE
  AND domains IS NOT NULL
  AND array_length(domains, 1) > 0;

-- name: insert_booking_engine<!
-- Insert a new booking engine (tier 2 = unknown/discovered)
-- Uses case-insensitive conflict check via functional index
INSERT INTO sadie_gtm.booking_engines (name, domains, tier)
VALUES (:name, :domains, :tier)
ON CONFLICT (LOWER(name)) DO UPDATE SET
    domains = COALESCE(EXCLUDED.domains, booking_engines.domains)
RETURNING id;
