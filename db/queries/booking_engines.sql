-- name: get_booking_engine_by_name^
-- Get booking engine by name
SELECT id, name, domains, tier, is_active
FROM booking_engines
WHERE name = :name;

-- name: get_all_booking_engines
-- Get all active booking engines with their domain patterns
SELECT id, name, domains, tier
FROM booking_engines
WHERE is_active = TRUE
  AND domains IS NOT NULL
  AND array_length(domains, 1) > 0;

-- name: insert_booking_engine<!
-- Insert a new booking engine (tier 2 = unknown/discovered)
INSERT INTO booking_engines (name, domains, tier)
VALUES (:name, :domains, :tier)
ON CONFLICT (name) DO UPDATE SET
    domains = COALESCE(EXCLUDED.domains, booking_engines.domains)
RETURNING id;
