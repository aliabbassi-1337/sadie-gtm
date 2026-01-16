-- Queries for scrape_target_cities table

-- name: get_target_cities_by_state
-- Get all target cities for a state
SELECT id, name, state, lat, lng, radius_km, population, display_name, source, created_at
FROM scrape_target_cities
WHERE UPPER(state) = UPPER(:state)
ORDER BY population DESC NULLS LAST, name
LIMIT :limit;

-- name: get_target_city^
-- Get a specific target city by name and state (returns single row)
SELECT id, name, state, lat, lng, radius_km, population, display_name, source, created_at
FROM scrape_target_cities
WHERE LOWER(name) = LOWER(:name) AND UPPER(state) = UPPER(:state);

-- name: insert_target_city$
-- Insert or update a target city
INSERT INTO scrape_target_cities (name, state, lat, lng, radius_km, population, display_name, source)
VALUES (:name, :state, :lat, :lng, :radius_km, :population, :display_name, :source)
ON CONFLICT (name, state) DO UPDATE SET
    lat = EXCLUDED.lat,
    lng = EXCLUDED.lng,
    radius_km = COALESCE(EXCLUDED.radius_km, scrape_target_cities.radius_km),
    population = COALESCE(EXCLUDED.population, scrape_target_cities.population),
    display_name = COALESCE(EXCLUDED.display_name, scrape_target_cities.display_name)
RETURNING id;

-- name: delete_target_city!
-- Delete a target city
DELETE FROM scrape_target_cities
WHERE LOWER(name) = LOWER(:name) AND UPPER(state) = UPPER(:state);

-- name: count_target_cities_by_state$
-- Count target cities for a state
SELECT COUNT(*) FROM scrape_target_cities WHERE UPPER(state) = UPPER(:state);
