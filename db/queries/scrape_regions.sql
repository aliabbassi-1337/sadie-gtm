-- Queries for scrape_regions table

-- name: get_regions_by_state
-- Get all regions for a state, ordered by priority
SELECT 
    id, name, state, region_type,
    ST_AsGeoJSON(polygon) as polygon_geojson,
    center_lat, center_lng, radius_km, cell_size_km, priority, created_at
FROM sadie_gtm.scrape_regions
WHERE UPPER(state) = UPPER(:state)
ORDER BY priority DESC, name;

-- name: get_region_by_name^
-- Get a specific region by name and state
SELECT 
    id, name, state, region_type,
    ST_AsGeoJSON(polygon) as polygon_geojson,
    center_lat, center_lng, radius_km, cell_size_km, priority, created_at
FROM sadie_gtm.scrape_regions
WHERE LOWER(name) = LOWER(:name) AND UPPER(state) = UPPER(:state);

-- name: insert_region$
-- Insert a new region from a center point and radius (creates circular polygon)
INSERT INTO sadie_gtm.scrape_regions (name, state, region_type, polygon, center_lat, center_lng, radius_km, cell_size_km, priority)
VALUES (
    :name, 
    :state, 
    :region_type,
    ST_Buffer(ST_SetSRID(ST_MakePoint(:center_lng, :center_lat), 4326)::geography, :radius_km * 1000),
    :center_lat, 
    :center_lng, 
    :radius_km,
    :cell_size_km,
    :priority
)
ON CONFLICT (name, state) DO UPDATE SET
    polygon = EXCLUDED.polygon,
    center_lat = EXCLUDED.center_lat,
    center_lng = EXCLUDED.center_lng,
    radius_km = EXCLUDED.radius_km,
    cell_size_km = EXCLUDED.cell_size_km,
    priority = EXCLUDED.priority
RETURNING id;

-- name: insert_region_geojson$
-- Insert a region from raw GeoJSON polygon
INSERT INTO sadie_gtm.scrape_regions (name, state, region_type, polygon, center_lat, center_lng, cell_size_km, priority)
VALUES (
    :name, 
    :state, 
    :region_type,
    ST_GeomFromGeoJSON(:polygon_geojson)::geography,
    :center_lat, 
    :center_lng,
    :cell_size_km,
    :priority
)
ON CONFLICT (name, state) DO UPDATE SET
    polygon = ST_GeomFromGeoJSON(:polygon_geojson)::geography,
    center_lat = EXCLUDED.center_lat,
    center_lng = EXCLUDED.center_lng,
    cell_size_km = EXCLUDED.cell_size_km,
    priority = EXCLUDED.priority
RETURNING id;

-- name: delete_region!
-- Delete a region
DELETE FROM sadie_gtm.scrape_regions
WHERE LOWER(name) = LOWER(:name) AND UPPER(state) = UPPER(:state);

-- name: delete_regions_by_state!
-- Delete all regions for a state
DELETE FROM sadie_gtm.scrape_regions WHERE UPPER(state) = UPPER(:state);

-- name: count_regions_by_state$
-- Count regions for a state
SELECT COUNT(*) FROM sadie_gtm.scrape_regions WHERE UPPER(state) = UPPER(:state);

-- name: point_in_any_region$
-- Check if a point is within any region for a state
SELECT COUNT(*) > 0 as is_in_region
FROM sadie_gtm.scrape_regions
WHERE UPPER(state) = UPPER(:state)
  AND ST_Covers(polygon, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography);

-- name: get_regions_containing_point
-- Get all regions containing a point
SELECT 
    id, name, state, region_type,
    ST_AsGeoJSON(polygon) as polygon_geojson,
    center_lat, center_lng, radius_km, cell_size_km, priority
FROM sadie_gtm.scrape_regions
WHERE UPPER(state) = UPPER(:state)
  AND ST_Covers(polygon, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography);

-- name: get_region_bounds^
-- Get bounding box for a region
SELECT 
    ST_XMin(polygon::geometry) as lng_min,
    ST_XMax(polygon::geometry) as lng_max,
    ST_YMin(polygon::geometry) as lat_min,
    ST_YMax(polygon::geometry) as lat_max
FROM sadie_gtm.scrape_regions
WHERE id = :region_id;

-- name: get_total_area_km2$
-- Get total area of all regions for a state in km²
SELECT COALESCE(SUM(ST_Area(polygon) / 1000000), 0) as total_area_km2
FROM sadie_gtm.scrape_regions
WHERE UPPER(state) = UPPER(:state);
