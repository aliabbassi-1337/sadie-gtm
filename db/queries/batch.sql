-- Batch SQL queries for executemany operations.
-- These use positional parameters ($1, $2, etc.) required by asyncpg executemany.
-- Loaded manually (not via aiosql) since executemany needs positional params.

-- BATCH_INSERT_HOTELS
-- Params: (name, source, status, address, city, state, country, phone, category, external_id, external_id_type, lat, lon)
-- Dedup on external_id. Updates existing records if external_id matches.
INSERT INTO sadie_gtm.hotels (name, source, status, address, city, state, country, phone_google, category, external_id, external_id_type, location)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        CASE WHEN $12::float8 IS NOT NULL AND $13::float8 IS NOT NULL
             THEN ST_SetSRID(ST_MakePoint($13::float8, $12::float8), 4326)::geography
             ELSE NULL END)
ON CONFLICT (external_id_type, external_id) WHERE external_id IS NOT NULL 
DO UPDATE SET
    address = COALESCE(EXCLUDED.address, sadie_gtm.hotels.address),
    phone_google = COALESCE(EXCLUDED.phone_google, sadie_gtm.hotels.phone_google),
    category = COALESCE(EXCLUDED.category, sadie_gtm.hotels.category),
    location = COALESCE(EXCLUDED.location, sadie_gtm.hotels.location);

-- BATCH_INSERT_ROOM_COUNTS
-- Params: (room_count, external_id_type, external_id, source_name, confidence)
-- Lookup hotel by external_id
INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, status, confidence)
SELECT h.id, $1, $4, 1, $5
FROM sadie_gtm.hotels h
WHERE h.external_id_type = $2 AND h.external_id = $3
ON CONFLICT (hotel_id) DO UPDATE SET
    room_count = EXCLUDED.room_count,
    confidence = EXCLUDED.confidence;

-- BATCH_INSERT_CRAWLED_HOTELS
-- Params: (name, source, external_id, external_id_type, booking_engine_id, booking_url, slug, detection_method)
-- Single query: Insert hotel + link to booking engine in one go
-- Uses CTE to insert hotel first, then links booking engine
-- Skips if booking_url already exists
WITH new_hotel AS (
    INSERT INTO sadie_gtm.hotels (name, source, external_id, external_id_type, status)
    VALUES ($1, $2, $3, $4, 0)
    ON CONFLICT (external_id_type, external_id) WHERE external_id IS NOT NULL 
    DO NOTHING
    RETURNING id
)
INSERT INTO sadie_gtm.hotel_booking_engines (hotel_id, booking_engine_id, booking_url, engine_property_id, detection_method, status, detected_at, updated_at)
SELECT id, $5, $6, $7, $8, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM new_hotel
WHERE NOT EXISTS (
    SELECT 1 FROM sadie_gtm.hotel_booking_engines WHERE booking_url = $6
);
