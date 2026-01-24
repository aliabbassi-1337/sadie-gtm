-- Batch SQL queries for executemany operations.
-- These use positional parameters ($1, $2, etc.) required by asyncpg executemany.
-- Loaded manually (not via aiosql) since executemany needs positional params.

-- BATCH_INSERT_HOTELS
-- Params: (name, source, status, address, city, state, country, phone, category, external_id, external_id_type)
-- Dedup on name+city. Uses DO NOTHING to handle any other unique constraint violations.
INSERT INTO sadie_gtm.hotels (name, source, status, address, city, state, country, phone_google, category, external_id, external_id_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (name, city) DO UPDATE SET
    address = COALESCE(EXCLUDED.address, sadie_gtm.hotels.address),
    phone_google = COALESCE(EXCLUDED.phone_google, sadie_gtm.hotels.phone_google),
    category = COALESCE(EXCLUDED.category, sadie_gtm.hotels.category),
    external_id = COALESCE(EXCLUDED.external_id, sadie_gtm.hotels.external_id),
    external_id_type = COALESCE(EXCLUDED.external_id_type, sadie_gtm.hotels.external_id_type);

-- BATCH_INSERT_ROOM_COUNTS
-- Params: (room_count, external_id_type, external_id, source_name)
-- Lookup hotel by external_id
INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, status)
SELECT h.id, $1, $4, 1
FROM sadie_gtm.hotels h
WHERE h.external_id_type = $2 AND h.external_id = $3
ON CONFLICT (hotel_id) DO UPDATE SET room_count = EXCLUDED.room_count;
