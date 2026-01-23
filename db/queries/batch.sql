-- Batch SQL queries for executemany operations.
-- These use positional parameters ($1, $2, etc.) required by asyncpg executemany.
-- Loaded manually (not via aiosql) since executemany needs positional params.

-- BATCH_INSERT_HOTELS
-- Params: (name, source, status, address, city, state, country, phone, category)
-- Dedup on name + city (external_id dedup handled in application code)
INSERT INTO sadie_gtm.hotels (name, source, status, address, city, state, country, phone_google, category)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT ON CONSTRAINT hotels_name_city_unique DO UPDATE SET
    address = COALESCE(EXCLUDED.address, sadie_gtm.hotels.address),
    phone_google = COALESCE(EXCLUDED.phone_google, sadie_gtm.hotels.phone_google),
    category = COALESCE(EXCLUDED.category, sadie_gtm.hotels.category);

-- BATCH_INSERT_EXTERNAL_IDS
-- Params: (id_type, external_id, hotel_id)
INSERT INTO sadie_gtm.hotel_external_ids (id_type, external_id, hotel_id)
VALUES ($1, $2, $3)
ON CONFLICT (id_type, external_id) DO NOTHING;

-- BATCH_INSERT_ROOM_COUNTS_BY_EXTERNAL_ID
-- Params: (room_count, id_type, external_id, source_name)
-- Lookup hotel by external_id
INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, status)
SELECT hei.hotel_id, $1, $4, 1
FROM sadie_gtm.hotel_external_ids hei
WHERE hei.id_type = $2 AND hei.external_id = $3
ON CONFLICT (hotel_id) DO UPDATE SET room_count = EXCLUDED.room_count;
