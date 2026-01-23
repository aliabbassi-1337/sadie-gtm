-- Batch SQL queries for executemany operations.
-- These use positional parameters ($1, $2, etc.) required by asyncpg executemany.
-- Loaded manually (not via aiosql) since executemany needs positional params.

-- BATCH_INSERT_HOTELS
-- Params: (name, source, status, address, city, state, country, phone, category)
INSERT INTO sadie_gtm.hotels (name, source, status, address, city, state, country, phone_google, category)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (source) DO NOTHING;

-- BATCH_INSERT_ROOM_COUNTS
-- Params: (room_count, source, source_name)
INSERT INTO sadie_gtm.hotel_room_count (hotel_id, room_count, source, status)
SELECT h.id, $1, $3, 1
FROM sadie_gtm.hotels h
WHERE h.source = $2
ON CONFLICT (hotel_id) DO UPDATE SET room_count = EXCLUDED.room_count;
