-- Batch SQL queries for enrichment operations.
-- These use positional parameters ($1, $2, etc.) with unnest for bulk updates.
-- Loaded via enrichment_batch.py (not aiosql, since they need array parameters).

-- BATCH_UPDATE_SITEMINDER_ENRICHMENT
-- Params: ($1::int[] hotel_ids, $2::text[] names, $3::text[] addresses, $4::text[] cities,
--          $5::text[] states, $6::text[] countries, $7::text[] emails, $8::text[] phones, $9::text[] websites)
-- API data wins when non-empty, falls back to existing DB value.
UPDATE sadie_gtm.hotels h
SET
    name = COALESCE(NULLIF(v.name, ''), h.name),
    address = COALESCE(NULLIF(v.address, ''), h.address),
    city = COALESCE(NULLIF(v.city, ''), h.city),
    state = COALESCE(NULLIF(v.state, ''), h.state),
    country = COALESCE(NULLIF(v.country, ''), h.country),
    email = COALESCE(NULLIF(v.email, ''), h.email),
    phone_website = COALESCE(NULLIF(v.phone, ''), h.phone_website),
    website = COALESCE(NULLIF(v.website, ''), h.website),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        unnest($1::int[]) as id,
        unnest($2::text[]) as name,
        unnest($3::text[]) as address,
        unnest($4::text[]) as city,
        unnest($5::text[]) as state,
        unnest($6::text[]) as country,
        unnest($7::text[]) as email,
        unnest($8::text[]) as phone,
        unnest($9::text[]) as website
) v
WHERE h.id = v.id;

-- BATCH_SET_SITEMINDER_ENRICHMENT_STATUS
-- Params: ($1::int[] hotel_ids, $2 enrichment_status)
-- Update enrichment status for SiteMinder hotels on hotel_booking_engines
UPDATE sadie_gtm.hotel_booking_engines hbe
SET enrichment_status = $2, last_enrichment_attempt = CURRENT_TIMESTAMP
FROM sadie_gtm.booking_engines be
WHERE hbe.hotel_id = ANY($1)
  AND hbe.booking_engine_id = be.id
  AND be.name = 'SiteMinder';

-- BATCH_UPDATE_MEWS_ENRICHMENT
-- Params: ($1::int[] hotel_ids, $2::text[] names, $3::text[] addresses, $4::text[] cities,
--          $5::text[] countries, $6::text[] emails, $7::text[] phones, $8::float[] lats, $9::float[] lons)
-- API data wins when non-empty, falls back to existing DB value.
UPDATE sadie_gtm.hotels h
SET
    name = COALESCE(NULLIF(v.name, ''), h.name),
    address = COALESCE(NULLIF(v.address, ''), h.address),
    city = COALESCE(NULLIF(v.city, ''), h.city),
    country = COALESCE(NULLIF(v.country, ''), h.country),
    email = COALESCE(NULLIF(v.email, ''), h.email),
    phone_website = COALESCE(NULLIF(v.phone, ''), h.phone_website),
    location = CASE WHEN v.lat IS NOT NULL AND v.lon IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)::geography
                ELSE h.location END,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT * FROM unnest(
        $1::integer[],
        $2::text[],
        $3::text[],
        $4::text[],
        $5::text[],
        $6::text[],
        $7::text[],
        $8::float[],
        $9::float[]
    ) AS t(hotel_id, name, address, city, country, email, phone, lat, lon)
) v
WHERE h.id = v.hotel_id;

-- BATCH_UPDATE_CLOUDBEDS_ENRICHMENT
-- Params: ($1::int[] hotel_ids, $2::text[] names, $3::text[] addresses, $4::text[] cities,
--          $5::text[] states, $6::text[] countries, $7::text[] phones, $8::text[] emails,
--          $9::float[] lats, $10::float[] lons, $11::text[] zip_codes, $12::text[] contact_names)
-- API data always overwrites existing (Cloudbeds scraped data is more reliable than crawl source).
UPDATE sadie_gtm.hotels h
SET
    name = COALESCE(v.name, h.name),
    address = COALESCE(v.address, h.address),
    city = COALESCE(v.city, h.city),
    state = COALESCE(v.state, h.state),
    country = COALESCE(v.country, h.country),
    phone_website = COALESCE(v.phone, h.phone_website),
    email = COALESCE(v.email, h.email),
    location = CASE
        WHEN v.lat IS NOT NULL AND v.lon IS NOT NULL
        THEN ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)::geography
        ELSE h.location
    END,
    zip_code = COALESCE(v.zip_code, h.zip_code),
    contact_name = COALESCE(v.contact_name, h.contact_name),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT * FROM unnest(
        $1::integer[],
        $2::text[],
        $3::text[],
        $4::text[],
        $5::text[],
        $6::text[],
        $7::text[],
        $8::text[],
        $9::float[],
        $10::float[],
        $11::text[],
        $12::text[]
    ) AS t(hotel_id, name, address, city, state, country, phone, email, lat, lon, zip_code, contact_name)
) v
WHERE h.id = v.hotel_id;

-- BATCH_UPDATE_GEOCODING
-- Params: ($1::int[] hotel_ids, $2::text[] addresses, $3::text[] cities, $4::text[] states,
--          $5::text[] countries, $6::float[] lats, $7::float[] lons, $8::text[] phones, $9::text[] emails)
UPDATE sadie_gtm.hotels h
SET
    address = COALESCE(v.address, h.address),
    city = COALESCE(v.city, h.city),
    state = COALESCE(v.state, h.state),
    country = COALESCE(v.country, h.country),
    location = CASE
        WHEN v.latitude IS NOT NULL AND v.longitude IS NOT NULL
        THEN ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography
        ELSE h.location
    END,
    phone_google = COALESCE(v.phone, h.phone_google),
    email = COALESCE(v.email, h.email),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT * FROM unnest(
        $1::integer[],
        $2::text[],
        $3::text[],
        $4::text[],
        $5::text[],
        $6::float[],
        $7::float[],
        $8::text[],
        $9::text[]
    ) AS t(hotel_id, address, city, state, country, latitude, longitude, phone, email)
) v
WHERE h.id = v.hotel_id;

-- BATCH_MARK_ENRICHMENT_DEAD
-- Params: ($1::int[] hotel_ids)
-- Mark booking URLs as permanently dead (404)
UPDATE sadie_gtm.hotel_booking_engines
SET enrichment_status = -1,
    last_enrichment_attempt = NOW()
WHERE hotel_id = ANY($1::integer[]);

-- BATCH_SET_LAST_ENRICHMENT_ATTEMPT
-- Params: ($1::int[] hotel_ids)
UPDATE sadie_gtm.hotel_booking_engines
SET last_enrichment_attempt = NOW()
WHERE hotel_id = ANY($1::integer[]);
