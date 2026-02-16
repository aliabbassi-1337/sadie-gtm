-- Batch SQL queries for enrichment operations.
-- These use positional parameters ($1, $2, etc.) with unnest for bulk updates.
-- Loaded via enrichment_batch.py (not aiosql, since they need array parameters).

-- BATCH_UPDATE_SITEMINDER_ENRICHMENT
-- Params: ($1::int[] hotel_ids, $2::text[] names, $3::text[] addresses, $4::text[] cities,
--          $5::text[] states, $6::text[] countries, $7::text[] emails, $8::text[] phones,
--          $9::text[] websites, $10::float[] lats, $11::float[] lons)
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
        $8::text[],
        $9::text[],
        $10::float[],
        $11::float[]
    ) AS t(id, name, address, city, state, country, email, phone, website, lat, lon)
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

-- BATCH_SET_MEWS_ENRICHMENT_STATUS
-- Params: ($1::int[] hotel_ids, $2 enrichment_status)
-- Update enrichment status for Mews hotels on hotel_booking_engines
UPDATE sadie_gtm.hotel_booking_engines hbe
SET enrichment_status = $2, last_enrichment_attempt = CURRENT_TIMESTAMP
FROM sadie_gtm.booking_engines be
WHERE hbe.hotel_id = ANY($1)
  AND hbe.booking_engine_id = be.id
  AND be.name ILIKE '%Mews%';

-- BATCH_UPDATE_MEWS_ENRICHMENT
-- Params: ($1::int[] hotel_ids, $2::text[] names, $3::text[] addresses, $4::text[] cities,
--          $5::text[] states, $6::text[] countries, $7::text[] emails, $8::text[] phones,
--          $9::float[] lats, $10::float[] lons)
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
        $8::text[],
        $9::float[],
        $10::float[]
    ) AS t(hotel_id, name, address, city, state, country, email, phone, lat, lon)
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

-- BATCH_BIG4_UPSERT
-- Params: ($1::text[] names, $2::text[] slugs, $3::text[] phones, $4::text[] emails,
--          $5::text[] websites, $6::text[] addresses, $7::text[] cities,
--          $8::text[] states, $9::text[] postcodes, $10::float[] lats, $11::float[] lons)
--
-- Two-phase upsert for BIG4 parks with cross-source dedup:
--   Phase 1 (CTE "matched"): UPDATE existing AU hotels whose normalized name+state
--           matches an incoming park. Fill-empty-only — never overwrites.
--   Phase 2 (final INSERT): Insert parks that didn't match any existing hotel.
--           On same-source conflict (external_id_type=big4), also fill-empty-only.
--
-- Name normalization: lowercase, strip common brand prefixes and property type
-- suffixes so "BIG4 Sydney Lakeside Holiday Park" matches "Sydney Lakeside".
WITH incoming AS (
    SELECT *,
        regexp_replace(
            regexp_replace(
                lower(trim(name)),
                '^(big4 |big 4 |nrma |ingenia holidays |tasman holiday parks - |tasman holiday parks |breeze holiday parks - |holiday haven )',
                ''
            ),
            ' (holiday park|caravan park|tourist park|holiday village|holiday resort|camping ground|glamping retreat|lifestyle park)$',
            ''
        ) AS norm_name
    FROM unnest(
        $1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
        $6::text[], $7::text[], $8::text[], $9::text[], $10::float[], $11::float[]
    ) AS t(name, slug, phone, email, website, address, city, state, postcode, lat, lon)
),
existing_norm AS (
    SELECT id,
        regexp_replace(
            regexp_replace(
                lower(trim(h.name)),
                '^(big4 |big 4 |nrma |ingenia holidays |tasman holiday parks - |tasman holiday parks |breeze holiday parks - |holiday haven )',
                ''
            ),
            ' (holiday park|caravan park|tourist park|holiday village|holiday resort|camping ground|glamping retreat|lifestyle park)$',
            ''
        ) AS norm_name,
        upper(h.state) AS norm_state
    FROM sadie_gtm.hotels h
    WHERE h.country IN ('Australia', 'AU') AND h.status >= 0
),
matched AS (
    UPDATE sadie_gtm.hotels h
    SET
        email     = CASE WHEN (h.email IS NULL OR h.email = '')             AND i.email IS NOT NULL AND i.email != ''   THEN i.email     ELSE h.email END,
        phone_website = CASE WHEN (h.phone_website IS NULL OR h.phone_website = '') AND i.phone IS NOT NULL AND i.phone != '' THEN i.phone     ELSE h.phone_website END,
        website   = CASE WHEN (h.website IS NULL OR h.website = '')         AND i.website IS NOT NULL AND i.website != '' THEN i.website   ELSE h.website END,
        address   = CASE WHEN (h.address IS NULL OR h.address = '')         AND i.address IS NOT NULL AND i.address != '' THEN i.address   ELSE h.address END,
        city      = CASE WHEN (h.city IS NULL OR h.city = '')               AND i.city IS NOT NULL AND i.city != ''     THEN i.city      ELSE h.city END,
        location  = CASE WHEN h.location IS NULL AND i.lat IS NOT NULL AND i.lon IS NOT NULL
                         THEN ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography
                         ELSE h.location END,
        updated_at = NOW()
    FROM incoming i
    JOIN existing_norm e ON e.norm_name = i.norm_name AND e.norm_state = upper(i.state)
    WHERE h.id = e.id
    RETURNING i.slug
)
INSERT INTO sadie_gtm.hotels (
    name, source, status, address, city, state, country,
    phone_google, category, external_id, external_id_type, location
)
SELECT
    i.name, 'big4_scrape', 1, i.address, i.city, i.state, 'Australia',
    i.phone, 'holiday_park', 'big4_' || i.slug, 'big4',
    CASE WHEN i.lat IS NOT NULL AND i.lon IS NOT NULL
         THEN ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography
         ELSE NULL END
FROM incoming i
WHERE i.slug NOT IN (SELECT slug FROM matched)
ON CONFLICT (external_id_type, external_id) WHERE external_id IS NOT NULL
DO UPDATE SET
    address    = CASE WHEN (sadie_gtm.hotels.address IS NULL OR sadie_gtm.hotels.address = '')
                      THEN COALESCE(EXCLUDED.address, sadie_gtm.hotels.address) ELSE sadie_gtm.hotels.address END,
    city       = CASE WHEN (sadie_gtm.hotels.city IS NULL OR sadie_gtm.hotels.city = '')
                      THEN COALESCE(EXCLUDED.city, sadie_gtm.hotels.city) ELSE sadie_gtm.hotels.city END,
    phone_google = CASE WHEN (sadie_gtm.hotels.phone_google IS NULL OR sadie_gtm.hotels.phone_google = '')
                        THEN COALESCE(EXCLUDED.phone_google, sadie_gtm.hotels.phone_google) ELSE sadie_gtm.hotels.phone_google END,
    category   = COALESCE(sadie_gtm.hotels.category, EXCLUDED.category),
    location   = COALESCE(sadie_gtm.hotels.location, EXCLUDED.location);
