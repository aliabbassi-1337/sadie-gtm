-- RMS Booking Engine Queries
-- Used for RMS hotel discovery and enrichment

-- name: get_rms_booking_engine_id^
-- Get RMS Cloud booking engine ID
SELECT id FROM sadie_gtm.booking_engines WHERE name = 'RMS Cloud';

-- name: get_rms_hotels_needing_enrichment
-- Get RMS hotels that need enrichment (missing name, email, phone, city, etc.)
-- Uses booking_engine_id for filtering (more efficient than name)
-- Treats "Unknown", "Online Bookings" as garbage that needs enrichment
SELECT 
    h.id AS hotel_id,
    hbe.booking_url
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
WHERE hbe.booking_engine_id = :booking_engine_id
  AND h.status = 1
  AND hbe.booking_url IS NOT NULL
  AND hbe.booking_url != ''
  AND (
      -- Missing or garbage name
      h.name IS NULL 
      OR h.name = '' 
      OR h.name LIKE 'Unknown%'
      OR h.name = 'Online Bookings'
      OR h.name LIKE '%rmscloud%'
      -- Missing location
      OR h.city IS NULL 
      OR h.city = ''
      OR h.state IS NULL
      OR h.state = ''
      -- Missing contact info
      OR h.email IS NULL 
      OR h.email = ''
      OR h.phone_website IS NULL 
      OR h.phone_website = ''
  )
ORDER BY hbe.last_enrichment_attempt ASC NULLS FIRST
LIMIT :limit;

-- name: insert_rms_hotel<!
-- Insert a new RMS hotel and return the ID
-- Uses external_id for upsert (RMS slug as external_id)
INSERT INTO sadie_gtm.hotels (
    name, address, city, state, country, phone_website, email, website,
    source, status, external_id, external_id_type, created_at, updated_at
) VALUES (
    :name, :address, :city, :state, :country, :phone, :email, :website,
    :source, :status, :external_id, 'rms_slug', NOW(), NOW()
)
ON CONFLICT (external_id_type, external_id) WHERE external_id IS NOT NULL
DO UPDATE SET
    name = COALESCE(EXCLUDED.name, sadie_gtm.hotels.name),
    address = COALESCE(EXCLUDED.address, sadie_gtm.hotels.address),
    city = COALESCE(EXCLUDED.city, sadie_gtm.hotels.city),
    state = COALESCE(EXCLUDED.state, sadie_gtm.hotels.state),
    country = COALESCE(EXCLUDED.country, sadie_gtm.hotels.country),
    phone_website = COALESCE(EXCLUDED.phone_website, sadie_gtm.hotels.phone_website),
    email = COALESCE(EXCLUDED.email, sadie_gtm.hotels.email),
    website = COALESCE(EXCLUDED.website, sadie_gtm.hotels.website),
    updated_at = NOW()
RETURNING id;

-- name: insert_rms_hotel_booking_engine!
-- Insert or update hotel booking engine relation
-- Primary key is hotel_id only (one booking engine per hotel)
INSERT INTO sadie_gtm.hotel_booking_engines (
    hotel_id, booking_engine_id, booking_url, enrichment_status,
    last_enrichment_attempt, detected_at, updated_at
) VALUES (
    :hotel_id, :booking_engine_id, :booking_url, :enrichment_status, NOW(), NOW(), NOW()
)
ON CONFLICT (hotel_id) 
DO UPDATE SET
    booking_engine_id = EXCLUDED.booking_engine_id,
    booking_url = EXCLUDED.booking_url,
    enrichment_status = EXCLUDED.enrichment_status,
    last_enrichment_attempt = NOW(),
    updated_at = NOW();

-- name: update_rms_hotel!
-- Update hotel with enriched data
-- Only fill NULL/empty fields, or replace garbage names (Unknown, Online Bookings, URLs)
-- Preserves real existing data
UPDATE sadie_gtm.hotels 
SET 
    name = CASE 
        WHEN name IS NULL OR name = '' OR name LIKE 'Unknown%' OR name = 'Online Bookings' OR name LIKE '%rmscloud.com%' 
        THEN COALESCE(:name, name)
        ELSE name 
    END,
    address = CASE WHEN address IS NULL OR address = '' THEN :address ELSE address END,
    city = CASE WHEN city IS NULL OR city = '' THEN :city ELSE city END,
    state = CASE WHEN state IS NULL OR state = '' THEN :state ELSE state END,
    country = CASE WHEN country IS NULL OR country = '' THEN :country ELSE country END,
    phone_website = CASE WHEN phone_website IS NULL OR phone_website = '' THEN :phone ELSE phone_website END,
    email = CASE WHEN email IS NULL OR email = '' THEN :email ELSE email END,
    website = CASE WHEN website IS NULL OR website = '' THEN :website ELSE website END,
    updated_at = NOW()
WHERE id = :hotel_id;

-- name: update_rms_enrichment_status!
-- Update enrichment status for a hotel booking engine
UPDATE sadie_gtm.hotel_booking_engines
SET 
    enrichment_status = :status,
    last_enrichment_attempt = NOW()
WHERE booking_url = :booking_url;

-- name: get_rms_stats^
-- Get RMS hotel statistics
-- Uses booking_engine_id for filtering
SELECT 
    COUNT(*) AS total,
    COUNT(CASE WHEN h.name IS NOT NULL AND h.name != '' THEN 1 END) AS with_name,
    COUNT(CASE WHEN h.city IS NOT NULL AND h.city != '' THEN 1 END) AS with_city,
    COUNT(CASE WHEN h.email IS NOT NULL AND h.email != '' THEN 1 END) AS with_email,
    COUNT(CASE WHEN h.phone_website IS NOT NULL AND h.phone_website != '' THEN 1 END) AS with_phone,
    COUNT(CASE WHEN hbe.enrichment_status = 'enriched' THEN 1 END) AS enriched,
    COUNT(CASE WHEN hbe.enrichment_status = 'no_data' THEN 1 END) AS no_data,
    COUNT(CASE WHEN hbe.enrichment_status = 'dead' THEN 1 END) AS dead
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
WHERE hbe.booking_engine_id = :booking_engine_id;

-- name: count_rms_needing_enrichment^
-- Count RMS hotels needing enrichment
-- Uses booking_engine_id for filtering
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
WHERE hbe.booking_engine_id = :booking_engine_id
  AND h.status = 1
  AND (
      -- Missing or garbage name
      h.name IS NULL 
      OR h.name = '' 
      OR h.name LIKE 'Unknown%'
      OR h.name = 'Online Bookings'
      OR h.name LIKE '%rmscloud%'
      -- Missing location
      OR h.city IS NULL 
      OR h.city = ''
      -- Missing contact info
      OR h.email IS NULL 
      OR h.email = ''
      OR h.phone_website IS NULL 
      OR h.phone_website = ''
  );
