-- RMS Booking Engine Queries
-- Used for RMS hotel discovery and enrichment

-- name: get_rms_booking_engine_id^
-- Get RMS Cloud booking engine ID
SELECT id FROM sadie_gtm.booking_engines WHERE name = 'RMS Cloud';

-- name: get_rms_hotels_needing_enrichment
-- Get RMS hotels that need enrichment (missing name, address, etc.)
SELECT 
    h.id AS hotel_id,
    hbe.booking_url
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
WHERE be.name = 'RMS Cloud'
  AND h.status = 1
  AND (
      h.name IS NULL 
      OR h.name = '' 
      OR h.name LIKE '%rmscloud%'
      OR h.city IS NULL 
      OR h.city = ''
      OR h.state IS NULL
      OR h.state = ''
  )
  AND (
      hbe.enrichment_status IS NULL 
      OR hbe.enrichment_status NOT IN ('dead', 'enriched')
      OR (hbe.enrichment_status = 'no_data' AND hbe.last_enrichment_attempt < NOW() - INTERVAL '7 days')
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
UPDATE sadie_gtm.hotels 
SET 
    name = COALESCE(:name, name),
    address = COALESCE(:address, address),
    city = COALESCE(:city, city),
    state = COALESCE(:state, state),
    country = COALESCE(:country, country),
    phone_website = COALESCE(:phone, phone_website),
    email = COALESCE(:email, email),
    website = COALESCE(:website, website),
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
JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
WHERE be.name = 'RMS Cloud';

-- name: count_rms_needing_enrichment^
-- Count RMS hotels needing enrichment
SELECT COUNT(*) AS count
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
WHERE be.name = 'RMS Cloud'
  AND h.status = 1
  AND (
      h.name IS NULL 
      OR h.name = '' 
      OR h.name LIKE '%rmscloud%'
      OR h.city IS NULL 
      OR h.city = ''
  )
  AND (
      hbe.enrichment_status IS NULL 
      OR hbe.enrichment_status NOT IN ('dead', 'enriched')
  );
