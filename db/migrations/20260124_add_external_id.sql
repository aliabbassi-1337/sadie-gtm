-- Create hotel_external_ids table for source-specific deduplication
-- Stores external identifiers from various sources:
--   - google_place: Google Places API place_id
--   - texas_hot: "taxpayer_number:location_number"
--   - dbpr_license: Florida DBPR license number
--   - booking_engine: Booking engine's hotel ID (from reverse lookup)

CREATE TABLE IF NOT EXISTS sadie_gtm.hotel_external_ids (
  id SERIAL PRIMARY KEY,
  hotel_id INT NOT NULL REFERENCES sadie_gtm.hotels(id) ON DELETE CASCADE,
  id_type TEXT NOT NULL,
  external_id TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(id_type, external_id)  -- prevents same external ID pointing to different hotels
);

CREATE INDEX IF NOT EXISTS idx_hotel_external_ids_hotel
ON sadie_gtm.hotel_external_ids(hotel_id);

CREATE INDEX IF NOT EXISTS idx_hotel_external_ids_lookup
ON sadie_gtm.hotel_external_ids(id_type, external_id);

-- Migrate existing google_place_id from hotels table
INSERT INTO sadie_gtm.hotel_external_ids (hotel_id, id_type, external_id)
SELECT id, 'google_place', google_place_id
FROM sadie_gtm.hotels
WHERE google_place_id IS NOT NULL
ON CONFLICT (id_type, external_id) DO NOTHING;

-- Migrate existing Texas hotels: extract external_id from compound source
-- e.g., "texas_hot:10105515737:00006" -> external_id="10105515737:00006"
INSERT INTO sadie_gtm.hotel_external_ids (hotel_id, id_type, external_id)
SELECT id, 'texas_hot', SUBSTRING(source FROM 'texas_hot:(.+)')
FROM sadie_gtm.hotels
WHERE source LIKE 'texas_hot:%'
ON CONFLICT (id_type, external_id) DO NOTHING;

-- Clean up Texas source strings to just category
UPDATE sadie_gtm.hotels
SET source = 'texas_hot'
WHERE source LIKE 'texas_hot:%';

-- Drop google_place_id column from hotels (now in external_ids table)
ALTER TABLE sadie_gtm.hotels DROP COLUMN IF EXISTS google_place_id;
