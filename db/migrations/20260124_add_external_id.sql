-- Add external_id columns for source-specific deduplication
-- Stores the primary external identifier from the data source:
--   - texas_hot: "taxpayer_number:location_number"
--   - dbpr_license: Florida DBPR license number
--   - google_place: Google Places API place_id

-- Drop old source unique constraint - no longer needed with external_id approach
-- Source is now just the data source name, not a unique identifier
ALTER TABLE sadie_gtm.hotels DROP CONSTRAINT IF EXISTS hotels_source_unique;

ALTER TABLE sadie_gtm.hotels ADD COLUMN IF NOT EXISTS external_id TEXT;
ALTER TABLE sadie_gtm.hotels ADD COLUMN IF NOT EXISTS external_id_type TEXT;

-- Partial unique index: enforces uniqueness when external_id is present
-- Hotels without external_id use name+city dedup in application code
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotels_external_id
ON sadie_gtm.hotels(external_id_type, external_id)
WHERE external_id IS NOT NULL;

-- Migrate existing Texas hotels: extract external_id from compound source
-- e.g., "texas_hot:10105515737:00006" -> external_id="10105515737:00006", source="texas_hot"
UPDATE sadie_gtm.hotels
SET
    external_id = SUBSTRING(source FROM 'texas_hot:(.+)'),
    external_id_type = 'texas_hot',
    source = 'texas_hot'
WHERE source LIKE 'texas_hot:%';

-- Migrate google_place_id to external_id (if column exists)
UPDATE sadie_gtm.hotels
SET
    external_id = google_place_id,
    external_id_type = 'google_place'
WHERE google_place_id IS NOT NULL
  AND external_id IS NULL;

-- Drop google_place_id column (now redundant)
ALTER TABLE sadie_gtm.hotels DROP COLUMN IF EXISTS google_place_id;
