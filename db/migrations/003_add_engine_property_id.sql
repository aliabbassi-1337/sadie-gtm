-- Add engine_property_id column to hotel_booking_engines
-- This stores the booking engine's identifier for the property (slug, UUID, ID)
-- Examples:
--   Cloudbeds: "cl6l0S" (6-char slug)
--   Mews: "ec832712-68fd-4c52-99fe-b1f701205661" (UUID)
--   RMS: "2675" (numeric ID)
--   SiteMinder: "100por100fundirect" (slug)

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS engine_property_id TEXT;

-- Add status column if it doesn't exist (used for detection status)
ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS status SMALLINT DEFAULT 1;

-- Index for lookups by engine property ID
CREATE INDEX IF NOT EXISTS idx_hbe_engine_property_id 
ON sadie_gtm.hotel_booking_engines(engine_property_id);

-- Unique constraint: one property ID per booking engine
CREATE UNIQUE INDEX IF NOT EXISTS idx_hbe_engine_property_unique 
ON sadie_gtm.hotel_booking_engines(booking_engine_id, engine_property_id) 
WHERE engine_property_id IS NOT NULL;
