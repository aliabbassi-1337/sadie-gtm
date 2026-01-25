-- Migration: Pipeline State Machine
-- Updates hotel status values to new pipeline stages
-- 
-- New stages:
--   0 = INGESTED (just ingested, needs enrichment)
--  10 = HAS_WEBSITE (website found)
--  20 = HAS_LOCATION (coordinates found)
--  30 = DETECTED (booking engine found)
--  40 = ENRICHED (all enrichments complete)
-- 100 = LAUNCHED (live lead)
--
-- Terminal (negative):
--  -1 = NO_BOOKING_ENGINE
--  -2 = LOCATION_MISMATCH
--  -3 = DETECTION_TIMEOUT
-- -11 = ENRICHMENT_FAILED
-- -12 = UNENRICHABLE
-- -21 = DUPLICATE (was -3)
-- -22 = NON_HOTEL (was -4)
-- -23 = INVALID_DATA

-- Step 1: Migrate old status values to new ones
-- Update DUPLICATE from -3 to -21
UPDATE sadie_gtm.hotels 
SET status = -21, updated_at = CURRENT_TIMESTAMP
WHERE status = -3;

-- Update NON_HOTEL from -4 to -22
UPDATE sadie_gtm.hotels 
SET status = -22, updated_at = CURRENT_TIMESTAMP
WHERE status = -4;

-- Update LAUNCHED from 1 to 100
UPDATE sadie_gtm.hotels 
SET status = 100, updated_at = CURRENT_TIMESTAMP
WHERE status = 1;

-- Step 2: Update status=0 hotels based on what they have
-- Hotels with booking engine detected → DETECTED (30)
UPDATE sadie_gtm.hotels h
SET status = 30, updated_at = CURRENT_TIMESTAMP
WHERE h.status = 0
  AND EXISTS (
    SELECT 1 FROM sadie_gtm.hotel_booking_engines hbe 
    WHERE hbe.hotel_id = h.id AND hbe.status = 1
  );

-- Hotels with website AND location but no detection → HAS_LOCATION (20)
UPDATE sadie_gtm.hotels 
SET status = 20, updated_at = CURRENT_TIMESTAMP
WHERE status = 0
  AND website IS NOT NULL AND website != ''
  AND location IS NOT NULL;

-- Hotels with website but no location → HAS_WEBSITE (10)
UPDATE sadie_gtm.hotels 
SET status = 10, updated_at = CURRENT_TIMESTAMP
WHERE status = 0
  AND website IS NOT NULL AND website != ''
  AND (location IS NULL);

-- Hotels with location but no website → still INGESTED (0)
-- They need coordinate enrichment to get website
-- (no update needed, stays at 0)

-- Step 3: Add index for pipeline queries
CREATE INDEX IF NOT EXISTS idx_hotels_status_pipeline 
ON sadie_gtm.hotels (status) 
WHERE status >= 0;

-- Step 4: Add comment documenting the new stages
COMMENT ON COLUMN sadie_gtm.hotels.status IS 
'Pipeline stage: 0=ingested, 10=has_website, 20=has_location, 30=detected, 40=enriched, 100=launched. Negative=terminal.';
