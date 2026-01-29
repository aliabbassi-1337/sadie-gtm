-- Migration: Simplify enrichment_status to INTEGER
-- Convert from TEXT to INTEGER for consistency with hotels.status
--
-- Universal status values:
--   NULL = not attempted
--   1 = success
--   -1 = failed/dead

-- Step 1: Add new integer column
ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS enrichment_status_new INTEGER;

-- Step 2: Migrate data
UPDATE sadie_gtm.hotel_booking_engines
SET enrichment_status_new = CASE
    WHEN enrichment_status IN ('success', 'enriched') THEN 1
    WHEN enrichment_status IN ('no_data', 'dead', 'failed') THEN -1
    ELSE NULL
END;

-- Step 3: Drop old column and rename new one
ALTER TABLE sadie_gtm.hotel_booking_engines DROP COLUMN IF EXISTS enrichment_status;
ALTER TABLE sadie_gtm.hotel_booking_engines RENAME COLUMN enrichment_status_new TO enrichment_status;

-- Step 4: Update index
DROP INDEX IF EXISTS sadie_gtm.idx_hotel_booking_engines_enrichment_status;
CREATE INDEX idx_hotel_booking_engines_enrichment_status
ON sadie_gtm.hotel_booking_engines (enrichment_status)
WHERE enrichment_status IS NOT NULL;

-- Step 5: Add comment
COMMENT ON COLUMN sadie_gtm.hotel_booking_engines.enrichment_status IS 
'Enrichment status: NULL=not attempted, 1=success, -1=failed/dead';
