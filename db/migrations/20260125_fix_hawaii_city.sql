-- Migration: Fix Hawaii hotels missing city data
-- For hawaii_vpi hotels where city is NULL, set city = county (island name)
-- This enables website enrichment which requires city

UPDATE sadie_gtm.hotels
SET city = county,
    updated_at = CURRENT_TIMESTAMP
WHERE source = 'hawaii_vpi'
  AND (city IS NULL OR city = '')
  AND county IS NOT NULL;

-- Show affected count
DO $$
DECLARE
    affected_count INTEGER;
BEGIN
    GET DIAGNOSTICS affected_count = ROW_COUNT;
    RAISE NOTICE 'Updated % Hawaii hotels with city = county (island)', affected_count;
END $$;
