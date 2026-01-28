-- Add enrichment_status to track permanent failures (404s)
-- This prevents endlessly re-queuing dead booking URLs

-- Status values:
-- NULL = pending (never attempted)
-- 'success' = extracted data successfully
-- 'no_data' = page exists but couldn't extract (retry later)
-- 'dead' = 404 or permanently gone (don't retry)

ALTER TABLE sadie_gtm.hotel_booking_engines
ADD COLUMN IF NOT EXISTS enrichment_status TEXT;

COMMENT ON COLUMN sadie_gtm.hotel_booking_engines.enrichment_status IS 
'Enrichment status: NULL=pending, success=done, no_data=retry later, dead=404/permanent failure';

-- Index for filtering out dead URLs
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_enrichment_status
ON sadie_gtm.hotel_booking_engines (enrichment_status)
WHERE enrichment_status IS NOT NULL;
