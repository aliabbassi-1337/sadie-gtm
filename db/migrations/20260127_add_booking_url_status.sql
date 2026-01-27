-- Track enrichment attempts to handle retries for transient failures
-- enrichment_attempts: count of failed attempts
-- Only stop retrying after 3 failed attempts

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS enrichment_attempts INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_enrichment_attempts 
ON sadie_gtm.hotel_booking_engines (enrichment_attempts) 
WHERE enrichment_attempts > 0;

COMMENT ON COLUMN sadie_gtm.hotel_booking_engines.enrichment_attempts IS 'Failed enrichment attempts (stop after 3)';
