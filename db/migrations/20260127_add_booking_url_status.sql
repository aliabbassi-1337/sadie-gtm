-- Track last enrichment attempt for rate limit handling
-- Hotels are re-enqueued only if last attempt was > 7 days ago
-- This allows automatic retry after transient failures (rate limits, timeouts)

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS last_enrichment_attempt TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_last_enrichment_attempt 
ON sadie_gtm.hotel_booking_engines (last_enrichment_attempt) 
WHERE last_enrichment_attempt IS NOT NULL;

COMMENT ON COLUMN sadie_gtm.hotel_booking_engines.last_enrichment_attempt IS 'Last failed enrichment attempt (retry after 7 days)';

-- Drop old column if exists (from previous migration version)
ALTER TABLE sadie_gtm.hotel_booking_engines 
DROP COLUMN IF EXISTS enrichment_attempts;
