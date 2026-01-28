-- Add enrichment_status column to hotel_booking_engines
ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS enrichment_status VARCHAR(50) DEFAULT 'pending';

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS last_enrichment_attempt TIMESTAMP;

-- Add index for ORDER BY last_enrichment_attempt queries
-- Composite index on (booking_engine_id, last_enrichment_attempt) for efficient filtering and sorting
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_enrichment_order
ON sadie_gtm.hotel_booking_engines (booking_engine_id, last_enrichment_attempt ASC NULLS FIRST);

-- Add index on enrichment_status for filtering
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_enrichment_status
ON sadie_gtm.hotel_booking_engines (enrichment_status);

-- Add RMS Cloud booking engine
INSERT INTO sadie_gtm.booking_engines (name, domains, tier)
VALUES ('RMS Cloud', ARRAY['rmscloud.com', 'ibe.rmscloud.com'], 1)
ON CONFLICT (name) DO NOTHING;
