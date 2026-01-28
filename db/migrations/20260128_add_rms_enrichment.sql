-- Add enrichment_status column to hotel_booking_engines
ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS enrichment_status VARCHAR(50) DEFAULT 'pending';

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS last_enrichment_attempt TIMESTAMP;

-- Add RMS Cloud booking engine
INSERT INTO sadie_gtm.booking_engines (name, domains, tier)
VALUES ('RMS Cloud', ARRAY['rmscloud.com', 'ibe.rmscloud.com'], 1)
ON CONFLICT (name) DO NOTHING;
