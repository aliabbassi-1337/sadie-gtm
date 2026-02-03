-- Add zip_code and contact_name columns to hotels table
-- These fields are available from Cloudbeds API enrichment

ALTER TABLE sadie_gtm.hotels
ADD COLUMN IF NOT EXISTS zip_code TEXT,
ADD COLUMN IF NOT EXISTS contact_name TEXT;

-- Add comments
COMMENT ON COLUMN sadie_gtm.hotels.zip_code IS 'Postal/ZIP code';
COMMENT ON COLUMN sadie_gtm.hotels.contact_name IS 'Primary contact name from booking engine';
