-- Add emails array column for storing all scraped emails (generic + personal)
-- The existing `email` (text) column stays as the primary/canonical email for backward compat.
ALTER TABLE sadie_gtm.hotels ADD COLUMN IF NOT EXISTS emails TEXT[];

-- Seed from existing email values
UPDATE sadie_gtm.hotels
SET emails = ARRAY[email]
WHERE email IS NOT NULL AND email != '' AND emails IS NULL;
