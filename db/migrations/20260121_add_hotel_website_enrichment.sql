-- Hotel website enrichment tracking table
-- Status values:
--   -1 = Processing (claimed by worker)
--    0 = Failed (couldn't find website)
--    1 = Success (website found and updated)

CREATE TABLE IF NOT EXISTS sadie_gtm.hotel_website_enrichment (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL UNIQUE REFERENCES sadie_gtm.hotels(id) ON DELETE CASCADE,
    status INTEGER NOT NULL DEFAULT -1,
    source TEXT,  -- serper, manual, etc
    enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotel_website_enrichment_hotel ON sadie_gtm.hotel_website_enrichment(hotel_id);
CREATE INDEX IF NOT EXISTS idx_hotel_website_enrichment_status ON sadie_gtm.hotel_website_enrichment(status);

-- Add status column to hotel_room_count if missing (for consistency)
ALTER TABLE sadie_gtm.hotel_room_count ADD COLUMN IF NOT EXISTS status INTEGER DEFAULT 1;
