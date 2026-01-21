-- Add category column for property type classification
-- Separates data source from property categorization for lead qualification

-- Add category column
ALTER TABLE sadie_gtm.hotels
ADD COLUMN IF NOT EXISTS category TEXT;

-- Create index for fast filtering by category
CREATE INDEX IF NOT EXISTS idx_hotels_category ON sadie_gtm.hotels(category);

-- Populate category from existing source data
UPDATE sadie_gtm.hotels SET category = 'hotel' WHERE source = 'dbpr_hotel';
UPDATE sadie_gtm.hotels SET category = 'motel' WHERE source = 'dbpr_motel';
UPDATE sadie_gtm.hotels SET category = 'vacation_rental' WHERE source IN ('dbpr_vacation_rental___dwelling', 'dbpr_vacation_rental___condo');
UPDATE sadie_gtm.hotels SET category = 'apartment' WHERE source = 'dbpr_nontransient_apartment';
UPDATE sadie_gtm.hotels SET category = 'rooming_house' WHERE source = 'dbpr_rooming_house';

-- For bbox/grid scraped data, leave category NULL (unknown until matched with DBPR)
-- These will be updated when matched against DBPR license data

COMMENT ON COLUMN sadie_gtm.hotels.category IS 'Property type: hotel, motel, vacation_rental, apartment, rooming_house. NULL = unknown/unclassified';
