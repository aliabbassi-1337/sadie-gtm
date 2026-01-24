-- Add unique constraint on (name, city) for batch insert deduplication
-- This is needed for the ON CONFLICT clause in batch inserts
-- Uses lowercase to match the existing idx_hotels_name_city_website_unique pattern

-- First, handle any existing duplicates by keeping the one with the most data
-- (e.g., has website, has external_id, etc.)
WITH duplicates AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY lower(name), lower(COALESCE(city, ''))
               ORDER BY 
                   CASE WHEN website IS NOT NULL THEN 0 ELSE 1 END,
                   CASE WHEN external_id IS NOT NULL THEN 0 ELSE 1 END,
                   id ASC
           ) as rn
    FROM sadie_gtm.hotels
)
DELETE FROM sadie_gtm.hotels
WHERE id IN (SELECT id FROM duplicates WHERE rn > 1);

-- Now create the unique index (lowercase for case-insensitive matching)
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotels_name_city_unique 
ON sadie_gtm.hotels(lower(name), lower(COALESCE(city, '')));
