-- Add index on booking_url for fast dedup checks during crawl ingestion
-- Without this, checking if a booking_url exists scans the entire table

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hotel_booking_engines_booking_url 
ON sadie_gtm.hotel_booking_engines (booking_url);

-- Also add engine_property_id (slug) for lookups by property ID
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hotel_booking_engines_property_id 
ON sadie_gtm.hotel_booking_engines (engine_property_id);
