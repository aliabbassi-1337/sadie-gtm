-- Add has_availability column for RMS Australia hotels
-- Tracks whether a hotel has availability for future dates

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS has_availability BOOLEAN DEFAULT NULL;

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS availability_checked_at TIMESTAMP;

-- Add index for filtering by availability status
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_availability 
ON sadie_gtm.hotel_booking_engines (has_availability) 
WHERE has_availability IS NOT NULL;

-- Add index for finding Australia RMS hotels needing availability check
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_australia_rms 
ON sadie_gtm.hotel_booking_engines (booking_engine_id, has_availability) 
WHERE has_availability IS NULL;
