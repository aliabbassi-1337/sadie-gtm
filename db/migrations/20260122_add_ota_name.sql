-- Add OTA name column to track hotels using OTAs (Booking.com, Expedia, etc.)
-- instead of independent booking engines

ALTER TABLE sadie_gtm.hotel_booking_engines
ADD COLUMN IF NOT EXISTS ota_name VARCHAR(50);

-- Index for OTA queries
CREATE INDEX IF NOT EXISTS idx_hbe_ota_name ON sadie_gtm.hotel_booking_engines(ota_name)
WHERE ota_name IS NOT NULL;

COMMENT ON COLUMN sadie_gtm.hotel_booking_engines.ota_name IS 'OTA name if hotel uses OTA (e.g., Booking.com, Expedia)';
