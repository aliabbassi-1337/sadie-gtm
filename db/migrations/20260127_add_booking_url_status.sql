-- Track booking URL status to avoid re-enqueuing broken URLs
-- status: NULL = not checked, 'ok' = working, '404' = not found

ALTER TABLE sadie_gtm.hotel_booking_engines 
ADD COLUMN IF NOT EXISTS url_status VARCHAR(20);

CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_url_status 
ON sadie_gtm.hotel_booking_engines (url_status) 
WHERE url_status IS NOT NULL;

COMMENT ON COLUMN sadie_gtm.hotel_booking_engines.url_status IS 'URL status: NULL=unchecked, ok=working, 404=not found';
