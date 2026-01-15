-- Add detection_errors table for tracking detection failures

CREATE TABLE IF NOT EXISTS detection_errors (
    id SERIAL PRIMARY KEY,
    hotel_id INTEGER NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    error_type TEXT NOT NULL,  -- precheck_failed, timeout, location_mismatch, junk_domain, etc.
    error_message TEXT,        -- Full error details
    detected_location TEXT,    -- What location was detected (for location_mismatch)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_detection_errors_hotel_id ON detection_errors(hotel_id);
CREATE INDEX IF NOT EXISTS idx_detection_errors_error_type ON detection_errors(error_type);
CREATE INDEX IF NOT EXISTS idx_detection_errors_created_at ON detection_errors(created_at);
