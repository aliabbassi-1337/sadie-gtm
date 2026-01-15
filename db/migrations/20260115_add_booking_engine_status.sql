-- Migration: Add status column to hotel_booking_engines
--
-- This tracks detection outcome:
--   -1 = failed (non-retriable error like timeout, precheck_failed)
--    1 = success (booking engine detected or confirmed no engine)
--
-- Without this, failed detections have no record and get retried infinitely.

SET search_path TO sadie_gtm;

-- Add status column to hotel_booking_engines if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'sadie_gtm'
        AND table_name = 'hotel_booking_engines'
        AND column_name = 'status'
    ) THEN
        ALTER TABLE hotel_booking_engines ADD COLUMN status SMALLINT DEFAULT 1;
        COMMENT ON COLUMN hotel_booking_engines.status IS '-1=failed, 1=success';
    END IF;
END $$;

-- Log migration result
DO $$
DECLARE
    col_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'sadie_gtm'
        AND table_name = 'hotel_booking_engines'
        AND column_name = 'status'
    ) INTO col_exists;

    IF col_exists THEN
        RAISE NOTICE 'hotel_booking_engines.status column exists';
    ELSE
        RAISE NOTICE 'ERROR: hotel_booking_engines.status column NOT created';
    END IF;
END $$;
