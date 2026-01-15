-- Migration: Simplify hotel status system
--
-- OLD STATUS VALUES:
--   0 = scraped
--   1 = detected
--   2 = enriching
--   3 = enriched
--   4 = scored
--   5 = live
--   6 = exported
--   10 = enqueued (processing)
--   98 = location_mismatch
--   99 = no_booking_engine
--
-- NEW STATUS VALUES:
--   -2 = location_mismatch (rejected)
--   -1 = no_booking_engine (rejected)
--    0 = pending (in pipeline, not yet launched)
--    1 = launched (live lead)
--
-- Detection/enrichment progress is now tracked by presence of records in:
--   - hotel_booking_engines (detection complete)
--   - hotel_room_count (room count enrichment complete)
--   - hotel_customer_proximity (proximity enrichment complete)

SET search_path TO sadie_gtm;

-- Step 1: Add status column to hotel_room_count if not exists
-- (This tracks enrichment claim status: -1=processing, 0=failed, 1=success)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'sadie_gtm'
        AND table_name = 'hotel_room_count'
        AND column_name = 'status'
    ) THEN
        ALTER TABLE hotel_room_count ADD COLUMN status SMALLINT DEFAULT 1;
        COMMENT ON COLUMN hotel_room_count.status IS '-1=processing, 0=failed, 1=success';
    END IF;
END $$;

-- Step 2: Convert rejected statuses (do this FIRST before the bulk update)
-- 98 (location_mismatch) -> -2
UPDATE hotels SET status = -2 WHERE status = 98;

-- 99 (no_booking_engine) -> -1
UPDATE hotels SET status = -1 WHERE status = 99;

-- Step 3: Convert all "in progress" statuses to pending (0)
-- This allows the launcher to pick up hotels that were mid-pipeline
-- Status 1 (detected), 2 (enriching), 3 (enriched), 4 (scored), 10 (enqueued) -> 0
UPDATE hotels SET status = 0 WHERE status IN (1, 2, 3, 4, 10);

-- Step 4: Convert old "live" status (5) and "exported" (6) to launched (1)
-- These hotels were already launched in the old system
UPDATE hotels SET status = 1 WHERE status IN (5, 6);

-- Step 5: Log the migration results
DO $$
DECLARE
    pending_count INTEGER;
    launched_count INTEGER;
    rejected_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO pending_count FROM hotels WHERE status = 0;
    SELECT COUNT(*) INTO launched_count FROM hotels WHERE status = 1;
    SELECT COUNT(*) INTO rejected_count FROM hotels WHERE status < 0;

    RAISE NOTICE 'Migration complete:';
    RAISE NOTICE '  - Pending (status=0): %', pending_count;
    RAISE NOTICE '  - Launched (status=1): %', launched_count;
    RAISE NOTICE '  - Rejected (status<0): %', rejected_count;
END $$;
