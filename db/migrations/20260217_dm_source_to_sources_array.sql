-- Change decision maker source tracking from single TEXT to TEXT[] array
-- so multiple enrichment layers confirming the same person are all recorded.

-- Convert existing data: wrap single source value into an array
ALTER TABLE sadie_gtm.hotel_decision_makers
    ALTER COLUMN source TYPE TEXT[] USING ARRAY[source];

-- Rename for clarity
ALTER TABLE sadie_gtm.hotel_decision_makers
    RENAME COLUMN source TO sources;

-- Replace B-tree index with GIN for array containment queries
DROP INDEX IF EXISTS sadie_gtm.idx_hotel_dm_source;
CREATE INDEX idx_hotel_dm_sources ON sadie_gtm.hotel_decision_makers USING GIN (sources);
