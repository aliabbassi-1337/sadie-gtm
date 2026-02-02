-- Migration: Sync schema with production database
-- Date: 2026-01-25
-- Description: Add missing columns that exist in production but not in schema.sql

-- Add status column to hotel_booking_engines (if not exists)
ALTER TABLE sadie_gtm.hotel_booking_engines 
    ADD COLUMN IF NOT EXISTS status SMALLINT DEFAULT 1;

-- Add engine_property_id for tracking booking engine's internal ID
ALTER TABLE sadie_gtm.hotel_booking_engines 
    ADD COLUMN IF NOT EXISTS engine_property_id TEXT;

-- Add enrichment tracking columns
ALTER TABLE sadie_gtm.hotel_booking_engines 
    ADD COLUMN IF NOT EXISTS last_enrichment_attempt TIMESTAMPTZ;
ALTER TABLE sadie_gtm.hotel_booking_engines 
    ADD COLUMN IF NOT EXISTS enrichment_status INTEGER DEFAULT 0;

-- Add status column to hotel_room_count (if not exists)
-- 0 = pending, 1 = verified, 2 = estimated
ALTER TABLE sadie_gtm.hotel_room_count 
    ADD COLUMN IF NOT EXISTS status INTEGER DEFAULT 0;

-- Make room_count nullable (some APIs don't return it)
ALTER TABLE sadie_gtm.hotel_room_count 
    ALTER COLUMN room_count DROP NOT NULL;

-- Add category and external_id to hotels (if not exists)
ALTER TABLE sadie_gtm.hotels 
    ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE sadie_gtm.hotels 
    ADD COLUMN IF NOT EXISTS external_id TEXT;
ALTER TABLE sadie_gtm.hotels 
    ADD COLUMN IF NOT EXISTS external_id_type TEXT;

-- Create indexes for new columns
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_status 
    ON sadie_gtm.hotel_booking_engines(status);
CREATE INDEX IF NOT EXISTS idx_hotel_booking_engines_enrichment_status 
    ON sadie_gtm.hotel_booking_engines(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_hotels_category 
    ON sadie_gtm.hotels(category);
CREATE INDEX IF NOT EXISTS idx_hotels_external_id 
    ON sadie_gtm.hotels(external_id, external_id_type);
