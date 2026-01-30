-- Migration: Fix garbage hotel names
-- HOS-1400: Data Quality - Fix garbage hotel names across engines

-- 1. Fix Mews "New booking" -> set to NULL so enrichment can retry
-- These are placeholders from page titles that didn't have real names
UPDATE sadie_gtm.hotels h
SET 
    name = NULL,
    updated_at = NOW()
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
WHERE hbe.hotel_id = h.id
  AND be.name = 'Mews' 
  AND h.name = 'New booking';

-- 2. Fix RMS "Online Bookings" -> set to NULL for re-enrichment
UPDATE sadie_gtm.hotels h
SET 
    name = NULL,
    updated_at = NOW()
FROM sadie_gtm.hotel_booking_engines hbe
WHERE hbe.hotel_id = h.id
  AND hbe.booking_engine_id = 12
  AND h.name = 'Online Bookings';

-- 3. Fix Cloudbeds HTML entities (html.unescape equivalent in SQL)
UPDATE sadie_gtm.hotels h
SET 
    name = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        name,
        '&amp;', '&'),
        '&#39;', ''''),
        '&quot;', '"'),
        '&lt;', '<'),
        '&gt;', '>'),
    updated_at = NOW()
FROM sadie_gtm.hotel_booking_engines hbe
JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
WHERE hbe.hotel_id = h.id
  AND be.name = 'Cloudbeds'
  AND (h.name LIKE '%&amp;%' 
       OR h.name LIKE '%&#39;%' 
       OR h.name LIKE '%&quot;%'
       OR h.name LIKE '%&lt;%'
       OR h.name LIKE '%&gt;%');
