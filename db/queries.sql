-- Sadie GTM - Useful Queries
-- ============================
-- Run these against the sadie_gtm schema

SET search_path TO sadie_gtm;

-- ============================================================================
-- STATS & OVERVIEW
-- ============================================================================

-- Total counts
SELECT
    (SELECT COUNT(*) FROM hotels) as total_hotels,
    (SELECT COUNT(*) FROM leads) as total_leads,
    (SELECT COUNT(*) FROM leads WHERE room_count IS NOT NULL) as enriched_leads;

-- Leads by state
SELECT state, COUNT(*) as leads
FROM leads
GROUP BY state
ORDER BY leads DESC;

-- Leads by city (top 20)
SELECT city, state, COUNT(*) as leads
FROM leads
GROUP BY city, state
ORDER BY leads DESC
LIMIT 20;

-- Pipeline status breakdown
SELECT status, COUNT(*) as count
FROM leads
GROUP BY status;

-- ============================================================================
-- BOOKING ENGINE ANALYSIS
-- ============================================================================

-- Top booking engines
SELECT
    booking_engine,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
FROM leads
WHERE booking_engine IS NOT NULL AND booking_engine != ''
GROUP BY booking_engine
ORDER BY count DESC
LIMIT 20;

-- Tier breakdown
SELECT
    tier,
    CASE tier WHEN 1 THEN 'Known Engine' WHEN 2 THEN 'Unknown Engine' END as tier_name,
    COUNT(*) as count
FROM leads
GROUP BY tier
ORDER BY tier;

-- Engines by tier
SELECT
    tier,
    booking_engine,
    COUNT(*) as count
FROM leads
WHERE booking_engine IS NOT NULL
GROUP BY tier, booking_engine
ORDER BY tier, count DESC;

-- ============================================================================
-- ENRICHMENT STATUS
-- ============================================================================

-- Room count coverage
SELECT
    COUNT(*) as total,
    COUNT(room_count) as with_room_count,
    ROUND(COUNT(room_count) * 100.0 / COUNT(*), 1) as pct
FROM leads;

-- Room count by city
SELECT
    city,
    state,
    COUNT(*) as total,
    COUNT(room_count) as enriched,
    ROUND(COUNT(room_count) * 100.0 / COUNT(*), 1) as pct
FROM leads
GROUP BY city, state
HAVING COUNT(*) > 10
ORDER BY total DESC;

-- Contact info coverage
SELECT
    COUNT(*) as total,
    COUNT(email) as with_email,
    COUNT(phone_website) as with_phone_website,
    COUNT(phone_google) as with_phone_google
FROM leads;

-- ============================================================================
-- DEDUPLICATION
-- ============================================================================

-- Find duplicates in hotels
SELECT name, website, COUNT(*) as cnt
FROM hotels
GROUP BY name, website
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- Find duplicates in leads
SELECT name, website, COUNT(*) as cnt
FROM leads
GROUP BY name, website
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- ============================================================================
-- FUNNEL METRICS (for a state)
-- ============================================================================

-- Florida funnel
WITH florida_stats AS (
    SELECT
        (SELECT COUNT(*) FROM hotels WHERE state = 'Florida') as scraped,
        (SELECT COUNT(*) FROM hotels WHERE state = 'Florida' AND website IS NOT NULL) as with_website,
        (SELECT COUNT(*) FROM leads WHERE state = 'Florida') as booking_found,
        (SELECT COUNT(*) FROM leads WHERE state = 'Florida' AND tier = 1) as tier1,
        (SELECT COUNT(*) FROM leads WHERE state = 'Florida' AND tier = 2) as tier2,
        (SELECT COUNT(*) FROM leads WHERE state = 'Florida' AND room_count IS NOT NULL) as with_room_count
)
SELECT
    scraped as "Hotels Scraped",
    with_website as "With Website",
    booking_found as "Booking Found",
    ROUND(booking_found * 100.0 / NULLIF(with_website, 0), 1) as "Detection Rate %",
    tier1 as "Tier 1",
    tier2 as "Tier 2",
    with_room_count as "With Room Count"
FROM florida_stats;

-- ============================================================================
-- EXPORT QUERIES
-- ============================================================================

-- Export leads for a city
-- COPY (
--     SELECT name, website, booking_url, booking_engine, phone_google, phone_website, email, address, room_count
--     FROM leads
--     WHERE city = 'Miami' AND state = 'Florida'
--     ORDER BY room_count DESC NULLS LAST
-- ) TO '/tmp/miami_leads.csv' WITH CSV HEADER;

-- Export all Florida leads
-- COPY (
--     SELECT *
--     FROM leads
--     WHERE state = 'Florida'
--     ORDER BY city, name
-- ) TO '/tmp/florida_leads.csv' WITH CSV HEADER;

-- ============================================================================
-- PENDING WORK
-- ============================================================================

-- Leads needing enrichment
SELECT city, state, COUNT(*) as pending
FROM leads
WHERE room_count IS NULL
GROUP BY city, state
ORDER BY pending DESC;

-- Get batch for enrichment
SELECT id, name, website
FROM leads
WHERE status = 'detected' AND room_count IS NULL
ORDER BY id
LIMIT 100;

-- Mark batch as enriching
-- UPDATE leads SET status = 'enriching' WHERE id IN (...);

-- ============================================================================
-- CLEANUP
-- ============================================================================

-- Remove leads without booking engine
-- DELETE FROM leads WHERE booking_engine IS NULL OR booking_engine = '';

-- Remove duplicate hotels (keep first)
-- DELETE FROM hotels a USING hotels b
-- WHERE a.id > b.id AND a.name = b.name AND COALESCE(a.website, '') = COALESCE(b.website, '');
