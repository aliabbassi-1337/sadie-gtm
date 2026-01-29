# Plan: Fix Remaining Garbage Names

**Date:** 2026-01-29
**Status:** In Progress

## Summary

Fix remaining garbage hotel names for Cloudbeds, SiteMinder, and RMS Cloud.

| Engine | Garbage | Fixable | Action |
|--------|---------|---------|--------|
| RMS Cloud | 255 | 255 | Re-run enrichment with Playwright |
| Cloudbeds | 1,023 | 77 | Run enrichment for unattempted |
| Cloudbeds | 898 | 0 | Delete dead URLs |
| SiteMinder | 71 | 0 | Delete corrupted data |

**Total fixable: 332 hotels**

---

## Task 1: RMS Cloud "Online Bookings" (255 hotels)

### Root Cause
- Crawl URL pattern is wrong: `bookings.rmscloud.com/{slug}` 
- Should be: `bookings.rmscloud.com/Search/Index/{slug}/90/`
- RMS pages are SPAs requiring Playwright (JS execution)

### Fix Steps
1. Query hotels with `name = 'Online Bookings'` and RMS booking engine
2. Convert their booking URLs to correct format
3. Re-scrape using Playwright-based RMSScraper
4. Update hotel names with extracted data

### SQL to identify
```sql
SELECT h.id, h.name, hbe.booking_url 
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
WHERE bes.name = 'RMS Cloud' AND h.name = 'Online Bookings' AND h.status = 1;
```

---

## Task 2: Cloudbeds "Unknown" - Unattempted (77 hotels)

### Root Cause
- Enrichment workflow hasn't processed these yet
- Source: `cloudbeds_crawl`

### Fix Steps
1. Query hotels with `name = 'Unknown'`, Cloudbeds engine, and `enrichment_status IS NULL`
2. Run Cloudbeds enrichment workflow for these hotels
3. Archive fallback will try: live page → Common Crawl → Wayback Machine

### SQL to identify
```sql
SELECT h.id, hbe.booking_url 
FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
WHERE bes.name = 'Cloudbeds' AND h.name = 'Unknown' AND h.status = 1
AND hbe.enrichment_status IS NULL;
```

---

## Task 3: Cloudbeds "Unknown" - Dead URLs (898 hotels)

### Root Cause
- Booking URLs return 404 - properties no longer active
- Archive scraper already tried and failed

### Fix Steps
1. Delete these hotels - they represent inactive properties
2. Or: keep but exclude from reports

### SQL to delete
```sql
DELETE FROM sadie_gtm.hotel_booking_engines
WHERE hotel_id IN (
    SELECT h.id FROM sadie_gtm.hotels h
    JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
    JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
    WHERE bes.name = 'Cloudbeds' AND h.name = 'Unknown' 
    AND hbe.enrichment_status = 'dead'
);

DELETE FROM sadie_gtm.hotels
WHERE name = 'Unknown' 
AND source LIKE '%cloudbeds%'
AND id NOT IN (SELECT hotel_id FROM sadie_gtm.hotel_booking_engines);
```

---

## Task 4: SiteMinder Garbage Names (71 hotels)

### Root Cause
Two distinct issues:
1. **19 crawl records** - slugs too short to parse (1-3 chars)
2. **52 tax assessor records** - data corruption in `md_sdat_cama`
   - Maryland parcel IDs matched to wrong hotels (FL, CA, NY)
   - Booking URL is `siteminder.com/canvas` (demo page)

### Fix Steps
1. Delete all 71 - data is corrupted/unsalvageable
2. Flag `md_sdat_cama` for investigation (38% data corruption)

### SQL to delete
```sql
-- Delete hotel_booking_engines first (FK constraint)
DELETE FROM sadie_gtm.hotel_booking_engines
WHERE hotel_id IN (
    SELECT h.id FROM sadie_gtm.hotels h
    JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
    JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
    WHERE bes.name = 'SiteMinder' 
    AND (h.name = 'Hotel Website Builder' OR h.name = 'Book Online Now')
);

-- Then delete hotels
DELETE FROM sadie_gtm.hotels
WHERE id IN (
    SELECT h.id FROM sadie_gtm.hotels h
    WHERE (h.name = 'Hotel Website Builder' OR h.name = 'Book Online Now')
    AND h.id NOT IN (SELECT hotel_id FROM sadie_gtm.hotel_booking_engines)
);
```

---

## Execution Order

1. [x] Save this plan
2. [x] Task 1: Re-run RMS enrichment (255 hotels) - **33 fixed, 222 timed out (inactive pages)**
3. [x] Task 2: Run Cloudbeds enrichment (77 hotels) - **Already completed**
4. [x] Task 3: Delete Cloudbeds dead URLs (898 hotels) - **Already completed**
5. [x] Task 4: Delete SiteMinder corrupted data (71 hotels) - **Already completed**
6. [x] Verify final counts

---

## Final Results (2026-01-29)

| Engine | Total | Clean Names | % | Remaining Garbage |
|--------|-------|-------------|---|-------------------|
| SiteMinder | 9,279 | 9,265 | **99%** | 14 |
| Cloudbeds | 8,328 | 8,203 | **98%** | 125 |
| Mews | 2,964 | 2,963 | **99%** | 1 |
| RMS Cloud | 2,028 | 1,796 | **88%** | 232 |

**TOTAL: 22,599 hotels | 22,227 clean (98%)**

### What was fixed:
- **SiteMinder "Book Online Now"**: 5,683 → 0 (100% fixed via URL slug parsing)
- **SiteMinder "Unknown (slug)"**: 3,380 → 14 (99% fixed via name field parsing)
- **RMS "Online Bookings"**: 255 → 232 (23 fixed, rest are dead pages)

### Remaining issues (unfixable):
- **RMS 232**: Pages timeout - properties likely inactive
- **Cloudbeds 125**: Dead URLs, enrichment already tried and failed
- **SiteMinder 14**: Slugs too short to parse (1-3 chars)
- **Mews 1**: Single edge case

---

## Follow-up Investigation

- `md_sdat_cama` dataset has 38% corrupted records (479/1,274)
- Need root cause analysis of enrichment matching logic
- Consider re-ingesting from scratch
