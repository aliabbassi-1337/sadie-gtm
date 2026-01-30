# Linear Issues - Sadie GTM Data Quality

## BUGS

### BUG-001: 1,158 Cloudbeds hotels with NULL country
**Priority:** P0 | **Engine:** Cloudbeds | **Count:** 1,158
**Description:** Cloudbeds hotels missing country despite having booking URLs.
**Query:**
```sql
SELECT COUNT(*) FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
WHERE hbe.booking_engine_id = 3 AND h.country IS NULL;
```
**Root Cause:** Cloudbeds scraper not extracting country, or pages don't have it.
**Fix:** Re-run enrichment with fixed scraper.

---

### BUG-002: "New booking" as hotel name (Mews)
**Priority:** P0 | **Engine:** Mews | **Count:** 2,915
**Description:** Mews hotels have generic "New booking" as name instead of actual hotel name.
**Root Cause:** Scraper extracting page title instead of hotel name.
**Fix:** Update Mews scraper to extract correct hotel name from page content.

---

### BUG-003: "Online Bookings" as hotel name (RMS)
**Priority:** P0 | **Engine:** RMS | **Count:** 222
**Description:** RMS hotels have "Online Bookings" as name.
**Root Cause:** Generic page title captured instead of hotel name.
**Fix:** Re-scrape RMS hotels with better name extraction.

---

### BUG-004: RMS "Book online" pages don't load
**Priority:** P1 | **Engine:** RMS
**Description:** Some RMS URLs like `https://ibe12.rmscloud.com/58` don't load properly.
**Example:** `rms_crawl` source has broken URLs.
**Root Cause:** Old URL format, need to convert to `bookings.rmscloud.com/Search/Index/{id}/90/`.
**Fix:** Update URL normalization in RMS scraper.

---

### BUG-005: State field contains region text
**Priority:** P1 | **Engine:** Cloudbeds | **Count:** ~100+
**Description:** State field has values like "Krung Thep Mahanakhon [Bangkok]" or "Buenos Aires Capital Federal".
**Fix:** Regex cleanup to extract just the state/province name.

---

### BUG-006: City field contains zip codes
**Priority:** P1 | **Engine:** Cloudbeds
**Description:** City field has values like "7770 Vestervig", "3000-150 Coimbra", "92103".
**Fix:** Parse out zip codes, move to proper field.

---

### BUG-007: HTML entities in names
**Priority:** P2 | **Engine:** Cloudbeds
**Description:** Names contain `&amp;` instead of `&`.
**Example:** `"Fairfield Place &amp; Fairfield Manor"`
**Fix:** Apply `html.unescape()` during extraction.

---

### BUG-008: Cloudbeds system emails captured
**Priority:** P2 | **Engine:** Cloudbeds | **Count:** ~5
**Description:** System emails stored as hotel contact.
**Examples:** `admin@cloudbeds.com`, `channels@cloudbeds.com`, `info@*.whistle.cloudbeds.com`
**Fix:** Filter out `@cloudbeds.com` emails in scraper.

---

### BUG-009: State/Country data not normalized
**Priority:** P1 | **All engines**
**Description:** States are mix of full names and abbreviations. Countries are mix of codes.
**Examples:** `"California"` vs `"CA"`, `"United States"` vs `"USA"` vs `"US"`
**Fix:** Normalize all states to 2-letter codes, countries to ISO 3166-1 alpha-2.

---

### BUG-010: City same as state (zip code in wrong field)
**Priority:** P1 | **Example:** ID 54093
**Description:** Zip codes ending up in city or state fields.
**Fix:** Validate city/state values, move numeric values to zip field.

---

### BUG-011: RMS timeout issues
**Priority:** P1 | **Engine:** RMS
**Description:** RMS pages timing out during scraping.
**Root Cause:** SPA takes too long to load, or rate limiting.
**Fix:** Increase timeout, add retry logic, check rate limiting.

---

### BUG-012: Low live hotel count
**Priority:** P0 | **All engines**
**Description:** Too few hotels reaching "live" status (status=1).
**Investigation needed:** Check pipeline bottlenecks, launcher logic.

---

### BUG-013: Database errors need investigation
**Priority:** P1 | **All engines**
**Description:** Need to audit error states in database.
**Query:**
```sql
SELECT status, COUNT(*) FROM sadie_gtm.hotels GROUP BY status;
SELECT enrichment_status, COUNT(*) FROM sadie_gtm.hotel_booking_engines GROUP BY enrichment_status;
```

---

## FEATURES

### FEAT-001: Scrape Cloudbeds Collection
**Priority:** P1 | **New Data Source**
**Description:** Scrape hotels from Cloudbeds Collection page.
**URL:** https://www.cloudbeds.com/cloudbeds-collection/
**Expected:** High-quality hotel listings with verified data.

---

### FEAT-002: Ingest RMS hex slugs from Wayback
**Priority:** P2 | **Engine:** RMS | **Count:** 153 hotels
**Description:** Found 153 RMS hotels with hex slugs in Wayback Machine.
**Example:** `https://bookings.rmscloud.com/Search/Index/4FF68C2A213D0E23/90/`
**Task doc:** `context/TASK_ingest_rms_hex_slugs.md`

---

### FEAT-003: Scrape more slugs from Wayback/Common Crawl
**Priority:** P2 | **All engines**
**Description:** Query Wayback Machine and Common Crawl for more booking URLs.
**Engines:** Cloudbeds, Mews, SiteMinder, RMS

---

### FEAT-004: IP rotation with Brightdata
**Priority:** P2 | **Infrastructure**
**Description:** Set up rotating proxies for large-scale scraping.
**Use case:** Avoid rate limiting when scanning more slugs.

---

### FEAT-005: Investigate iPMS247 booking engine
**Priority:** P3 | **New Engine**
**Description:** New booking engine found.
**Example:** https://live.ipms247.com/booking/book-rooms-safarihotelboardwalk

---

### FEAT-006: Merge export workflows
**Priority:** P2 | **Refactoring**
**Description:** Consolidate the two export workflows in reporting service.
**Goal:** Cleaner codebase, single export entry point.

---

### FEAT-007: Normalize country data
**Priority:** P1 | **Data Quality**
**Description:** Standardize all country values to ISO 3166-1 alpha-2 codes.
**Current state:** Mix of `USA`, `US`, `United States`, full names, etc.

---

### FEAT-008: Normalize state data
**Priority:** P1 | **Data Quality**
**Description:** Standardize all US state values to 2-letter abbreviations.
**Current state:** Mix of `California`, `CA`, `Calif.`, etc.

---

### FEAT-009: Cleanup S3 export sheets
**Priority:** P2 | **Reporting**
**Description:** Consolidate S3 exports into fewer sheets, reduce redundancy.
**Current state:** Too many sheets with overlapping data.

---

### FEAT-010: Contact enrichment
**Priority:** P1 | **Data Quality**
**Description:** Improve email/phone extraction across all engines.
**Current gaps:**
- Missing Contact: 15,312 hotels total
- Cloudbeds: 2,661 missing
- SiteMinder: 9,096 missing
- Mews: 2,927 missing
- RMS: 628 missing

---

### FEAT-011: Geocoding for missing coordinates
**Priority:** P1 | **Data Quality**
**Description:** Run Serper Places API geocoding for hotels missing coordinates.
**Current gaps:**
- Missing Coordinates: 22,118 hotels total
- Cost estimate: ~$66 (at $0.003/query)

---

### FEAT-012: Investigate low room_count enrichment
**Priority:** P2 | **Data Quality** | **Count:** 464
**Description:** Only 464 hotels have room_count data. Investigate why.
**Query:**
```sql
SELECT COUNT(*) FROM sadie_gtm.hotel_room_count WHERE status = 1;
```

---

## DATA GAPS SUMMARY

| Issue | Cloudbeds | SiteMinder | Mews | RMS | Total |
|-------|-----------|------------|------|-----|-------|
| Missing State | 2,607 | 9,046 | 2,916 | 1,927 | **16,496** |
| Missing City | 129 | 9,053 | 2,916 | 1,929 | **14,027** |
| Missing Coordinates | 8,127 | 9,124 | 2,934 | 1,933 | **22,118** |
| Missing Contact | 2,661 | 9,096 | 2,927 | 628 | **15,312** |

---

## PRIORITY ORDER

### P0 (Critical - Do Now)
1. BUG-012: Low live hotel count
2. BUG-001: 1,158 Cloudbeds NULL country
3. BUG-002: 2,915 Mews "New booking" names
4. BUG-003: 222 RMS "Online Bookings" names

### P1 (High - This Week)
5. BUG-009: Normalize state/country data
6. BUG-004: RMS URL format issues
7. BUG-005: State contains region text
8. BUG-011: RMS timeout issues
9. FEAT-007: Normalize country data
10. FEAT-008: Normalize state data
11. FEAT-010: Contact enrichment

### P2 (Medium - This Sprint)
12. FEAT-001: Scrape Cloudbeds Collection
13. FEAT-002: Ingest RMS hex slugs
14. FEAT-006: Merge export workflows
15. FEAT-009: Cleanup S3 exports
16. FEAT-012: Investigate room_count

### P3 (Low - Backlog)
17. FEAT-003: Scrape more from Wayback/CC
18. FEAT-004: IP rotation with Brightdata
19. FEAT-005: iPMS247 investigation
