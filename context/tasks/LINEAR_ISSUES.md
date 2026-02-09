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

## EXPORT DATA QUALITY ISSUES (Jan 25, 2026)

Issues discovered by sampling 9,572 launched US hotels for the USA_leads.xlsx export.

### BUG-014: ~1,041 non-US hotels contaminating US leads export
**Priority:** P0 | **Count:** 1,041 | **Source:** rms_scan (831), archive_discovery (114), others
**Description:** Hotels in UAE, India, Indonesia, China, Bermuda, Sri Lanka, Poland, Australia etc. are marked as `country='United States'` and `status=1` (launched). They appear in USA_leads.xlsx and state exports with NULL city/state.
**Examples:**
- `Carpe Diem Lifestyle - Palm Jumeria` (addr: "Palm Jumeriah, UAE")
- `Citadines Berawa Beach Bali Resort` (addr: Bali, Indonesia)
- `DUBAI HILLS ESTATE EMAAR` (addr: Dubai)
- `Hotel Warszawa **Demo Version**` (addr: Poland)
- `Sterling Athirapilly` (addr: India)
**Root Cause:** RMS Cloud is a global PMS. The `rms_scan` and `archive_discovery` sources imported all RMS properties worldwide and set `country='United States'` by default. 1,036 of 1,041 NULL-city US hotels are RMS Cloud.
**Fix:** Query hotels with `country='United States' AND city IS NULL` → reverse geocode or reclassify using address field. For those with foreign addresses, update country to correct value and set status=-1 for US pipeline.
```sql
SELECT count(*) FROM sadie_gtm.hotels
WHERE country='United States' AND status=1 AND city IS NULL;
-- Result: 1,086
```

---

### BUG-015: 668 hotels with abbreviated states excluded from exports
**Priority:** P1 | **Count:** 668
**Description:** Hotels have 2-letter state codes (CA: 100, FL: 85, NY: 43, TX: 34, etc.) while exports use full state names. The `get_distinct_states_for_country` query filters `LENGTH(h.state) > 3`, so these 668 hotels are silently excluded from all per-state Excel exports. They DO appear in the country-level USA_leads.xlsx.
**Additional junk states:** `*` (2), `-` (1), `Wa` (1), `Ky` (1), `Ma` (1), `nc` (1), `nv` (1), `Fl` (1), `` (empty, 1)
**Fix:** Run state normalization to expand abbreviations to full names (CA → California, etc.). Already have SQL queries `normalize_us_state` in hotels.sql. Need to execute normalization script.
```sql
SELECT state, count(*) FROM sadie_gtm.hotels
WHERE country='United States' AND status=1 AND length(state) <= 2
GROUP BY state ORDER BY count(*) DESC;
-- Top: CA=100, FL=85, NY=43, TX=34, MI=25...
```

---

### BUG-016: 504 OTA/chain-engine hotels in leads
**Priority:** P1 | **Count:** 504
**Description:** Hotels with OTA booking engines showing up as leads. These are NOT independent hotels using a PMS — they're OTA listings that shouldn't be in sales lists.
**Breakdown:** Booking.com (166), Hotels.com (130), Airbnb (61), Kayak (38), VRBO (34), Expedia (26), Marriott (22), IHG (8), Agoda (7), Priceline (7), Hilton (4), Trivago (1)
**Fix:** Either exclude OTA engines from exports, or don't launch hotels whose only engine is an OTA. Add OTA tier=0 filter in export queries.
```sql
SELECT be.name, count(*) FROM sadie_gtm.hotels h
JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id
JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
WHERE h.country='United States' AND h.status=1
AND be.name IN ('Hotels.com','Booking.com','Airbnb','VRBO','Expedia','Kayak','Priceline','Agoda','Trivago','Marriott','IHG','Hilton')
GROUP BY be.name ORDER BY count(*) DESC;
```

---

### BUG-017: 80 demo/test hotels in launched data
**Priority:** P1 | **Count:** 80
**Description:** Hotels with "DEMO", "TEST", or "SAMPLE" in names are launched (status=1). The launcher now has filters to prevent this, but these were launched before filters were added.
**Examples:** `FOX Lite Hotel Grogol Jakarta - DEMO`, `Hotel Warszawa **Demo Version**`
**Fix:** Batch update these to status=-1.
```sql
UPDATE sadie_gtm.hotels SET status = -1
WHERE status = 1 AND (name ILIKE '%demo%' OR name ILIKE '%test%' OR name ILIKE '%sample%');
```

---

### BUG-018: 130 duplicate hotel entries in exports
**Priority:** P2 | **Count:** 130 extra rows
**Description:** Same hotel (name + city + state) appearing multiple times. Duplicates inflate lead counts.
**Examples:**
- BLUE OCEAN VACATION RENTALS LLC | South Padre Island | Texas → 12 copies
- BUZZ VACATION RENTALS, INC | Houston | Texas → 4 copies
- Nautica Residences | NULL | NULL → 6 copies
**Fix:** Deduplicate by keeping newest (or most complete) record, set others to status=-1.
```sql
SELECT name, city, state, count(*) FROM sadie_gtm.hotels
WHERE country='United States' AND status=1
GROUP BY name, city, state HAVING count(*) > 1
ORDER BY count(*) DESC;
```

---

### BUG-019: Rating (98.3%) and review_count (100%) empty
**Priority:** P2 | **Count:** 9,414 / 9,572 missing rating; 9,572 / 9,572 missing reviews
**Description:** These columns appear in exports but are almost entirely blank. Rating and review_count come from Google Places API but were never populated for most hotels.
**Fix:** Either remove from exports (columns auto-hide when all empty — already implemented) or batch-enrich via Places API. Current dynamic column filter should already handle this, but verify.

---

### BUG-020: Hotels with engine but no booking_url
**Priority:** P2 | **Count:** varies (Hotels.com, VRBO, FareHarbor, Streamline)
**Description:** Hotels have a detected engine but empty booking_url field. These show in exports with engine name but no actionable link.
**Examples:** COACHLIGHT INN (Hotels.com), BARE FEET RETREAT (VRBO)
**Fix:** For OTAs this is expected (fix via BUG-016). For real PMS engines, re-run enrichment.

---

## PRIORITY ORDER

### P0 (Critical - Do Now)
1. **BUG-014**: 1,041 non-US hotels contaminating US leads export ⚠️ NEW
2. BUG-012: Low live hotel count
3. BUG-001: 1,158 Cloudbeds NULL country
4. BUG-002: 2,915 Mews "New booking" names
5. BUG-003: 222 RMS "Online Bookings" names

### P1 (High - This Week)
6. **BUG-015**: 668 abbreviated-state hotels excluded from exports ⚠️ NEW
7. **BUG-016**: 504 OTA/chain-engine hotels in leads ⚠️ NEW
8. **BUG-017**: 80 demo/test hotels launched ⚠️ NEW
9. BUG-009: Normalize state/country data
10. BUG-004: RMS URL format issues
11. BUG-005: State contains region text
12. BUG-011: RMS timeout issues
13. FEAT-007: Normalize country data
14. FEAT-008: Normalize state data
15. FEAT-010: Contact enrichment

### P2 (Medium - This Sprint)
16. **BUG-018**: 130 duplicate hotel entries ⚠️ NEW
17. **BUG-019**: Rating/review_count 98-100% empty ⚠️ NEW
18. **BUG-020**: Hotels with engine but no booking_url ⚠️ NEW
19. FEAT-001: Scrape Cloudbeds Collection
20. FEAT-002: Ingest RMS hex slugs
21. FEAT-006: Merge export workflows
22. FEAT-009: Cleanup S3 exports
23. FEAT-012: Investigate room_count

### P3 (Low - Backlog)
24. FEAT-003: Scrape more from Wayback/CC
25. FEAT-004: IP rotation with Brightdata
26. FEAT-005: iPMS247 investigation
