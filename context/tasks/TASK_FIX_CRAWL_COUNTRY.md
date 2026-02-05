# Task: Fix Incorrect Country Data from Crawl Sources

**Status:** Not Started  
**Priority:** High  
**Affected Hotels:** ~12,000+ incorrectly marked as "United States"

---

## Problem

Crawl sources are setting `country = 'United States'` for ALL hotels regardless of actual location. This is causing:

1. International hotels polluting US data
2. Launcher blocking hotels due to missing state (can't have US state for non-US hotel)
3. Incorrect geographic reporting

---

## Evidence

```sql
SELECT h.source, COUNT(*) as cnt
FROM sadie_gtm.hotels h
INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
WHERE h.country = 'United States' AND h.status = 0
  AND (h.state IS NULL OR h.state = '')
GROUP BY h.source ORDER BY cnt DESC
```

| Source | Count | Has Address |
|--------|-------|-------------|
| siteminder_crawl | 5,198 | 0 |
| siteminder_crawl::commoncrawl | 3,862 | 0 |
| mews_crawl | 2,115 | 670 |
| rms_crawl | 1,131 | 0 |

**Sample "US" addresses that are clearly international:**
- Amsterdam, Netherlands
- Hamburg, Germany
- Bilbao, Spain
- Lisboa, Portugal
- Byron Bay, Australia

---

## Fix Strategies

### 1. Mews (2,115 hotels) - Address Parsing

Has addresses - can parse to determine actual country.

```python
# Parse address to extract country
# Look for country names, postal code patterns, city names
def extract_country_from_address(address: str, city: str) -> str:
    # Check for known non-US patterns
    # European postal codes, city names, etc.
    pass
```

**Implementation:**
- Create `services/enrichment/country_utils.py`
- Add country extraction logic
- Create workflow `workflows/fix_crawl_country.py`

---

### 2. SiteMinder (9,060 hotels) - API Re-enrichment or Null Country

No address data available. Options:

a) **Set country to NULL** - honest about not knowing
b) **Re-crawl with better detection** - expensive
c) **Use website TLD** - `.de`, `.fr`, `.au` can indicate country

```sql
-- Option A: Set country to NULL for siteminder without state
UPDATE sadie_gtm.hotels
SET country = NULL
WHERE source LIKE 'siteminder%'
  AND country = 'United States'
  AND (state IS NULL OR state = '');
```

---

### 3. RMS (1,131 hotels) - API Re-enrichment

Can re-fetch from RMS API which returns location data.

```bash
uv run python workflows/enrich_rms_enqueue.py --force --limit 2000
```

---

## Action Plan

### Phase 1: Mews Address Parsing
1. Create country extraction utility
2. Parse addresses for mews_crawl hotels
3. Update country field

### Phase 2: RMS Re-enrichment
1. Re-enqueue RMS hotels for API enrichment
2. API returns proper country/state data

### Phase 3: SiteMinder Decision
1. Decide: NULL country vs keep as-is
2. If NULL: update ~9,000 hotels
3. These will be excluded from US reports until properly enriched

---

## Queries

```sql
-- Check mews addresses for country patterns
SELECT h.name, h.address, h.city
FROM sadie_gtm.hotels h
WHERE h.source = 'mews_crawl'
  AND h.country = 'United States'
  AND (h.state IS NULL OR h.state = '')
  AND h.address IS NOT NULL
LIMIT 50;

-- Count by TLD (if website available)
SELECT 
  SUBSTRING(h.website FROM '\.([a-z]{2,3})(/|$)') as tld,
  COUNT(*) as cnt
FROM sadie_gtm.hotels h
WHERE h.source LIKE 'siteminder%'
  AND h.country = 'United States'
  AND (h.state IS NULL OR h.state = '')
GROUP BY tld
ORDER BY cnt DESC
LIMIT 20;
```
