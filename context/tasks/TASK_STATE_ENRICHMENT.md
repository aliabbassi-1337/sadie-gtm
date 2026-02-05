# Task: State Enrichment

**Status:** In Progress  
**Priority:** High  
**Blocked Hotels:** 12,358 US hotels with booking engines

---

## Problem

12,358 US hotels with detected booking engines are blocked from launching because they have no state data.

**Root Cause Found:** RMS enqueue query was filtering `h.status = 1` (launched only), missing all pending hotels.

**Fix Applied:** Changed to `h.status >= 0` in `db/queries/rms.sql`

```sql
SELECT COUNT(*)
FROM sadie_gtm.hotels h
INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
WHERE h.country = 'United States' AND h.status = 0
  AND (h.state IS NULL OR h.state = '')
-- Result: 12,358
```

---

## Enrichment Strategies

### 1. Reverse Geocoding (coordinates → state)

**Hotels with coordinates:** 6

Use PostGIS or Google Geocoding API to get state from lat/lng.

```sql
-- Hotels that can be reverse geocoded
SELECT COUNT(*) FROM sadie_gtm.hotels h
INNER JOIN sadie_gtm.hotel_booking_engines hbe ON h.id = hbe.hotel_id AND hbe.status = 1
WHERE h.country = 'United States' AND h.status = 0
  AND (h.state IS NULL OR h.state = '')
  AND h.location IS NOT NULL
```

**Implementation:**
- Use `workflows/reverse_lookup.py` or create new workflow
- Google Geocoding API or free alternative (Nominatim)

---

### 2. Address Parsing (address → state)

**Hotels with addresses:** 671

Parse US state from address string using regex.

```python
# Already implemented in services/enrichment/state_utils.py
from services.enrichment.state_utils import extract_state

state = extract_state(address, city)
```

**Workflow:** `workflows/extract_state_from_address.py`

---

### 3. API Enrichment (re-fetch from source)

**RMS hotels missing state:** Can re-enrich via RMS API  
**Cloudbeds hotels missing state:** Can re-enrich via Cloudbeds API

```bash
# Re-enqueue for enrichment
uv run python workflows/enrich_rms_enqueue.py --force --limit 5000
uv run python workflows/enrich_cloudbeds_enqueue.py --force --limit 5000
```

---

## Data by Source

| Source | Missing State | Has Address | Has City |
|--------|---------------|-------------|----------|
| siteminder_crawl | 5,198 | 0 | 0 |
| siteminder_crawl::commoncrawl | 3,862 | 0 | 0 |
| mews_crawl | 2,115 | 670 | 673 |
| rms_crawl | 1,131 | 0 | 0 |
| archive_discovery | 48 | 0 | 0 |

---

## Action Plan

1. Run address parsing workflow on 671 hotels with addresses
2. Run reverse geocoding on 6 hotels with coordinates
3. Re-enqueue RMS/Cloudbeds hotels for API enrichment
4. For remaining (SiteMinder) - see TASK_FIX_CRAWL_COUNTRY.md

---

## Commands

```bash
# 1. Extract state from address
uv run python workflows/extract_state_from_address.py --limit 1000

# 2. Reverse geocode (if workflow exists)
uv run python workflows/reverse_lookup.py --limit 100

# 3. Re-enqueue for API enrichment
uv run python workflows/enrich_rms_enqueue.py --force --limit 5000
uv run python workflows/enrich_cloudbeds_enqueue.py --force --limit 5000
```
