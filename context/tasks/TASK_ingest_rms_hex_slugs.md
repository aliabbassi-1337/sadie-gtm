# Task: Ingest RMS Hotels with Hex Slugs from Wayback Machine

## Summary
Discovered 153 RMS hotels that use hex slugs instead of numeric IDs. These are not being captured by our current numeric ID scanner.

## Problem
Our RMS scanner only scans numeric IDs:
```
https://bookings.rmscloud.com/Search/Index/{numeric_id}/90/
```

But RMS also has hex-slug URLs:
```
https://bookings.rmscloud.com/Search/Index/{hex_slug}/90/
```

Example: `https://bookings.rmscloud.com/Search/Index/4FF68C2A213D0E23/90/` → "The Carrington Inn"

## Data Source
Wayback Machine CDX API query:
```bash
curl -s "https://web.archive.org/cdx/search/cdx?url=bookings.rmscloud.com/Search/*&output=json&limit=1000"
```

Found **153 unique hex slugs** (16 hex characters each).

## Slugs Found
```
01564727DDE41549
01E6474FF1B12ECF
051C81861958DE69
084E522943B5B387
08F13B02C8713A3B
09ADA56C3F70FF02
0A8102506C18D75B
0C958BB809A5F422
12D597AFA22DAFD0
12DC2D97D168E907
... (153 total)
```

Full list saved at: `/tmp/rms_hex_slugs.txt`

## Implementation Plan

### 1. Create hex slug ingestor script
**File:** `workflows/ingest_rms_hex_slugs.py`

```python
# Fetch slugs from Wayback CDX API
# For each slug:
#   1. Build URL: https://bookings.rmscloud.com/Search/Index/{slug}/90/
#   2. Use RMSScraper to extract hotel data
#   3. Upsert to hotels table with external_id_type='rms_hex_slug'
```

### 2. Modify RMS scraper to accept hex slugs
Current `lib/rms/scraper.py` expects numeric IDs. Need to:
- Accept hex slug as `external_id`
- Use different URL normalization (no need to convert ibe→bookings)

### 3. Add database support
- Add `rms_hex_slug` to `external_id_type` enum/constraint
- Update upsert queries to handle hex slugs

### 4. Periodic Wayback refresh
- Add cron job to periodically query Wayback for new hex slugs
- Dedupe against existing hotels

## Validation
- [x] Tested URL `4FF68C2A213D0E23` - returns HTTP 200
- [x] Page contains hotel data ("The Carrington Inn")
- [ ] Test scraper extracts name, address, email correctly

## Estimated Impact
- **153 new RMS hotels** (potentially more from other Wayback queries)
- These are likely Australian/NZ hotels we're missing

## Priority
Medium - Good source of new leads, but small volume compared to other sources.

## Created
2026-01-29
