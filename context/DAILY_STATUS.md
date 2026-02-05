# Daily Status

**Last Updated:** Feb 6, 2026 14:10 UTC

---

## Pipeline Summary

| Metric | Count |
|--------|-------|
| Total Hotels | 81,708 |
| Launched (status=1) | 22,858 |
| With Booking Engine | 44,392 |

---

## California Focus

| Metric | Count |
|--------|-------|
| Total | 2,452 |
| Launched | 775 |
| With Booking Engine | 783 |
| Exported | 774 leads → S3 |

**Gap Analysis:**
- 447 have website, need detection
- 794 have no website, need enrichment

---

## Recent Work (Feb 5-6)

### Completed
- **PR #141** - Launcher refactor: removed email requirement, standardized name filters
- **State normalization** - Centralized in `state_utils.py`, 3-layer architecture
- **California export** - 774 leads to `s3://sadie-gtm/HotelLeadGen/USA/California/California.xlsx`

### In Progress
- Room count enrichment (2,631 pending globally)
- Detection consumer running (500 hotels queued)

---

## Pending Enrichment

| Type | Pending |
|------|---------|
| Room Count | 2,631 |
| Proximity | 0 |

---

## Key Workflows

```bash
# Export state leads
uv run python workflows/export.py --state California

# Run room count enrichment
uv run python workflows/enrichment.py room-counts --limit 100

# Run proximity calculation  
uv run python workflows/enrichment.py proximity --limit 100

# Enqueue detection
uv run python workflows/enqueue_detection.py --limit 500

# Run detection consumer
uv run python workflows/detection_consumer.py --preset small

# Launch hotels
uv run python workflows/launcher.py launch --limit 100
```

---

## Critical Blockers

### 12,358 US Hotels Blocked by Missing State

```
LAUNCHER FUNNEL (US Hotels with Booking Engine):
TOTAL:                    25,676
Already launched/error:  -13,179 → 12,497 remaining
STATE IS NULL/EMPTY:     -12,358 → 139 remaining  ← BLOCKER
Name filters:                 -1 → 138 remaining
```

**Root cause:** Hotels never went through enrichment - RMS enqueue was filtering to `status=1` only

**Fix applied:** Changed RMS query to `status >= 0` (PR #141)

| Source | Count | Issue |
|--------|-------|-------|
| siteminder_crawl | 9,060 | No address data |
| mews_crawl | 2,115 | Has addresses (international) |
| rms_crawl | 1,131 | No address data |

**Currently running:**
- RMS enqueue: 8,140 hotels
- Cloudbeds enqueue: 9,584 hotels

**See:** `context/tasks/TASK_STATE_ENRICHMENT.md`, `context/tasks/TASK_FIX_CRAWL_COUNTRY.md`

---

## Next Steps

1. **Fix country data** (HIGH PRIORITY)
   - Parse mews addresses to get real country
   - Re-enrich RMS via API
   - Decide on SiteMinder (NULL or re-crawl)

2. **State enrichment**
   - Address parsing (671 hotels)
   - Reverse geocoding (6 hotels)
   - API re-enrichment

3. **Increase CA leads to 800+**
   - Run detection on 447 hotels with websites

4. **Merge export workflows**
   - Combine `export.py` and `export_crawl.py`
