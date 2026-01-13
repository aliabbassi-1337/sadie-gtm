# Pipeline Speed Optimization Ideas

## Current Bottlenecks
- Detection: Browser automation is slow (5-10s per hotel)
- Enrichment: API rate limits and sequential processing
- Single machine execution

---

## Quick Wins (Easy)

### 1. Tiered Detection
Most booking engines are detectable from static HTML without browser clicks:

```
Pass 1: requests + regex scan (0.5s/hotel) → catches 60-70%
Pass 2: Browser only for unknowns (5-10s/hotel) → remaining 30-40%
```

### 2. Block Unnecessary Resources
Skip images, fonts, CSS to speed up page loads:

```python
await page.route("**/*.{png,jpg,gif,css,woff,woff2}", lambda r: r.abort())
```

### 3. Increase Concurrency
With context pooling, push to 20-30 concurrent workers on a 4GB machine.

### 4. Domain Caching
If hotel X uses `cloudbeds.com`, all hotels on same domain likely do too:

```python
# Cache: domain -> booking_engine
if domain in cache:
    return cache[domain]  # Skip detection entirely
```

---

## Medium Effort

### 5. Multi-Machine Parallelization

```bash
# Split into chunks, run on 3 EC2 instances
./run_pipeline.sh scraper_output/florida_ceo/cities_1-9.csv      # EC2 #1
./run_pipeline.sh scraper_output/florida_ceo/cities_10-18.csv    # EC2 #2
./run_pipeline.sh scraper_output/florida_ceo/cities_19-27.csv    # EC2 #3

# Merge results
python3 scripts/utils/merge_leads.py --inputs *.csv --output merged.csv
```

### 6. Serverless Detection
AWS Lambda with Playwright (pay per use, infinite parallelism):
- 1000 hotels × $0.0001/request = $0.10
- All done in 2-3 minutes vs 2-3 hours

### 7. Pre-Filter Aggressively
- Skip hotels with no website (already doing)
- Skip social media URLs
- Skip known chains
- Skip already-enriched rows

---

## Bigger Changes

### 8. Replace Browser with HTTP Where Possible

90% of detection can be done with simple HTTP requests:

```python
resp = httpx.get(url, follow_redirects=True)
html = resp.text
# Check for engine patterns in HTML
# Check redirect chain
# Check iframe src attributes
```

### 9. Batch Enrichment API Calls
Send 10 hotels per Groq/Google AI request instead of 1.

### 10. Use a Detection Database
Store (domain → engine) mappings permanently:
- After detecting `example.com` → Cloudbeds once, never detect again
- Over time, detection becomes instant for repeat domains

---

## Estimated Impact

| Optimization | Time Saved | Effort |
|-------------|-----------|--------|
| Tiered detection (HTTP first) | 50-70% | Medium |
| Block resources | 10-20% | Easy |
| Increase concurrency to 25 | 20-30% | Easy |
| Domain caching | 20-40% | Easy |
| Multi-machine | 60-80% | Medium |
| Serverless | 90%+ | Hard |

---

## Recommended First Steps

1. Block resources + domain caching + tiered HTTP detection
2. Could cut time from 3 hours to under 1 hour

---

## Architecture Vision

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Scraper   │ ──► │  HTTP Detect │ ──► │   Browser   │
│  (Serper)   │     │  (Fast Pass) │     │ (Fallback)  │
└─────────────┘     └──────────────┘     └─────────────┘
                           │                    │
                           ▼                    ▼
                    ┌──────────────────────────────┐
                    │      Domain Cache (SQLite)    │
                    │  domain → booking_engine      │
                    └──────────────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │    Enrichment (Batch API)    │
                    └──────────────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │   Export → OneDrive Sync     │
                    └──────────────────────────────┘
```
